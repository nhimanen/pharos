// ============================================================
// Pharos Web UI  ·  cluster.js  ·  the Cluster page, as pure functions
// ============================================================
// GET /cluster/status (the public view: no reader addresses) -> toView() -> renderPage() -> an
// HTML string. Nothing here touches the DOM, the network or the clock, so a status renders the
// same page wherever it is rendered. app.js fetches the status, follows /cluster/events and sets
// the HTML.

(function (root) {
'use strict';

// Every /cluster/status field this file reads, and nothing else (`[]` marks the elements of a list).
// ClusterPageTest checks the golden fixture (src/test/resources/cluster/status-golden.json) has each
// of them, and that this file reads no status field missing here. Add a field here before reading it.
const STATUS_FIELDS = [
  'mode',
  'asOf',
  'layout.summary.shards',
  'layout.summary.writers',
  'layout.summary.readers',
  'layout.shards[].shard',
  'layout.shards[].specVersion',
  'layout.shards[].keys',
  'layout.shards[].docCount',
  'layout.shards[].sizeBytes',
  'layout.shards[].generation',
  'layout.shards[].ageSeconds',
  'layout.shards[].writer',
  'layout.shards[].lease',
  'layout.shards[].lease.writer',
  'layout.shards[].lease.ageSeconds',
  'layout.shards[].lease.expired',
  'layout.shards[].lease.released',
  'layout.shards[].readers',
  'layout.shards[].backlog',
  'layout.shards[].backlog.sourceRevision',
  'layout.shards[].backlog.publishedRevision',
  'layout.shards[].backlog.pending',
  'layout.shards[].backlog.oldestUnpublishedChangeAgeSeconds',
  'layout.shards[].state',
  'layout.writers[].node',
  'layout.writers[].leases',
  'layout.writers[].pinned',
  'layout.writers[].indexing',
  'layout.writers[].indexing.shard',
  'layout.writers[].indexing.phase',
  'layout.writers[].indexing.progress',
  'layout.writers[].indexing.startedAt',
  'layout.writers[].stopping',
  'layout.writers[].disk',
  'layout.writers[].disk.freeBytes',
  'layout.writers[].disk.totalBytes',
  'layout.writers[].disk.low',
  'layout.writers[].heartbeatAgeSeconds',
  'layout.readers[].node',
  'layout.readers[].shards',
  'layout.readers[].state',
  'layout.readers[].degradedShards',
  'layout.readers[].readerCache',
  'layout.readers[].readerCache.bytesOnDisk',
  'layout.readers[].readerCache.cacheMaxBytes',
  'layout.readers[].readerCache.hitRate',
  'layout.readers[].inFlight',
  'layout.readers[].qps',
  'layout.readers[].warmP95Millis',
  'layout.readers[].heartbeatAgeSeconds',
  'backlogSummary.shardsPending',
  'backlogSummary.oldestPendingAgeSeconds',
  'health[].kind',
  'health[].target',
  'health[].message',
  'proposals[].id',
  'proposals[].kind',
  'proposals[].target',
  'proposals[].reason',
  'proposals[].metrics',
  'proposals[].action',
  'proposals[].since',
];

/** Shard chip states, in the order the legend shows them. */
const STATES = ['current', 'lagging', 'lease-expired', 'unpublished'];
/** Reader heartbeat states; anything else (an older or newer node, a hostile heartbeat) is 'unknown'. */
const READER_STATES = ['warming', 'ready', 'degraded', 'draining'];

/**
 * The only way a status string becomes a CSS class or other attribute token: a value from
 * {@code allowed}, else {@code fallback}. Every other status string is text, and goes through esc().
 */
function token(v, allowed, fallback) {
  return allowed.includes(v) ? v : fallback;
}

function readerState(v) {
  return v == null ? 'ready' : token(v, READER_STATES, 'unknown');
}
/** Above this many boxes in a diagram column, boxes with the same shards and state are merged. */
const MAX_BOXES = 12;
/** Below this width the diagram stacks writers, bucket and readers in one column. */
const NARROW = 640;

// ── Formatting ───────────────────────────────────────────────
function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function fmtBytes(n) {
  if (n == null || !isFinite(n)) return '—';
  const u = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  let i = 0, v = Number(n);
  while (Math.abs(v) >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return (i === 0 ? String(Math.round(v)) : v.toFixed(v >= 100 ? 0 : 1)) + ' ' + u[i];
}

function fmtAge(sec) {
  if (sec == null || !isFinite(sec)) return '—';
  const s = Math.max(0, Math.round(sec));
  if (s < 60) return s + ' s';
  if (s < 3600) return Math.floor(s / 60) + ' m ' + (s % 60) + ' s';
  if (s < 86400) return Math.floor(s / 3600) + ' h ' + Math.floor((s % 3600) / 60) + ' m';
  return Math.floor(s / 86400) + ' d ' + Math.floor((s % 86400) / 3600) + ' h';
}

function fmtInt(n) {
  if (n == null || !isFinite(n)) return '—';
  return String(Math.round(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function fmtPct(r) {
  if (r == null || !isFinite(r)) return '—';
  const t = (r * 100).toFixed(1);
  return (t.endsWith('.0') ? t.slice(0, -2) : t) + '%';
}

function fmtNum(n, digits) {
  if (n == null || !isFinite(n)) return '—';
  return Number(n).toFixed(digits == null ? 1 : digits);
}

/** A proposal metric, formatted by its name's suffix. */
function fmtMetric(name, v) {
  if (v == null) return '—';
  if (typeof v !== 'number') return String(v);
  if (/Bytes$/.test(name)) return fmtBytes(v);
  if (/Millis$/.test(name)) return fmtInt(v) + ' ms';
  if (/Seconds$/.test(name)) return fmtAge(v);
  if (/^shareOf|Rate$|Ratio$/.test(name)) return fmtPct(v);
  return Number.isInteger(v) ? fmtInt(v) : fmtNum(v, 2);
}

function plural(n, word) {
  return fmtInt(n) + ' ' + word + (n === 1 ? '' : 's');
}

function sum(list) {
  let t = 0, any = false;
  for (const v of list) if (v != null && isFinite(v)) { t += v; any = true; }
  return any ? t : null;
}

function keysText(keys) {
  if (!keys || typeof keys !== 'object') return '—';
  const ks = Object.keys(keys);
  return ks.length ? ks.map(k => k + '=' + keys[k]).join(', ') : '—';
}

// ── Status -> view ───────────────────────────────────────────
/**
 * The page's model of a status: plain objects with the numbers the panels show. Tolerates missing
 * sections (an older node's status) by treating them as empty.
 */
function toView(st) {
  st = st || {};
  const L = st.layout || {};
  const shards = L.shards || [];
  const writers = L.writers || [];
  const readers = L.readers || [];
  const single = st.mode === 'single-node';
  const asOfMs = st.asOf ? Date.parse(st.asOf) : NaN;
  const byName = new Map(shards.map(s => [s.shard, s]));
  const liveWriters = new Set(writers.map(w => w.node));

  // The current owner is the lease holder; without a lease (single node), the writer listing it.
  const ownerOf = s => {
    if (s.lease) return s.lease.writer;
    const w = writers.find(x => (x.leases || []).includes(s.shard));
    return w ? w.node : null;
  };
  const chip = (s, degraded) => ({
    shard: String(s.shard),
    state: token(s.state, STATES, 'unpublished'),
    pending: !!(s.backlog && s.backlog.pending),
    degraded: !!degraded,
    generation: s.generation == null ? null : s.generation,
  });

  const shardRows = shards.map(s => {
    const owner = ownerOf(s);
    return {
      shard: s.shard,
      state: chip(s).state,
      pending: chip(s).pending,
      specVersion: s.specVersion == null ? null : s.specVersion,
      keys: keysText(s.keys),
      owner,
      ownerLive: owner != null && liveWriters.has(owner),
      leaseAge: s.lease ? s.lease.ageSeconds : null,
      leaseExpired: !!(s.lease && s.lease.expired),
      leaseReleased: !!(s.lease && s.lease.released),
      publisher: s.writer || null,
      generation: s.generation == null ? null : s.generation,
      docCount: s.docCount == null ? null : s.docCount,
      sizeBytes: s.sizeBytes == null ? null : s.sizeBytes,
      publishedAge: s.ageSeconds == null ? null : s.ageSeconds,
      readers: s.readers || [],
    };
  });
  const ownedBy = node => shards.filter(s => ownerOf(s) === node);

  const job = w => {
    const ix = w.indexing;
    if (!ix) return null;
    const p = Math.max(0, Math.min(1, Number(ix.progress) || 0));
    const elapsed = isFinite(asOfMs) && ix.startedAt != null ? Math.max(0, (asOfMs - ix.startedAt) / 1000) : null;
    return { node: w.node, shard: ix.shard, phase: ix.phase, progress: p, elapsed };
  };
  const diskOf = w => w.disk ? {
    free: w.disk.freeBytes, total: w.disk.totalBytes, low: !!w.disk.low,
    share: w.disk.totalBytes ? w.disk.freeBytes / w.disk.totalBytes : null,
  } : null;

  // Diagram boxes
  let writerBoxes = writers.map(w => ({
    names: [w.node], kind: 'writer', live: true, stopping: !!w.stopping,
    disk: diskOf(w), job: job(w), pinned: w.pinned || [],
    chips: ownedBy(w.node).map(s => chip(s)),
  }));
  const ghosts = new Map();
  for (const s of shards) {
    const o = ownerOf(s);
    if (o != null && liveWriters.has(o)) continue;
    const key = o == null ? '' : o;
    if (!ghosts.has(key)) ghosts.set(key, { names: [o == null ? 'no writer' : String(o)], kind: 'writer', live: false,
      unassigned: o == null,
      stopping: false, disk: null, job: null, pinned: [], chips: [] });
    ghosts.get(key).chips.push(chip(s));
  }
  // Ghost writers (a lease holder without a heartbeat) sort in among the live ones by node id;
  // the 'no writer' box (shards without a lease holder) comes last.
  const nodeKey = b => b.unassigned ? '\uffff' : String(b.names[0]);
  writerBoxes = writerBoxes.concat([...ghosts.values()])
    .sort((a, b) => nodeKey(a) < nodeKey(b) ? -1 : nodeKey(a) > nodeKey(b) ? 1 : 0);
  let readerBoxes = readers.map(r => ({
    names: [r.node], kind: 'reader', state: readerState(r.state),
    chips: (r.shards || []).map(n => chip(byName.get(n) || { shard: n, state: 'unpublished' },
      (r.degradedShards || []).includes(n))),
  }));

  let nodeBox = null;
  if (single) {
    const n = (writers[0] && writers[0].node) || (readers[0] && readers[0].node) || 'this node';
    nodeBox = { names: [n], kind: 'all', live: true, state: readerState(readers[0] && readers[0].state),
      job: writers[0] ? job(writers[0]) : null, disk: writers[0] ? diskOf(writers[0]) : null,
      chips: shards.map(s => chip(s)) };
    writerBoxes = []; readerBoxes = [];
  }

  // Node table
  const nodes = [];
  const writerRow = (w, role) => {
    const own = ownedBy(w.node);
    return { node: w.node, role, state: w.stopping ? 'stopping' : null, shards: own.map(s => s.shard),
      docs: sum(own.map(s => s.docCount)), size: sum(own.map(s => s.sizeBytes)), disk: diskOf(w),
      cacheBytes: null, cacheMax: null, hitRate: null, qps: null, inFlight: null, warmP95: null,
      heartbeatAge: w.heartbeatAgeSeconds };
  };
  const readerRow = r => {
    const served = (r.shards || []).map(n => byName.get(n)).filter(Boolean);
    const rc = r.readerCache || null;
    return { node: r.node, role: 'reader', state: readerState(r.state), shards: r.shards || [],
      degraded: r.degradedShards || [],
      docs: sum(served.map(s => s.docCount)), size: sum(served.map(s => s.sizeBytes)), disk: null,
      cacheBytes: rc ? rc.bytesOnDisk : null, cacheMax: rc ? rc.cacheMaxBytes : null,
      hitRate: rc ? rc.hitRate : null, qps: r.qps, inFlight: r.inFlight == null ? null : r.inFlight,
      warmP95: r.warmP95Millis == null ? null : r.warmP95Millis, heartbeatAge: r.heartbeatAgeSeconds };
  };
  if (single) {
    const w = writers[0], r = readers[0];
    const row = w ? writerRow(w, 'writer + reader') : readerRow(r);
    if (r) {
      const rr = readerRow(r);
      Object.assign(row, { role: 'writer + reader', state: rr.state === 'ready' ? row.state : rr.state,
        shards: shards.map(s => s.shard), docs: sum(shards.map(s => s.docCount)),
        size: sum(shards.map(s => s.sizeBytes)), cacheBytes: rr.cacheBytes, cacheMax: rr.cacheMax,
        hitRate: rr.hitRate, qps: rr.qps, inFlight: rr.inFlight, warmP95: rr.warmP95, degraded: rr.degraded });
    }
    if (w || r) nodes.push(row);
  } else {
    for (const w of writers) nodes.push(writerRow(w, 'writer'));
    for (const r of readers) nodes.push(readerRow(r));
  }

  // Backlog: pending shards, the longest wait first (an unknown age last), then by name.
  const pending = shards.filter(s => s.backlog && s.backlog.pending).map(s => ({
    shard: s.shard, owner: ownerOf(s), state: chip(s).state,
    sourceRevision: s.backlog.sourceRevision, publishedRevision: s.backlog.publishedRevision,
    waiting: s.backlog.oldestUnpublishedChangeAgeSeconds,
  })).sort((a, b) => {
    const wa = a.waiting == null ? -1 : a.waiting, wb = b.waiting == null ? -1 : b.waiting;
    return wb - wa || (a.shard < b.shard ? -1 : a.shard > b.shard ? 1 : 0);
  });
  const bs = st.backlogSummary || {};
  const jobs = writers.map(w => ({ node: w.node, job: job(w), stopping: !!w.stopping }));

  const summary = L.summary || {};
  const specs = [...new Set(shards.map(s => s.specVersion).filter(v => v != null))];
  const keyNames = [...new Set(shards.flatMap(s => s.keys ? Object.keys(s.keys) : []))];

  return {
    mode: single ? 'single-node' : (st.mode || 'cluster'),
    single,
    asOf: st.asOf || null,
    counts: {
      shards: summary.shards != null ? summary.shards : shards.length,
      writers: summary.writers != null ? summary.writers : writers.length,
      readers: summary.readers != null ? summary.readers : readers.length,
    },
    bucket: {
      shards: shards.length,
      docs: sum(shards.map(s => s.docCount)),
      size: sum(shards.map(s => s.sizeBytes)),
      chips: shards.map(s => chip(s)),
    },
    diagram: {
      single,
      node: nodeBox,
      writers: groupBoxes(writerBoxes, MAX_BOXES),
      readers: groupBoxes(readerBoxes, MAX_BOXES),
    },
    nodes,
    sharding: { specVersions: specs, keys: keyNames, rows: shardRows },
    scale: {
      writers: summary.writers != null ? summary.writers : writers.length,
      readers: summary.readers != null ? summary.readers : readers.length,
      proposals: (st.proposals || []).map(p => ({
        id: p.id, kind: p.kind, target: p.target, reason: p.reason, action: p.action, since: p.since,
        metrics: p.metrics && typeof p.metrics === 'object'
          ? Object.keys(p.metrics).map(k => ({ name: k, value: fmtMetric(k, p.metrics[k]) })) : [],
      })),
    },
    backlog: {
      shardsPending: bs.shardsPending != null ? bs.shardsPending : pending.length,
      oldest: bs.oldestPendingAgeSeconds != null ? bs.oldestPendingAgeSeconds : null,
      pending,
      jobs,
    },
    health: (st.health || []).map(h => ({ kind: h.kind, target: h.target, message: h.message })),
  };
}

/**
 * Merges boxes with the same kind, liveness, state and shards once a column has more than
 * {@code max} of them, so a large cluster keeps a legible diagram. Order is kept (first seen).
 */
function groupBoxes(boxes, max) {
  if (boxes.length <= max) return boxes;
  const out = [], bySig = new Map();
  for (const b of boxes) {
    const sig = [b.kind, b.live, b.state, b.stopping, b.job ? 'job' : '', b.disk && b.disk.low ? 'low' : '',
      b.chips.map(c => c.shard + ':' + c.state + ':' + c.pending + ':' + c.degraded).join(',')].join('|');
    if (bySig.has(sig)) bySig.get(sig).names.push(b.names[0]);
    else { const g = Object.assign({}, b, { names: b.names.slice() }); bySig.set(sig, g); out.push(g); }
  }
  return out;
}

// ── Diagram geometry ─────────────────────────────────────────
const CHIP_H = 18, CHIP_GAP = 4, CHAR_W = 6.7, BOX_PAD = 8, HEAD_H = 22, SUB_H = 16, BOX_GAP = 10;

function chipWidth(c) { return Math.ceil(c.shard.length * CHAR_W) + 16; }

/** Places chips in rows inside a box of inner width {@code w}; returns the positions and the height. */
function flowChips(chips, w) {
  const pos = [];
  let x = 0, y = 0;
  for (const c of chips) {
    const cw = Math.min(chipWidth(c), w);
    if (x > 0 && x + cw > w) { x = 0; y += CHIP_H + CHIP_GAP; }
    pos.push({ chip: c, x, y, w: cw });
    x += cw + CHIP_GAP;
  }
  return { pos, h: chips.length ? y + CHIP_H : 0 };
}

function boxLabel(b) {
  if (b.names.length === 1) return b.names[0];
  return b.names[0] + ' … ' + b.names[b.names.length - 1] + ' ×' + b.names.length;
}

function boxSub(b) {
  if (b.kind === 'reader') return b.state && b.state !== 'ready' ? b.state : null;
  if (b.kind === 'bucket') return fmtInt(b.chips.length) + ' shards · ' + fmtInt(b.docs) + ' docs · ' + fmtBytes(b.size);
  const parts = [];
  if (b.kind === 'writer' && !b.live) parts.push(b.names[0] === 'no writer' ? 'unassigned' : 'no heartbeat');
  if (b.stopping) parts.push('stopping');
  if (b.job) parts.push(b.job.phase + ' ' + b.job.shard + ' ' + fmtPct(b.job.progress));
  if (b.disk) parts.push((b.disk.low ? 'LOW disk ' : 'disk ') + fmtBytes(b.disk.free) + ' free');
  return parts.length ? parts.join(' · ') : null;
}

function placeBox(b, x, y, w) {
  const inner = w - 2 * BOX_PAD;
  const flow = flowChips(b.chips, inner);
  const sub = boxSub(b);
  const h = HEAD_H + (sub ? SUB_H : 0) + (flow.h ? flow.h + BOX_PAD : 0) + (b.chips.length ? 0 : 14) + 4;
  return { box: b, x, y, w, h, flow, sub };
}

function stack(boxes, x, y, w) {
  const out = [];
  for (const b of boxes) { const p = placeBox(b, x, y, w); out.push(p); y += p.h + BOX_GAP; }
  return { placed: out, bottom: out.length ? y - BOX_GAP : y };
}

/**
 * Lays the diagram out for a panel {@code width} px wide: writers, bucket and readers side by side,
 * or (narrow) one above the other. Returns boxes and links with absolute coordinates.
 */
function diagramLayout(view, width) {
  const d = view.diagram;
  const W = Math.max(280, Math.floor(width || 960));
  const pad = 12, top = 26;
  const bucketBox = { names: ['bucket'], kind: 'bucket', chips: view.bucket.chips, live: true,
    docs: view.bucket.docs, size: view.bucket.size };
  if (d.single) {
    const w = Math.min(W - 2 * pad, 520);
    const p = placeBox(d.node, Math.round((W - w) / 2), top, w);
    return { width: W, height: top + p.h + pad, narrow: W < NARROW, titles: [{ x: W / 2, text: 'single node (writer + reader)' }],
      boxes: [p], links: [] };
  }
  if (W < NARROW) {
    const x = pad + 18, w = W - x - pad;
    const ws = stack(d.writers, x, top, w);
    const b = placeBox(bucketBox, x, ws.bottom + 34, w);
    const rs = stack(d.readers, x, b.y + b.h + 34, w);
    const links = [];
    const busX = pad + 6;
    for (const p of ws.placed) links.push({ kind: 'publish', dead: !p.box.live,
      path: `M${p.x},${p.y + 12} H${busX} V${b.y + 12} H${b.x}` });
    for (const p of rs.placed) links.push({ kind: 'sync', dead: false,
      path: `M${b.x},${b.y + b.h - 12} H${busX} V${p.y + 12} H${p.x}` });
    return { width: W, height: rs.bottom + pad, narrow: true,
      titles: [{ x: x, y: 16, text: 'writers', anchor: 'start' },
               { x: x, y: b.y - 10, text: 'publish ↓  ·  bucket  ·  sync ↓', anchor: 'start' },
               { x: x, y: b.y + b.h + 24, text: 'readers', anchor: 'start' }],
      boxes: ws.placed.concat([b], rs.placed), links };
  }
  const gutter = Math.min(90, Math.max(48, Math.round(W * 0.06)));
  const colW = Math.floor((W - 2 * pad - 2 * gutter) / 3);
  const xW = pad, xB = pad + colW + gutter, xR = pad + 2 * (colW + gutter);
  const ws = stack(d.writers, xW, top, colW);
  const rs = stack(d.readers, xR, top, colW);
  const bh = placeBox(bucketBox, xB, top, colW).h;
  const colBottom = Math.max(ws.bottom, rs.bottom, top + bh);
  const by = Math.max(top, Math.round(top + (colBottom - top - bh) / 2));
  const b = placeBox(bucketBox, xB, by, colW);
  const links = [];
  const cy = p => p.y + Math.min(p.h / 2, 20);
  const bIn = i => b.y + 16 + (b.h - 32) * (ws.placed.length > 1 ? i / (ws.placed.length - 1) : 0.5);
  const bOut = i => b.y + 16 + (b.h - 32) * (rs.placed.length > 1 ? i / (rs.placed.length - 1) : 0.5);
  ws.placed.forEach((p, i) => {
    const x1 = p.x + p.w, y1 = cy(p), x2 = b.x, y2 = bIn(i), mx = (x1 + x2) / 2;
    links.push({ kind: 'publish', dead: !p.box.live, path: `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}` });
  });
  rs.placed.forEach((p, i) => {
    const x1 = b.x + b.w, y1 = bOut(i), x2 = p.x, y2 = cy(p), mx = (x1 + x2) / 2;
    links.push({ kind: 'sync', dead: false, path: `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}` });
  });
  return { width: W, height: Math.max(colBottom, b.y + b.h) + pad, narrow: false,
    titles: [{ x: xW + colW / 2, text: 'writers  ·  publish →' }, { x: xB + colW / 2, text: 'bucket' },
             { x: xR + colW / 2, text: '→ sync  ·  readers' }],
    boxes: ws.placed.concat([b], rs.placed), links };
}

// ── HTML ─────────────────────────────────────────────────────
function chipTitle(c) {
  const t = [c.shard, c.state];
  if (c.generation != null) t.push('generation ' + c.generation);
  if (c.pending) t.push('unpublished change pending');
  if (c.degraded) t.push('degraded on this reader');
  return t.join(' · ');
}

function svgChip(p, ox, oy) {
  const c = p.chip, x = ox + p.x, y = oy + p.y;
  let s = `<g class="cl-chip cl-st-${token(c.state, STATES, 'unpublished')}${c.degraded ? ' cl-degraded' : ''}"><title>${esc(chipTitle(c))}</title>` +
    `<rect x="${x}" y="${y}" width="${p.w}" height="${CHIP_H}" rx="4"/>` +
    `<text x="${x + p.w / 2}" y="${y + 12.5}" text-anchor="middle">${esc(c.shard)}</text>`;
  if (c.pending) s += `<circle class="cl-pending" cx="${x + p.w - 1}" cy="${y + 1}" r="3.5"/>`;
  return s + '</g>';
}

function svgBox(p) {
  const b = p.box;
  const cls = ['cl-box', 'cl-box-' + b.kind];
  if (b.live === false) cls.push('cl-dead');
  if (b.disk && b.disk.low) cls.push('cl-low');
  if (b.stopping) cls.push('cl-stopping');
  if (b.kind === 'reader' && b.state !== 'ready') cls.push('cl-rs-' + token(b.state, READER_STATES, 'unknown'));
  const label = b.kind === 'bucket' ? 'object store' : boxLabel(b);
  let s = `<g class="${cls.join(' ')}"><title>${esc(b.names.join(', '))}</title>` +
    `<rect class="cl-box-bg" x="${p.x}" y="${p.y}" width="${p.w}" height="${p.h}" rx="6"/>` +
    `<text class="cl-box-name" x="${p.x + BOX_PAD}" y="${p.y + 15}">${esc(label)}</text>`;
  if (b.kind === 'all' && !b.job) s += `<text class="cl-box-tag" x="${p.x + p.w - BOX_PAD}" y="${p.y + 15}" text-anchor="end">writer + reader</text>`;
  let y = p.y + HEAD_H;
  if (p.sub) {
    s += `<text class="cl-box-sub${b.disk && b.disk.low ? ' cl-warn' : ''}" x="${p.x + BOX_PAD}" y="${y + 9}">${esc(p.sub)}</text>`;
    y += SUB_H;
  }
  if (b.job) {
    const bw = Math.max(20, Math.min(80, p.w - 2 * BOX_PAD - 4));
    s += `<rect class="cl-bar-bg" x="${p.x + p.w - BOX_PAD - bw}" y="${p.y + 8}" width="${bw}" height="5" rx="2"/>` +
      `<rect class="cl-bar" x="${p.x + p.w - BOX_PAD - bw}" y="${p.y + 8}" width="${Math.round(bw * b.job.progress)}" height="5" rx="2"/>`;
  }
  if (!b.chips.length) s += `<text class="cl-box-sub" x="${p.x + BOX_PAD}" y="${y + 10}">no shards</text>`;
  for (const cp of p.flow.pos) s += svgChip(cp, p.x + BOX_PAD, y);
  return s + '</g>';
}

function renderDiagram(view, width) {
  const L = diagramLayout(view, width);
  let s = `<svg class="cl-diagram" width="${L.width}" height="${L.height}" viewBox="0 0 ${L.width} ${L.height}" role="img" aria-label="Cluster layout">`;
  for (const t of L.titles) {
    s += `<text class="cl-col-title" x="${t.x}" y="${t.y || 14}" text-anchor="${t.anchor || 'middle'}">${esc(t.text)}</text>`;
  }
  for (const l of L.links) s += `<path class="cl-link cl-link-${l.kind}${l.dead ? ' cl-dead' : ''}" d="${l.path}"/>`;
  for (const p of L.boxes) s += svgBox(p);
  return s + '</svg>';
}

function legend() {
  const items = STATES.map(st => `<span class="cl-leg"><span class="cl-sw cl-st-${st}"></span>${st}</span>`);
  items.push('<span class="cl-leg"><span class="cl-dot"></span>change pending</span>');
  items.push('<span class="cl-leg"><span class="cl-sw cl-sw-degraded"></span>degraded on reader</span>');
  return `<div class="cl-legend">${items.join('')}</div>`;
}

function chipHtml(state, text) {
  return `<span class="cl-chip-h cl-st-${token(state, STATES, 'unpublished')}">${esc(text)}</span>`;
}

function badge(text, cls) {
  return `<span class="cl-badge${cls ? ' ' + cls : ''}">${esc(text)}</span>`;
}

function td(label, html, cls) {
  return `<td data-label="${esc(label)}"${cls ? ` class="${cls}"` : ''}>${html}</td>`;
}

function renderNodes(view) {
  if (!view.nodes.length) return '<div class="cl-empty">No live nodes.</div>';
  const head = ['Node', 'Role', 'Shards', 'Docs', 'Size', 'Disk free', 'Cache / cap', 'Hit rate', 'QPS',
    'Warm p95', 'Heartbeat'];
  const rows = view.nodes.map(n => {
    const role = esc(n.role) + (n.state && n.state !== 'ready'
      ? ' ' + badge(n.state, n.state === 'stopping' || n.state === 'draining' || n.state === 'degraded' ? 'cl-b-warn' : '') : '');
    const shards = n.shards.length
      ? '<span>' + n.shards.map(s => `<span class="${(n.degraded || []).includes(s) ? 'cl-degr-name' : ''}">${esc(s)}</span>`).join(', ') + '</span>'
      : '<span class="cl-dim">—</span>';
    const disk = n.disk ? `${fmtBytes(n.disk.free)} <span class="cl-dim">of ${fmtBytes(n.disk.total)}</span>` +
      (n.disk.low ? ' ' + badge('low', 'cl-b-bad') : '') : '<span class="cl-dim">—</span>';
    const cache = n.cacheMax != null ? `${fmtBytes(n.cacheBytes)} <span class="cl-dim">/ ${fmtBytes(n.cacheMax)}</span>`
      : '<span class="cl-dim">—</span>';
    return '<tr>' + td('Node', `<span class="cl-mono">${esc(n.node)}</span>`) + td('Role', role) +
      td('Shards', shards) + td('Docs', fmtInt(n.docs), 'cl-num') + td('Size', fmtBytes(n.size), 'cl-num') +
      td('Disk free', disk, 'cl-num') + td('Cache / cap', cache, 'cl-num') +
      td('Hit rate', fmtPct(n.hitRate), 'cl-num') + td('QPS', n.qps == null ? '—' : fmtNum(n.qps, 1), 'cl-num') +
      td('Warm p95', n.warmP95 == null ? '—' : fmtInt(n.warmP95) + ' ms', 'cl-num') +
      td('Heartbeat', fmtAge(n.heartbeatAge) + ' ago', 'cl-num') + '</tr>';
  });
  return `<table class="cl-table"><thead><tr>${head.map(h => `<th>${h}</th>`).join('')}</tr></thead>` +
    `<tbody>${rows.join('')}</tbody></table>`;
}

function renderSharding(view) {
  const sh = view.sharding;
  const spec = `<div class="cl-kv"><span>Spec version</span><b>${sh.specVersions.length ? sh.specVersions.map(esc).join(', ') : '—'}</b>` +
    `<span>Keys</span><b class="cl-mono">${sh.keys.length ? sh.keys.map(esc).join(', ') : '—'}</b>` +
    `<span>Shards</span><b>${sh.rows.length}</b></div>`;
  if (!sh.rows.length) return spec + '<div class="cl-empty">No shards in the catalog.</div>';
  const head = ['Shard', 'State', 'Keys', 'Owner', 'Generation', 'Docs', 'Size', 'Published', 'Readers'];
  const rows = sh.rows.map(r => {
    let owner = r.owner == null ? '<span class="cl-dim">none</span>' : `<span class="cl-mono">${esc(r.owner)}</span>`;
    if (r.owner != null && r.leaseExpired) owner += ' ' + badge('lease expired', 'cl-b-bad');
    else if (r.owner != null && !r.ownerLive) owner += ' ' + badge('no heartbeat', 'cl-b-warn');
    if (r.leaseReleased) owner += ' ' + badge('released', '');
    if (r.publisher && r.publisher !== r.owner) owner += ` <span class="cl-dim">(last published by ${esc(r.publisher)})</span>`;
    const state = chipHtml(r.state, r.state) + (r.pending ? ' <span class="cl-dot" title="unpublished change pending"></span>' : '');
    return '<tr>' + td('Shard', `<span class="cl-mono">${esc(r.shard)}</span>`) + td('State', state) +
      td('Keys', `<span class="cl-mono">${esc(r.keys)}</span>`) + td('Owner', owner) +
      td('Generation', r.generation == null ? '—' : esc(r.generation), 'cl-num') +
      td('Docs', fmtInt(r.docCount), 'cl-num') + td('Size', fmtBytes(r.sizeBytes), 'cl-num') +
      td('Published', r.publishedAge == null ? '—' : fmtAge(r.publishedAge) + ' ago', 'cl-num') +
      td('Readers', r.readers.length ? '<span>' + r.readers.map(n => `<span class="cl-mono">${esc(n)}</span>`).join(', ') + '</span>'
        : badge('none', 'cl-b-bad')) + '</tr>';
  });
  return spec + `<table class="cl-table"><thead><tr>${head.map(h => `<th>${h}</th>`).join('')}</tr></thead>` +
    `<tbody>${rows.join('')}</tbody></table>`;
}

function renderScale(view) {
  const sc = view.scale;
  const tiles = `<div class="cl-tiles">` +
    `<div class="cl-tile"><div class="cl-tile-n">${fmtInt(sc.writers)}</div><div class="cl-tile-l">writers</div></div>` +
    `<div class="cl-tile"><div class="cl-tile-n">${fmtInt(sc.readers)}</div><div class="cl-tile-l">readers</div></div>` +
    `<div class="cl-tile"><div class="cl-tile-n">${fmtInt(view.counts.shards)}</div><div class="cl-tile-l">shards</div></div>` +
    `</div>`;
  if (!sc.proposals.length) return tiles + '<div class="cl-empty">No proposals in force: the current scale fits.</div>';
  const props = sc.proposals.map(p => `<div class="cl-prop">` +
    `<div class="cl-prop-head">${badge(p.kind, 'cl-b-accent')} <span class="cl-mono">${esc(p.target)}</span>` +
    `<span class="cl-prop-since">since ${esc(p.since)}</span></div>` +
    `<div class="cl-prop-reason">${esc(p.reason)}</div>` +
    `<div class="cl-prop-action">→ ${esc(p.action)}</div>` +
    (p.metrics.length ? `<div class="cl-metrics">${p.metrics.map(m =>
      `<span class="cl-metric"><span>${esc(m.name)}</span> <b>${esc(m.value)}</b></span>`).join('')}</div>` : '') +
    `</div>`);
  return tiles + `<div class="cl-sub-title">Proposals in force (${sc.proposals.length})</div>` + props.join('');
}

function renderBacklog(view) {
  const bl = view.backlog;
  let s = `<div class="cl-kv"><span>Shards pending</span><b>${fmtInt(bl.shardsPending)}</b>` +
    `<span>Oldest waiting</span><b>${bl.oldest == null ? '—' : fmtAge(bl.oldest)}</b></div>`;
  if (bl.pending.length) {
    s += `<table class="cl-table cl-backlog"><thead><tr><th>Shard</th><th>Owner</th><th>Source rev</th>` +
      `<th>Published rev</th><th>Waiting</th></tr></thead><tbody>` +
      bl.pending.map(p => '<tr>' + td('Shard', chipHtml(p.state, p.shard)) +
        td('Owner', p.owner == null ? '<span class="cl-dim">none</span>' : `<span class="cl-mono">${esc(p.owner)}</span>`) +
        td('Source rev', p.sourceRevision == null ? '<span class="cl-dim">unknown</span>'
          : `<span class="cl-mono cl-rev" title="${esc(p.sourceRevision)}">${esc(p.sourceRevision)}</span>`) +
        td('Published rev', p.publishedRevision == null ? '<span class="cl-dim">none</span>'
          : `<span class="cl-mono cl-rev" title="${esc(p.publishedRevision)}">${esc(p.publishedRevision)}</span>`) +
        td('Waiting', p.waiting == null ? '—' : fmtAge(p.waiting), 'cl-num') + '</tr>').join('') +
      '</tbody></table>';
  } else {
    s += '<div class="cl-empty">Every shard is published at its source revision.</div>';
  }
  s += '<div class="cl-sub-title">Writer jobs</div>';
  if (!bl.jobs.length) return s + '<div class="cl-empty">No live writers.</div>';
  s += bl.jobs.map(j => {
    if (!j.job) return `<div class="cl-job"><span class="cl-mono">${esc(j.node)}</span>` +
      `<span class="cl-dim">${j.stopping ? 'stopping' : 'idle'}</span></div>`;
    const pct = Math.round(j.job.progress * 100);
    return `<div class="cl-job"><span class="cl-mono">${esc(j.node)}</span>` +
      `<span>${esc(j.job.shard)} · <b>${esc(j.job.phase)}</b></span>` +
      `<span class="cl-progress" role="progressbar" aria-valuenow="${pct}" aria-valuemin="0" aria-valuemax="100">` +
      `<span class="cl-progress-fill" style="width:${pct}%"></span></span>` +
      `<span class="cl-num">${pct}%</span>` +
      `<span class="cl-dim">${j.job.elapsed == null ? '' : fmtAge(j.job.elapsed)}</span></div>`;
  }).join('');
  return s;
}

function renderHealth(view) {
  if (!view.health.length) return '<div class="cl-empty cl-ok">No health findings.</div>';
  return '<ul class="cl-health">' + view.health.map(h =>
    `<li>${badge(h.kind, /expired|unassigned|unserved|no-readers|low-disk/.test(h.kind) ? 'cl-b-bad' : 'cl-b-warn')}` +
    ` <span>${esc(h.message)}</span></li>`).join('') + '</ul>';
}

function panel(id, title, body, extra) {
  return `<section class="cl-panel" id="${id}"><div class="cl-panel-title">${esc(title)}${extra || ''}</div>` +
    `<div class="cl-panel-body">${body}</div></section>`;
}

/** The whole page for a view; {@code width} is the diagram panel's inner width in px. */
function renderPage(view, width) {
  const c = view.counts;
  const head = `<div class="cl-head"><span class="cl-mode">${esc(view.single ? 'single node' : 'cluster')}</span>` +
    `<span>${plural(c.shards, 'shard')} · ${plural(c.writers, 'writer')} · ${plural(c.readers, 'reader')}</span>` +
    `<span class="cl-dim">as of ${esc(view.asOf || '—')}</span></div>`;
  return head +
    panel('cl-layout', 'Layout', `<div class="cl-diagram-wrap">${renderDiagram(view, width)}</div>${legend()}`) +
    `<div class="cl-grid">` +
    panel('cl-backlog', 'Indexing backlog', renderBacklog(view)) +
    panel('cl-scale', 'Scale', renderScale(view)) +
    `</div>` +
    panel('cl-health', 'Health', renderHealth(view), view.health.length ? ` <span class="cl-count">${view.health.length}</span>` : '') +
    panel('cl-nodes', 'Nodes', renderNodes(view)) +
    panel('cl-sharding', 'Sharding', renderSharding(view));
}

const api = { STATUS_FIELDS, STATES, toView, groupBoxes, diagramLayout, renderDiagram, renderPage,
  fmtBytes, fmtAge, fmtPct, fmtInt, fmtMetric, esc };
if (typeof module !== 'undefined' && module.exports) module.exports = api;
else root.PharosCluster = api;
})(typeof window !== 'undefined' ? window : globalThis);
