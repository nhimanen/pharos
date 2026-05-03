// ============================================================
// CodeSearch Web UI  ·  app.js  ·  D3 v7 force graph
// ============================================================

// ── Config ───────────────────────────────────────────────────
const COLORS = {
  method:      '#42a5f5',
  constructor: '#66bb6a',
  class:       '#ab47bc',
  document:    '#26c6da',
  chunk:       '#26c6da',
  indexed:     '#26a69a',
  external:    '#78909c',
  selected:    '#ff7043',
  highlight:   '#ffd54f'
};

// ── State ────────────────────────────────────────────────────
const S = {
  view:            'modules', // 'modules' | 'classes' | 'call'
  selectedProject: null,
  selectedClass:   null,
  selectedNode:    null,
  projects:        [],
  graphData:       null,
  searchQuery:     '',
  hideExternal:    false,
};

// ── D3 refs ──────────────────────────────────────────────────
let svgSel, gSel, simulation;

// ── Boot ─────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  initGraph();
  initControls();
  await refreshProjects();
  await loadModuleGraph();
});

// ── Graph canvas setup ───────────────────────────────────────
function initGraph() {
  const container = document.getElementById('graph-container');
  svgSel = d3.select('#graph-canvas');

  function resize() {
    const r = container.getBoundingClientRect();
    svgSel.attr('width', r.width).attr('height', r.height);
    if (simulation) simulation.force('center', d3.forceCenter(r.width / 2, r.height / 2)).alpha(0.1).restart();
  }
  new ResizeObserver(resize).observe(container);
  resize();

  // Arrow marker
  const defs = svgSel.append('defs');
  defs.append('marker')
    .attr('id', 'arrow')
    .attr('viewBox', '0 -4 9 8')
    .attr('refX', 8).attr('refY', 0)
    .attr('markerWidth', 7).attr('markerHeight', 7)
    .attr('orient', 'auto')
    .append('path').attr('d', 'M0,-4L9,0L0,4').attr('fill', '#555');

  gSel = svgSel.append('g');

  svgSel.call(d3.zoom().scaleExtent([0.04, 10])
    .on('zoom', e => gSel.attr('transform', e.transform)));

  svgSel.on('click', () => {
    S.selectedNode = null;
    closeDetail();
    clearHighlight();
  });
}

// ── Controls ─────────────────────────────────────────────────
function initControls() {
  document.getElementById('btn-modules').addEventListener('click', loadModuleGraph);

  document.getElementById('btn-hide-external').addEventListener('click', () => {
    S.hideExternal = !S.hideExternal;
    document.getElementById('btn-hide-external').classList.toggle('active', S.hideExternal);
    if (S.graphData) rerender();
  });

  const searchInput = document.getElementById('search-input');
  searchInput.addEventListener('input', e => {
    S.searchQuery = e.target.value.toLowerCase();
    applyLocalFilter();
  });
  searchInput.addEventListener('keydown', e => {
    if (e.key === 'Enter') doSearch();
  });
  document.getElementById('search-btn').addEventListener('click', doSearch);

  document.getElementById('pkg-filter').addEventListener('change', e => {
    if (S.view === 'call' && S.selectedProject) {
      loadCallGraph(S.selectedProject, e.target.value, S.selectedClass);
    }
  });

  document.getElementById('detail-close').addEventListener('click', closeDetail);

  // Detail panel event delegation
  document.getElementById('detail-content').addEventListener('click', e => {
    const btn = e.target.closest('[data-action]');
    if (!btn) return;
    const { action, fqn, project, cls } = btn.dataset;
    if (action === 'callers')     loadRelated(fqn, 'callers');
    if (action === 'callees')     loadRelated(fqn, 'callees');
    if (action === 'drill-proj')  loadClassGraph(project);
    if (action === 'drill-class') loadMethodGraph(S.selectedProject, cls);
  });
}

// ── Data loading ─────────────────────────────────────────────
async function refreshProjects() {
  const res = await fetch('/api/projects');
  S.projects = await res.json();
  renderProjectList();
}

async function loadModuleGraph() {
  S.view = 'modules';
  S.selectedProject = null;
  S.selectedClass = null;
  setActiveBtn('btn-modules');
  document.getElementById('pkg-filter').innerHTML = '<option value="">All packages</option>';
  document.getElementById('pkg-filter').style.display = 'none';
  renderBreadcrumb();

  const res = await fetch('/api/graph/modules');
  S.graphData = await res.json();
  rerender();
  const vis = S.hideExternal ? S.graphData.nodes.filter(n => n.status !== 'external') : S.graphData.nodes;
  setStatus(`${vis.length} modules · ${S.graphData.edges.length} dependencies`);
  renderProjectList();
}

async function loadClassGraph(projectName) {
  S.view = 'classes';
  S.selectedProject = projectName;
  S.selectedClass = null;
  setActiveBtn(null);
  document.getElementById('pkg-filter').style.display = 'none';
  closeDetail();
  renderBreadcrumb();
  renderProjectList();

  setStatus('Loading class graph…');
  const res = await fetch(`/api/graph/classes/${encodeURIComponent(projectName)}`);
  S.graphData = await res.json();
  rerender();
  const vis = S.hideExternal ? S.graphData.nodes.filter(n => !n.external) : S.graphData.nodes;
  setStatus(`${vis.length} classes · ${S.graphData.edges.length} dependencies`);
}

async function loadMethodGraph(projectName, className) {
  S.view = 'call';
  S.selectedProject = projectName;
  S.selectedClass = className;
  setActiveBtn(null);
  closeDetail();
  renderBreadcrumb();

  setStatus('Loading method graph…');
  const res = await fetch(
    `/api/graph/call/${encodeURIComponent(projectName)}?class=${encodeURIComponent(className)}&limit=500`
  );
  S.graphData = await res.json();
  rerender();
  document.getElementById('pkg-filter').style.display = 'none';
  const vis = S.hideExternal ? S.graphData.nodes.filter(n => !n.external) : S.graphData.nodes;
  const simple = className.split('.').pop();
  setStatus(`${vis.length} methods · ${S.graphData.edges.length} calls in ${simple}`);
}

// kept for pkg-filter reloads
async function loadCallGraph(project, pkg, className) {
  S.view = 'call';
  S.selectedProject = project;
  const pkgParam  = pkg       ? `&package=${encodeURIComponent(pkg)}`       : '';
  const clsParam  = className ? `&class=${encodeURIComponent(className)}`   : '';
  const res = await fetch(`/api/graph/call/${encodeURIComponent(project)}?limit=150${pkgParam}${clsParam}`);
  S.graphData = await res.json();

  renderGraph(S.graphData.nodes, S.graphData.edges, 'call');
  populatePkgFilter(S.graphData.nodes);

  const extra = S.graphData.truncated ? ` (top 150 of ${S.graphData.total})` : '';
  setStatus(`${S.graphData.nodes.length} methods · ${S.graphData.edges.length} calls${extra}`);
}

// entry from sidebar project list click
async function browseProject(name) {
  S.selectedProject = name;
  renderProjectList();
  await loadClassGraph(name);
}

// ── Breadcrumb ────────────────────────────────────────────────
function renderBreadcrumb() {
  const el = document.getElementById('breadcrumb');
  if (!el) return;

  if (S.view === 'modules') {
    el.style.display = 'none';
    return;
  }
  el.style.display = 'flex';

  const parts = [];
  parts.push({ label: 'Projects', action: loadModuleGraph });
  if (S.selectedProject) {
    if (S.view === 'classes') {
      parts.push({ label: S.selectedProject, action: null });
    } else {
      parts.push({ label: S.selectedProject, action: () => loadClassGraph(S.selectedProject) });
    }
  }
  if (S.selectedClass) {
    parts.push({ label: S.selectedClass.split('.').pop(), action: null });
  }

  const hintText = S.view === 'classes'
    ? '<span class="breadcrumb-hint">double-click class → methods</span>'
    : '';

  el.innerHTML = parts.map((p, i) => {
    const sep = i > 0 ? '<span class="breadcrumb-sep"> › </span>' : '';
    if (p.action) {
      return `${sep}<span class="breadcrumb-link" data-idx="${i}">${esc(p.label)}</span>`;
    }
    return `${sep}<span class="breadcrumb-current">${esc(p.label)}</span>`;
  }).join('') + hintText;

  el.querySelectorAll('.breadcrumb-link').forEach(span => {
    const idx = parseInt(span.dataset.idx);
    span.addEventListener('click', parts[idx].action);
  });
}

// ── Re-render with current filter state ──────────────────────
function rerender() {
  if (!S.graphData) return;
  let nodes = S.graphData.nodes;
  let edges = S.graphData.edges;
  if (S.hideExternal) {
    // For modules: hide status==='external'; for classes/methods: hide external===true
    const isExt = d => S.view === 'modules' ? d.status === 'external' : d.external === true;
    const visibleIds = new Set(nodes.filter(n => !isExt(n)).map(n => n.id));
    nodes = nodes.filter(n => !isExt(n));
    edges = edges.filter(e => visibleIds.has(e.source) && visibleIds.has(e.target));
  }
  renderGraph(nodes, edges, S.view);
}

// ── Rendering ────────────────────────────────────────────────
function renderGraph(nodes, edges, mode) {
  if (simulation) { simulation.stop(); simulation = null; }
  gSel.selectAll('*').remove();

  const emptyHint = document.getElementById('empty-hint');
  emptyHint.style.display = nodes.length ? 'none' : 'block';
  if (!nodes.length) return;

  const rect = svgSel.node().getBoundingClientRect();

  // Clone arrays for D3 mutation
  const ns = nodes.map(d => ({ ...d }));
  const es = edges.map(e => ({ ...e }));

  // Links
  const link = gSel.append('g')
    .selectAll('line').data(es).join('line')
    .attr('class', 'link')
    .attr('stroke', '#2d333b')
    .attr('stroke-width', 1)
    .attr('stroke-opacity', 0.8)
    .attr('marker-end', 'url(#arrow)');

  // Node groups
  const node = gSel.append('g')
    .selectAll('g').data(ns).join('g')
    .attr('class', 'node')
    .call(d3.drag()
      .on('start', (ev, d) => { if (!ev.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
      .on('drag',  (ev, d) => { d.fx = ev.x; d.fy = ev.y; })
      .on('end',   (ev, d) => { if (!ev.active) simulation.alphaTarget(0); d.fx = null; d.fy = null; }))
    .on('click',   (ev, d) => { ev.stopPropagation(); selectNode(d); })
    .on('dblclick', (ev, d) => { ev.stopPropagation(); drillDown(d); })
    .on('mouseover', showTooltip)
    .on('mouseout',  hideTooltip);

  node.append('circle')
    .attr('r', d => nodeR(d, mode))
    .attr('fill', d => nodeC(d, mode))
    .attr('stroke', '#0d1117')
    .attr('stroke-width', 1.5);

  node.append('text')
    .attr('dy', d => nodeR(d, mode) + 11)
    .attr('text-anchor', 'middle')
    .attr('fill', '#6e7681')
    .attr('font-size', '10px')
    .text(d => trunc(d.label || shortId(d.id), 18));

  // Force simulation
  const linkDist = mode === 'modules' ? 120 : mode === 'classes' ? 100 : 70;
  const charge   = mode === 'modules' ? -400 : mode === 'classes' ? -300 : -180;

  simulation = d3.forceSimulation(ns)
    .force('link', d3.forceLink(es).id(d => d.id).distance(linkDist).strength(0.4))
    .force('charge', d3.forceManyBody().strength(charge))
    .force('center', d3.forceCenter(rect.width / 2, rect.height / 2))
    .force('collide', d3.forceCollide().radius(d => nodeR(d, mode) + 4))
    .on('tick', () => {
      link
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => tgtX(d, mode))
        .attr('y2', d => tgtY(d, mode));
      node.attr('transform', d => `translate(${d.x ?? 0},${d.y ?? 0})`);
    });
}

function nodeR(d, mode) {
  if (mode === 'modules') return d.status === 'indexed' ? 16 : 9;
  if (mode === 'classes') return Math.max(8, Math.min(30, 8 + Math.log((d.methodCount || 0) + 1) * 4));
  return Math.max(5, Math.min(26, 5 + Math.log((d.inDegree || 0) + 1) * 4.5));
}

function nodeC(d, mode) {
  if (mode === 'modules') return d.status === 'indexed' ? COLORS.indexed : COLORS.external;
  if (d.external) return COLORS.external;
  if (mode === 'classes') return COLORS.class;
  return COLORS[d.kind] || COLORS.method;
}

function tgtX(d, mode) {
  const r = nodeR(d.target, mode);
  const dx = d.target.x - d.source.x, dy = d.target.y - d.source.y;
  const dist = Math.hypot(dx, dy) || 1;
  return d.target.x - (dx / dist) * (r + 8);
}
function tgtY(d, mode) {
  const r = nodeR(d.target, mode);
  const dx = d.target.x - d.source.x, dy = d.target.y - d.source.y;
  const dist = Math.hypot(dx, dy) || 1;
  return d.target.y - (dy / dist) * (r + 8);
}

// ── Drill-down ────────────────────────────────────────────────
function drillDown(d) {
  if (S.view === 'modules' && d.status === 'indexed' && d.projectName) {
    loadClassGraph(d.projectName);
  } else if (S.view === 'classes') {
    loadMethodGraph(S.selectedProject, d.id);
  }
}

// ── Node selection ────────────────────────────────────────────
function selectNode(d) {
  S.selectedNode = d;
  gSel.selectAll('.node circle')
    .attr('stroke', n => n.id === d.id ? COLORS.selected : '#0d1117')
    .attr('stroke-width', n => n.id === d.id ? 3 : 1.5);
  showDetail(d);
}

function clearHighlight() {
  gSel.selectAll('.node circle')
    .attr('stroke', '#0d1117').attr('stroke-width', 1.5)
    .attr('fill', d => nodeC(d, S.view)).attr('opacity', 1);
  gSel.selectAll('.node text').attr('opacity', 1);
}

// ── Detail panel ──────────────────────────────────────────────
async function showDetail(d) {
  document.getElementById('detail-panel').classList.add('open');

  if (S.view === 'modules') {
    renderModuleDetail(d);
    return;
  }
  if (S.view === 'classes') {
    renderClassDetail(d);
    return;
  }

  document.getElementById('detail-content').innerHTML = '<div class="loading">Loading…</div>';
  try {
    const fqn = extractFqn(d.id);
    const res = await fetch(`/api/method?fqn=${encodeURIComponent(fqn)}`);
    if (res.ok) renderMethodDetail(await res.json(), d);
    else         renderFqnDetail(d);
  } catch (_) { renderFqnDetail(d); }
}

function renderMethodDetail(m, d) {
  const fqn = extractFqn(d.id);
  const kind = m.docType || d.kind || 'method';
  const el = document.getElementById('detail-content');
  el.innerHTML = `
    <div class="d-header">
      <span class="badge badge-${esc(kind)}">${esc(kind)}</span>
      <span class="badge badge-project">${esc(m.project)}</span>
    </div>
    <div class="d-name">${esc(m.methodName || m.label || '')}</div>
    <div class="d-class">${esc(m.className || '')}</div>
    ${m.signature ? `<div class="d-sig">${esc(m.signature)}</div>` : ''}
    ${m.javadoc   ? `<div class="d-javadoc">${esc(m.javadoc)}</div>` : ''}
    <div class="d-file">${esc(shortPath(m.filePath))}:${m.startLine}–${m.endLine}</div>
    ${m.body ? `<pre class="d-body"><code>${esc(m.body)}</code></pre>` : ''}
    <div class="d-actions">
      <button data-action="callers" data-fqn="${esc(fqn)}">Callers</button>
      <button data-action="callees" data-fqn="${esc(fqn)}">Callees</button>
    </div>
    <div id="related-area"></div>
  `;
}

function renderFqnDetail(d) {
  const fqn = extractFqn(d.id);
  document.getElementById('detail-content').innerHTML = `
    <div class="d-name">${esc(d.label || shortId(d.id))}</div>
    <div class="d-class">${esc(d.className || '')}</div>
    <div class="d-file">${esc(d.id)}</div>
    <div class="d-actions">
      <button data-action="callers" data-fqn="${esc(fqn)}">Callers</button>
      <button data-action="callees" data-fqn="${esc(fqn)}">Callees</button>
    </div>
    <div id="related-area"></div>
  `;
}

function renderClassDetail(d) {
  const proj = S.selectedProject;
  document.getElementById('detail-content').innerHTML = `
    <div class="d-header">
      <span class="badge badge-class">class</span>
      <span class="badge badge-project">${esc(proj)}</span>
    </div>
    <div class="d-name">${esc(d.label || d.id.split('.').pop())}</div>
    <div class="d-class">${esc(d.id)}</div>
    <div class="d-stats">
      <div><b>${(d.methodCount || 0).toLocaleString()}</b> methods</div>
      <div><b>${(d.inDegree || 0)}</b> incoming deps</div>
    </div>
    <div class="d-actions">
      <button data-action="drill-class" data-cls="${esc(d.id)}">Methods →</button>
    </div>
  `;
}

function renderModuleDetail(d) {
  const proj = S.projects.find(p => p.name === d.projectName);
  document.getElementById('detail-content').innerHTML = `
    <div class="d-header">
      <span class="badge badge-${esc(d.status)}">${esc(d.status)}</span>
    </div>
    <div class="d-name">${esc(d.label || d.id)}</div>
    <div class="d-class">${esc(d.id)}</div>
    ${d.version ? `<div class="d-file">Version: ${esc(d.version)}</div>` : ''}
    ${proj ? `<div class="d-stats">
      <div><b>${proj.methodCount.toLocaleString()}</b> methods</div>
      <div><b>${proj.classCount.toLocaleString()}</b> classes</div>
      <div><b>${proj.fileCount.toLocaleString()}</b> files</div>
    </div>` : ''}
    ${d.status === 'indexed' && d.projectName ? `
    <div class="d-actions">
      <button data-action="drill-proj" data-project="${esc(d.projectName)}">Classes →</button>
    </div>` : ''}
  `;
}

async function loadRelated(fqn, type) {
  const el = document.getElementById('related-area');
  if (!el) return;
  el.innerHTML = '<div class="loading">Loading…</div>';
  try {
    const res = await fetch(`/api/${type}?fqn=${encodeURIComponent(fqn)}`);
    const items = await res.json();
    renderRelatedList(el, type === 'callers' ? 'Callers' : 'Callees', items);
  } catch (_) {
    el.innerHTML = '<div class="related-empty">Error loading</div>';
  }
}

function renderRelatedList(el, title, items) {
  if (!items.length) {
    el.innerHTML = `<div class="related-empty">No ${title.toLowerCase()}</div>`;
    return;
  }
  const rows = items.slice(0, 25).map(r => `
    <li>
      <span class="related-label" title="${esc(r.id)}">${esc(r.label)}</span>
      <span class="related-file">${esc(shortPath(r.filePath))}:${r.startLine}</span>
    </li>`).join('');
  const more = items.length > 25
    ? `<li class="related-more">…and ${items.length - 25} more</li>` : '';
  el.innerHTML = `
    <div class="related-header">${title} (${items.length})</div>
    <ul class="related-list">${rows}${more}</ul>`;
}

function closeDetail() {
  document.getElementById('detail-panel').classList.remove('open');
  S.selectedNode = null;
  clearHighlight();
}

// ── Project list ──────────────────────────────────────────────
function renderProjectList() {
  const el = document.getElementById('project-list');
  if (!S.projects.length) {
    el.innerHTML = '<div class="loading">No projects indexed</div>';
    return;
  }
  el.innerHTML = S.projects.map(p => `
    <div class="project-item${S.selectedProject === p.name ? ' active' : ''}"
         data-project="${esc(p.name)}">
      <div class="project-name">${esc(p.name)}</div>
      <div class="project-stats">${p.methodCount.toLocaleString()} methods · ${p.classCount.toLocaleString()} classes</div>
    </div>`).join('');

  el.querySelectorAll('.project-item').forEach(item => {
    item.addEventListener('click', () => browseProject(item.dataset.project));
  });
}

// ── Search & filter ───────────────────────────────────────────
function applyLocalFilter() {
  const q = S.searchQuery;
  gSel.selectAll('.node').each(function(d) {
    const match = !q
      || (d.id    || '').toLowerCase().includes(q)
      || (d.label || '').toLowerCase().includes(q)
      || (d.className || '').toLowerCase().includes(q);
    d3.select(this).select('circle').attr('opacity', !q || match ? 1 : 0.12);
    d3.select(this).select('text').attr('opacity',   !q || match ? 1 : 0.08);
  });
}

async function doSearch() {
  const q = document.getElementById('search-input').value.trim();
  if (!q) { applyLocalFilter(); return; }

  const proj = S.selectedProject ? `&project=${encodeURIComponent(S.selectedProject)}` : '';
  const res  = await fetch(`/api/search?q=${encodeURIComponent(q)}${proj}&limit=30`);
  const hits = await res.json();
  if (!hits.length) return;

  const hitIds = new Set(hits.map(r => r.id));
  gSel.selectAll('.node').each(function(d) {
    const match = hitIds.has(d.id);
    d3.select(this).select('circle')
      .attr('fill',    match ? COLORS.highlight : nodeC(d, S.view))
      .attr('opacity', match ? 1 : 0.15);
    d3.select(this).select('text').attr('opacity', match ? 1 : 0.1);
  });
  setStatus(`${hits.length} search hits highlighted`);
}

// ── Package filter dropdown ───────────────────────────────────
function populatePkgFilter(nodes) {
  const pkgs = [...new Set(nodes.map(n => n.packageName).filter(Boolean))].sort();
  const sel = document.getElementById('pkg-filter');
  sel.style.display = pkgs.length > 1 ? '' : 'none';
  sel.innerHTML = '<option value="">All packages</option>' +
    pkgs.map(p => `<option value="${esc(p)}">${esc(p)}</option>`).join('');
}

// ── Tooltip ───────────────────────────────────────────────────
function showTooltip(event, d) {
  const tip = document.getElementById('tooltip');
  tip.style.display = 'block';
  tip.style.left = (event.pageX + 14) + 'px';
  tip.style.top  = (event.pageY - 12) + 'px';
  const drillHint = (S.view === 'modules' && d.status === 'indexed') || S.view === 'classes'
    ? '<small style="color:#ffd54f">double-click to drill down</small>' : '';
  tip.innerHTML = `<b>${esc(d.label || shortId(d.id))}</b><small>${esc(d.id)}</small>${drillHint}`;
}
function hideTooltip() {
  document.getElementById('tooltip').style.display = 'none';
}

// ── Utilities ─────────────────────────────────────────────────
function setStatus(txt)    { document.getElementById('status-bar').textContent = txt; }
function setActiveBtn(id)  {
  document.querySelectorAll('.btn-view').forEach(b => b.classList.remove('active'));
  if (id) {
    const el = document.getElementById(id);
    if (el) el.classList.add('active');
  }
}
function trunc(s, n)   { return s && s.length > n ? s.slice(0, n) + '…' : (s || ''); }
function shortId(id)   { return id ? id.split(/[.#]/).pop() : id; }
function shortPath(p)  { return p ? p.replace(/^.*?\/src\//, 'src/') : (p || ''); }
function extractFqn(id){ const c = id.indexOf(':'); return c >= 0 ? id.substring(c + 1) : id; }
function esc(s) {
  if (s == null) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
