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
  highlight:   '#ffd54f',
};

const CANVAS_THRESHOLD = 300;

const PKG_HULL_FILL = [
  'rgba(56,139,253,0.07)',  'rgba(63,185,80,0.07)',
  'rgba(171,71,188,0.07)',  'rgba(38,198,218,0.07)',
  'rgba(255,213,79,0.07)',  'rgba(255,112,67,0.07)',
  'rgba(120,144,156,0.07)', 'rgba(38,166,154,0.07)',
];
const PKG_HULL_STROKE = [
  'rgba(56,139,253,0.28)',  'rgba(63,185,80,0.28)',
  'rgba(171,71,188,0.28)',  'rgba(38,198,218,0.28)',
  'rgba(255,213,79,0.28)',  'rgba(255,112,67,0.28)',
  'rgba(120,144,156,0.28)', 'rgba(38,166,154,0.28)',
];

// ── State ────────────────────────────────────────────────────
const S = {
  view:            'modules',
  selectedProject: null,
  selectedClass:   null,
  selectedNode:    null,
  projects:        [],
  graphData:       null,
  searchQuery:     '',
  hideExternal:    true,
  // Focus modes
  focusMode:       'none',   // 'none'|'ego'|'expand'|'topn'|'collapse-leaves'
  egoHops:         2,
  egoCenter:       null,     // node id
  topN:            50,
  expandRoot:      null,     // node id
  expandedNodes:   new Set(),
  // Caller-depth filter
  callerDepth:     2,
  callerDepthRoot: null,
  // Visual
  pinnedNodes:     new Set(),
  showClusters:    false,
  showOverview:    false,
  searchToFocus:   false,
  // Language filter
  langFilter:      null,    // e.g. 'java', 'py', null = all
  langCounts:      {},      // { java: 120, py: 5, ... }
  // Test/production filter
  testFilter:      'all',   // 'all' | 'prod' | 'test'
  // Renderer
  useCanvas:       false,
  zoomTransform:   d3.zoomIdentity,
  // Context menu target
  ctxMenuNode:     null,
};

// ── D3 refs ──────────────────────────────────────────────────
let svgSel, gSel, simulation, zoomBehavior;
let canvasEl, canvasCtx;
let overviewEl, overviewCtx;
let hullLayer;
let svgLinkSel, svgNodeSel;
let currentNodes = [], currentEdges = [], currentMode = 'modules';
let minimapState = { scale: 1, ox: 0, oy: 0 };

// Canvas interaction state
let canvasDragNode = null, canvasDragActive = false, canvasHoverNode = null;

// ── Boot ─────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  initGraph();
  initControls();
  initContextMenu();
  await loadPipelines();
  await refreshProjects();
  await loadModuleGraph();
});

async function loadPipelines() {
  const sel = document.getElementById('pipeline-select');
  try {
    const pipelines = await fetch('/api/pipelines').then(r => r.json());
    sel.innerHTML = pipelines.map(p =>
      `<option value="${p.id}" ${p.available ? '' : 'disabled'} title="${p.description}">` +
      `${p.label}${p.available ? '' : ' (unavailable)'}</option>`
    ).join('');
    const autoOpt = sel.querySelector('option[value="auto"]');
    if (autoOpt) autoOpt.selected = true;
  } catch {
    sel.innerHTML = '<option value="auto">Auto</option><option value="hybrid">Hybrid</option>';
  }
}

// ── Graph canvas setup ────────────────────────────────────────
function initGraph() {
  const container = document.getElementById('graph-container');
  svgSel    = d3.select('#graph-canvas');
  canvasEl  = document.getElementById('gl-canvas');
  canvasCtx = canvasEl.getContext('2d');
  overviewEl  = document.getElementById('overview-canvas');
  overviewCtx = overviewEl.getContext('2d');

  function resize() {
    const r = container.getBoundingClientRect();
    svgSel.attr('width', r.width).attr('height', r.height);
    canvasEl.width  = r.width;
    canvasEl.height = r.height;
    if (simulation)
      simulation.force('center', d3.forceCenter(r.width / 2, r.height / 2)).alpha(0.1).restart();
    if (S.showOverview) drawOverview();
  }
  new ResizeObserver(resize).observe(container);
  resize();

  // SVG arrow marker
  const defs = svgSel.append('defs');
  defs.append('marker')
    .attr('id', 'arrow')
    .attr('viewBox', '0 -4 9 8')
    .attr('refX', 8).attr('refY', 0)
    .attr('markerWidth', 7).attr('markerHeight', 7)
    .attr('orient', 'auto')
    .append('path').attr('d', 'M0,-4L9,0L0,4').attr('fill', '#555');

  hullLayer = svgSel.append('g').attr('class', 'hull-layer');
  gSel      = svgSel.append('g');

  // Zoom – applied to SVG by default; switches to canvas when in canvas mode
  zoomBehavior = d3.zoom()
    .scaleExtent([0.04, 10])
    .filter(ev => {
      if (S.useCanvas && ev.type === 'mousedown' && canvasDragActive) return false;
      return !ev.ctrlKey && !ev.button;
    })
    .on('zoom', e => {
      S.zoomTransform = e.transform;
      if (S.useCanvas) {
        drawCanvasFrame();
      } else {
        gSel.attr('transform', e.transform);
        hullLayer.attr('transform', e.transform);
      }
      if (S.showOverview) drawOverview();
    });

  svgSel.call(zoomBehavior);
  svgSel.on('click', ev => {
    if (ev.target === svgSel.node()) {
      S.selectedNode = null;
      closeDetail();
      clearHighlight();
      hideCtxMenu();
    }
  });

  // Canvas events
  canvasEl.addEventListener('mousedown',   onCanvasMouseDown, { passive: false });
  canvasEl.addEventListener('mousemove',   onCanvasMouseMove);
  canvasEl.addEventListener('mouseup',     onCanvasMouseUp);
  canvasEl.addEventListener('click',       onCanvasClick);
  canvasEl.addEventListener('dblclick',    onCanvasDbClick);
  canvasEl.addEventListener('contextmenu', onCanvasContextMenu);
  document.addEventListener('mouseup', () => {
    if (canvasDragNode) {
      if (!S.pinnedNodes.has(canvasDragNode.id)) { canvasDragNode.fx = null; canvasDragNode.fy = null; }
      if (simulation) simulation.alphaTarget(0);
    }
    canvasDragNode  = null;
    canvasDragActive = false;
  });

  overviewEl.addEventListener('click', ev => panToMinimapClick(ev, overviewEl));
}

function panToMinimapClick(ev, canvasElem) {
  if (!currentNodes.length) return;
  const r  = canvasElem.getBoundingClientRect();
  const gx = (ev.clientX - r.left - minimapState.ox) / minimapState.scale;
  const gy = (ev.clientY - r.top  - minimapState.oy) / minimapState.scale;
  const cr = document.getElementById('graph-container').getBoundingClientRect();
  const t  = S.zoomTransform;
  const target = S.useCanvas ? d3.select(canvasEl) : svgSel;
  target.call(zoomBehavior.transform,
    d3.zoomIdentity.translate(cr.width / 2 - gx * t.k, cr.height / 2 - gy * t.k).scale(t.k));
}

// ── Canvas event handlers ─────────────────────────────────────
function canvasGraphCoords(clientX, clientY) {
  const r = canvasEl.getBoundingClientRect(), t = S.zoomTransform;
  return { x: (clientX - r.left - t.x) / t.k, y: (clientY - r.top - t.y) / t.k };
}

function canvasHitTest(clientX, clientY) {
  const { x, y } = canvasGraphCoords(clientX, clientY);
  for (let i = currentNodes.length - 1; i >= 0; i--) {
    const n = currentNodes[i];
    if (n.x != null && Math.hypot(x - n.x, y - n.y) <= nodeR(n, currentMode) + 4) return n;
  }
  return null;
}

function onCanvasMouseDown(ev) {
  const n = canvasHitTest(ev.clientX, ev.clientY);
  if (n) {
    canvasDragActive = true;
    canvasDragNode   = n;
    if (simulation) simulation.alphaTarget(0.3).restart();
    n.fx = n.x; n.fy = n.y;
    ev.stopPropagation();
  }
}

function onCanvasMouseMove(ev) {
  if (canvasDragNode) {
    const { x, y } = canvasGraphCoords(ev.clientX, ev.clientY);
    canvasDragNode.fx = x;
    canvasDragNode.fy = y;
    return;
  }
  const n = canvasHitTest(ev.clientX, ev.clientY);
  if (n !== canvasHoverNode) {
    canvasHoverNode = n;
    if (n) showTooltip(ev, n); else hideTooltip();
  } else if (n) {
    const tip = document.getElementById('tooltip');
    tip.style.left = (ev.pageX + 14) + 'px';
    tip.style.top  = (ev.pageY - 12) + 'px';
  }
}

function onCanvasMouseUp() { canvasDragActive = false; }

function onCanvasClick(ev) {
  if (Math.abs(ev.movementX) + Math.abs(ev.movementY) > 4) return;
  const n = canvasHitTest(ev.clientX, ev.clientY);
  hideCtxMenu();
  if (n) { ev.stopPropagation(); selectNode(n); }
  else   { S.selectedNode = null; closeDetail(); clearHighlight(); }
}

function onCanvasDbClick(ev) {
  const n = canvasHitTest(ev.clientX, ev.clientY);
  if (n) { ev.stopPropagation(); drillDown(n); }
}

function onCanvasContextMenu(ev) {
  ev.preventDefault();
  const n = canvasHitTest(ev.clientX, ev.clientY);
  if (n) showCtxMenu(ev.clientX, ev.clientY, n);
}

// ── Controls ─────────────────────────────────────────────────
function initControls() {
  document.getElementById('btn-modules').addEventListener('click', loadModuleGraph);

  document.getElementById('btn-hide-external').addEventListener('click', () => {
    S.hideExternal = !S.hideExternal;
    document.getElementById('btn-hide-external').classList.toggle('active', S.hideExternal);
    if (S.graphData) { rerender(); fitToView(); }
  });

  const searchInput = document.getElementById('search-input');
  searchInput.addEventListener('input', e => {
    S.searchQuery = e.target.value.toLowerCase();
    applyLocalFilter();
  });
  searchInput.addEventListener('keydown', e => { if (e.key === 'Enter') doSearch(); });
  document.getElementById('search-btn').addEventListener('click', doSearch);

  document.getElementById('pkg-filter').addEventListener('change', e => {
    if (S.view === 'call' && S.selectedProject)
      loadCallGraph(S.selectedProject, e.target.value, S.selectedClass);
  });

  document.getElementById('detail-close').addEventListener('click', closeDetail);
  document.getElementById('detail-content').addEventListener('click', e => {
    const btn = e.target.closest('[data-action]');
    if (!btn) return;
    const { action, fqn, project, cls, filepath } = btn.dataset;
    if (action === 'callers')      loadRelated(fqn, 'callers');
    if (action === 'callees')      loadRelated(fqn, 'callees');
    if (action === 'drill-proj')   loadClassGraph(project);
    if (action === 'drill-class')  loadMethodGraph(S.selectedProject, cls);
    if (action === 'focus-node')   focusNode(fqn);
    if (action === 'locate-file')  locateFileInTree(filepath, project);
  });

  // Focus mode
  document.getElementById('focus-mode-select').addEventListener('change', e => {
    S.focusMode = e.target.value;
    if (S.focusMode !== 'ego')    S.egoCenter = null;
    if (S.focusMode !== 'expand') { S.expandRoot = null; S.expandedNodes.clear(); }
    syncFocusOpts();
    if (S.graphData) rerender();
  });

  const egoSlider = document.getElementById('ego-hops-slider');
  egoSlider.addEventListener('input', () => {
    S.egoHops = +egoSlider.value;
    document.getElementById('ego-hops-display').textContent = S.egoHops;
    if (S.egoCenter && S.graphData) rerender();
  });

  document.getElementById('topn-input').addEventListener('input', e => {
    S.topN = Math.max(5, +e.target.value || 50);
    if (S.graphData) rerender();
  });

  document.getElementById('btn-expand-reset').addEventListener('click', () => {
    S.expandRoot = null;
    S.expandedNodes.clear();
    if (S.graphData) rerender();
  });

  // Caller-depth
  document.getElementById('caller-depth-input').addEventListener('input', e => {
    S.callerDepth = Math.max(1, +e.target.value || 2);
    if (S.callerDepthRoot && S.graphData) rerender();
  });
  document.getElementById('btn-caller-depth-clear').addEventListener('click', () => {
    S.callerDepthRoot = null;
    syncCallerDepthUI();
    if (S.graphData) rerender();
  });

  // Layout toggles
  document.getElementById('btn-clusters').addEventListener('click', () => {
    S.showClusters = !S.showClusters;
    document.getElementById('btn-clusters').classList.toggle('active', S.showClusters);
    if (currentNodes.length) {
      if (S.useCanvas) drawCanvasFrame();
      else if (S.showClusters) updateHulls(currentNodes);
      else hullLayer.selectAll('*').remove();
    }
  });

  document.getElementById('btn-overview').addEventListener('click', () => {
    S.showOverview = !S.showOverview;
    document.getElementById('btn-overview').classList.toggle('active', S.showOverview);
    document.getElementById('overview-panel').style.display = S.showOverview ? 'block' : 'none';
    if (S.showOverview && currentNodes.length) drawOverview();
  });

  document.getElementById('btn-search-focus').addEventListener('click', () => {
    S.searchToFocus = !S.searchToFocus;
    document.getElementById('btn-search-focus').classList.toggle('active', S.searchToFocus);
    applyLocalFilter();
  });

  document.getElementById('btn-zoom-in').addEventListener('click',  () => zoomBy(1.4));
  document.getElementById('btn-zoom-out').addEventListener('click', () => zoomBy(1 / 1.4));
  document.getElementById('btn-reset-view').addEventListener('click', resetZoom);
}

function syncFocusOpts() {
  document.getElementById('ego-opts').style.display    = S.focusMode === 'ego'    ? 'flex' : 'none';
  document.getElementById('topn-opts').style.display   = S.focusMode === 'topn'   ? 'flex' : 'none';
  document.getElementById('expand-opts').style.display = S.focusMode === 'expand' ? 'flex' : 'none';
}

function syncCallerDepthUI() {
  const lbl = document.getElementById('caller-depth-label');
  if (lbl) lbl.textContent = S.callerDepthRoot ? shortId(S.callerDepthRoot) : '—';
  const clr = document.getElementById('btn-caller-depth-clear');
  if (clr) clr.style.display = S.callerDepthRoot ? 'inline-block' : 'none';
}

function zoomBy(factor) {
  const target = S.useCanvas ? d3.select(canvasEl) : svgSel;
  target.transition().duration(250).call(zoomBehavior.scaleBy, factor);
}

function resetZoom() {
  const target = S.useCanvas ? d3.select(canvasEl) : svgSel;
  target.transition().duration(350).call(zoomBehavior.transform, d3.zoomIdentity);
}

function fitToView(delayMs = 600) {
  setTimeout(() => {
    if (!currentNodes.length) return;
    const rect = document.getElementById('graph-container').getBoundingClientRect();
    const W = rect.width, H = rect.height;
    const pad = 48;
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    for (const n of currentNodes) {
      if (n.x == null) continue;
      minX = Math.min(minX, n.x); maxX = Math.max(maxX, n.x);
      minY = Math.min(minY, n.y); maxY = Math.max(maxY, n.y);
    }
    if (!isFinite(minX)) return;
    const dx = maxX - minX || 1, dy = maxY - minY || 1;
    const scale = Math.min(2, 0.92 * Math.min((W - pad * 2) / dx, (H - pad * 2) / dy));
    const tx = W / 2 - scale * (minX + maxX) / 2;
    const ty = H / 2 - scale * (minY + maxY) / 2;
    const target = S.useCanvas ? d3.select(canvasEl) : svgSel;
    target.transition().duration(400).call(
      zoomBehavior.transform, d3.zoomIdentity.translate(tx, ty).scale(scale)
    );
  }, delayMs);
}

// ── Context menu ─────────────────────────────────────────────
function initContextMenu() {
  const menu = document.getElementById('ctx-menu');
  document.addEventListener('click', hideCtxMenu);
  menu.addEventListener('click', e => {
    e.stopPropagation();
    const item = e.target.closest('[data-ctx]');
    if (!item || !S.ctxMenuNode) return;
    handleCtxAction(item.dataset.ctx, S.ctxMenuNode);
    hideCtxMenu();
  });
  // SVG right-click: use event delegation on the g container
  gSel.on('contextmenu.ctx', ev => {
    ev.preventDefault();
    const nodeG = ev.target.closest('.node');
    if (!nodeG) return;
    const d = d3.select(nodeG).datum();
    if (d) showCtxMenu(ev.clientX, ev.clientY, d);
  });
}

function showCtxMenu(x, y, node) {
  S.ctxMenuNode = node;
  const menu    = document.getElementById('ctx-menu');
  const isPinned = S.pinnedNodes.has(node.id);
  menu.querySelector('[data-ctx="pin"]').textContent = isPinned ? '📌 Unpin node' : '📌 Pin node';
  menu.style.display = 'block';
  const vw = window.innerWidth, vh = window.innerHeight;
  const mw = menu.offsetWidth || 190, mh = menu.offsetHeight || 110;
  menu.style.left = Math.min(x, vw - mw - 8) + 'px';
  menu.style.top  = Math.min(y, vh - mh - 8) + 'px';
}

function hideCtxMenu() { document.getElementById('ctx-menu').style.display = 'none'; }

function handleCtxAction(action, node) {
  switch (action) {
    case 'pin':
      togglePin(node);
      break;
    case 'ego':
      document.getElementById('focus-mode-select').value = 'ego';
      S.focusMode = 'ego';
      S.egoCenter = node.id;
      syncFocusOpts();
      if (S.graphData) rerender();
      break;
    case 'caller-root':
      S.callerDepthRoot = node.id;
      syncCallerDepthUI();
      if (S.graphData) rerender();
      break;
    case 'expand-root':
      document.getElementById('focus-mode-select').value = 'expand';
      S.focusMode   = 'expand';
      S.expandRoot  = node.id;
      S.expandedNodes.clear();
      S.expandedNodes.add(node.id);
      syncFocusOpts();
      if (S.graphData) rerender();
      break;
  }
}

// ── Pinning ───────────────────────────────────────────────────
function togglePin(node) {
  const live = currentNodes.find(n => n.id === node.id) || node;
  if (S.pinnedNodes.has(live.id)) {
    S.pinnedNodes.delete(live.id);
    live.fx = null; live.fy = null;
  } else {
    S.pinnedNodes.add(live.id);
    live.fx = live.x; live.fy = live.y;
  }
  if (!S.useCanvas) {
    svgNodeSel && svgNodeSel.select('circle')
      .filter(d => d.id === live.id)
      .attr('stroke',           S.pinnedNodes.has(live.id) ? '#ffd54f' : '#0d1117')
      .attr('stroke-width',     S.pinnedNodes.has(live.id) ? 2.5 : 1.5)
      .attr('stroke-dasharray', S.pinnedNodes.has(live.id) ? '4,2' : null);
  }
}

// ── Data loading ──────────────────────────────────────────────
async function refreshProjects() {
  const res = await fetch('/api/projects');
  S.projects = await res.json();
  renderProjectList();
}

async function loadModuleGraph() {
  S.view = 'modules'; S.selectedProject = null; S.selectedClass = null;
  S.langFilter = null; S.langCounts = {}; S.testFilter = 'all';
  document.getElementById('file-tree-section').style.display = 'none';
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
  renderTestFilter();
  renderLangFilter();
}

async function loadClassGraph(projectName) {
  S.view = 'classes'; S.selectedProject = projectName; S.selectedClass = null;
  S.langFilter = null; S.testFilter = 'all';
  setActiveBtn(null);
  document.getElementById('pkg-filter').style.display = 'none';
  closeDetail(); renderBreadcrumb(); renderProjectList();
  fetchLanguages(projectName);
  setStatus('Loading class graph…');
  const res = await fetch(`/api/graph/classes/${encodeURIComponent(projectName)}`);
  S.graphData = await res.json();
  rerender();
  const vis = S.hideExternal ? S.graphData.nodes.filter(n => !n.external) : S.graphData.nodes;
  setStatus(`${vis.length} classes · ${S.graphData.edges.length} dependencies`);
}

async function loadMethodGraph(projectName, className) {
  S.view = 'call'; S.selectedProject = projectName; S.selectedClass = className;
  S.langFilter = null; S.testFilter = 'all';
  setActiveBtn(null); closeDetail(); renderBreadcrumb();
  fetchLanguages(projectName);
  setStatus('Loading method graph…');
  const res = await fetch(
    `/api/graph/call/${encodeURIComponent(projectName)}?class=${encodeURIComponent(className)}&limit=500`
  );
  S.graphData = await res.json();
  rerender();
  document.getElementById('pkg-filter').style.display = 'none';
  const vis = S.hideExternal ? S.graphData.nodes.filter(n => !n.external) : S.graphData.nodes;
  setStatus(`${vis.length} methods · ${S.graphData.edges.length} calls in ${className.split('.').pop()}`);
}

async function loadCallGraph(project, pkg, className) {
  const projectChanged = S.selectedProject !== project;
  S.view = 'call'; S.selectedProject = project;
  renderBreadcrumb(); renderProjectList();
  if (projectChanged) initFileTree(project);
  const pkgParam = pkg       ? `&package=${encodeURIComponent(pkg)}`     : '';
  const clsParam = className ? `&class=${encodeURIComponent(className)}` : '';
  const res = await fetch(`/api/graph/call/${encodeURIComponent(project)}?limit=150${pkgParam}${clsParam}`);
  S.graphData = await res.json();
  renderGraph(S.graphData.nodes, S.graphData.edges, 'call');
  populatePkgFilter(S.graphData.nodes);
  const extra = S.graphData.truncated ? ` (top 150 of ${S.graphData.total})` : '';
  setStatus(`${S.graphData.nodes.length} methods · ${S.graphData.edges.length} calls${extra}`);
}

async function fetchLanguages(projectName) {
  S.langCounts = {};
  renderLangFilter();
  renderTestFilter();
  try {
    const res = await fetch(`/api/languages/${encodeURIComponent(projectName)}`);
    S.langCounts = await res.json();
  } catch (_) { S.langCounts = {}; }
  renderLangFilter();
}

async function browseProject(name) {
  S.selectedProject = name;
  renderProjectList();
  initFileTree(name);
  await loadClassGraph(name);
}

// ── Breadcrumb ────────────────────────────────────────────────
function renderBreadcrumb() {
  const el = document.getElementById('breadcrumb');
  if (!el) return;
  if (S.view === 'modules') { el.style.display = 'none'; return; }
  el.style.display = 'flex';

  const parts = [];
  parts.push({ label: 'Projects', action: loadModuleGraph });
  if (S.selectedProject) {
    if (S.view === 'classes') parts.push({ label: S.selectedProject, action: null });
    else parts.push({ label: S.selectedProject, action: () => loadClassGraph(S.selectedProject) });
  }
  if (S.selectedClass) parts.push({ label: S.selectedClass.split('.').pop(), action: null });

  const hint = S.view === 'classes'
    ? '<span class="breadcrumb-hint">double-click class → methods</span>' : '';

  el.innerHTML = parts.map((p, i) => {
    const sep = i > 0 ? '<span class="breadcrumb-sep"> › </span>' : '';
    return p.action
      ? `${sep}<span class="breadcrumb-link" data-idx="${i}">${esc(p.label)}</span>`
      : `${sep}<span class="breadcrumb-current">${esc(p.label)}</span>`;
  }).join('') + hint;

  el.querySelectorAll('.breadcrumb-link').forEach(span =>
    span.addEventListener('click', parts[+span.dataset.idx].action));
}

// ── Re-render pipeline ────────────────────────────────────────
function rerender() {
  if (!S.graphData) return;
  let nodes = S.graphData.nodes;
  let edges = S.graphData.edges;

  // 1. Hide external
  if (S.hideExternal) {
    const isExt  = d => S.view === 'modules' ? d.status === 'external' : d.external === true;
    const vis    = new Set(nodes.filter(n => !isExt(n)).map(n => n.id));
    nodes = nodes.filter(n => !isExt(n));
    edges = edges.filter(e => vis.has(eid(e.source)) && vis.has(eid(e.target)));
  }

  // 2. Language filter
  if (S.langFilter && S.view !== 'modules') {
    const keep = new Set(nodes
      .filter(n => n.filePath && extension(n.filePath) === S.langFilter)
      .map(n => n.id));
    nodes = nodes.filter(n => keep.has(n.id));
    edges = edges.filter(e => keep.has(eid(e.source)) && keep.has(eid(e.target)));
  }

  // 3. Test/production filter
  if (S.testFilter !== 'all' && S.view !== 'modules') {
    const keep = new Set(nodes
      .filter(n => isTestNode(n) === (S.testFilter === 'test'))
      .map(n => n.id));
    nodes = nodes.filter(n => keep.has(n.id));
    edges = edges.filter(e => keep.has(eid(e.source)) && keep.has(eid(e.target)));
  }

  // 4. Focus mode
  ({ nodes, edges } = applyFocusMode(nodes, edges));

  // 5. Caller-depth
  if (S.callerDepthRoot) {
    ({ nodes, edges } = callerDepthFilter(nodes, edges, S.callerDepthRoot, S.callerDepth));
  }

  // 6. Re-apply hide-external after focus modes (traversals can reach external nodes)
  if (S.hideExternal && S.view !== 'modules') {
    const vis = new Set(nodes.filter(n => !n.external).map(n => n.id));
    nodes = nodes.filter(n => !n.external);
    edges = edges.filter(e => vis.has(eid(e.source)) && vis.has(eid(e.target)));
  }

  renderGraph(nodes, edges, S.view);
}

// ── Focus modes ───────────────────────────────────────────────
function applyFocusMode(nodes, edges) {
  switch (S.focusMode) {
    case 'ego':
      return S.egoCenter ? egoGraph(nodes, edges, S.egoCenter, S.egoHops) : { nodes, edges };
    case 'expand':
      return expandGraph(nodes, edges);
    case 'topn':
      return topNCentrality(nodes, edges, S.topN);
    case 'collapse-leaves':
      return collapseLeaves(nodes, edges);
    default:
      return { nodes, edges };
  }
}

function egoGraph(nodes, edges, centerId, hops) {
  const adj     = buildBiAdj(edges);
  const visited = new Set([centerId]);
  let frontier  = [centerId];
  for (let h = 0; h < hops; h++) {
    const next = [];
    for (const n of frontier)
      for (const nb of (adj.get(n) || []))
        if (!visited.has(nb)) { visited.add(nb); next.push(nb); }
    frontier = next;
  }
  return filterByIds(nodes, edges, visited);
}

function expandGraph(nodes, edges) {
  // Auto-seed on first invocation
  if (!S.expandRoot && S.expandedNodes.size === 0) {
    const sorted = [...nodes].sort((a, b) => (b.inDegree || 0) - (a.inDegree || 0));
    if (!sorted.length) return { nodes, edges };
    S.expandRoot = sorted[0].id;
    S.expandedNodes.add(S.expandRoot);
  }
  const adj     = buildBiAdj(edges);
  const visible = new Set(S.expandedNodes);
  for (const nid of S.expandedNodes)
    for (const nb of (adj.get(nid) || [])) visible.add(nb);
  return filterByIds(nodes, edges, visible);
}

function topNCentrality(nodes, edges, n) {
  const top = [...nodes].sort((a, b) => (b.inDegree || 0) - (a.inDegree || 0)).slice(0, n);
  return filterByIds(nodes, edges, new Set(top.map(x => x.id)));
}

function collapseLeaves(nodes, edges) {
  const inDeg = new Map(), outDeg = new Map();
  for (const n of nodes) { inDeg.set(n.id, 0); outDeg.set(n.id, 0); }
  for (const e of edges) {
    const s = eid(e.source), t = eid(e.target);
    outDeg.set(s, (outDeg.get(s) || 0) + 1);
    inDeg.set(t,  (inDeg.get(t)  || 0) + 1);
  }
  const keep = new Set(nodes
    .filter(n => !(inDeg.get(n.id) === 0 && outDeg.get(n.id) === 1))
    .map(n => n.id));
  return filterByIds(nodes, edges, keep);
}

function callerDepthFilter(nodes, edges, rootId, depth) {
  const fwdAdj = new Map(), revAdj = new Map();
  for (const e of edges) {
    const s = eid(e.source), t = eid(e.target);
    if (!fwdAdj.has(s)) fwdAdj.set(s, []);
    if (!revAdj.has(t)) revAdj.set(t, []);
    fwdAdj.get(s).push(t);
    revAdj.get(t).push(s);
  }
  const visible = new Set([rootId]);
  let fwd = [rootId], bwd = [rootId];
  for (let i = 0; i < depth; i++) {
    const nf = [], nb = [];
    for (const n of fwd) for (const t of (fwdAdj.get(n) || []))
      if (!visible.has(t)) { visible.add(t); nf.push(t); }
    for (const n of bwd) for (const s of (revAdj.get(n) || []))
      if (!visible.has(s)) { visible.add(s); nb.push(s); }
    fwd = nf; bwd = nb;
  }
  return filterByIds(nodes, edges, visible);
}

// ── Graph helpers ─────────────────────────────────────────────
function buildBiAdj(edges) {
  const adj = new Map();
  for (const e of edges) {
    const s = eid(e.source), t = eid(e.target);
    if (!adj.has(s)) adj.set(s, []);
    if (!adj.has(t)) adj.set(t, []);
    adj.get(s).push(t);
    adj.get(t).push(s);
  }
  return adj;
}

function filterByIds(nodes, edges, ids) {
  return {
    nodes: nodes.filter(n => ids.has(n.id)),
    edges: edges.filter(e => ids.has(eid(e.source)) && ids.has(eid(e.target))),
  };
}

function eid(x) { return typeof x === 'string' ? x : (x?.id ?? String(x)); }

// ── Rendering dispatcher ──────────────────────────────────────
function renderGraph(nodes, edges, mode) {
  if (simulation) { simulation.stop(); simulation = null; }
  gSel.selectAll('*').remove();
  hullLayer.selectAll('*').remove();
  svgLinkSel = null; svgNodeSel = null;

  const emptyHint = document.getElementById('empty-hint');
  emptyHint.style.display = nodes.length ? 'none' : 'block';
  if (!nodes.length) {
    if (S.useCanvas) canvasCtx.clearRect(0, 0, canvasEl.width, canvasEl.height);
    return;
  }

  // Decide renderer
  const needCanvas = nodes.length >= CANVAS_THRESHOLD;
  if (needCanvas !== S.useCanvas) {
    S.useCanvas = needCanvas;
    svgSel.style('display',   needCanvas ? 'none'  : null);
    canvasEl.style.display =  needCanvas ? 'block' : 'none';
    hullLayer.attr('display', needCanvas ? 'none'  : null);
    if (needCanvas) {
      svgSel.on('.zoom', null);
      d3.select(canvasEl).call(zoomBehavior);
    } else {
      d3.select(canvasEl).on('.zoom', null);
      svgSel.call(zoomBehavior);
    }
    S.zoomTransform = d3.zoomIdentity;
  }

  const rect = document.getElementById('graph-container').getBoundingClientRect();
  if (S.useCanvas) { canvasEl.width = rect.width; canvasEl.height = rect.height; }

  currentMode = mode;

  // Clone with position preservation
  const prevById = new Map(currentNodes.map(n => [n.id, n]));
  const ns = nodes.map(d => {
    const p   = prevById.get(d.id);
    const out = { ...d };
    if (p && p.x != null) { out.x = p.x; out.y = p.y; }
    if (S.pinnedNodes.has(d.id) && p) { out.fx = p.fx ?? p.x; out.fy = p.fy ?? p.y; }
    return out;
  });
  const es = edges.map(e => ({ ...e }));
  currentNodes = ns;
  currentEdges = es;

  if (!S.useCanvas) renderSVGGraph(ns, es, mode);

  // Force simulation — scale forces with node count so large graphs spread out
  const n = ns.length;
  const scaleFactor = Math.max(1, Math.sqrt(n / 10));
  const baseLinkDist  = mode === 'modules' ? 90  : mode === 'classes' ? 70  : 50;
  const baseCharge    = mode === 'modules' ? -220 : mode === 'classes' ? -160 : -100;
  const baseChargeCap = mode === 'modules' ? 220  : mode === 'classes' ? 180  : 140;
  const linkDist  = baseLinkDist  * scaleFactor;
  const charge    = baseCharge    * scaleFactor;
  const chargeCap = baseChargeCap * scaleFactor;
  const centerStr = Math.max(0.01, 0.08 / scaleFactor);
  const axisStr   = Math.max(0.005, 0.04 / scaleFactor);
  const cx = rect.width / 2, cy = rect.height / 2;

  simulation = d3.forceSimulation(ns)
    .force('link',    d3.forceLink(es).id(d => d.id).distance(linkDist).strength(0.65))
    .force('charge',  d3.forceManyBody().strength(charge).distanceMax(chargeCap))
    .force('center',  d3.forceCenter(cx, cy).strength(centerStr))
    .force('gx',      d3.forceX(cx).strength(axisStr))
    .force('gy',      d3.forceY(cy).strength(axisStr))
    .force('collide', d3.forceCollide().radius(d => nodeR(d, mode) + 4))
    .on('tick', () => {
      if (S.useCanvas) {
        drawCanvasFrame();
      } else {
        tickSVG();
        if (S.showClusters) updateHulls(ns);
      }
      if (S.showOverview) drawOverview();
    });

  if (S.searchQuery) applyLocalFilter();
}

// ── SVG rendering ─────────────────────────────────────────────
function renderSVGGraph(ns, es, mode) {
  svgLinkSel = gSel.append('g')
    .selectAll('line').data(es).join('line')
    .attr('class', 'link')
    .attr('stroke', '#2d333b')
    .attr('stroke-width', 1)
    .attr('stroke-opacity', 0.8)
    .attr('marker-end', 'url(#arrow)');

  svgNodeSel = gSel.append('g')
    .selectAll('g').data(ns).join('g')
    .attr('class', 'node')
    .call(d3.drag()
      .on('start', (ev, d) => { if (!ev.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
      .on('drag',  (ev, d) => { d.fx = ev.x; d.fy = ev.y; })
      .on('end',   (ev, d) => {
        if (!ev.active) simulation.alphaTarget(0);
        if (!S.pinnedNodes.has(d.id)) { d.fx = null; d.fy = null; }
      }))
    .on('click',       (ev, d) => { ev.stopPropagation(); selectNode(d); })
    .on('dblclick',    (ev, d) => { ev.stopPropagation(); drillDown(d); })
    .on('mouseover',   showTooltip)
    .on('mouseout',    hideTooltip)
    .on('contextmenu', (ev, d) => { ev.preventDefault(); ev.stopPropagation(); showCtxMenu(ev.clientX, ev.clientY, d); });

  svgNodeSel.append('circle')
    .attr('r', d => nodeR(d, mode))
    .attr('fill', d => nodeC(d, mode))
    .attr('stroke', d => S.pinnedNodes.has(d.id) ? '#ffd54f' : '#0d1117')
    .attr('stroke-width', d => S.pinnedNodes.has(d.id) ? 2.5 : 1.5)
    .attr('stroke-dasharray', d => S.pinnedNodes.has(d.id) ? '4,2' : null);

  svgNodeSel.append('text')
    .attr('dy', d => nodeR(d, mode) + 11)
    .attr('text-anchor', 'middle')
    .attr('fill', '#6e7681')
    .attr('font-size', '10px')
    .text(d => trunc(d.label || shortId(d.id), 18));

  // Expand mode: highlight expanded nodes
  if (S.focusMode === 'expand') {
    svgNodeSel.select('circle')
      .attr('stroke', d => S.expandedNodes.has(d.id) ? '#ffd54f' : '#0d1117')
      .attr('stroke-width', d => S.expandedNodes.has(d.id) ? 2.5 : 1.5);
  }
}

function tickSVG() {
  if (!svgLinkSel || !svgNodeSel) return;
  svgLinkSel
    .attr('x1', d => d.source.x)
    .attr('y1', d => d.source.y)
    .attr('x2', d => tgtX(d, currentMode))
    .attr('y2', d => tgtY(d, currentMode));
  svgNodeSel.attr('transform', d => `translate(${d.x ?? 0},${d.y ?? 0})`);
}

// ── Canvas rendering ──────────────────────────────────────────
function drawCanvasFrame() {
  const ns  = currentNodes, es = currentEdges, mode = currentMode;
  const t   = S.zoomTransform;
  const ctx = canvasCtx;
  const w   = canvasEl.width, h = canvasEl.height;

  ctx.clearRect(0, 0, w, h);
  ctx.save();
  ctx.translate(t.x, t.y);
  ctx.scale(t.k, t.k);

  const lw = Math.max(0.5, 1 / t.k);

  // Cluster hulls
  if (S.showClusters) drawCanvasHulls(ns, ctx, t.k);

  // Edges
  ctx.strokeStyle = 'rgba(45,51,59,0.8)';
  ctx.lineWidth   = lw;
  for (const e of es) {
    if (!e.source || e.source.x == null || !e.target || e.target.x == null) continue;
    const dx   = e.target.x - e.source.x, dy = e.target.y - e.source.y;
    const dist = Math.hypot(dx, dy) || 1;
    const r    = nodeR(e.target, mode);
    const ex   = e.target.x - (dx / dist) * (r + 8 / t.k);
    const ey   = e.target.y - (dy / dist) * (r + 8 / t.k);

    ctx.beginPath();
    ctx.moveTo(e.source.x, e.source.y);
    ctx.lineTo(ex, ey);
    ctx.stroke();

    // Arrow head
    const angle = Math.atan2(dy, dx);
    ctx.save();
    ctx.translate(ex, ey);
    ctx.rotate(angle);
    ctx.beginPath();
    ctx.moveTo(0, 0);
    ctx.lineTo(-9 / t.k, -4 / t.k);
    ctx.lineTo(-9 / t.k,  4 / t.k);
    ctx.closePath();
    ctx.fillStyle = '#555';
    ctx.fill();
    ctx.restore();
  }

  // Nodes
  for (const n of ns) {
    if (n.x == null) continue;
    const r          = nodeR(n, mode);
    const isSelected = S.selectedNode && S.selectedNode.id === n.id;
    const isPinned   = S.pinnedNodes.has(n.id);
    const isExpanded = S.expandedNodes.has(n.id);
    const opacity    = n._opacity != null ? n._opacity : 1;

    ctx.globalAlpha = opacity;

    ctx.beginPath();
    ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
    ctx.fillStyle = n._highlight ? COLORS.highlight : nodeC(n, mode);
    ctx.fill();

    ctx.strokeStyle  = isSelected ? COLORS.selected
      : (isPinned || isExpanded) ? '#ffd54f' : '#0d1117';
    ctx.lineWidth    = isSelected ? 3 / t.k : isPinned ? 2.5 / t.k : 1.5 / t.k;
    if (isPinned) ctx.setLineDash([4 / t.k, 2 / t.k]);
    ctx.stroke();
    ctx.setLineDash([]);

    ctx.globalAlpha = opacity * 0.85;
    ctx.fillStyle   = '#6e7681';
    ctx.font        = `${10 / t.k}px system-ui, -apple-system, sans-serif`;
    ctx.textAlign   = 'center';
    ctx.fillText(trunc(n.label || shortId(n.id), 18), n.x, n.y + r + 11 / t.k);
    ctx.globalAlpha = 1;
  }

  ctx.restore();
}

// ── Cluster hulls ─────────────────────────────────────────────
function groupByPackage(ns) {
  const groups = new Map();
  for (const n of ns) {
    const pkg = n.packageName
      || (n.id.includes('.') ? n.id.split('.').slice(0, -1).join('.') : '__default__');
    if (!groups.has(pkg)) groups.set(pkg, []);
    groups.get(pkg).push(n);
  }
  return groups;
}

function updateHulls(ns) {
  hullLayer.selectAll('*').remove();
  if (!S.showClusters) return;
  let ci = 0;
  for (const [, gnodes] of groupByPackage(ns)) {
    if (gnodes.length < 3) continue;
    const hull = d3.polygonHull(gnodes.map(n => [n.x ?? 0, n.y ?? 0]));
    if (!hull) { ci++; continue; }
    const cen  = d3.polygonCentroid(hull);
    const exp  = hull.map(([x, y]) => {
      const dx = x - cen[0], dy = y - cen[1], d = Math.hypot(dx, dy) || 1;
      return [x + (dx / d) * 24, y + (dy / d) * 24];
    });
    const c = ci % PKG_HULL_FILL.length;
    hullLayer.append('path')
      .attr('d', 'M' + exp.join('L') + 'Z')
      .attr('fill', PKG_HULL_FILL[c])
      .attr('stroke', PKG_HULL_STROKE[c])
      .attr('stroke-width', 1)
      .attr('stroke-linejoin', 'round');
    ci++;
  }
}

function drawCanvasHulls(ns, ctx, scale) {
  let ci = 0;
  for (const [, gnodes] of groupByPackage(ns)) {
    const valid = gnodes.filter(n => n.x != null);
    if (valid.length < 3) { ci++; continue; }
    const hull = d3.polygonHull(valid.map(n => [n.x, n.y]));
    if (!hull) { ci++; continue; }
    const cen = d3.polygonCentroid(hull);
    const exp = hull.map(([x, y]) => {
      const dx = x - cen[0], dy = y - cen[1], d = Math.hypot(dx, dy) || 1;
      return [x + (dx / d) * 24, y + (dy / d) * 24];
    });
    const c = ci % PKG_HULL_FILL.length;
    ctx.beginPath();
    ctx.moveTo(exp[0][0], exp[0][1]);
    for (let i = 1; i < exp.length; i++) ctx.lineTo(exp[i][0], exp[i][1]);
    ctx.closePath();
    ctx.fillStyle   = PKG_HULL_FILL[c];
    ctx.fill();
    ctx.strokeStyle = PKG_HULL_STROKE[c];
    ctx.lineWidth   = 1 / scale;
    ctx.stroke();
    ci++;
  }
}

// ── Mini-view renderer (shared by minimap & overview) ─────────
function drawMiniView(cfg) {
  if (!cfg.ctx || !cfg.show) return;
  const w = cfg.width, h = cfg.height;
  cfg.ctx.clearRect(0, 0, w, h);
  cfg.ctx.fillStyle = cfg.bg;
  cfg.ctx.fillRect(0, 0, w, h);

  const ns = currentNodes.filter(n => n.x != null);
  if (!ns.length) return;

  // Bounding box
  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  for (const n of ns) {
    minX = Math.min(minX, n.x); maxX = Math.max(maxX, n.x);
    minY = Math.min(minY, n.y); maxY = Math.max(maxY, n.y);
  }
  const gw = Math.max(maxX - minX, 1), gh = Math.max(maxY - minY, 1);
  const pad = cfg.pad || 16;
  const topPad = cfg.topPad ?? pad;
  const scale = Math.min((w - pad) / gw, (h - topPad - pad) / gh);
  const ox = (w - gw * scale) / 2 - minX * scale;
  const oy = topPad + (h - topPad - pad - gh * scale) / 2 - minY * scale;
  minimapState = { scale, ox, oy };

  // Edges (optional)
  if (cfg.drawEdges) {
    cfg.ctx.strokeStyle = cfg.edgeColor || 'rgba(45,51,59,0.4)';
    cfg.ctx.lineWidth   = cfg.edgeWidth || 0.5;
    for (const e of currentEdges) {
      if (!e.source || e.source.x == null || !e.target || e.target.x == null) continue;
      cfg.ctx.beginPath();
      cfg.ctx.moveTo(e.source.x * scale + ox, e.source.y * scale + oy);
      cfg.ctx.lineTo(e.target.x * scale + ox, e.target.y * scale + oy);
      cfg.ctx.stroke();
    }
  }

  // Nodes
  const rMult = cfg.radiusMult || 0.6;
  for (const n of ns) {
    cfg.ctx.beginPath();
    cfg.ctx.arc(n.x * scale + ox, n.y * scale + oy,
      Math.max(cfg.minR || 1.5, nodeR(n, currentMode) * scale * rMult), 0, Math.PI * 2);
    cfg.ctx.fillStyle = nodeC(n, currentMode);
    cfg.ctx.fill();
  }

  // Viewport rect
  if (cfg.showViewport !== false) {
    const t  = S.zoomTransform;
    const cr = document.getElementById('graph-container').getBoundingClientRect();
    const x0 = -t.x / t.k, y0 = -t.y / t.k;
    const x1 = x0 + cr.width / t.k, y1 = y0 + cr.height / t.k;
    const rx = x0 * scale + ox, ry = y0 * scale + oy,
          rw = (x1 - x0) * scale, rh = (y1 - y0) * scale;
    if (cfg.vpFill) { cfg.ctx.fillStyle = cfg.vpFill; cfg.ctx.fillRect(rx, ry, rw, rh); }
    cfg.ctx.fillStyle  = cfg.vpStroke || '#388bfd';
    cfg.ctx.strokeStyle = cfg.vpStroke || '#388bfd';
    cfg.ctx.lineWidth   = cfg.vpStrokeWidth ?? 1;
    cfg.ctx.strokeRect(rx, ry, rw, rh);
  }

  // Border
  if (cfg.border) {
    cfg.ctx.strokeStyle = cfg.border;
    cfg.ctx.lineWidth   = cfg.borderWidth ?? 1;
    cfg.ctx.strokeRect(0, 0, w, h);
  }

  // Title (optional)
  if (cfg.title) {
    cfg.ctx.fillStyle  = cfg.titleColor || '#6e7681';
    cfg.ctx.font       = cfg.titleFont || '10px system-ui';
    cfg.ctx.textAlign  = 'left';
    cfg.ctx.fillText(cfg.title, 8, 12);
  }
}

function drawOverview() {
  const panel = document.getElementById('overview-panel');
  drawMiniView({
    ctx: overviewCtx, show: S.showOverview,
    width: panel.clientWidth  || 220,
    height: panel.clientHeight || 180,
    bg: '#161b22', topPad: 16,
    drawEdges: true, edgeColor: 'rgba(45,51,59,0.4)', edgeWidth: 0.5,
    radiusMult: 0.8, minR: 2,
    showViewport: true,
    vpFill: 'rgba(56,139,253,0.06)', vpStroke: 'rgba(56,139,253,0.7)',
    title: 'Overview  (click to pan)',
  });
}

// ── Node helpers ──────────────────────────────────────────────
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
  const r = nodeR(d.target, mode), dx = d.target.x - d.source.x, dy = d.target.y - d.source.y;
  return d.target.x - (dx / (Math.hypot(dx, dy) || 1)) * (r + 8);
}
function tgtY(d, mode) {
  const r = nodeR(d.target, mode), dx = d.target.x - d.source.x, dy = d.target.y - d.source.y;
  return d.target.y - (dy / (Math.hypot(dx, dy) || 1)) * (r + 8);
}

// ── Drill-down ────────────────────────────────────────────────
function drillDown(d) {
  if (S.view === 'modules' && d.status === 'indexed' && d.projectName)
    loadClassGraph(d.projectName);
  else if (S.view === 'classes')
    loadMethodGraph(S.selectedProject, d.id);
}

// ── Node selection ────────────────────────────────────────────
function selectNode(d) {
  S.selectedNode = d;

  if (!S.useCanvas) {
    gSel.selectAll('.node circle')
      .attr('stroke',       n => n.id === d.id ? COLORS.selected
        : S.pinnedNodes.has(n.id) ? '#ffd54f' : '#0d1117')
      .attr('stroke-width', n => n.id === d.id ? 3
        : S.pinnedNodes.has(n.id) ? 2.5 : 1.5);
  }

  // Ego mode: set center and re-filter
  if (S.focusMode === 'ego') {
    S.egoCenter = d.id;
    rerender();
    return;
  }

  // Expand mode: expand clicked node
  if (S.focusMode === 'expand' && !S.expandedNodes.has(d.id)) {
    S.expandedNodes.add(d.id);
    rerender();
    return;
  }

  showDetail(d);
}

function clearHighlight() {
  if (!S.useCanvas) {
    gSel.selectAll('.node circle')
      .attr('stroke',           d => S.pinnedNodes.has(d.id) ? '#ffd54f' : '#0d1117')
      .attr('stroke-width',     d => S.pinnedNodes.has(d.id) ? 2.5 : 1.5)
      .attr('stroke-dasharray', d => S.pinnedNodes.has(d.id) ? '4,2' : null)
      .attr('fill',    d => nodeC(d, S.view))
      .attr('opacity', 1);
    gSel.selectAll('.node text').attr('opacity', 1);
  } else {
    for (const n of currentNodes) { n._opacity = 1; n._highlight = false; }
    drawCanvasFrame();
  }
}

// ── Detail panel ──────────────────────────────────────────────
async function showDetail(d) {
  document.getElementById('detail-panel').classList.add('open');
  if (S.view === 'modules') { renderModuleDetail(d); return; }
  if (S.view === 'classes') { renderClassDetail(d);  return; }

  document.getElementById('detail-content').innerHTML = '<div class="loading">Loading…</div>';
  try {
    const fqn = extractFqn(d.id);
    const res = await fetch(`/api/method?fqn=${encodeURIComponent(fqn)}`);
    if (res.ok) renderMethodDetail(await res.json(), d);
    else         renderFqnDetail(d);
  } catch (_) { renderFqnDetail(d); }
}

function renderMethodDetail(m, d) {
  const fqn = extractFqn(d.id), kind = m.docType || d.kind || 'method';
  document.getElementById('detail-content').innerHTML = `
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
      ${m.filePath ? `<button data-action="locate-file" data-filepath="${esc(m.filePath)}" data-project="${esc(m.project || '')}">Locate File</button>` : ''}
    </div>
    <div id="related-area"></div>`;
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
    <div id="related-area"></div>`;
}

function renderClassDetail(d) {
  document.getElementById('detail-content').innerHTML = `
    <div class="d-header">
      <span class="badge badge-class">class</span>
      <span class="badge badge-project">${esc(S.selectedProject)}</span>
    </div>
    <div class="d-name">${esc(d.label || d.id.split('.').pop())}</div>
    <div class="d-class">${esc(d.id)}</div>
    <div class="d-stats">
      <div><b>${(d.methodCount || 0).toLocaleString()}</b> methods</div>
      <div><b>${(d.inDegree || 0)}</b> incoming deps</div>
    </div>
    <div class="d-actions">
      <button data-action="drill-class" data-cls="${esc(d.id)}">Methods →</button>
    </div>`;
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
    </div>` : ''}`;
}

async function loadRelated(fqn, type) {
  const el = document.getElementById('related-area');
  if (!el) return;
  el.innerHTML = '<div class="loading">Loading…</div>';
  try {
    const res = await fetch(`/api/${type}?fqn=${encodeURIComponent(fqn)}`);
    renderRelatedList(el, type === 'callers' ? 'Callers' : 'Callees', await res.json());
  } catch (_) { el.innerHTML = '<div class="related-empty">Error loading</div>'; }
}

function renderRelatedList(el, title, items) {
  if (!items.length) { el.innerHTML = `<div class="related-empty">No ${title.toLowerCase()}</div>`; return; }
  const rows = items.slice(0, 25).map(r => {
    const inGraph = !!currentNodes.find(n => n.id === r.id || extractFqn(n.id) === r.id);
    const cls = inGraph ? 'related-label related-label--link' : 'related-label';
    const action = inGraph ? `data-action="focus-node" data-fqn="${esc(r.id)}"` : '';
    const hint = inGraph ? ' title="Click to focus in graph"' : `title="${esc(r.id)}"`;
    return `
    <li>
      <span class="${cls}" ${action}${hint}>${esc(r.label)}</span>
      <span class="related-file">${esc(shortPath(r.filePath))}:${r.startLine}</span>
    </li>`;
  }).join('');
  const more = items.length > 25 ? `<li class="related-more">…and ${items.length - 25} more</li>` : '';
  el.innerHTML = `
    <div class="related-header">${title} (${items.length})</div>
    <ul class="related-list">${rows}${more}</ul>`;
}

function focusNode(fqnOrId, zoomScale) {
  // Match by exact id or by the FQN embedded in id (after ':')
  const node = currentNodes.find(n => n.id === fqnOrId || extractFqn(n.id) === fqnOrId);
  if (!node || node.x == null) return false;

  // Pan + optionally zoom so the node is centred
  const cr = document.getElementById('graph-container').getBoundingClientRect();
  const k  = zoomScale != null ? zoomScale : S.zoomTransform.k;
  const newTransform = d3.zoomIdentity
    .translate(cr.width / 2 - node.x * k, cr.height / 2 - node.y * k)
    .scale(k);

  const target = S.useCanvas ? d3.select(canvasEl) : svgSel;
  target.transition().duration(500).call(zoomBehavior.transform, newTransform);

  // Select the node (shows detail, highlights circle)
  selectNode(node);
  return true;
}

function closeDetail() {
  document.getElementById('detail-panel').classList.remove('open');
  S.selectedNode = null;
  clearHighlight();
}

// ── Project list ──────────────────────────────────────────────
function renderProjectList() {
  const el = document.getElementById('project-list');
  if (!S.projects.length) { el.innerHTML = '<div class="loading">No projects indexed</div>'; return; }
  el.innerHTML = S.projects.map(p => `
    <div class="project-item${S.selectedProject === p.name ? ' active' : ''}"
         data-project="${esc(p.name)}">
      <div class="project-name">${esc(p.name)}</div>
      <div class="project-stats">${p.methodCount.toLocaleString()} methods · ${p.classCount.toLocaleString()} classes</div>
    </div>`).join('');
  el.querySelectorAll('.project-item').forEach(item =>
    item.addEventListener('click', () => browseProject(item.dataset.project)));
}

// ── File tree ─────────────────────────────────────────────────
async function initFileTree(projectName) {
  const section   = document.getElementById('file-tree-section');
  const container = document.getElementById('file-tree-root');
  if (!projectName) { section.style.display = 'none'; return; }
  section.style.display = 'block';
  container.innerHTML = '<div class="loading">Loading…</div>';

  const meta     = S.projects.find(p => p.name === projectName);
  const rootPath = meta?.rootPath || '';

  try {
    const { dirs, files } = await loadTreeLevel(projectName, rootPath);
    container.innerHTML = '';
    if (!dirs.size && !files.size) {
      container.innerHTML = '<div class="loading">No indexed files found</div>';
    } else {
      renderTreeEntries(container, projectName, rootPath, dirs, files, 0);
    }
  } catch (_) {
    container.innerHTML = '<div class="loading">Failed to load tree</div>';
  }
}

async function loadTreeLevel(project, path) {
  const params = new URLSearchParams({ project, limit: 2000 });
  if (path) params.set('path', path);
  const data = await fetch(`/api/skeleton?${params}`).then(r => r.json());
  return deriveTreeEntries(data, path);
}

function deriveTreeEntries(skeletonData, basePath) {
  const normBase = (basePath || '').replace(/\\/g, '/').replace(/\/$/, '');
  const dirs  = new Map(); // dirName  → class count
  const files = new Map(); // fileName → [qualifiedClassName, ...]

  for (const cls of skeletonData) {
    const fp  = (cls.filePath || '').replace(/\\/g, '/');
    let rel;
    if (normBase && fp.startsWith(normBase + '/')) {
      rel = fp.slice(normBase.length + 1);
    } else if (!normBase) {
      rel = fp.replace(/^\/+/, '');
    } else {
      continue;
    }

    const sep = rel.indexOf('/');
    if (sep === -1) {
      if (!files.has(rel)) files.set(rel, []);
      files.get(rel).push(cls.qualifiedClassName);
    } else {
      const dir = rel.slice(0, sep);
      dirs.set(dir, (dirs.get(dir) || 0) + 1);
    }
  }
  return { dirs, files };
}

function renderTreeEntries(container, project, basePath, dirs, files, depth) {
  const indent = depth * 14;

  for (const [name] of [...dirs.entries()].sort()) {
    const dirPath  = basePath.replace(/\/$/, '') + '/' + name;
    const row      = document.createElement('div');
    row.className  = 'ft-row ft-dir';
    row.style.paddingLeft = (12 + indent) + 'px';
    row.dataset.treepath = dirPath;
    row.innerHTML  =
      `<span class="ft-chevron">▶</span>` +
      `<span class="ft-icon">📁</span>` +
      `<span class="ft-name" title="${esc(name)}">${esc(name)}/</span>`;

    const childWrap = document.createElement('div');
    childWrap.style.display = 'none';
    let loaded = false;

    row.addEventListener('click', async () => {
      const chevron = row.querySelector('.ft-chevron');
      const isOpen  = chevron.classList.contains('open');
      if (isOpen) {
        chevron.classList.remove('open');
        childWrap.style.display = 'none';
        return;
      }
      chevron.classList.add('open');
      childWrap.style.display = 'block';
      if (loaded) return;
      loaded = true;
      childWrap.innerHTML =
        `<div class="loading" style="padding-left:${12 + indent + 14}px">Loading…</div>`;
      try {
        const { dirs: sd, files: sf } = await loadTreeLevel(project, dirPath);
        childWrap.innerHTML = '';
        if (!sd.size && !sf.size) {
          childWrap.innerHTML =
            `<div class="ft-empty" style="padding-left:${12 + indent + 14}px">Empty</div>`;
        } else {
          renderTreeEntries(childWrap, project, dirPath, sd, sf, depth + 1);
        }
      } catch (_) {
        childWrap.innerHTML =
          `<div class="loading" style="padding-left:${12 + indent + 14}px">Error</div>`;
      }
    });

    container.appendChild(row);
    container.appendChild(childWrap);
  }

  for (const [name, classes] of [...files.entries()].sort()) {
    const row     = document.createElement('div');
    row.className = 'ft-row ft-file';
    row.style.paddingLeft = (12 + indent) + 'px';
    row.innerHTML =
      `<span class="ft-chevron" style="visibility:hidden">▶</span>` +
      `<span class="ft-icon">📄</span>` +
      `<span class="ft-name" title="${esc(name)}">${esc(name)}</span>` +
      `<span class="ft-badge">${classes.length}</span>`;
    row.dataset.treepath = basePath.replace(/\/$/, '') + '/' + name;
    row.addEventListener('click', () => drillToFile(project, classes));
    container.appendChild(row);
  }
}

function drillToFile(project, qualifiedClassNames) {
  if (!qualifiedClassNames.length) return;
  if (qualifiedClassNames.length === 1) {
    loadMethodGraph(project, qualifiedClassNames[0]);
  } else {
    loadCallGraph(project, null, qualifiedClassNames[0]);
  }
}

async function locateFileInTree(filePath, projectHint) {
  const project = projectHint || S.selectedProject;
  if (!project || !filePath) return;

  const normFile  = filePath.replace(/\\/g, '/');
  const lastSlash = normFile.lastIndexOf('/');
  const dirPath   = lastSlash >= 0 ? normFile.slice(0, lastSlash) : '';

  try {
    const params = new URLSearchParams({ project, limit: 500 });
    if (dirPath) params.set('path', dirPath);
    const data = await fetch(`/api/skeleton?${params}`).then(r => r.json());

    const classNames = data
      .filter(c => (c.filePath || '').replace(/\\/g, '/') === normFile)
      .map(c => c.qualifiedClassName)
      .filter(Boolean);

    if (classNames.length > 0) {
      document.getElementById('file-tree-section').style.display = 'block';
      drillToFile(project, classNames);
    }
  } catch (_) {}
}

// ── Test/production filter ────────────────────────────────────
function renderTestFilter() {
  const el = document.getElementById('test-filter');
  if (!el) return;
  if (S.view === 'modules' || !S.selectedProject) { el.style.display = 'none'; return; }
  el.style.display = 'block';
  const opts = [
    { value: 'all',  label: 'All code' },
    { value: 'prod', label: 'Production' },
    { value: 'test', label: 'Tests only' },
  ];
  el.innerHTML = `<div class="sidebar-title">Code type</div>` +
    `<div class="lang-chips">` +
    opts.map(o =>
      `<span class="lang-chip${S.testFilter === o.value ? ' active' : ''}" data-val="${o.value}">${o.label}</span>`
    ).join('') +
    `</div>`;
  el.querySelectorAll('.lang-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      S.testFilter = chip.dataset.val;
      renderTestFilter();
      if (S.graphData) rerender();
    });
  });
}

// ── Language filter ───────────────────────────────────────────
const LANG_LABELS = {
  java: 'Java', kt: 'Kotlin', py: 'Python', js: 'JS', ts: 'TS',
  scala: 'Scala', groovy: 'Groovy', xml: 'XML', json: 'JSON',
  yaml: 'YAML', yml: 'YAML', md: 'Markdown', sql: 'SQL', other: 'Other',
};

function renderLangFilter() {
  const el = document.getElementById('lang-filter');
  if (!el) return;
  const entries = Object.entries(S.langCounts);
  if (entries.length <= 1) { el.style.display = 'none'; return; }
  el.style.display = 'block';
  el.innerHTML = `<div class="sidebar-title">Language</div>` +
    `<div class="lang-chips">` +
    `<span class="lang-chip${S.langFilter === null ? ' active' : ''}" data-lang="">All</span>` +
    entries.map(([ext, count]) => {
      const label = LANG_LABELS[ext] || ext.toUpperCase();
      const active = S.langFilter === ext ? ' active' : '';
      return `<span class="lang-chip${active}" data-lang="${esc(ext)}">${esc(label)}<span class="lang-count">${count}</span></span>`;
    }).join('') +
    `</div>`;
  el.querySelectorAll('.lang-chip').forEach(chip => {
    chip.addEventListener('click', () => {
      S.langFilter = chip.dataset.lang || null;
      renderLangFilter();
      if (S.graphData) rerender();
    });
  });
}

// ── Search & filter ───────────────────────────────────────────
function applyLocalFilter() {
  const q    = S.searchQuery;
  const hide = S.searchToFocus;

  if (S.useCanvas) {
    for (const n of currentNodes) {
      const match = !q
        || (n.id        || '').toLowerCase().includes(q)
        || (n.label     || '').toLowerCase().includes(q)
        || (n.className || '').toLowerCase().includes(q);
      n._opacity   = !q || match ? 1 : (hide ? 0 : 0.08);
      n._highlight = false;
    }
    drawCanvasFrame();
  } else {
    gSel.selectAll('.node').each(function(d) {
      const match = !q
        || (d.id        || '').toLowerCase().includes(q)
        || (d.label     || '').toLowerCase().includes(q)
        || (d.className || '').toLowerCase().includes(q);
      const op = !q || match ? 1 : (hide ? 0 : 0.12);
      d3.select(this).select('circle').attr('opacity', op);
      d3.select(this).select('text').attr('opacity',   !q || match ? 1 : (hide ? 0 : 0.08));
    });
  }
}

// ── Search results dropdown ───────────────────────────────
const srPanel = document.getElementById('search-results');
let lastHits  = [];

function closeSearchResults() { srPanel.classList.remove('open'); srPanel.innerHTML = ''; }

// ── Document viewer ───────────────────────────────────────
const docViewer = document.getElementById('doc-viewer');

function closeDocViewer() { docViewer.style.display = 'none'; }

document.getElementById('dv-close').addEventListener('click', closeDocViewer);
docViewer.addEventListener('click', e => { if (e.target === docViewer) closeDocViewer(); });
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDocViewer(); });

function openDocViewer(r) {
  const label = r.docType === 'class' || r.docType === 'document'
    ? (r.qualifiedClassName || r.id)
    : r.docType === 'chunk'
      ? (r.qualifiedClassName || '') + ' § ' + (r.methodName || '')
      : (r.qualifiedClassName || '') + '#' + (r.methodName || '');

  document.getElementById('dv-title').textContent = label;

  const docTypeCls = `dv-badge-${esc(r.docType || 'method')}`;
  const fileLine = r.filePath
    ? `${r.filePath}${r.startLine ? `:${r.startLine}–${r.endLine}` : ''}`
    : '';
  document.getElementById('dv-meta').innerHTML = `
    <span class="dv-badge dv-badge-project">${esc(r.project || '')}</span>
    <span class="dv-badge ${docTypeCls}">${esc(r.docType || 'method')}</span>
    ${r.searchType ? `<span class="dv-badge dv-badge-type">${esc(r.searchType)}</span>` : ''}
    ${r.score != null ? `<span class="dv-score">score ${r.score.toFixed(3)}</span>` : ''}
    <button class="dv-nav-btn" id="dv-nav-btn">Locate in graph →</button>
    ${fileLine ? `<div class="dv-filepath">${esc(fileLine)}</div>` : ''}
  `;

  document.getElementById('dv-nav-btn').addEventListener('click', () => {
    closeDocViewer();
    navigateToResult(r);
  });

  const isChunk = r.docType === 'chunk' || r.docType === 'document';
  const isClass = r.docType === 'class';

  const sections = [];

  // Matched excerpt (snippet) — always first when available
  if (r.snippet && r.snippet.text) {
    const lineRange = r.snippet.startLine
      ? ` · lines ${r.snippet.startLine}–${r.snippet.endLine}`
      : '';
    sections.push({
      label: `Matched Excerpt${lineRange}`,
      html: `<pre class="dv-code dv-snippet">${esc(r.snippet.text)}</pre>`
    });
  }

  if (!isChunk && (r.accessModifier || r.returnType || r.signature)) {
    const sig = [r.accessModifier, r.returnType, r.signature].filter(Boolean).join(' ');
    sections.push({ label: 'Signature', html: `<div class="dv-sig">${esc(sig)}</div>` });
  }
  if (r.javadoc) {
    const javadocLabel = isChunk ? 'Document' : 'Javadoc';
    sections.push({ label: javadocLabel, html: `<div class="dv-javadoc">${esc(r.javadoc.trim())}</div>` });
  }
  if (r.body) {
    const bodyLabel = isChunk ? 'Content' : isClass ? 'Members' : 'Body';
    const bodyHtml  = isChunk
      ? `<div class="dv-javadoc">${esc(r.body)}</div>`
      : `<pre class="dv-code">${esc(r.body)}</pre>`;
    sections.push({ label: bodyLabel, html: bodyHtml });
  }
  if (!sections.length) {
    sections.push({ label: 'Info', html: `<div class="dv-javadoc">No additional content stored for this result.</div>` });
  }

  document.getElementById('dv-sections').innerHTML = sections.map(s => `
    <div class="dv-section">
      <div class="dv-section-label">${s.label}</div>
      <div class="dv-section-body">${s.html}</div>
    </div>`).join('');

  docViewer.style.display = 'flex';
}

document.addEventListener('click', e => {
  if (!e.target.closest('.search-wrap')) closeSearchResults();
});

document.getElementById('search-input').addEventListener('keydown', e => {
  if (e.key === 'Escape') { closeSearchResults(); e.target.blur(); }
});

/** Highlight query terms in text (simple token matching). */
function highlightTerms(text, query) {
  if (!text) return '';
  const tokens = query.trim().split(/\s+/).filter(t => t.length > 1)
    .map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  if (!tokens.length) return esc(text);
  const re = new RegExp(`(${tokens.join('|')})`, 'gi');
  return esc(text).replace(re, '<span class="sr-hit">$1</span>');
}

async function doSearch() {
  const q = document.getElementById('search-input').value.trim();
  if (!q) { applyLocalFilter(); closeSearchResults(); return; }

  const proj     = S.selectedProject ? `&project=${encodeURIComponent(S.selectedProject)}` : '';
  const pipeline = document.getElementById('pipeline-select').value;
  const pipelineParam = pipeline ? `&pipeline=${encodeURIComponent(pipeline)}` : '';
  let hits;
  try {
    const res = await fetch(`/api/search?q=${encodeURIComponent(q)}${proj}${pipelineParam}&limit=30`);
    if (!res.ok) {
      const msg = await res.text().catch(() => res.statusText);
      throw new Error(`Search failed (${res.status}): ${msg}`);
    }
    hits = await res.json();
    lastHits = hits;
  } catch (err) {
    srPanel.innerHTML = `<div class="sr-none">Search error: ${esc(err.message)}</div>`;
    srPanel.classList.add('open');
    setStatus('Search error');
    console.error('doSearch error:', err);
    return;
  }

  // Highlight matching nodes in the current graph
  const hitIds = new Set(hits.map(r => r.id));
  const hide   = S.searchToFocus;
  if (S.useCanvas) {
    for (const n of currentNodes) {
      const match  = hitIds.has(n.id);
      n._highlight = match;
      n._opacity   = match ? 1 : (hide ? 0 : 0.15);
    }
    drawCanvasFrame();
  } else {
    gSel.selectAll('.node').each(function(d) {
      const match = hitIds.has(d.id);
      d3.select(this).select('circle')
        .attr('fill',    match ? COLORS.highlight : nodeC(d, S.view))
        .attr('opacity', match ? 1 : (hide ? 0 : 0.15));
      d3.select(this).select('text').attr('opacity', match ? 1 : (hide ? 0 : 0.1));
    });
  }

  // Render results dropdown
  if (!hits.length) {
    srPanel.innerHTML = '<div class="sr-none">No results found</div>';
  } else {
    srPanel.innerHTML = hits.map((r, i) => {
      const method   = r.label || r.id || '';
      const cls      = r.className || '';
      const sig      = r.signature || '';
      const snip     = (r.javadoc || '').replace(/\s+/g, ' ').slice(0, 200);
      const project  = r.project || '';
      const docType  = r.docType || 'method';
      const srchType = r.searchType || '';
      const badge    = `<span class="sr-badge sr-badge-${esc(docType)}">${esc(docType)}</span>`;
      const typeBadge = srchType && srchType !== 'hybrid' && srchType !== 'related'
        ? `<span class="sr-badge sr-badge-type">${esc(srchType)}</span>` : '';
      return `<div class="sr-item" data-idx="${i}" data-fqn="${esc(extractFqn(r.id))}"
                   data-project="${esc(project)}" data-class="${esc(cls)}"
                   data-doc-type="${esc(docType)}">
        <div class="sr-method">${highlightTerms(method, q)}${badge}${typeBadge}<span class="sr-project">${esc(project)}</span></div>
        ${cls  ? `<div class="sr-class">${esc(cls)}</div>` : ''}
        ${sig  ? `<div class="sr-sig">${highlightTerms(sig, q)}</div>` : ''}
        ${snip ? `<div class="sr-snip">${highlightTerms(snip, q)}</div>` : ''}
      </div>`;
    }).join('');
  }
  srPanel.classList.add('open');
  setStatus(`${hits.length} search hits`);
}

// Navigate the graph to a search result (used by doc viewer "Locate in graph" button)
async function navigateToResult(r) {
  const fqn     = extractFqn(r.id);
  const project = r.project;
  const cls     = r.className;
  const docType = r.docType;

  if (!fqn || !project) return;

  const ZOOM_SCALE = 2.0;
  const tryFocus = (fqn, attempts) => {
    if (focusNode(fqn, ZOOM_SCALE)) return;
    if (attempts < 8) setTimeout(() => tryFocus(fqn, attempts + 1), 250);
  };

  if (docType === 'class') {
    if (S.view !== 'classes' || S.selectedProject !== project) {
      await loadClassGraph(project);
      setTimeout(() => tryFocus(fqn, 0), 400);
    } else {
      tryFocus(fqn, 0);
    }
  } else {
    if (S.view !== 'call' || S.selectedProject !== project) {
      await loadCallGraph(project, null, cls || null);
    }
    showDetail({ id: fqn, label: fqn.split('#').pop(), kind: docType || 'method' });
    setTimeout(() => tryFocus(fqn, 0), 200);
  }
}

// Click on a search result → open document viewer
srPanel.addEventListener('click', e => {
  const item = e.target.closest('.sr-item');
  if (!item) return;
  closeSearchResults();

  const idx = parseInt(item.dataset.idx, 10);
  const hit = lastHits[idx];
  if (hit) openDocViewer(hit);
});

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
  tip.style.left    = (event.pageX + 14) + 'px';
  tip.style.top     = (event.pageY - 12) + 'px';
  const drillHint = (S.view === 'modules' && d.status === 'indexed') || S.view === 'classes'
    ? '<small style="color:#ffd54f">double-click to drill down</small>' : '';
  const pinHint = S.pinnedNodes.has(d.id)
    ? '<small style="color:#ffd54f"> · pinned</small>' : '';
  tip.innerHTML = `<b>${esc(d.label || shortId(d.id))}</b><small>${esc(d.id)}</small>${drillHint}${pinHint}`;
}
function hideTooltip() { document.getElementById('tooltip').style.display = 'none'; }

// ── Utilities ─────────────────────────────────────────────────
function setStatus(txt)   { document.getElementById('status-bar').textContent = txt; }
function setActiveBtn(id) {
  document.querySelectorAll('.btn-view').forEach(b => b.classList.remove('active'));
  if (id) { const el = document.getElementById(id); if (el) el.classList.add('active'); }
  // Restore toggle-buttons that track independent state
  document.getElementById('btn-hide-external').classList.toggle('active', S.hideExternal);
}
function trunc(s, n)    { return s && s.length > n ? s.slice(0, n) + '…' : (s || ''); }
function shortId(id)    { return id ? id.split(/[.#]/).pop() : id; }
function shortPath(p)   { return p ? p.replace(/^.*?\/src\//, 'src/') : (p || ''); }
function extractFqn(id) { const c = id.indexOf(':'); return c >= 0 ? id.substring(c + 1) : id; }
// Returns true if the node looks like a test class/method.
// Checks file path segments (/src/test/, /test-framework/, /testFixtures/, etc.)
// and class name heuristics (starts or ends with Test/Tests/Spec/IT).
function isTestNode(n) {
  if (n.filePath) {
    const p = n.filePath.replace(/\\/g, '/');
    if (/\/src\/test\//.test(p))        return true;
    if (/\/src\/testFixtures\//.test(p)) return true;
    if (/\/test-framework\//.test(p))   return true;
    if (/\/test\/java\//.test(p))       return true;
    if (/\/it\/java\//.test(p))         return true;  // integration tests
  }
  const cls = n.className || (n.id ? n.id.split('#')[0].split('.').pop() : '');
  if (/^Test[A-Z]/.test(cls))           return true;
  if (/Tests?$/.test(cls))              return true;
  if (/IT$/.test(cls))                  return true;  // integration test
  if (/Spec$/.test(cls))                return true;
  return false;
}

function extension(fp) {
  if (!fp) return 'other';
  const dot = fp.lastIndexOf('.');
  return dot >= 0 && dot < fp.length - 1 ? fp.slice(dot + 1).toLowerCase() : 'other';
}

function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
