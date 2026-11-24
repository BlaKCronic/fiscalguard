'use strict';
const $ = (s) => document.querySelector(s);
const h = (t, p = {}, ...k) => { const e = document.createElement(t);
  for (const [a, v] of Object.entries(p)) { if (a === 'class') e.className = v; else if (a.startsWith('on')) e.addEventListener(a.slice(2), v); else if (v != null && v !== false) e.setAttribute(a, v === true ? '' : v); }
  e.append(...k.filter((x) => x != null && x !== false)); return e; };
const money = (n) => '$' + new Intl.NumberFormat('es-MX', { notation: 'compact', maximumFractionDigits: 1 }).format(n || 0);
const num = (n) => new Intl.NumberFormat('es-MX').format(n || 0);
const fdate = (d) => (d ? new Date(d).toLocaleString('es-MX', { dateStyle: 'short', timeStyle: 'short' }) : '');
const pill = (n) => h('span', { class: 'pill ' + n }, n);
const masked = (r) => String(r).includes('*');
let S = JSON.parse(sessionStorage.getItem('fg') || 'null');
const charts = {};
if (window.matchMedia('(prefers-reduced-motion: reduce)').matches && window.Chart) Chart.defaults.animation = false;

async function api(path, o = {}) {
  const r = await fetch(path, { method: o.method || 'GET', headers: { 'content-type': 'application/json', ...(S && { authorization: 'Bearer ' + S.token }) }, body: o.body && JSON.stringify(o.body) });
  if (r.status === 401 && S) { logout(); throw new Error('Tu sesión expiró. Inicia sesión de nuevo.'); }
  const d = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(d.error || 'Error ' + r.status);
  return d;
}
const say = (m) => { $('#msg').textContent = m || ''; };
const guard = (fn) => (...a) => fn(...a).catch((e) => say(e.message));

function logout() { sessionStorage.removeItem('fg'); S = null; location.reload(); }
$('#lf').addEventListener('submit', async (e) => {
  e.preventDefault(); $('#le').textContent = '';
  try { S = await api('/auth/login', { method: 'POST', body: { usuario: $('#u').value, password: $('#p').value } }); sessionStorage.setItem('fg', JSON.stringify(S)); start(); }
  catch (er) { $('#le').textContent = er.message; }
});
$('#out').addEventListener('click', logout);
document.querySelectorAll('nav button').forEach((b) => b.addEventListener('click', () => view(b.dataset.v)));

function view(v) {
  say(''); $('#panel').hidden = true;
  document.querySelectorAll('nav button').forEach((b) => b.classList.toggle('on', b.dataset.v === v));
  $('#vt').textContent = { resumen: 'Resumen', alertas: 'Alertas', contrib: 'Contribuyente y red' }[v];
  for (const s of ['resumen', 'alertas', 'contrib']) $('#' + s).hidden = s !== v;
  if (v === 'resumen') guard(resumen)(); if (v === 'alertas') guard(alertas)(); if (v === 'contrib' && !$('#contrib').children.length) contribUI();
}
function start() {
  $('#login').hidden = true; $('#app').hidden = false; $('#who').textContent = S.usuario + ' · ' + S.rol; $('.av').textContent = String(S.usuario || '?').slice(0, 1).toUpperCase(); view('resumen');
}
function openRfc(r) { if (masked(r)) return say('Tu rol ve los RFC enmascarados; para consultar uno, escríbelo completo en la pestaña Contribuyente y red.'); view('contrib'); $('#rq').value = r; guard(buscar)(r); }

/* ---------- Resumen ---------- */
const GRID = { 'Baja California': [0, 0, 'BC'], 'Sonora': [1, 0, 'SON'], 'Chihuahua': [2, 0, 'CHH'], 'Coahuila': [3, 0, 'COA'], 'Nuevo León': [4, 0, 'NL'], 'Tamaulipas': [5, 0, 'TAM'],
  'Baja California Sur': [0, 1, 'BCS'], 'Sinaloa': [1, 1, 'SIN'], 'Durango': [2, 1, 'DUR'], 'Zacatecas': [3, 1, 'ZAC'], 'San Luis Potosí': [4, 1, 'SLP'], 'Veracruz': [5, 1, 'VER'],
  'Nayarit': [1, 2, 'NAY'], 'Jalisco': [2, 2, 'JAL'], 'Aguascalientes': [3, 2, 'AGS'], 'Guanajuato': [4, 2, 'GTO'], 'Querétaro': [5, 2, 'QRO'], 'Hidalgo': [6, 2, 'HGO'],
  'Tabasco': [7, 2, 'TAB'], 'Campeche': [8, 2, 'CAM'], 'Yucatán': [9, 2, 'YUC'], 'Quintana Roo': [10, 2, 'ROO'], 'Colima': [1, 3, 'COL'], 'Michoacán': [2, 3, 'MIC'],
  'México': [3, 3, 'MEX'], 'Ciudad de México': [4, 3, 'CDMX'], 'Tlaxcala': [5, 3, 'TLA'], 'Puebla': [6, 3, 'PUE'], 'Oaxaca': [7, 3, 'OAX'], 'Chiapas': [8, 3, 'CHP'],
  'Guerrero': [2, 4, 'GRO'], 'Morelos': [3, 4, 'MOR'] };

function chart(id, cfg) { if (charts[id]) charts[id].destroy(); charts[id] = new Chart($('#' + id), cfg); }

const C = { blue: '#2F6BFF', blue2: '#7FA2FF', blue3: '#C9D8FF', green: '#2BC48A', coral: '#FF6B6B', amber: '#FFB547', ink: '#2B2F3A', mute: '#9AA3B5', grid: '#EEF1F7' };
const EST_COL = { nueva: C.blue, en_revision: C.blue2, revisada: C.green, escalada: C.coral, descartada: C.blue3 };
const pct = (a, b) => (b ? (a / b) * 100 : 0);
const box = (id, title, sub, cls) => h('div', { class: 'box' + (cls ? ' ' + cls : '') }, h('div', { class: 'bh' }, h('h2', {}, title)), sub && h('p', { class: 'sub' }, sub), h('div', { class: 'cw' }, h('canvas', { id })));
const noLegend = { legend: { display: false } };
const legendB = { legend: { position: 'bottom', labels: { boxWidth: 10, boxHeight: 10, usePointStyle: true, pointStyle: 'rectRounded' } } };
if (window.Chart) {
  Object.assign(Chart.defaults, { maintainAspectRatio: false, color: C.mute, borderColor: C.grid });
  Chart.defaults.font.family = '"Segoe UI Variable","Segoe UI",system-ui,sans-serif';
  Chart.defaults.scale.grid.color = C.grid; Chart.defaults.scale.border.display = false;
  Chart.defaults.elements.bar.borderRadius = 6; Chart.defaults.elements.bar.borderSkipped = false;
  Chart.defaults.plugins.tooltip.backgroundColor = '#fff'; Chart.defaults.plugins.tooltip.titleColor = C.ink; Chart.defaults.plugins.tooltip.bodyColor = C.ink;
  Chart.defaults.plugins.tooltip.borderColor = C.grid; Chart.defaults.plugins.tooltip.borderWidth = 1; Chart.defaults.plugins.tooltip.padding = 10;
}
const grad = (c1, c2) => (ctx) => { const a = ctx.chart.chartArea; if (!a) return c2; const g = ctx.chart.ctx.createLinearGradient(0, a.top, 0, a.bottom); g.addColorStop(0, c1); g.addColorStop(1, c2); return g; };
// texto al centro de una dona / medidor
const centro = { id: 'centro', afterDraw(ch, _a, o) { if (!o || !o.text) return; const { ctx, chartArea: a } = ch; const x = (a.left + a.right) / 2, y = o.y === 'bottom' ? a.bottom - 8 : (a.top + a.bottom) / 2;
  ctx.save(); ctx.textAlign = 'center'; ctx.fillStyle = C.ink; ctx.font = '300 26px "Segoe UI",system-ui'; ctx.fillText(o.text, x, y); if (o.sub) { ctx.font = '12px "Segoe UI",system-ui'; ctx.fillStyle = C.mute; ctx.fillText(o.sub, x, y + 18); } ctx.restore(); } };
const CORTO = { 'Montos redondos frecuentes': 'Montos redondos', 'Factura conceptos ajenos a su sector': 'Conceptos ajenos', 'Contribuyente reciente': 'Recién creado',
  'Empezó a facturar poco después de su alta': 'Recién creado', 'Factura mucho a pocos clientes': 'Pocos clientes', 'Montos que se desvían de la Ley de Benford': 'Benford' };
const corto = (f) => CORTO[f] || (f.startsWith('Opera con') ? 'Contrapartes 69-B' : f.startsWith('Participa en') ? 'Fact. circular' : /grupo atípico/i.test(f) ? 'Grupo atípico' : f);

const RV = { anio: '', vista: 'mensual', anios: [] };
async function resumen() {
  const q = RV.anio ? `?desde=${RV.anio}-01&hasta=${RV.anio}-12` : '';
  const k = await api('/kpis' + q), r = k.resumen, n = r.contribuyentes_por_nivel || {}, root = $('#resumen'), pf = k.perfil;
  const pr = r.total_facturado ? (r.total_en_riesgo / r.total_facturado) * 100 : 0;
  if (!RV.anio) RV.anios = [...new Set(k.tendencia.map((x) => String(x.clave).slice(0, 4)))];
  const seg = (lab, val, on, fn) => h('button', { class: 'seg' + (on ? ' on' : ''), onclick: fn }, lab);
  const top = h('div', { class: 'top' },
    h('div', {}, h('p', { class: 'eyebrow' }, RV.anio ? `Año ${RV.anio}` : `${RV.anios[0] || ''} – ${RV.anios[RV.anios.length - 1] || ''}`),
      h('div', { class: 'hero' }, h('b', {}, money(r.total_facturado)), h('span', {}, 'facturado')), h('h1', {}, 'Panorama de riesgo fiscal')),
    h('div', { class: 'ctrl' },
      h('select', { id: 'fa', 'aria-label': 'Periodo', onchange: (e) => { RV.anio = e.target.value; if (RV.anio) RV.vista = 'mensual'; guard(resumen)(); } },
        h('option', { value: '' }, 'Todo el periodo'), ...RV.anios.map((y) => h('option', { value: y, selected: y === RV.anio }, y))),
      h('div', { class: 'segs' }, seg('Mes', 'mensual', RV.vista === 'mensual' || RV.anio, () => { RV.vista = 'mensual'; guard(resumen)(); }),
        seg('Año', 'anual', RV.vista === 'anual' && !RV.anio, () => { if (!RV.anio) { RV.vista = 'anual'; guard(resumen)(); } }))));
  const kp = (l, v, s, cls) => h('div', { class: 'kpi' }, h('small', {}, l), h('b', {}, v), s && h('i', { class: cls || '' }, s));
  root.replaceChildren(top,
    h('div', { class: 'kpis' },
      h('div', { class: 'kpi gauge' }, h('small', {}, 'En riesgo'), h('div', { class: 'gw' }, h('canvas', { id: 'g0' }))),
      kp('Monto en riesgo', money(r.total_en_riesgo), pr.toFixed(1) + ' % del total', 'up'),
      kp('Riesgo alto', num(n.alto), num(n.medio) + ' en riesgo medio'),
      kp('Alertas nuevas', num(r.alertas_nuevas), 'pendientes de revisar', 'up'),
      kp('Redes circulares', num(r.redes), 'detectadas en el grafo')),
    h('div', { class: 'grid' },
      box('c1', (RV.vista === 'anual' && !RV.anio ? 'Facturación anual' : 'Facturación mensual') + ' y participación en riesgo', 'Área: monto facturado. Línea punteada: % emitido por contribuyentes en riesgo (eje derecho).', 'wide2'),
      pf ? box('c6', '¿Ya estaban en la 69-B?', 'Contribuyentes de riesgo alto y medio.', 'dn') : h('div'),
      pf && box('c4', 'Puntaje de riesgo', 'Contribuyentes por rango de puntaje (0-100).'),
      pf && box('c5', 'Factores de riesgo', 'Cuántos contribuyentes presentan cada señal.'),
      pf && box('c3', 'Alertas por tipo y estatus', 'Avance de los analistas.'),
      box('c2', 'Riesgo por sector', '% del facturado del sector emitido por contribuyentes en riesgo.'),
      h('div', { class: 'box' }, h('div', { class: 'bh' }, h('h2', {}, 'Riesgo por entidad')), h('p', { class: 'sub' }, 'Más intenso = más monto en riesgo.'), h('div', { class: 'tiles', id: 'tiles' })),
      pf && box('c7', 'Antigüedad', 'Años desde su alta en el padrón.')),
    h('div', { class: 'box tbox' }, h('div', { class: 'bh' }, h('h2', {}, 'Los 20 contribuyentes con mayor riesgo')), h('div', { class: 'tw', id: 't20' })));

  // medidor (% en riesgo)
  const gmax = Math.max(20, Math.ceil(pr / 10) * 10);
  chart('g0', { type: 'doughnut', data: { datasets: [{ data: [pr, gmax - pr], backgroundColor: [C.blue, C.grid], borderWidth: 0, borderRadius: 6 }] },
    options: { rotation: -90, circumference: 180, cutout: '78%', plugins: { ...noLegend, tooltip: { enabled: false }, centro: { text: pr.toFixed(1) + '%', y: 'bottom' } } }, plugins: [centro] });

  // 1. tendencia
  let t = k.tendencia;
  if (RV.vista === 'anual' && !RV.anio) {
    const m = {}; for (const x of t) { const y = String(x.clave).slice(0, 4); const e = (m[y] = m[y] || { clave: y, total_facturado: 0, total_en_riesgo: 0 }); e.total_facturado += x.total_facturado; e.total_en_riesgo += x.total_en_riesgo; }
    t = Object.values(m);
  }
  const porc = t.map((x) => +pct(x.total_en_riesgo, x.total_facturado).toFixed(1));
  chart('c1', { type: 'line', data: { labels: t.map((x) => x.clave), datasets: [
    { label: 'Facturado', data: t.map((x) => x.total_facturado), yAxisID: 'y', borderColor: C.blue, borderWidth: 2, fill: true,
      backgroundColor: grad('rgba(47,107,255,.28)', 'rgba(47,107,255,0)'), tension: .35, pointRadius: t.length > 40 ? 0 : 3, pointBackgroundColor: '#fff', pointBorderColor: C.blue, pointHoverRadius: 5 },
    { label: '% en riesgo', data: porc, yAxisID: 'y1', borderColor: C.green, borderWidth: 2, borderDash: [5, 4], tension: .35, pointRadius: 0, pointHoverRadius: 4 }] },
    options: { interaction: { mode: 'index', intersect: false },
      scales: { x: { grid: { display: false }, ticks: { maxRotation: 0, autoSkipPadding: 16 } }, y: { ticks: { callback: (v) => money(v) } },
        y1: { position: 'right', min: 0, suggestedMax: Math.max(10, Math.ceil(Math.max(...porc, 0) / 5) * 5 + 5), grid: { drawOnChartArea: false }, ticks: { callback: (v) => v + '%' } } },
      plugins: { ...legendB, legend: { ...legendB.legend, position: 'top', align: 'end' },
        tooltip: { callbacks: { label: (c) => c.datasetIndex ? ` En riesgo: ${money(t[c.dataIndex].total_en_riesgo)} (${c.parsed.y}%)` : ` Facturado: ${money(c.parsed.y)}` } } } } });

  // 2. sector
  const ps = [...k.por_sector].map((x) => ({ ...x, p: pct(x.total_en_riesgo, x.total_facturado) })).sort((a, b) => b.p - a.p);
  chart('c2', { type: 'bar', data: { labels: ps.map((x) => x.clave), datasets: [{ data: ps.map((x) => +x.p.toFixed(1)), backgroundColor: ps.map((_, i) => (i ? C.blue3 : C.blue)), maxBarThickness: 14 }] },
    options: { indexAxis: 'y', plugins: { ...noLegend, tooltip: { callbacks: { label: (c) => ` ${c.parsed.x}% · ${money(ps[c.dataIndex].total_en_riesgo)} de ${money(ps[c.dataIndex].total_facturado)}` } } },
      scales: { x: { ticks: { callback: (v) => v + '%' } }, y: { grid: { display: false }, ticks: { autoSkip: false } } } } });

  // mapa
  const ent = Object.fromEntries(k.por_entidad.map((x) => [x.clave, x])), cnt = Object.fromEntries(k.contribuyentes_por_estado.map((x) => [x.estado, x]));
  const max = Math.max(1, ...k.por_entidad.map((x) => x.total_en_riesgo)), tiles = $('#tiles');
  tiles.style.gridTemplateRows = 'repeat(5,1fr)';
  for (const [name, [c, rw, ab]] of Object.entries(GRID)) {
    const e = ent[name] || {}, qn = cnt[name] || {}, a = (e.total_en_riesgo || 0) / max;
    const d = h('div', { title: `${name}\nEn riesgo: ${money(e.total_en_riesgo)} de ${money(e.total_facturado)}\nAlto: ${qn.alto || 0} · Medio: ${qn.medio || 0}` }, ab);
    d.style.gridColumn = c + 1; d.style.gridRow = rw + 1;
    if (a > 0) { d.style.background = `rgba(47,107,255,${(0.1 + a * 0.9).toFixed(2)})`; d.style.color = a > .45 ? '#fff' : C.ink; }
    tiles.append(d);
  }

  if (pf) {
    const at = pf.alertas_por_tipo;
    chart('c3', { type: 'bar', data: { labels: at.map((x) => TIPOS[x.tipo] || x.tipo),
      datasets: Object.keys(ESTS).map((e) => ({ label: ESTS[e], data: at.map((x) => x[e] || 0), backgroundColor: EST_COL[e], maxBarThickness: 18, borderRadius: 4 })) },
      options: { indexAxis: 'y', scales: { x: { stacked: true }, y: { stacked: true, grid: { display: false } } }, plugins: legendB } });
    chart('c4', { type: 'bar', data: { labels: pf.puntaje.map((x) => x.rango), datasets: [
      { label: 'Alto', data: pf.puntaje.map((x) => x.alto), backgroundColor: C.blue, maxBarThickness: 16 },
      { label: 'Medio', data: pf.puntaje.map((x) => x.medio), backgroundColor: C.blue3, maxBarThickness: 16 }] },
      options: { scales: { x: { stacked: true, grid: { display: false } }, y: { stacked: true } }, plugins: legendB } });
    const fm = {}; for (const f of pf.factores) fm[corto(f.clave)] = (fm[corto(f.clave)] || 0) + f.n;
    const fx = Object.entries(fm).map(([clave, v]) => ({ clave, n: v })).sort((a, b) => b.n - a.n).slice(0, 8);
    chart('c5', { type: 'radar', data: { labels: fx.map((x) => x.clave), datasets: [{ data: fx.map((x) => x.n), borderColor: C.blue, borderWidth: 2,
      backgroundColor: 'rgba(47,107,255,.22)', pointBackgroundColor: C.blue, pointRadius: 3 }] },
      options: { plugins: noLegend, scales: { r: { beginAtZero: true, grid: { color: C.grid }, angleLines: { color: C.grid }, ticks: { display: false }, pointLabels: { color: C.ink, font: { size: 11 } } } } } });
    const l = pf.lista_69b, nuevos = l.alto.nuevos + l.medio.nuevos, ya = l.alto.en_69b + l.medio.en_69b;
    chart('c6', { type: 'doughnut', data: { labels: ['Ya en la lista 69-B', 'Nuevos candidatos'], datasets: [{ data: [ya, nuevos], backgroundColor: [C.blue, C.green], borderWidth: 0, spacing: 3, borderRadius: 6 }] },
      options: { cutout: '72%', plugins: { ...legendB, centro: { text: num(nuevos), sub: 'nuevos' } } }, plugins: [centro] });
    const am = Math.max(...pf.antiguedad.map((x) => x.n));
    chart('c7', { type: 'bar', data: { labels: pf.antiguedad.map((x) => x.clave), datasets: [{ data: pf.antiguedad.map((x) => x.n), backgroundColor: pf.antiguedad.map((x) => (x.n === am ? C.blue : C.blue3)), maxBarThickness: 22 }] },
      options: { plugins: noLegend, scales: { x: { grid: { display: false } } } } });
  }

  $('#t20').replaceChildren(tabla(['RFC', 'Razón social', 'Entidad', 'Actividad', 'Puntaje', 'Nivel', 'Emitido'],
    k.top20.map((c) => ({ r: c.rfc, cells: [c.rfc, c.razon_social, c.estado, c.actividad, h('span', { class: 'n' }, c.puntaje_riesgo), pill(c.nivel_riesgo), money(c.monto_emitido)] })), [4, 6]));
}
function tabla(cols, rows, numCols = []) {
  const t = h('table', {}, h('thead', {}, h('tr', {}, ...cols.map((c, i) => h('th', { class: numCols.includes(i) ? 'n' : '' }, c)))), h('tbody'));
  for (const r of rows) t.tBodies[0].append(h('tr', { class: 'cl', tabindex: 0, onclick: r.fn || (() => openRfc(r.r)), onkeydown: (e) => { if (e.key === 'Enter') (r.fn || (() => openRfc(r.r)))(); } },
    ...r.cells.map((c, i) => h('td', { class: numCols.includes(i) ? 'n' : '' }, c))));
  if (!rows.length) t.tBodies[0].append(h('tr', {}, h('td', { colspan: cols.length, class: 'mut' }, 'Sin resultados.')));
  return t;
}

/* ---------- Alertas ---------- */
const F = { tipo: '', estatus: '', min: '', page: 1 };
const TIPOS = { clasificador: 'Clasificador', red_circular: 'Red circular', streaming: 'Streaming (en vivo)', anomalia: 'Anomalía' };
const ESTS = { nueva: 'Nueva', en_revision: 'En revisión', revisada: 'Revisada', descartada: 'Descartada', escalada: 'Escalada' };
const sel = (id, label, opts, val) => h('label', {}, label, h('select', { id }, h('option', { value: '' }, 'Todos'), ...Object.entries(opts).map(([k, v]) => h('option', { value: k, selected: k === val }, v))));
async function alertas() {
  const q = new URLSearchParams({ page: F.page, limit: 15 }); for (const k of ['tipo', 'estatus']) if (F[k]) q.set(k, F[k]); if (F.min) q.set('min_puntaje', F.min);
  const d = await api('/alertas?' + q), root = $('#alertas'), pages = Math.max(1, Math.ceil(d.total / d.limit));
  root.replaceChildren(
    h('div', { class: 'bar' }, sel('ft', 'Tipo', TIPOS, F.tipo), sel('fe', 'Estatus', ESTS, F.estatus),
      h('label', {}, 'Puntaje mínimo', h('input', { id: 'fm', type: 'number', min: 0, max: 100, value: F.min })),
      h('button', { onclick: () => { F.tipo = $('#ft').value; F.estatus = $('#fe').value; F.min = $('#fm').value; F.page = 1; guard(alertas)(); } }, 'Filtrar')),
    h('div', { class: 'tw' }, tabla(['Tipo', 'RFC', 'Puntaje', 'Descripción', 'Estatus', 'Creada'], d.items.map((a) => ({ fn: () => panel(a),
      cells: [TIPOS[a.tipo] || a.tipo, a.rfc, a.puntaje, a.descripcion, ESTS[a.estatus] || a.estatus, fdate(a.fecha_creacion)] })), [2])),
    h('div', { class: 'pg' }, h('button', { class: 'ghost', disabled: F.page <= 1, onclick: () => { F.page--; guard(alertas)(); } }, 'Anterior'),
      h('span', {}, `Página ${d.page} de ${pages} · ${num(d.total)} alertas`), h('button', { class: 'ghost', disabled: F.page >= pages, onclick: () => { F.page++; guard(alertas)(); } }, 'Siguiente')));
}
function panel(a) {
  const edit = S.rol !== 'auditor', p = $('#panel');
  p.replaceChildren(
    h('button', { class: 'ghost', onclick: () => { p.hidden = true; } }, 'Cerrar'), h('h2', { style: 'margin-top:1rem' }, (TIPOS[a.tipo] || a.tipo) + ' · puntaje ' + a.puntaje),
    h('p', {}, a.descripcion), h('p', { class: 'mut' }, 'RFC ' + a.rfc + ' · Analista: ' + (a.analista_asignado || 'sin asignar')),
    h('button', { class: 'ghost', onclick: () => openRfc(a.rfc) }, 'Ver contribuyente y red'),
    h('h3', { style: 'margin-top:1.2rem' }, 'Comentarios'), ...(a.comentarios || []).map((c) => h('div', { class: 'c' }, h('b', {}, c.usuario), ' ', h('span', { class: 'mut' }, fdate(c.fecha)), h('div', {}, c.texto))),
    !(a.comentarios || []).length && h('p', { class: 'mut' }, 'Aún no hay comentarios.'),
    edit ? h('div', {}, sel('pe', 'Estatus', ESTS, a.estatus), h('label', {}, 'Analista asignado', h('input', { id: 'pa', value: a.analista_asignado || S.usuario, maxlength: 60 })),
      h('label', {}, 'Agregar comentario', h('textarea', { id: 'pc', rows: 3, maxlength: 1000 })),
      h('p', {}, h('button', { onclick: guard(async () => {
        const b = { estatus: $('#pe').value || a.estatus, analista_asignado: $('#pa').value }; if ($('#pc').value.trim()) b.comentario = $('#pc').value.trim();
        const u = await api('/alertas/' + a.id_alerta, { method: 'PATCH', body: b }); say('Alerta actualizada.'); panel(u); alertas();
      }) }, 'Guardar cambios'))) : h('p', { class: 'mut' }, 'Tu rol (auditor) puede consultar alertas pero no modificarlas.'));
  p.hidden = false;
}

/* ---------- Contribuyente y red ---------- */
function contribUI() {
  $('#contrib').replaceChildren(h('div', { class: 'bar' }, h('label', {}, 'RFC', h('input', { id: 'rq', placeholder: 'Ej. ABC240101XY1', maxlength: 13 })),
    h('button', { onclick: () => guard(buscar)($('#rq').value.trim().toUpperCase()) }, 'Buscar')), h('div', { id: 'cres' }));
  $('#rq').addEventListener('keydown', (e) => { if (e.key === 'Enter') guard(buscar)($('#rq').value.trim().toUpperCase()); });
}
async function buscar(rfc) {
  say(''); const c = await api('/contribuyentes/' + encodeURIComponent(rfc)), box = $('#cres');
  box.replaceChildren(h('div', { class: 'box' }, h('h2', {}, c.razon_social + ' ', pill(c.nivel_riesgo)),
    h('p', { class: 'mut' }, `${c.rfc} · ${c.estado} · ${c.actividad} · puntaje ${c.puntaje_riesgo} · emitió ${money(c.monto_emitido)}${c.en_lista_69b ? ' · en lista 69-B' : ''}`),
    h('h3', {}, 'Por qué se marcó'), h('ul', { class: 'f' }, ...(c.factores_riesgo.length ? c.factores_riesgo : ['Sin factores destacados']).map((f) => h('li', {}, f))),
    h('p', { class: 'mut' }, `${c.alertas.length} alerta(s) asociada(s).`)), h('h2', { style: 'margin:1.2rem 0 .6rem' }, 'Red de facturación'), h('div', { id: 'rinfo' }), h('div', { id: 'net' }));
  try {
    const r = await api('/red/' + encodeURIComponent(rfc)), col = { alto: '#FF6B6B', medio: '#FFB547' };
    $('#rinfo').replaceChildren(h('p', { class: 'mut' }, `${r.numero_nodos} contribuyentes, ${r.numero_ciclos} ciclo(s), ${money(r.monto_total)} entre ellos. Las flechas van de quien emite a quien recibe.`));
    new vis.Network($('#net'), { nodes: new vis.DataSet(r.nodes.map((n) => ({ id: n.rfc, label: n.rfc, color: { background: col[n.nivel_riesgo] || '#C9D8FF', border: '#1b2a41' }, borderWidth: n.rfc === r.consultado ? 4 : 1, font: { color: '#fff', size: 11 } }))),
      edges: new vis.DataSet(r.edges.map((e) => ({ from: e.from, to: e.to, arrows: 'to', color: '#9AA3B5' }))) },
      { nodes: { shape: 'box', margin: 6 }, physics: { stabilization: { iterations: 150 }, barnesHut: { springLength: 140 } }, interaction: { hover: true } });
  } catch (e) { $('#rinfo').replaceChildren(h('p', { class: 'mut' }, e.message)); $('#net').remove(); }
}
if (S) start();
