'use strict';
// FiscalGuard - Fase 5: medición de tiempos de respuesta de la API.
//
// Con la API corriendo (npm start), en otra terminal dentro de la carpeta api:
//     $env:BENCH_USER="analista"; $env:BENCH_PASS="<contraseña>"; npm run bench
//   opcionales: BENCH_URL (def. http://localhost:3000), BENCH_N (peticiones por endpoint, def. 50),
//               BENCH_CONC (peticiones simultáneas en la prueba de carga, def. 10)
//
// Hace UN solo login (el límite es de 10 intentos cada 15 min) y mide cada endpoint:
//   1) en serie (latencia de un usuario)  y  2) con BENCH_CONC peticiones a la vez (carga).
// Guarda el resultado en ../data/resultados/bench_api.json  y  bench_api.csv
const fs = require('fs');
const path = require('path');

const URL0 = (process.env.BENCH_URL || 'http://localhost:3000').replace(/\/$/, '');
const N = parseInt(process.env.BENCH_N, 10) || 50;
const CONC = parseInt(process.env.BENCH_CONC, 10) || 10;
const USER = process.env.BENCH_USER;
const PASS = process.env.BENCH_PASS;

const pct = (a, p) => { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)]; };
const r1 = (x) => (x == null ? null : Math.round(x * 10) / 10);

async function call(p, token) {
  const t = process.hrtime.bigint();
  const res = await fetch(URL0 + p, { headers: token ? { authorization: 'Bearer ' + token } : {} });
  await res.arrayBuffer();
  return { ms: Number(process.hrtime.bigint() - t) / 1e6, status: res.status };
}

async function medir(nombre, p, token) {
  await call(p, token); // calentamiento (conexión, caché del driver)
  const serie = [];
  let errores = 0;
  for (let i = 0; i < N; i++) { const r = await call(p, token); serie.push(r.ms); if (r.status >= 400) errores++; }
  const carga = [];
  const t0 = Date.now();
  let next = 0;
  await Promise.all(Array.from({ length: CONC }, async () => {
    while (next < N) { next++; const r = await call(p, token); carga.push(r.ms); if (r.status >= 400) errores++; }
  }));
  const seg = (Date.now() - t0) / 1000;
  return {
    endpoint: nombre, ruta: p, n: N, errores,
    serie_p50_ms: r1(pct(serie, 50)), serie_p95_ms: r1(pct(serie, 95)), serie_max_ms: r1(Math.max(...serie)),
    carga_concurrencia: CONC, carga_p50_ms: r1(pct(carga, 50)), carga_p95_ms: r1(pct(carga, 95)),
    carga_peticiones_por_s: r1(N / seg),
  };
}

(async () => {
  if (!USER || !PASS) { console.error('Define BENCH_USER y BENCH_PASS (un usuario de la API, p. ej. analista).'); process.exit(1); }
  const h = await fetch(URL0 + '/health').catch(() => null);
  if (!h || !h.ok) { console.error(`La API no responde en ${URL0}/health. ¿Está corriendo npm start?`); process.exit(1); }

  const tl = process.hrtime.bigint();
  const lr = await fetch(URL0 + '/auth/login', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ usuario: USER, password: PASS }) });
  const loginMs = Number(process.hrtime.bigint() - tl) / 1e6;
  const login = await lr.json();
  if (!lr.ok) { console.error('Login falló:', login.error || lr.status); process.exit(1); }
  const tk = login.token;

  // RFC reales para los endpoints de detalle: uno de riesgo alto y uno que esté en una red circular
  const top = await (await fetch(URL0 + '/contribuyentes?nivel=alto&limit=100', { headers: { authorization: 'Bearer ' + tk } })).json();
  const items = top.items || [];
  const rfc = items[0] && items[0].rfc;
  const rfcRed = (items.find((c) => c.id_red) || {}).rfc;

  const casos = [
    ['health', '/health', null],
    ['kpis (tablero)', '/kpis', tk],
    ['alertas (página 1)', '/alertas?limit=25', tk],
    ['alertas streaming', '/alertas?tipo=streaming&limit=25', tk],
    ['alertas puntaje >= 80', '/alertas?min_puntaje=80&limit=25', tk],
    ['contribuyentes alto', '/contribuyentes?nivel=alto&limit=25', tk],
  ];
  if (rfc) casos.push(['contribuyente por RFC', '/contribuyentes/' + rfc, tk]);
  if (rfcRed) casos.push(['red por RFC', '/red/' + rfcRed, tk]);

  console.log(`FiscalGuard API ${URL0}  ·  ${N} peticiones por endpoint  ·  carga con ${CONC} simultáneas`);
  console.log(`login: ${loginMs.toFixed(0)} ms (incluye bcrypt, es intencionalmente lento)\n`);
  const res = [];
  for (const [n, p, t] of casos) {
    process.stdout.write(`midiendo ${n} ... `);
    const r = await medir(n, p, t);
    res.push(r);
    console.log(`p50 ${r.serie_p50_ms} ms · p95 ${r.serie_p95_ms} ms · carga p95 ${r.carga_p95_ms} ms · ${r.carga_peticiones_por_s} req/s${r.errores ? ` · ${r.errores} errores` : ''}`);
  }
  console.log();
  console.table(res.map((r) => ({ endpoint: r.endpoint, 'p50 ms': r.serie_p50_ms, 'p95 ms': r.serie_p95_ms, 'carga p95 ms': r.carga_p95_ms, 'req/s': r.carga_peticiones_por_s, errores: r.errores })));

  const out = path.join(__dirname, '..', '..', 'data', 'resultados');
  fs.mkdirSync(out, { recursive: true });
  const doc = { fecha: new Date().toISOString(), url: URL0, n: N, concurrencia: CONC, login_ms: r1(loginMs), resultados: res };
  fs.writeFileSync(path.join(out, 'bench_api.json'), JSON.stringify(doc, null, 2));
  const cols = Object.keys(res[0]);
  fs.writeFileSync(path.join(out, 'bench_api.csv'), [cols.join(','), ...res.map((r) => cols.map((c) => JSON.stringify(r[c] ?? '')).join(','))].join('\n'));
  console.log('Guardado en data/resultados/bench_api.json y bench_api.csv');
})().catch((e) => { console.error(e.message); process.exit(1); });
