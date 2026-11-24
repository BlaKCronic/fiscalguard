'use strict';
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { createApp } = require('../src/app');
const fakeDb = require('./fake_db');

const SECRET = 'x'.repeat(40);
const R1 = 'ABC240101XY1', R2 = 'DEF240202XY2', R3 = 'GHI240303XY3';
let server, base, db, ana, aud;
const j = async (path, { method = 'GET', token, body } = {}) => {
  const r = await fetch(base + path, { method, headers: { 'content-type': 'application/json', ...(token && { authorization: `Bearer ${token}` }) }, body: body && JSON.stringify(body) });
  return { status: r.status, body: await r.json().catch(() => null) };
};
const login = async (u, p) => (await j('/auth/login', { method: 'POST', body: { usuario: u, password: p } }));

before(async () => {
  const h = (p) => bcrypt.hashSync(p, 4);
  db = fakeDb({
    usuarios: [{ usuario: 'admin', password_hash: h('pw-admin'), rol: 'admin' }, { usuario: 'ana', password_hash: h('pw-ana'), rol: 'analista' },
      { usuario: 'aud', password_hash: h('pw-aud'), rol: 'auditor' }, { usuario: 'baja', password_hash: h('pw-baja'), rol: 'analista', activo: false }],
    alertas: [
      { id_alerta: 'ALR-1', rfc: R1, tipo: 'clasificador', puntaje: 99, estatus: 'nueva', comentarios: [], descripcion: 'x', fecha_creacion: new Date() },
      { id_alerta: 'ALR-2', rfc: R2, tipo: 'red_circular', puntaje: 90, estatus: 'nueva', comentarios: [], descripcion: 'y', fecha_creacion: new Date() }],
    contribuyentes: [
      { rfc: R1, razon_social: 'Com*** X', estado: 'Jalisco', nivel_riesgo: 'alto', puntaje_riesgo: 99, monto_emitido: 5, id_red: 'RED_1', corrida_id: 'c',
        en_lista_69b: false, factores_riesgo: ['Montos redondos frecuentes (40% de sus CFDI)', 'Contribuyente reciente (200 días de operación)'], fecha_alta: new Date(Date.now() - 200 * 864e5) },
      { rfc: R2, razon_social: 'Com*** Y', estado: 'Jalisco', nivel_riesgo: 'medio', puntaje_riesgo: 60, monto_emitido: 9, id_red: 'RED_1', corrida_id: 'c' }],
    redes: [{ id_red: 'RED_1', rfcs_involucrados: [R1, R2, R3], ciclos: [[R1, R2, R3]], monto_total: 1000, numero_ciclos: 1, numero_nodos: 3 }],
    kpis_agregados: [{ periodo: '2025-01', entidad: 'Jalisco', sector: 'Transporte', total_facturado: 100, total_en_riesgo: 10 },
      { periodo: '2025-02', entidad: 'Jalisco', sector: 'Transporte', total_facturado: 200, total_en_riesgo: 30 }],
  });
  const app = createApp({ db, jwtSecret: SECRET, loginMax: 1000 });
  server = app.listen(0); base = `http://127.0.0.1:${server.address().port}`;
  ana = (await login('ana', 'pw-ana')).body.token; aud = (await login('aud', 'pw-aud')).body.token;
});
after(() => server.close());

test('JWT_SECRET corto se rechaza', () => assert.throws(() => createApp({ db, jwtSecret: 'corto' })));

test('login: éxito, contraseña mala, usuario inexistente y usuario inactivo', async () => {
  assert.equal((await login('ana', 'pw-ana')).status, 200);
  assert.equal((await login('ana', 'mal')).status, 401);
  assert.equal((await login('nadie', 'pw')).status, 401);
  assert.equal((await login('baja', 'pw-baja')).status, 401);
  assert.equal((await j('/auth/login', { method: 'POST', body: { usuario: { $ne: 'x' }, password: 'a' } })).status, 400);
});

test('sin token o token manipulado -> 401', async () => {
  assert.equal((await j('/alertas')).status, 401);
  assert.equal((await j('/alertas', { token: 'abc.def.ghi' })).status, 401);
  const none = Buffer.from('{"alg":"none","typ":"JWT"}').toString('base64url') + '.' + Buffer.from('{"sub":"x","rol":"admin"}').toString('base64url') + '.';
  assert.equal((await j('/alertas', { token: none })).status, 401);
  assert.equal((await j('/alertas', { token: jwt.sign({ sub: 'x', rol: 'admin' }, 'otra-clave-distinta-'.repeat(3)) })).status, 401);
});

test('RBAC: auditor no puede modificar alertas; analista sí', async () => {
  assert.equal((await j('/alertas/ALR-1', { method: 'PATCH', token: aud, body: { estatus: 'revisada' } })).status, 403);
  const r = await j('/alertas/ALR-1', { method: 'PATCH', token: ana, body: { estatus: 'en_revision', analista_asignado: 'ana', comentario: 'revisando' } });
  assert.equal(r.status, 200); assert.equal(r.body.estatus, 'en_revision'); assert.equal(r.body.comentarios[0].usuario, 'ana');
});

test('descartar fija expira_en (TTL 90 días) y reabrir lo quita', async () => {
  const d = await j('/alertas/ALR-2', { method: 'PATCH', token: ana, body: { estatus: 'descartada' } });
  const dias = (new Date(d.body.expira_en) - Date.now()) / 864e5;
  assert.ok(dias > 89 && dias < 91);
  const re = await j('/alertas/ALR-2', { method: 'PATCH', token: ana, body: { estatus: 'nueva' } });
  assert.equal(re.body.expira_en, undefined);
});

test('validaciones de PATCH y filtros', async () => {
  assert.equal((await j('/alertas/ALR-1', { method: 'PATCH', token: ana, body: { estatus: 'inventado' } })).status, 400);
  assert.equal((await j('/alertas/ALR-1', { method: 'PATCH', token: ana, body: { comentario: 'a'.repeat(1001) } })).status, 400);
  assert.equal((await j('/alertas/ALR-1', { method: 'PATCH', token: ana, body: {} })).status, 400);
  assert.equal((await j('/alertas/NOEXISTE', { method: 'PATCH', token: ana, body: { estatus: 'revisada' } })).status, 404);
  assert.equal((await j('/alertas?tipo=zzz', { token: ana })).status, 400);
  assert.equal((await j('/contribuyentes/%7B%22%24ne%22%3A1%7D', { token: ana })).status, 400);
});

test('inyección NoSQL por query string no rompe ni filtra de más', async () => {
  const r = await j('/alertas?estatus[$ne]=nueva&rfc[$regex]=.*', { token: ana });
  assert.equal(r.status, 200); assert.equal(r.body.total, 2); // los objetos se ignoran: no se convierten en operadores
});

test('enmascaramiento de RFC: auditor enmascarado, analista completo', async () => {
  const a = await j('/alertas', { token: ana }); assert.ok(a.body.items.some((x) => x.rfc === R1));
  const m = await j('/alertas', { token: aud }); assert.ok(m.body.items.every((x) => x.rfc.includes('*') && !x.rfc.includes('240101')));
  const c = await j(`/contribuyentes/${R1}`, { token: aud });
  assert.equal(c.status, 200); assert.ok(c.body.rfc.includes('*')); assert.ok(c.body.alertas[0].rfc.includes('*'));
  const red = await j(`/red/${R1}`, { token: aud });
  assert.ok(JSON.stringify(red.body).indexOf(R1) === -1 && JSON.stringify(red.body).indexOf(R3) === -1);
});

test('/red/:rfc devuelve nodos y aristas del ciclo', async () => {
  const r = await j(`/red/${R3}`, { token: ana });
  assert.equal(r.status, 200); assert.equal(r.body.nodes.length, 3); assert.equal(r.body.edges.length, 3);
  assert.deepEqual(r.body.edges.find((e) => e.from === R3), { from: R3, to: R1 }); // el ciclo se cierra
  assert.equal((await j('/red/ZZZ990101AAA', { token: ana })).status, 404);
});

test('/kpis agrega y filtra por periodo', async () => {
  const k = await j('/kpis', { token: ana });
  assert.equal(k.body.resumen.total_facturado, 300); assert.equal(k.body.resumen.total_en_riesgo, 40);
  assert.equal(k.body.tendencia.length, 2); assert.equal(k.body.top20[0].rfc, R1);
  const f = await j('/kpis?desde=2025-02', { token: ana }); assert.equal(f.body.resumen.total_facturado, 200);
  assert.equal((await j('/kpis?desde=2025-13', { token: ana })).status, 400);
});

test('bitácora registra accesos', async () => {
  await new Promise((r) => setTimeout(r, 50));
  const b = db._cols.bitacora.docs;
  assert.ok(b.length > 5); assert.ok(b.some((x) => x.usuario === 'ana' && x.ruta.startsWith('/alertas')));
});

test('límite de intentos de login (429)', async () => {
  const lim = createApp({ db, jwtSecret: SECRET, loginMax: 3 }).listen(0);
  let last;
  for (let i = 0; i < 6; i++) {
    const r = await fetch(`http://127.0.0.1:${lim.address().port}/auth/login`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ usuario: 'ana', password: 'mal' }) });
    last = r.status;
  }
  lim.close();
  assert.equal(last, 429);
});

test('/kpis incluye el perfil de riesgo', async () => {
  const k = (await j('/kpis', { token: ana })).body;
  assert.equal(k.perfil.puntaje[9].alto, 1);
  assert.equal(k.perfil.puntaje[6].medio, 1);
  assert.deepEqual(k.perfil.lista_69b.alto, { en_69b: 0, nuevos: 1 });
  assert.equal(k.perfil.factores[0].clave, 'Montos redondos frecuentes');
  assert.equal(k.perfil.antiguedad[0].n, 1);
  const total = k.perfil.alertas_por_tipo.reduce((a, t) => a + Object.entries(t).filter(([x]) => x !== 'tipo').reduce((b, [, n]) => b + n, 0), 0);
  assert.equal(total, 2);
  assert.deepEqual(k.perfil.alertas_por_tipo.map((t) => t.tipo).sort(), ['clasificador', 'red_circular']);
});
