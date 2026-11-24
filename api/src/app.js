'use strict';
const path = require('path');
const fs = require('fs');
const express = require('express');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');

const RFC_RE = /^[A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3}$/;
const ESTATUS = ['nueva', 'en_revision', 'revisada', 'descartada', 'escalada'];
const TIPOS = ['clasificador', 'red_circular', 'anomalia', 'streaming'];
const NIVELES = ['alto', 'medio'];
const PERIODO_RE = /^\d{4}-(0[1-9]|1[0-2])$/;
const DUMMY_HASH = bcrypt.hashSync('no-existe-este-usuario', 10); // iguala tiempos si el usuario no existe

// ---- utilidades ----
const str = (v, max = 100) => (typeof v === 'string' && v.length > 0 && v.length <= max ? v : undefined); // rechaza objetos (?x[$ne]=1)
const maskRfc = (r) => r.slice(0, 4) + '*'.repeat(r.length - 6) + r.slice(-2);
function maskDeep(v) {
  if (typeof v === 'string') return RFC_RE.test(v) ? maskRfc(v) : v;
  if (Array.isArray(v)) return v.map(maskDeep);
  if (v && typeof v === 'object' && !(v instanceof Date)) {
    const o = {};
    for (const k of Object.keys(v)) o[k] = maskDeep(v[k]);
    return o;
  }
  return v;
}
const page = (q) => {
  const p = Math.max(1, parseInt(q.page, 10) || 1);
  const l = Math.min(100, Math.max(1, parseInt(q.limit, 10) || 25));
  return { p, l, skip: (p - 1) * l };
};
const clean = (d) => { if (!d) return d; const { _id, password_hash, ...rest } = d; return rest; };

function createApp({ db, jwtSecret, maskRoles = ['auditor'], staticDir = null, loginMax = 10 }) {
  if (!jwtSecret || jwtSecret.length < 32) throw new Error('JWT_SECRET debe tener al menos 32 caracteres');
  const app = express();
  app.set('trust proxy', 1);
  app.disable('x-powered-by');
  app.use(helmet({
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'", 'https://cdn.jsdelivr.net'],
        styleSrc: ["'self'", "'unsafe-inline'", 'https://cdn.jsdelivr.net'],
        imgSrc: ["'self'", 'data:'],
        connectSrc: ["'self'"],
      },
    },
  }));
  app.use(express.json({ limit: '50kb' }));

  // ---- bitácora de accesos (se escribe al terminar cada petición) ----
  app.use((req, res, next) => {
    res.on('finish', () => {
      if (req.path === '/health') return;
      db.collection('bitacora').insertOne({
        fecha: new Date(), usuario: req.user ? req.user.sub : (req.body && str(req.body.usuario, 60)) || null,
        rol: req.user ? req.user.rol : null, metodo: req.method, ruta: req.path, status: res.statusCode, ip: req.ip,
      }).catch(() => {});
    });
    next();
  });

  // ---- auth ----
  const auth = (...roles) => (req, res, next) => {
    const h = req.headers.authorization || '';
    const token = h.startsWith('Bearer ') ? h.slice(7) : null;
    if (!token) return res.status(401).json({ error: 'No autenticado' });
    try {
      req.user = jwt.verify(token, jwtSecret, { algorithms: ['HS256'] });
    } catch (e) {
      return res.status(401).json({ error: 'Token inválido o expirado' });
    }
    if (roles.length && !roles.includes(req.user.rol)) return res.status(403).json({ error: 'Permisos insuficientes' });
    next();
  };
  // responde aplicando enmascaramiento de RFC según el rol
  const send = (req, res, body) => res.json(maskRoles.includes(req.user.rol) ? maskDeep(body) : body);
  const wrap = (fn) => (req, res) => fn(req, res).catch((e) => { console.error(e); res.status(500).json({ error: 'Error interno' }); });

  app.get('/health', wrap(async (req, res) => { await db.command({ ping: 1 }); res.json({ ok: true }); }));

  const loginLimiter = rateLimit({ windowMs: 15 * 60 * 1000, max: loginMax, standardHeaders: true, legacyHeaders: false,
    message: { error: 'Demasiados intentos, intenta más tarde' } });
  app.post('/auth/login', loginLimiter, wrap(async (req, res) => {
    const usuario = str(req.body && req.body.usuario, 60);
    const password = str(req.body && req.body.password, 200);
    if (!usuario || !password) return res.status(400).json({ error: 'usuario y password son obligatorios' });
    const u = await db.collection('usuarios').findOne({ usuario });
    const ok = await bcrypt.compare(password, u ? u.password_hash : DUMMY_HASH);
    if (!u || !ok || u.activo === false) return res.status(401).json({ error: 'Credenciales inválidas' });
    const token = jwt.sign({ sub: u.usuario, rol: u.rol }, jwtSecret, { algorithm: 'HS256', expiresIn: '8h' });
    res.json({ token, usuario: u.usuario, rol: u.rol, expira_en_horas: 8 });
  }));
  app.get('/auth/me', auth(), (req, res) => res.json({ usuario: req.user.sub, rol: req.user.rol }));

  // ---- alertas ----
  app.get('/alertas', auth(), wrap(async (req, res) => {
    const f = {};
    const tipo = str(req.query.tipo, 20), estatus = str(req.query.estatus, 20), rfc = str(req.query.rfc, 13);
    if (tipo) { if (!TIPOS.includes(tipo)) return res.status(400).json({ error: 'tipo inválido' }); f.tipo = tipo; }
    if (estatus) { if (!ESTATUS.includes(estatus)) return res.status(400).json({ error: 'estatus inválido' }); f.estatus = estatus; }
    if (rfc) { if (!RFC_RE.test(rfc)) return res.status(400).json({ error: 'rfc inválido' }); f.rfc = rfc; }
    const min = parseFloat(req.query.min_puntaje);
    if (!Number.isNaN(min)) f.puntaje = { $gte: min };
    const { p, l, skip } = page(req.query);
    const col = db.collection('alertas');
    const [total, items] = await Promise.all([
      col.countDocuments(f),
      col.find(f, { projection: { _id: 0 } }).sort({ puntaje: -1, fecha_creacion: -1 }).skip(skip).limit(l).toArray()]);
    send(req, res, { page: p, limit: l, total, items });
  }));

  app.get('/alertas/:id', auth(), wrap(async (req, res) => {
    const a = await db.collection('alertas').findOne({ id_alerta: str(req.params.id, 30) }, { projection: { _id: 0 } });
    if (!a) return res.status(404).json({ error: 'Alerta no encontrada' });
    send(req, res, a);
  }));

  app.patch('/alertas/:id', auth('admin', 'analista'), wrap(async (req, res) => {
    const b = req.body || {};
    const now = new Date();
    const $set = { fecha_actualizacion: now };
    const upd = { $set };
    if (b.estatus !== undefined) {
      if (!ESTATUS.includes(b.estatus)) return res.status(400).json({ error: 'estatus inválido' });
      $set.estatus = b.estatus;
      if (b.estatus === 'descartada') $set.expira_en = new Date(now.getTime() + 90 * 864e5); // TTL: se borra sola a los 90 días
      else upd.$unset = { expira_en: '' };
    }
    if (b.analista_asignado !== undefined) {
      const an = str(b.analista_asignado, 60);
      if (!an) return res.status(400).json({ error: 'analista_asignado inválido' });
      $set.analista_asignado = an;
    }
    if (b.comentario !== undefined) {
      const t = str(b.comentario, 1000);
      if (!t) return res.status(400).json({ error: 'comentario inválido (1-1000 caracteres)' });
      upd.$push = { comentarios: { usuario: req.user.sub, texto: t, fecha: now } };
    }
    if (Object.keys($set).length === 1 && !upd.$push) return res.status(400).json({ error: 'Nada que actualizar' });
    const r = await db.collection('alertas').findOneAndUpdate({ id_alerta: str(req.params.id, 30) }, upd,
      { returnDocument: 'after', projection: { _id: 0 } });
    const doc = r && r.value !== undefined ? r.value : r; // compatibilidad entre versiones del driver
    if (!doc) return res.status(404).json({ error: 'Alerta no encontrada' });
    send(req, res, doc);
  }));

  // ---- contribuyentes ----
  app.get('/contribuyentes', auth(), wrap(async (req, res) => {
    const f = {};
    const nivel = str(req.query.nivel, 10), estado = str(req.query.estado, 40);
    if (nivel) { if (!NIVELES.includes(nivel)) return res.status(400).json({ error: 'nivel inválido' }); f.nivel_riesgo = nivel; }
    if (estado) f.estado = estado;
    const { p, l, skip } = page(req.query);
    const col = db.collection('contribuyentes');
    const [total, items] = await Promise.all([
      col.countDocuments(f),
      col.find(f, { projection: { _id: 0, corrida_id: 0 } }).sort({ puntaje_riesgo: -1, monto_emitido: -1 }).skip(skip).limit(l).toArray()]);
    send(req, res, { page: p, limit: l, total, items });
  }));

  app.get('/contribuyentes/:rfc', auth(), wrap(async (req, res) => {
    const rfc = str(req.params.rfc, 13);
    if (!rfc || !RFC_RE.test(rfc)) return res.status(400).json({ error: 'rfc inválido' });
    const c = await db.collection('contribuyentes').findOne({ rfc }, { projection: { _id: 0, corrida_id: 0 } });
    if (!c) return res.status(404).json({ error: 'Contribuyente no encontrado entre los de riesgo medio/alto' });
    const alertas = await db.collection('alertas').find({ rfc }, { projection: { _id: 0 } }).sort({ puntaje: -1 }).toArray();
    send(req, res, { ...c, alertas });
  }));

  // ---- red de facturación circular ----
  app.get('/red/:rfc', auth(), wrap(async (req, res) => {
    const rfc = str(req.params.rfc, 13);
    if (!rfc || !RFC_RE.test(rfc)) return res.status(400).json({ error: 'rfc inválido' });
    const red = await db.collection('redes').findOne({ rfcs_involucrados: rfc }, { projection: { _id: 0, corrida_id: 0 } });
    if (!red) return res.status(404).json({ error: 'El RFC no pertenece a ninguna red circular detectada' });
    const info = await db.collection('contribuyentes').find({ rfc: { $in: red.rfcs_involucrados } },
      { projection: { _id: 0, rfc: 1, nivel_riesgo: 1, puntaje_riesgo: 1, en_lista_69b: 1 } }).toArray();
    const byRfc = Object.fromEntries(info.map((i) => [i.rfc, i]));
    const edges = new Set();
    for (const c of red.ciclos || []) for (let i = 0; i < c.length; i++) edges.add(`${c[i]}>${c[(i + 1) % c.length]}`);
    send(req, res, {
      id_red: red.id_red, monto_total: red.monto_total, numero_ciclos: red.numero_ciclos, numero_nodos: red.numero_nodos,
      consultado: rfc,
      nodes: red.rfcs_involucrados.map((r) => byRfc[r] || { rfc: r }),
      edges: [...edges].map((e) => { const [from, to] = e.split('>'); return { from, to }; }),
    });
  }));

  // ---- KPIs para el tablero ----
  app.get('/kpis', auth(), wrap(async (req, res) => {
    const desde = str(req.query.desde, 7), hasta = str(req.query.hasta, 7);
    if ((desde && !PERIODO_RE.test(desde)) || (hasta && !PERIODO_RE.test(hasta))) return res.status(400).json({ error: 'desde/hasta deben ser YYYY-MM' });
    const match = {};
    if (desde || hasta) match.periodo = { ...(desde && { $gte: desde }), ...(hasta && { $lte: hasta }) };
    const sum = (id) => ({ $group: { _id: id, total_facturado: { $sum: '$total_facturado' }, total_en_riesgo: { $sum: '$total_en_riesgo' } } });
    const [agg] = await db.collection('kpis_agregados').aggregate([
      { $match: match },
      { $facet: {
        resumen: [sum(null)],
        por_entidad: [sum('$entidad'), { $sort: { total_en_riesgo: -1 } }],
        por_sector: [sum('$sector'), { $sort: { total_en_riesgo: -1 } }],
        tendencia: [sum('$periodo'), { $sort: { _id: 1 } }],
      } }]).toArray();
    const cont = db.collection('contribuyentes');
    const [porNivel, porEstado, top20, nuevas, redes, alTipo, perfil] = await Promise.all([
      cont.aggregate([{ $group: { _id: '$nivel_riesgo', n: { $sum: 1 } } }]).toArray(),
      cont.aggregate([{ $group: { _id: { estado: '$estado', nivel: '$nivel_riesgo' }, n: { $sum: 1 } } }]).toArray(),
      cont.find({}, { projection: { _id: 0, rfc: 1, razon_social: 1, estado: 1, actividad: 1, puntaje_riesgo: 1, nivel_riesgo: 1, monto_emitido: 1, id_red: 1 } })
        .sort({ puntaje_riesgo: -1, monto_emitido: -1 }).limit(20).toArray(),
      db.collection('alertas').countDocuments({ estatus: 'nueva' }),
      db.collection('redes').countDocuments({}),
      db.collection('alertas').aggregate([{ $group: { _id: { tipo: '$tipo', estatus: '$estatus' }, n: { $sum: 1 } } }]).toArray(),
      // perfil de los contribuyentes en riesgo (son pocos miles: se resume aquí)
      cont.find({}, { projection: { _id: 0, puntaje_riesgo: 1, nivel_riesgo: 1, en_lista_69b: 1, factores_riesgo: 1, fecha_alta: 1 } }).toArray()]);
    const r0 = (agg && agg.resumen && agg.resumen[0]) || { total_facturado: 0, total_en_riesgo: 0 };
    const rename = (a) => (a || []).map(({ _id, ...x }) => ({ clave: _id, ...x }));
    const estados = {};
    for (const x of porEstado) { const e = (estados[x._id.estado] = estados[x._id.estado] || { estado: x._id.estado, alto: 0, medio: 0 }); e[x._id.nivel] = x.n; }
    // --- perfil de riesgo ---
    const hist = Array.from({ length: 10 }, (_, i) => ({ rango: `${i * 10}-${i * 10 + 10}`, alto: 0, medio: 0 }));
    const lista = { alto: { en_69b: 0, nuevos: 0 }, medio: { en_69b: 0, nuevos: 0 } };
    const factores = {};
    const ANT = ['< 1 año', '1-2 años', '2-5 años', '5-10 años', '> 10 años'];
    const antig = Object.fromEntries(ANT.map((k) => [k, 0]));
    const ref = Date.now();
    for (const c of perfil) {
      const nv = c.nivel_riesgo === 'alto' ? 'alto' : 'medio';
      const b = Math.min(9, Math.max(0, Math.floor((Number(c.puntaje_riesgo) || 0) / 10)));
      hist[b][nv]++;
      lista[nv][c.en_lista_69b ? 'en_69b' : 'nuevos']++;
      for (const f of c.factores_riesgo || []) { const k = String(f).split(' (')[0]; factores[k] = (factores[k] || 0) + 1; }
      if (c.fecha_alta) {
        const y = (ref - new Date(c.fecha_alta).getTime()) / (365.25 * 864e5);
        antig[y < 1 ? ANT[0] : y < 2 ? ANT[1] : y < 5 ? ANT[2] : y < 10 ? ANT[3] : ANT[4]]++;
      }
    }
    const alertasTipo = {};
    for (const x of alTipo) { const t = (alertasTipo[x._id.tipo] = alertasTipo[x._id.tipo] || { tipo: x._id.tipo }); t[x._id.estatus] = x.n; }
    send(req, res, {
      perfil: {
        puntaje: hist, lista_69b: lista, antiguedad: ANT.map((k) => ({ clave: k, n: antig[k] })),
        factores: Object.entries(factores).map(([clave, n]) => ({ clave, n })).sort((a, b) => b.n - a.n),
        alertas_por_tipo: Object.values(alertasTipo),
      },
      resumen: { total_facturado: r0.total_facturado, total_en_riesgo: r0.total_en_riesgo,
        contribuyentes_por_nivel: Object.fromEntries(porNivel.map((x) => [x._id, x.n])), alertas_nuevas: nuevas, redes },
      por_entidad: rename(agg && agg.por_entidad), por_sector: rename(agg && agg.por_sector), tendencia: rename(agg && agg.tendencia),
      contribuyentes_por_estado: Object.values(estados), top20,
    });
  }));

  // ---- tablero estático (si existe la carpeta dashboard) ----
  if (staticDir && fs.existsSync(staticDir)) app.use(express.static(staticDir));
  app.use((req, res) => res.status(404).json({ error: 'Ruta no encontrada' }));
  return app;
}

module.exports = { createApp, maskRfc, RFC_RE };
