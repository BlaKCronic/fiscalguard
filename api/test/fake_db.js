'use strict';
// Base de datos en memoria con SOLO lo que usa la API. Sirve para probar autenticación, RBAC,
// validación y enmascaramiento; NO sustituye una prueba contra MongoDB real.
const match = (doc, f) => Object.entries(f || {}).every(([k, c]) => {
  const v = doc[k];
  if (c && typeof c === 'object' && !(c instanceof Date)) {
    if ('$gte' in c && !(v >= c.$gte)) return false;
    if ('$lte' in c && !(v <= c.$lte)) return false;
    if ('$in' in c && !(Array.isArray(v) ? v.some((x) => c.$in.includes(x)) : c.$in.includes(v))) return false;
    return true;
  }
  return Array.isArray(v) ? v.includes(c) : v === c;
});
const proj = (d, p) => {
  if (!p) return { ...d };
  const keys = Object.keys(p); const inc = keys.some((k) => p[k] === 1);
  const o = {};
  if (inc) { for (const k of keys) if (p[k] === 1 && k in d) o[k] = d[k]; } else { Object.assign(o, d); for (const k of keys) delete o[k]; }
  return o;
};
class Col {
  constructor(docs) { this.docs = docs; }
  async findOne(f, o = {}) { const d = this.docs.find((x) => match(x, f)); return d ? proj(d, o.projection) : null; }
  find(f, o = {}) {
    let r = this.docs.filter((x) => match(x, f)); const self = { };
    self.sort = (s) => { const [k, dir] = Object.entries(s)[0]; r = [...r].sort((a, b) => (a[k] > b[k] ? 1 : -1) * dir); return self; };
    self.skip = (n) => { r = r.slice(n); return self; };
    self.limit = (n) => { r = r.slice(0, n); return self; };
    self.toArray = async () => r.map((d) => proj(d, o.projection));
    return self;
  }
  async countDocuments(f) { return this.docs.filter((x) => match(x, f)).length; }
  async insertOne(d) { this.docs.push({ ...d }); return {}; }
  async createIndex() {}
  async findOneAndUpdate(f, u) {
    const d = this.docs.find((x) => match(x, f)); if (!d) return null;
    Object.assign(d, u.$set || {}); for (const k of Object.keys(u.$unset || {})) delete d[k];
    for (const [k, v] of Object.entries(u.$push || {})) (d[k] = d[k] || []).push(v);
    return { ...d };
  }
  aggregate(p) {
    const docs = this.docs; const g = (p.find((s) => s.$group) || {}).$group || {}; const toArray = async () => {
      if (p[1] && p[1].$facet) { // kpis_agregados
        const s = (k) => { const m = new Map(); for (const d of docs.filter((x) => match(x, p[0].$match))) { const e = m.get(d[k]) || { _id: d[k], total_facturado: 0, total_en_riesgo: 0 }; e.total_facturado += d.total_facturado; e.total_en_riesgo += d.total_en_riesgo; m.set(d[k], e); } return [...m.values()]; };
        const all = docs.filter((x) => match(x, p[0].$match));
        return [{ resumen: [{ _id: null, total_facturado: all.reduce((a, d) => a + d.total_facturado, 0), total_en_riesgo: all.reduce((a, d) => a + d.total_en_riesgo, 0) }],
          por_entidad: s('entidad'), por_sector: s('sector'), tendencia: s('periodo') }];
      }
      if (g._id && g._id.tipo) { const m = {}; docs.forEach((d) => { const k = `${d.tipo}|${d.estatus}`; m[k] = (m[k] || 0) + 1; }); return Object.entries(m).map(([k, n]) => { const [tipo, estatus] = k.split('|'); return { _id: { tipo, estatus }, n }; }); }
      if (g._id === '$nivel_riesgo') { const m = {}; docs.forEach((d) => { m[d.nivel_riesgo] = (m[d.nivel_riesgo] || 0) + 1; }); return Object.entries(m).map(([k, n]) => ({ _id: k, n })); }
      const m = {}; docs.forEach((d) => { const k = `${d.estado}|${d.nivel_riesgo}`; m[k] = (m[k] || 0) + 1; });
      return Object.entries(m).map(([k, n]) => { const [estado, nivel] = k.split('|'); return { _id: { estado, nivel }, n }; });
    };
    return { toArray };
  }
}
module.exports = function fakeDb(data) {
  const cols = {}; const get = (n) => (cols[n] = cols[n] || new Col(data[n] || []));
  return { collection: get, command: async () => ({ ok: 1 }), _cols: cols };
};
