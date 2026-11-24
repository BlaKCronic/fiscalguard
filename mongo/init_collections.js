// mongosh "$MONGO_URI" mongo/init_collections.js   (Atlas M0 o Mongo local)
// Solo resultados resumidos; el detalle completo vive en HDFS.
const d = db.getSiblingDB("fiscalguard");

d.contribuyentes.createIndex({ rfc: 1 }, { unique: true });
d.contribuyentes.createIndex({ nivel_riesgo: 1, puntaje_riesgo: -1 });
d.contribuyentes.createIndex({ estado: 1 });

d.alertas.createIndex({ rfc: 1 });
d.alertas.createIndex({ estatus: 1, puntaje: -1 });
// TTL: la app fija `expira_en` (p. ej. +90 días) al descartar una alerta; Mongo la borra sola.
d.alertas.createIndex({ expira_en: 1 }, { expireAfterSeconds: 0 });

d.redes.createIndex({ id_red: 1 }, { unique: true });
d.kpis_agregados.createIndex({ periodo: 1, entidad: 1, sector: 1 });
d.usuarios.createIndex({ usuario: 1 }, { unique: true });

print("Colecciones e índices listos");
