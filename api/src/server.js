'use strict';
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '..', '.env') }); // .env en la raíz del proyecto
require('dotenv').config();                                                    // o dentro de api/
const { MongoClient } = require('mongodb');
const { createApp } = require('./app');

(async () => {
  const uri = process.env.MONGO_URI;
  if (!uri) { console.error('Falta MONGO_URI en .env'); process.exit(1); }
  if (!process.env.JWT_SECRET || process.env.JWT_SECRET.length < 32) {
    console.error('JWT_SECRET debe existir y tener al menos 32 caracteres'); process.exit(1);
  }
  const client = new MongoClient(uri, { serverSelectionTimeoutMS: 15000 });
  await client.connect();
  const db = client.db(process.env.MONGO_DB || 'fiscalguard');
  await db.collection('bitacora').createIndex({ fecha: 1 }, { expireAfterSeconds: 180 * 86400 }).catch(() => {});
  const app = createApp({ db, jwtSecret: process.env.JWT_SECRET,
    maskRoles: (process.env.MASK_RFC_ROLES || 'auditor').split(',').map((s) => s.trim()).filter(Boolean),
    staticDir: path.join(__dirname, '..', '..', 'dashboard') });
  const port = parseInt(process.env.PORT, 10) || 3000;
  app.listen(port, () => console.log(`FiscalGuard API en http://localhost:${port}  (db: ${db.databaseName})`));
})().catch((e) => { console.error('No se pudo iniciar:', e.message); process.exit(1); });
