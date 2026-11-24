'use strict';
// Crea admin / analista / auditor si no existen y muestra sus contraseñas UNA sola vez.
// Para fijarlas tú: SEED_PASS_ADMIN, SEED_PASS_ANALISTA, SEED_PASS_AUDITOR en el entorno.
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '..', '.env') });
require('dotenv').config();
const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const { MongoClient } = require('mongodb');

(async () => {
  const client = new MongoClient(process.env.MONGO_URI, { serverSelectionTimeoutMS: 15000 });
  await client.connect();
  const col = client.db(process.env.MONGO_DB || 'fiscalguard').collection('usuarios');
  await col.createIndex({ usuario: 1 }, { unique: true });
  for (const rol of ['admin', 'analista', 'auditor']) {
    if (await col.findOne({ usuario: rol })) { console.log(`${rol}: ya existe (sin cambios)`); continue; }
    const pass = process.env[`SEED_PASS_${rol.toUpperCase()}`] || crypto.randomBytes(12).toString('base64url');
    await col.insertOne({ usuario: rol, password_hash: await bcrypt.hash(pass, 12), rol, activo: true, creado: new Date() });
    console.log(`${rol}: usuario="${rol}"  password="${pass}"   <- guárdala, no se vuelve a mostrar`);
  }
  await client.close();
})().catch((e) => { console.error(e.message); process.exit(1); });
