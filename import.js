// Utilizzo:
//   node import.js <città>                          importa GTFS in locale
//   node import.js <città> --sync                   importa + synca daily_* su Atlas
//   node import.js <città> --skip-import            nessuna operazione
//   node import.js <città> --skip-import --sync     synca dati già presenti su Atlas
//
// Città disponibili: Torino, Roma, Firenze, Bari
// Per il feed realtime: node realtime.js <città>

require('dotenv').config();
const { MongoClient } = require('mongodb');
const axios = require('axios');
const AdmZip = require('adm-zip');
const { parse } = require('csv-parse/sync');
const { parse: parseStream } = require('csv-parse');
const fs = require('fs');
const os = require('os');
const path = require('path');

const MONGO_URI  = 'mongodb://localhost:27017';
const REMOTE_URI = process.env.REMOTE_URI || '';

const CITIES = {
  Torino:  { url: 'https://www.gtt.to.it/open_data/gtt_gtfs.zip',                                                        agencyIds: ['U']    },
  Roma:    { url: 'https://romamobilita.it/sites/default/files/rome_static_gtfs.zip',                                     agencyIds: ['OP1'] },
  Firenze: { url: 'https://stg-regionetoscana.smartregion.toscana.it/mobility/autolinee-toscane/gtfs.zip',                agencyIds: ['UFI'] },
  Bari:    { url: 'https://www.amtabservizio.it/gtfs/google_transit.zip',                                                  agencyIds: ['Amtab  S.p.A'] },
};

const EXCLUDE_FIELDS = {
  routes:     ['route_url', 'route_color', 'route_text_color'],
  stop_times: ['stop_headsign', 'pickup_type', 'drop_off_type', 'shape_dist_traveled', 'timepoint'],
  stops:      ['zone_id', 'stop_url', 'location_type', 'parent_station', 'stop_timezone', 'wheelchair_boarding'],
  trips:      ['block_id', 'wheelchair_accessible', 'bike_allowed', 'bikes_allowed', 'limited_route'],
};

const GTFS_FILES = [
  'stops',
  'routes',
  'trips',
  'stop_times',
  'calendar_dates',
  'shapes',
  'frequencies',
  'transfers',
  'pathways',
  'levels',
];

function ms(start) {
  return `${(performance.now() - start).toFixed(0)} ms`;
}

async function downloadZip(url) {
  console.log(`[download] ${url}`);
  const t = performance.now();
  const response = await axios.get(url, { responseType: 'arraybuffer', timeout: 60000 });
  console.log(`[download] ${(response.data.byteLength / 1024 / 1024).toFixed(2)} MB — ${ms(t)}`);
  return Buffer.from(response.data);
}

function extractFiles(zipBuffer) {
  const t = performance.now();
  const zip = new AdmZip(zipBuffer);
  const entries = {};
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gtfs-'));

  for (const entry of zip.getEntries()) {
    const name = entry.entryName.replace(/^.*\//, '');
    const baseName = name.replace('.txt', '');
    if (STREAM_FILES.has(baseName)) {
      // File grandi: scrivi su disco per non saturare l'heap
      const tmpPath = path.join(tmpDir, name);
      fs.writeFileSync(tmpPath, entry.getData());
      entries[name] = { tmpPath };
    } else {
      entries[name] = entry.getData();
    }
  }
  console.log(`[extract] ${Object.keys(entries).length} files — ${ms(t)}\n`);
  return { files: entries, tmpDir };
}

function dropFields(records, fields) {
  return records.map((r) => {
    for (const f of fields) delete r[f];
    return r;
  });
}

function sanitize(value) {
  if (typeof value !== 'string') return value;
  return value
    .replace(/,/g, ' ')   // virgola -> spazio
    .replace(/'/g, '\u2019'); // apostrofo dritto -> apostrofo tipografico
}

function sanitizeRecord(record) {
  const out = {};
  for (const [k, v] of Object.entries(record)) {
    out[k] = sanitize(v);
  }
  return out;
}

function parseCsv(buffer) {
  const text = buffer.toString('utf8').replace(/^\uFEFF/, '');
  const records = parse(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_quotes: true,
    relax_column_count: true,
  });
  return records.map(sanitizeRecord);
}

function transformStops(records) {
  return records.map((r) => {
    const lat = parseFloat(r.stop_lat);
    const lon = parseFloat(r.stop_lon);
    if (!isNaN(lat) && !isNaN(lon)) {
      r.location = { type: 'Point', coordinates: [lon, lat] };
    }
    return r;
  });
}

async function importCollection(db, name, records) {
  const collection = db.collection(name);
  const t = performance.now();
  if (records.length === 0) {
    console.log(`  [${name}] empty, skipped`);
    return;
  }
  const BATCH = 5000;
  for (let i = 0; i < records.length; i += BATCH) {
    await collection.insertMany(records.slice(i, i + BATCH), { ordered: false });
  }
  if (name === 'stops') {
    await collection.createIndex({ location: '2dsphere' });
    console.log(`  [${name}] 2dsphere index created`);
  }
  console.log(`  [${name}] ${records.length.toLocaleString()} records — ${ms(t)}`);
}

const STREAM_FILES = new Set(['stop_times', 'shapes']);

async function importCollectionStreamed(db, name, bufferOrPath, excludeFields) {
  const t = performance.now();

  const csvOpts = {
    columns: true, skip_empty_lines: true, trim: true,
    relax_quotes: true, relax_column_count: true, bom: true,
  };

  let parser;
  if (bufferOrPath && typeof bufferOrPath === 'object' && bufferOrPath.tmpPath) {
    // File su disco: pipe il ReadStream nel parser (nessun buffer in heap)
    parser = parseStream(csvOpts);
    fs.createReadStream(bufferOrPath.tmpPath).pipe(parser);
  } else {
    let buf = bufferOrPath;
    if (buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) buf = buf.slice(3);
    parser = parseStream(buf, csvOpts);
  }

  const collection = db.collection(name);
  const BATCH = 5000;
  let batch = [];
  let total = 0;

  process.stdout.write(`  [${name}] streaming...`);
  for await (const record of parser) {
    let r = sanitizeRecord(record);
    if (excludeFields) for (const f of excludeFields) delete r[f];
    batch.push(r);
    if (batch.length >= BATCH) {
      await collection.insertMany(batch, { ordered: false });
      total += batch.length;
      batch = [];
    }
  }
  if (batch.length > 0) {
    await collection.insertMany(batch, { ordered: false });
    total += batch.length;
  }
  console.log(` ${total.toLocaleString()} records — ${ms(t)}`);
  return total;
}

const SKIP_IMPORT = process.argv.includes('--skip-import');

async function importGTFS(db, url, agencyIds) {
  const tTotal = performance.now();

  // zipBuffer liberato subito dopo l'estrazione per non tenerlo in heap
  const { files, tmpDir } = await downloadZip(url).then(extractFiles);

  const tDrop = performance.now();
  await db.dropDatabase();
  console.log(`[drop DB] ${db.databaseName} dropped — ${ms(tDrop)}`);
  console.log(`[mongo] database: ${db.databaseName} | agencies: ${agencyIds.join(', ')}\n`);

  const tImport = performance.now();
  for (const name of GTFS_FILES) {
    const filename = `${name}.txt`;
    if (!files[filename]) {
      console.log(`  [${name}] not in feed, skipped`);
      continue;
    }
    try {
      if (STREAM_FILES.has(name)) {
        await importCollectionStreamed(db, name, files[filename], EXCLUDE_FIELDS[name]);
      } else {
        const tParse = performance.now();
        let records = parseCsv(files[filename]);
        if (EXCLUDE_FIELDS[name]) records = dropFields(records, EXCLUDE_FIELDS[name]);
        if (name === 'stops') records = transformStops(records);
        const parseTime = ms(tParse);
        process.stdout.write(`  [${name}] parsed ${records.length.toLocaleString()} records in ${parseTime} — inserting...`);
        const tIns = performance.now();
        const collection = db.collection(name);
        const BATCH = 5000;
        for (let i = 0; i < records.length; i += BATCH) {
          await collection.insertMany(records.slice(i, i + BATCH), { ordered: false });
        }
        if (name === 'stops') {
          await collection.createIndex({ location: '2dsphere' });
          console.log(` done in ${ms(tIns)} [2dsphere index created]`);
        } else {
          console.log(` done in ${ms(tIns)}`);
        }
      }
    } catch (err) {
      console.error(`\n  [${name}] ERROR — ${err.message}`);
    }
    // libera il buffer subito dopo l'import per evitare OOM su feed grandi
    files[filename] = null;
  }

  // rimuovi file temporanei su disco
  fs.rmSync(tmpDir, { recursive: true, force: true });
  console.log(`\n[import total] ${ms(tImport)}`);

  // --- daily_trips ---
  const today = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  console.log(`\n[daily_trips] data: ${today}`);
  const tDaily = performance.now();
  const activeServices = await db.collection('calendar_dates').distinct('service_id', { date: today, exception_type: '1' });
  console.log(`[daily_trips] service_id attivi: ${activeServices.length}`);
  await db.collection('daily_trips').drop().catch(() => {});
  const dailyTrips = await db.collection('trips').find({ service_id: { $in: activeServices } }).toArray();
  if (dailyTrips.length > 0) await db.collection('daily_trips').insertMany(dailyTrips, { ordered: false });
  console.log(`[daily_trips] ${dailyTrips.length.toLocaleString()} trips inseriti — ${ms(tDaily)}`);

  // --- daily_routes (filtro per agencyIds) ---
  const tRoutes = performance.now();
  const dailyRouteIds = await db.collection('daily_trips').distinct('route_id');
  await db.collection('daily_routes').drop().catch(() => {});
  const dailyRoutes = await db.collection('routes').find({ route_id: { $in: dailyRouteIds }, agency_id: { $in: agencyIds } }).toArray();
  if (dailyRoutes.length > 0) await db.collection('daily_routes').insertMany(dailyRoutes, { ordered: false });
  console.log(`[daily_routes] ${dailyRoutes.length.toLocaleString()} routes inserite (agencies: ${agencyIds.join(',')}) — ${ms(tRoutes)}`);

  // --- filtra daily_trips per agency ---
  const filteredRouteIds = dailyRoutes.map((r) => r.route_id);
  await db.collection('daily_trips').deleteMany({ route_id: { $nin: filteredRouteIds } });
  const filteredTripsCount = await db.collection('daily_trips').countDocuments();
  console.log(`[daily_trips] dopo filtro agency: ${filteredTripsCount.toLocaleString()} trips`);

  // --- daily_shapes ---
  const tShapes = performance.now();
  const dailyShapeIds = await db.collection('daily_trips').distinct('shape_id');
  await db.collection('daily_shapes').drop().catch(() => {});
  const dailyShapes = await db.collection('shapes').find({ shape_id: { $in: dailyShapeIds } }).toArray();
  if (dailyShapes.length > 0) await db.collection('daily_shapes').insertMany(dailyShapes, { ordered: false });
  console.log(`[daily_shapes] ${dailyShapes.length.toLocaleString()} punti inseriti — ${ms(tShapes)}`);

  // --- daily_stop_times ---
  const tStopTimes = performance.now();
  const dailyTripIds = await db.collection('daily_trips').distinct('trip_id');
  await db.collection('daily_stop_times').drop().catch(() => {});
  const BATCH = 5000;
  let totalStopTimes = 0;
  for (let i = 0; i < dailyTripIds.length; i += BATCH) {
    const chunk = dailyTripIds.slice(i, i + BATCH);
    const docs = await db.collection('stop_times').find({ trip_id: { $in: chunk } }).toArray();
    if (docs.length > 0) {
      await db.collection('daily_stop_times').insertMany(docs, { ordered: false });
      totalStopTimes += docs.length;
    }
  }
  console.log(`[daily_stop_times] ${totalStopTimes.toLocaleString()} records inseriti — ${ms(tStopTimes)}`);

  // --- daily_stops ---
  const tDailyStops = performance.now();
  const dailyStopIds = await db.collection('daily_stop_times').distinct('stop_id');
  await db.collection('daily_stops').drop().catch(() => {});
  const dailyStops = await db.collection('stops').find({ stop_id: { $in: dailyStopIds } }).toArray();
  if (dailyStops.length > 0) {
    await db.collection('daily_stops').insertMany(dailyStops, { ordered: false });
    await db.collection('daily_stops').createIndex({ location: '2dsphere' });
  }
  console.log(`[daily_stops] ${dailyStops.length.toLocaleString()} fermate inserite [2dsphere index created] — ${ms(tDailyStops)}`);

  // --- routes per fermata ---
  const tStopRoutes = performance.now();
  const stopRoutes = await db.collection('daily_stop_times').aggregate([
    { $group: { _id: '$stop_id', trip_ids: { $addToSet: '$trip_id' } } },
    { $lookup: { from: 'daily_trips', localField: 'trip_ids', foreignField: 'trip_id', as: 'trips' } },
    { $project: { _id: 0, stop_id: '$_id', routes: { $setUnion: '$trips.route_id' } } },
  ]).toArray();
  const bulk = stopRoutes.map((r) => ({ updateOne: { filter: { stop_id: r.stop_id }, update: { $set: { routes: r.routes } } } }));
  if (bulk.length > 0) await db.collection('daily_stops').bulkWrite(bulk);
  console.log(`[daily_stops.routes] ${bulk.length.toLocaleString()} fermate aggiornate — ${ms(tStopRoutes)}`);

  // --- closeStops ---
  const tClose = performance.now();
  const allStops = await db.collection('daily_stops').find({}, { projection: { stop_id: 1, location: 1 } }).toArray();
  const BULK_SIZE = 500;
  let closeBulk = [];
  for (const stop of allStops) {
    if (!stop.location) continue;
    const nearby = await db.collection('daily_stops').aggregate([
      { $geoNear: { near: stop.location, distanceField: 'dist', maxDistance: 250, spherical: true, query: { stop_id: { $ne: stop.stop_id } } } },
      { $limit: 10 },
      { $project: { _id: 0, stop_id: 1 } },
    ]).toArray();
    closeBulk.push({ updateOne: { filter: { stop_id: stop.stop_id }, update: { $set: { closeStops: nearby.map((s) => s.stop_id) } } } });
    if (closeBulk.length >= BULK_SIZE) { await db.collection('daily_stops').bulkWrite(closeBulk); closeBulk = []; }
  }
  if (closeBulk.length > 0) await db.collection('daily_stops').bulkWrite(closeBulk);
  console.log(`[daily_stops.closeStops] ${allStops.length.toLocaleString()} fermate elaborate — ${ms(tClose)}`);

  // --- indici ---
  const tIdx = performance.now();
  await Promise.all([
    db.collection('daily_stop_times').createIndex({ stop_id: 1, departure_time: 1 }),
    db.collection('daily_stop_times').createIndex({ trip_id: 1, stop_id: 1 }),
    db.collection('daily_trips').createIndex({ trip_id: 1 }),
    db.collection('daily_trips').createIndex({ route_id: 1 }),
    db.collection('daily_trips').createIndex({ route_id: 1, trip_headsign: 1 }),
    db.collection('daily_stops').createIndex({ stop_id: 1 }),
    db.collection('daily_routes').createIndex({ route_id: 1 }),
    db.collection('daily_shapes').createIndex({ shape_id: 1 }),
  ]);
  console.log(`[indexes] creati — ${ms(tIdx)}`);

  // --- drop collections base ---
  const tDrop2 = performance.now();
  const toDrop = ['calendar_dates', 'routes', 'shapes', 'stop_times', 'stops', 'trips'];
  for (const name of toDrop) await db.collection(name).drop().catch(() => {});
  console.log(`\n[drop collections] ${toDrop.join(', ')} — ${ms(tDrop2)}`);

  console.log(`\n[importGTFS total] ${ms(tTotal)}`);
}

async function syncToRemote(localDb) {
  if (!REMOTE_URI) return;
  const DAILY = ['daily_stops', 'daily_routes', 'daily_trips', 'daily_stop_times', 'daily_shapes'];
  const BATCH = 50000;

  console.log(`\n[sync-remote] connessione a remote...`);
  const remoteClient = new MongoClient(REMOTE_URI);
  await remoteClient.connect();
  const remoteDb = remoteClient.db(localDb.databaseName);

  for (const name of DAILY) {
    const t = performance.now();
    const total = await localDb.collection(name).countDocuments();
    if (total === 0) { console.log(`  [${name}] vuota, skip`); continue; }

    await remoteDb.collection(name).drop().catch(() => {});
    let synced = 0;
    const cursor = localDb.collection(name).find({}, { projection: { _id: 0 } });
    let batch = [];
    for await (const doc of cursor) {
      batch.push(doc);
      if (batch.length >= BATCH) {
        await remoteDb.collection(name).insertMany(batch, { ordered: false });
        synced += batch.length;
        batch = [];
        process.stdout.write(`\r  [${name}] ${synced.toLocaleString()}/${total.toLocaleString()}...`);
      }
    }
    if (batch.length > 0) {
      await remoteDb.collection(name).insertMany(batch, { ordered: false });
      synced += batch.length;
    }
    console.log(`\r  [${name}] ${synced.toLocaleString()} doc — ${ms(t)}`);
  }

  // indici sul remote
  const tIdx = performance.now();
  await Promise.all([
    remoteDb.collection('daily_stops').createIndex({ location: '2dsphere' }),
    remoteDb.collection('daily_stops').createIndex({ stop_id: 1 }),
    remoteDb.collection('daily_stop_times').createIndex({ stop_id: 1, departure_time: 1 }),
    remoteDb.collection('daily_stop_times').createIndex({ trip_id: 1, stop_id: 1 }),
    remoteDb.collection('daily_trips').createIndex({ trip_id: 1 }),
    remoteDb.collection('daily_trips').createIndex({ route_id: 1 }),
    remoteDb.collection('daily_trips').createIndex({ route_id: 1, trip_headsign: 1 }),
    remoteDb.collection('daily_routes').createIndex({ route_id: 1 }),
    remoteDb.collection('daily_shapes').createIndex({ shape_id: 1 }),
  ]);
  console.log(`[sync-remote] indici creati — ${ms(tIdx)}`);

  await remoteClient.close();
  console.log(`[sync-remote] completato`);
}

async function logToAtlas(cityName, startedAt, durationMs, status, counts, errorMsg) {
  if (!REMOTE_URI) return;
  try {
    const remoteClient = new MongoClient(REMOTE_URI);
    await remoteClient.connect();
    const logCol = remoteClient.db('gtfs_system').collection('import_log');
    await logCol.insertOne({
      city:        cityName,
      date:        new Date().toISOString().slice(0, 10),
      startedAt,
      completedAt: new Date(),
      durationMs,
      status,
      counts:      counts || {},
      error:       errorMsg || null,
    });
    await remoteClient.close();
    console.log(`[import_log] scritto su Atlas (${status})`);
  } catch (e) {
    console.error(`[import_log] errore scrittura Atlas: ${e.message}`);
  }
}

async function main() {
  const tTotal = performance.now();
  const startedAt = new Date();

  const cityName = process.argv.find(a => CITIES[a]);
  if (!cityName) {
    console.error(`Città non specificata. Disponibili: ${Object.keys(CITIES).join(', ')}`);
    process.exit(1);
  }
  const { url, agencyIds } = CITIES[cityName];

  const client = new MongoClient(MONGO_URI);
  await client.connect();
  const db = client.db(cityName);

  const SYNC = process.argv.includes('--sync');

  try {
    if (!SKIP_IMPORT) {
      await importGTFS(db, url, agencyIds);
    }
    if (SYNC) await syncToRemote(db);

    // raccogli conteggi dalle daily collections
    const counts = {};
    for (const name of ['daily_stops', 'daily_routes', 'daily_trips', 'daily_stop_times', 'daily_shapes']) {
      counts[name] = await db.collection(name).countDocuments().catch(() => 0);
    }

    const durationMs = Math.round(performance.now() - tTotal);
    console.log(`\n[total] ${durationMs} ms`);
    await logToAtlas(cityName, startedAt, durationMs, 'success', counts, null);
  } catch (err) {
    const durationMs = Math.round(performance.now() - tTotal);
    console.error('Fatal:', err.message);
    await logToAtlas(cityName, startedAt, durationMs, 'error', null, err.message);
    process.exit(1);
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
