const { MongoClient } = require('mongodb');
const axios = require('axios');
const AdmZip = require('adm-zip');
const { parse } = require('csv-parse/sync');

const GTFS_URL = 'https://www.gtt.to.it/open_data/gtt_gtfs.zip';
const MONGO_URI = 'mongodb://localhost:27017';
const DB_NAME = 'Torino';

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
  for (const entry of zip.getEntries()) {
    const name = entry.entryName.replace(/^.*\//, '');
    entries[name] = entry.getData();
  }
  console.log(`[extract] ${Object.keys(entries).length} files — ${ms(t)}\n`);
  return entries;
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

const SKIP_IMPORT = process.argv.includes('--skip-import');

const ORIGIN      = { lat: 45.07251373016942,  lon: 7.657059739198934  };
const DESTINATION = { lat: 45.062609036476736, lon: 7.6626132784813645 };
const SEARCH_RADIUS = 400; // metri
const TIME_FROM = '08:00:00';
const TIME_TO   = '09:00:00';

const TRANSFER_MARGIN_SEC = 2 * 60;
const MAX_DURATION_SEC    = 60 * 60;

function timeToSec(t) {
  const [h, m, s] = t.split(':').map(Number);
  return h * 3600 + m * 60 + s;
}

function formatTime(t) {
  return t ? t.substring(0, 5) : '??:??';
}

function secToMin(sec) {
  return Math.round(sec / 60);
}

function haversine(lat1, lon1, lat2, lon2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.asin(Math.sqrt(a));
}

function walkMin(meters) {
  return Math.ceil(meters / 80); // 80 m/min ≈ 4.8 km/h
}

async function enrichWithWalking(db, directResults, transferResults) {
  // Raccogli tutti gli stop_id che servono
  const stopIds = new Set();
  for (const r of directResults) {
    stopIds.add(r.boarding_stop_id);
    stopIds.add(r.alighting_stop_id);
  }
  for (const r of transferResults) {
    stopIds.add(r.legA_boarding_stop_id);
    stopIds.add(r.legA_alighting_stop_id);
    stopIds.add(r.legB_boarding_stop_id);
    stopIds.add(r.legB_alighting_stop_id);
  }
  stopIds.delete(undefined);

  const stops = await db.collection('daily_stops')
    .find({ stop_id: { $in: [...stopIds] } }, { projection: { _id: 0, stop_id: 1, location: 1 } })
    .toArray();
  const coordMap = {}; // stop_id -> { lat, lon }
  for (const s of stops) {
    if (s.location) coordMap[s.stop_id] = { lon: s.location.coordinates[0], lat: s.location.coordinates[1] };
  }

  const walkInfo = (fromLat, fromLon, toLat, toLon) => {
    const d = haversine(fromLat, fromLon, toLat, toLon);
    return { m: Math.round(d), min: walkMin(d) };
  };

  for (const r of directResults) {
    const boarding   = coordMap[r.boarding_stop_id];
    const alighting  = coordMap[r.alighting_stop_id];
    r.walk_to_start  = boarding  ? walkInfo(ORIGIN.lat, ORIGIN.lon, boarding.lat, boarding.lon)  : null;
    r.walk_to_dest   = alighting ? walkInfo(alighting.lat, alighting.lon, DESTINATION.lat, DESTINATION.lon) : null;
    r.total_min      = secToMin(r.duration_sec) + (r.walk_to_start?.min ?? 0) + (r.walk_to_dest?.min ?? 0);
  }

  for (const r of transferResults) {
    const boardA   = coordMap[r.legA_boarding_stop_id];
    const alightA  = coordMap[r.legA_alighting_stop_id];
    const boardB   = coordMap[r.legB_boarding_stop_id];
    const alightB  = coordMap[r.legB_alighting_stop_id];
    r.walk_to_start    = boardA  ? walkInfo(ORIGIN.lat, ORIGIN.lon, boardA.lat, boardA.lon)     : null;
    r.walk_transfer    = (alightA && boardB) ? walkInfo(alightA.lat, alightA.lon, boardB.lat, boardB.lon) : null;
    r.walk_to_dest     = alightB ? walkInfo(alightB.lat, alightB.lon, DESTINATION.lat, DESTINATION.lon)  : null;
    r.total_min        = secToMin(r.duration_sec) + (r.walk_to_start?.min ?? 0) + (r.walk_transfer?.min ?? 0) + (r.walk_to_dest?.min ?? 0);
  }
}

function displayResults(directResults, transferResults) {
  const all = [
    ...directResults.map(r => ({ ...r, type: 'direct' })),
    ...transferResults.map(r => ({ ...r, type: 'transfer' })),
  ].sort((a, b) => a.total_min - b.total_min);

  const filtered = all.filter(r => r.total_min <= 60);
  const sep = '─'.repeat(80);
  console.log(`\n${sep}`);
  console.log(`RISULTATI ORDINATI PER DURATA TOTALE  (${TIME_FROM} - ${TIME_TO})`);
  console.log(sep);
  for (const r of filtered) {
    const w0 = r.walk_to_start ? `🚶${r.walk_to_start.min}min(${r.walk_to_start.m}m)` : '';
    const w2 = r.walk_to_dest  ? `🚶${r.walk_to_dest.min}min(${r.walk_to_dest.m}m)`   : '';
    if (r.type === 'direct') {
      console.log(`  [DIRETTO]   [${r.route_short}]  ${w0} + ${formatTime(r.dep)}→${formatTime(r.arr)}(${secToMin(r.duration_sec)}min) + ${w2}  = ${r.total_min}min totali`);
    } else {
      const wt = r.walk_transfer ? `🚶${r.walk_transfer.min}min(${r.walk_transfer.m}m)` : '';
      console.log(`  [TRASBORDO] [${r.routeA_short}]→[${r.routeB_short}]  ${w0} + ${formatTime(r.dep)}→arr${formatTime(r.arr_transfer)} ${wt} dep${formatTime(r.dep_transfer)}→${formatTime(r.arr)}(${secToMin(r.duration_sec)}min) + ${w2}  = ${r.total_min}min totali`);
    }
  }
  console.log(sep);
}

async function findTransferRoutes(db, originStopIds, destStopIds, tripOriginSeq, directRouteIds, originTimeMap) {
  console.log(`\n[transfer-finder] margine trasbordo: 2 min`);
  const t = performance.now();

  const legATripIds = Object.keys(tripOriginSeq);

  // Info trip leg A (direction_id, route_id)
  const legATrips = await db.collection('daily_trips').find(
    { trip_id: { $in: legATripIds } },
    { projection: { _id: 0, trip_id: 1, route_id: 1, direction_id: 1 } }
  ).toArray();
  const legATripInfo = {};
  for (const tr of legATrips) legATripInfo[tr.trip_id] = tr;

  // Tutte le fermate successive al boarding per i trip leg A
  const legAAfterBoarding = await db.collection('daily_stop_times').find(
    { trip_id: { $in: legATripIds } },
    { projection: { _id: 0, trip_id: 1, stop_id: 1, stop_sequence: 1, arrival_time: 1 } }
  ).toArray();

  // Build transferMap: stop_id -> earliest arrival + info leg A
  const originSet = new Set(originStopIds);
  const destSet   = new Set(destStopIds);
  const transferMap = {};
  for (const st of legAAfterBoarding) {
    const seq = parseInt(st.stop_sequence);
    if (seq <= tripOriginSeq[st.trip_id]) continue;
    if (destSet.has(st.stop_id) || originSet.has(st.stop_id)) continue;
    const arrSec = timeToSec(st.arrival_time);
    const info = legATripInfo[st.trip_id];
    if (!transferMap[st.stop_id] || arrSec < transferMap[st.stop_id].arrival_sec) {
      transferMap[st.stop_id] = {
        arrival_sec:  arrSec,
        arrival_time: st.arrival_time,
        trip_id:      st.trip_id,
        direction_id: info.direction_id,
        route_id:     info.route_id,
      };
    }
  }
  const transferStopIds = Object.keys(transferMap);

  // Espandi con closeStops
  const closeStopsDocs = await db.collection('daily_stops').find(
    { stop_id: { $in: transferStopIds }, closeStops: { $exists: true, $not: { $size: 0 } } },
    { projection: { stop_id: 1, closeStops: 1 } }
  ).toArray();
  for (const doc of closeStopsDocs) {
    const parent = transferMap[doc.stop_id];
    for (const csId of doc.closeStops) {
      if (destSet.has(csId) || originSet.has(csId)) continue;
      if (!transferMap[csId] || parent.arrival_sec < transferMap[csId].arrival_sec) {
        transferMap[csId] = { ...parent };
      }
    }
  }
  const expandedTransferStopIds = Object.keys(transferMap);
  console.log(`[transfer-finder] fermate trasbordo potenziali: ${transferStopIds.length} (+${expandedTransferStopIds.length - transferStopIds.length} closeStops)`);

  // Leg B candidates
  const legATripIdSet = new Set(legATripIds);
  const legBCandidates = await db.collection('daily_stop_times').find(
    { stop_id: { $in: expandedTransferStopIds } },
    { projection: { _id: 0, trip_id: 1, stop_id: 1, stop_sequence: 1, departure_time: 1 } }
  ).toArray();

  const legBTripTransfer = {};
  for (const st of legBCandidates) {
    if (legATripIdSet.has(st.trip_id)) continue;
    const transfer = transferMap[st.stop_id];
    const depSec = timeToSec(st.departure_time);
    if (depSec < transfer.arrival_sec + TRANSFER_MARGIN_SEC) continue;
    if (!(st.trip_id in legBTripTransfer) || depSec < timeToSec(legBTripTransfer[st.trip_id].dep_time)) {
      legBTripTransfer[st.trip_id] = {
        stop_id:           st.stop_id,
        stop_sequence:     parseInt(st.stop_sequence),
        dep_time:          st.departure_time,
        legA_trip_id:      transfer.trip_id,
        legA_arr_transfer: transfer.arrival_time,
        legA_route_id:     transfer.route_id,
        legA_direction_id: transfer.direction_id,
      };
    }
  }

  const legBTripIds = Object.keys(legBTripTransfer);
  console.log(`[transfer-finder] leg B candidati (timing ok): ${legBTripIds.length}`);

  const legBTrips = await db.collection('daily_trips').find(
    { trip_id: { $in: legBTripIds } },
    { projection: { _id: 0, trip_id: 1, route_id: 1, direction_id: 1 } }
  ).toArray();
  const legBTripInfo = {};
  for (const tr of legBTrips) legBTripInfo[tr.trip_id] = tr;

  const dirFilteredTripIds = legBTripIds.filter((tripId) => {
    const bInfo = legBTripInfo[tripId];
    return bInfo && bInfo.direction_id === legBTripTransfer[tripId].legA_direction_id;
  });
  console.log(`[transfer-finder] leg B con direction_id ok: ${dirFilteredTripIds.length}`);

  // Verifica destinazione + fetch arrival_time
  const legBDestTimes = await db.collection('daily_stop_times').find(
    { trip_id: { $in: dirFilteredTripIds }, stop_id: { $in: [...destSet] } },
    { projection: { _id: 0, trip_id: 1, stop_id: 1, stop_sequence: 1, arrival_time: 1 } }
  ).toArray();

  const validConnections = new Map();
  for (const dt of legBDestTimes) {
    const transfer = legBTripTransfer[dt.trip_id];
    if (!transfer || parseInt(dt.stop_sequence) <= transfer.stop_sequence) continue;
    const bInfo = legBTripInfo[dt.trip_id];
    if (!bInfo) continue;
    if (bInfo.route_id === transfer.legA_route_id) continue;
    if (directRouteIds.includes(transfer.legA_route_id)) continue;

    const originInfo = originTimeMap[transfer.legA_trip_id];
    if (!originInfo) continue;

    const duration = timeToSec(dt.arrival_time) - timeToSec(originInfo.dep);
    const key = `${transfer.legA_route_id}|${bInfo.route_id}`;
    if (duration > MAX_DURATION_SEC) continue;
    if (!validConnections.has(key) || duration < validConnections.get(key).duration_sec) {
      validConnections.set(key, {
        routeA_id:              transfer.legA_route_id,
        routeB_id:              bInfo.route_id,
        transfer_stop_id:       transfer.stop_id,
        dep:                    originInfo.dep,
        arr_transfer:           transfer.legA_arr_transfer,
        dep_transfer:           transfer.dep_time,
        arr:                    dt.arrival_time,
        duration_sec:           duration,
        legA_boarding_stop_id:  originInfo.stop_id,
        legA_alighting_stop_id: transfer.stop_id,
        legB_boarding_stop_id:  legBTripTransfer[dt.trip_id].stop_id,
        legB_alighting_stop_id: dt.stop_id,
      });
    }
  }

  if (validConnections.size === 0) {
    console.log('[transfer-finder] nessuna connessione con trasbordo trovata.');
    return [];
  }

  const allRouteIds      = [...new Set([...[...validConnections.values()].flatMap(c => [c.routeA_id, c.routeB_id])])];
  const allTransferStops = [...new Set([...[...validConnections.values()].map(c => c.transfer_stop_id)])];

  const [routesInfo, stopsInfo] = await Promise.all([
    db.collection('daily_routes').find({ route_id: { $in: allRouteIds } }, { projection: { _id: 0, route_id: 1, route_short_name: 1 } }).toArray(),
    db.collection('daily_stops').find({ stop_id: { $in: allTransferStops } }, { projection: { _id: 0, stop_id: 1, stop_name: 1 } }).toArray(),
  ]);

  const routeMap = Object.fromEntries(routesInfo.map(r => [r.route_id, r.route_short_name]));
  const stopMap  = Object.fromEntries(stopsInfo.map(s => [s.stop_id, s.stop_name]));

  const results = [];
  for (const conn of validConnections.values()) {
    results.push({
      routeA_short:           routeMap[conn.routeA_id]  || conn.routeA_id,
      routeB_short:           routeMap[conn.routeB_id]  || conn.routeB_id,
      transfer_stop:          stopMap[conn.transfer_stop_id] || conn.transfer_stop_id,
      dep:                    conn.dep,
      arr_transfer:           conn.arr_transfer,
      dep_transfer:           conn.dep_transfer,
      arr:                    conn.arr,
      duration_sec:           conn.duration_sec,
      legA_boarding_stop_id:  conn.legA_boarding_stop_id,
      legA_alighting_stop_id: conn.legA_alighting_stop_id,
      legB_boarding_stop_id:  conn.legB_boarding_stop_id,
      legB_alighting_stop_id: conn.legB_alighting_stop_id,
    });
  }
  console.log(`[transfer-finder] completato in ${ms(t)}`);
  return results;
}

async function findRoutesBetween(db, origin, destination) {
  console.log(`\n[route-finder] finestra oraria: ${TIME_FROM} - ${TIME_TO} | raggio: ${SEARCH_RADIUS}m`);
  const t = performance.now();

  const stopsNear = async (point) => db.collection('daily_stops').aggregate([
    {
      $geoNear: {
        near: { type: 'Point', coordinates: [point.lon, point.lat] },
        distanceField: 'dist',
        maxDistance: SEARCH_RADIUS,
        spherical: true,
      },
    },
    { $project: { _id: 0, stop_id: 1, stop_name: 1, dist: { $round: ['$dist', 0] } } },
  ]).toArray();

  const [originStops, destStops] = await Promise.all([stopsNear(origin), stopsNear(destination)]);
  const originStopIds = originStops.map((s) => s.stop_id);
  const destStopIds   = destStops.map((s) => s.stop_id);

  console.log(`[route-finder] fermate vicino origine: ${originStops.length} | destinazione: ${destStops.length}`);

  const originTimes = await db.collection('daily_stop_times').find(
    { stop_id: { $in: originStopIds }, departure_time: { $gte: TIME_FROM, $lte: TIME_TO } },
    { projection: { _id: 0, trip_id: 1, stop_id: 1, stop_sequence: 1, departure_time: 1 } }
  ).toArray();

  // trip_id -> stop_sequence e departure_time minimi all'origine
  const tripOriginSeq = {};
  const originTimeMap = {}; // trip_id -> { dep, stop_id }
  for (const st of originTimes) {
    const seq = parseInt(st.stop_sequence);
    if (!(st.trip_id in tripOriginSeq) || seq < tripOriginSeq[st.trip_id]) {
      tripOriginSeq[st.trip_id] = seq;
      originTimeMap[st.trip_id] = { dep: st.departure_time, stop_id: st.stop_id };
    }
  }
  const candidateTripIds = Object.keys(tripOriginSeq);
  console.log(`[route-finder] trip nell'orario: ${candidateTripIds.length}`);

  // Trova trip validi che raggiungono la destinazione
  const destTimes = await db.collection('daily_stop_times').find(
    { trip_id: { $in: candidateTripIds }, stop_id: { $in: destStopIds } },
    { projection: { _id: 0, trip_id: 1, stop_sequence: 1, arrival_time: 1 } }
  ).toArray();

  const validTripIds = new Set();
  const tripDestMap = {}; // trip_id -> { arr, stop_id }
  for (const dt of destTimes) {
    if (parseInt(dt.stop_sequence) > tripOriginSeq[dt.trip_id]) {
      validTripIds.add(dt.trip_id);
      if (!tripDestMap[dt.trip_id] || dt.arrival_time < tripDestMap[dt.trip_id].arr) {
        tripDestMap[dt.trip_id] = { arr: dt.arrival_time, stop_id: dt.stop_id };
      }
    }
  }
  console.log(`[route-finder] trip validi (direzione corretta): ${validTripIds.size}`);

  if (validTripIds.size === 0) {
    console.log('[route-finder] nessuna route diretta trovata.');
  }

  // Route dai trip validi
  const validTripInfos = await db.collection('daily_trips').find(
    { trip_id: { $in: [...validTripIds] } },
    { projection: { _id: 0, trip_id: 1, route_id: 1 } }
  ).toArray();
  const tripRouteMap = Object.fromEntries(validTripInfos.map(t => [t.trip_id, t.route_id]));

  // Per ogni route, trova il trip con durata minima
  const directRouteMap = {};
  for (const tripId of validTripIds) {
    const routeId = tripRouteMap[tripId];
    if (!routeId) continue;
    const origin = originTimeMap[tripId];
    const dest   = tripDestMap[tripId];
    if (!origin || !dest) continue;
    const duration = timeToSec(dest.arr) - timeToSec(origin.dep);
    if (!directRouteMap[routeId] || duration < directRouteMap[routeId].duration_sec) {
      directRouteMap[routeId] = {
        dep:               origin.dep,
        arr:               dest.arr,
        duration_sec:      duration,
        boarding_stop_id:  origin.stop_id,
        alighting_stop_id: dest.stop_id,
      };
    }
  }

  const validRouteIds = Object.keys(directRouteMap);
  const routes = validRouteIds.length > 0
    ? await db.collection('daily_routes').find({ route_id: { $in: validRouteIds } }, { projection: { _id: 0, route_id: 1, route_short_name: 1, route_long_name: 1 } }).toArray()
    : [];

  const directResults = routes.map(r => ({
    route_short:       r.route_short_name,
    route_long:        r.route_long_name,
    dep:               directRouteMap[r.route_id].dep,
    arr:               directRouteMap[r.route_id].arr,
    duration_sec:      directRouteMap[r.route_id].duration_sec,
    boarding_stop_id:  directRouteMap[r.route_id].boarding_stop_id,
    alighting_stop_id: directRouteMap[r.route_id].alighting_stop_id,
  }));

  console.log(`[route-finder] completato in ${ms(t)}`);

  const transferResults = await findTransferRoutes(db, originStopIds, destStopIds, tripOriginSeq, validRouteIds, originTimeMap);
  await enrichWithWalking(db, directResults, transferResults);
  displayResults(directResults, transferResults);
}

async function main() {
  const tTotal = performance.now();

  const client = new MongoClient(MONGO_URI);
  await client.connect();
  const db = client.db(DB_NAME);

  if (SKIP_IMPORT) {
    console.log('[--skip-import] uso dati già presenti nel DB\n');
    await findRoutesBetween(db, ORIGIN, DESTINATION);
    await client.close();
    console.log(`\n[total] ${ms(tTotal)}`);
    return;
  }

  const zipBuffer = await downloadZip(GTFS_URL);
  const files = extractFiles(zipBuffer);

  // Drop entire database to start fresh
  const tDrop = performance.now();
  await db.dropDatabase();
  console.log(`[drop DB] ${DB_NAME} dropped — ${ms(tDrop)}`);

  console.log(`[mongo] connected — database: ${DB_NAME}\n`);

  const tImport = performance.now();
  for (const name of GTFS_FILES) {
    const filename = `${name}.txt`;
    if (!files[filename]) {
      console.log(`  [${name}] not in feed, skipped`);
      continue;
    }
    try {
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
    } catch (err) {
      console.error(`\n  [${name}] ERROR — ${err.message}`);
    }
  }

  console.log(`\n[import total] ${ms(tImport)}`);

  // --- daily_trips ---
  const today = new Date().toISOString().slice(0, 10).replace(/-/g, ''); // es. '20260406'
  console.log(`\n[daily_trips] data: ${today}`);
  const tDaily = performance.now();

  const activeServices = await db.collection('calendar_dates').distinct('service_id', {
    date: today,
    exception_type: '1',
  });
  console.log(`[daily_trips] service_id attivi: ${activeServices.length}`);

  await db.collection('daily_trips').drop().catch(() => {});
  const dailyTrips = await db.collection('trips').find({ service_id: { $in: activeServices } }).toArray();
  if (dailyTrips.length > 0) {
    await db.collection('daily_trips').insertMany(dailyTrips, { ordered: false });
  }
  console.log(`[daily_trips] ${dailyTrips.length.toLocaleString()} trips inseriti — ${ms(tDaily)}`);

  // --- daily_routes (solo agency_id = 'U') ---
  const tRoutes = performance.now();
  const dailyRouteIds = await db.collection('daily_trips').distinct('route_id');
  await db.collection('daily_routes').drop().catch(() => {});
  const dailyRoutes = await db.collection('routes').find({ route_id: { $in: dailyRouteIds }, agency_id: 'U' }).toArray();
  if (dailyRoutes.length > 0) {
    await db.collection('daily_routes').insertMany(dailyRoutes, { ordered: false });
  }
  console.log(`[daily_routes] ${dailyRoutes.length.toLocaleString()} routes inserite (agency_id='U') — ${ms(tRoutes)}`);

  // --- filtra daily_trips per le route agency 'U' ---
  const filteredRouteIds = dailyRoutes.map((r) => r.route_id);
  await db.collection('daily_trips').deleteMany({ route_id: { $nin: filteredRouteIds } });
  const filteredTripsCount = await db.collection('daily_trips').countDocuments();
  console.log(`[daily_trips] dopo filtro agency: ${filteredTripsCount.toLocaleString()} trips`);

  // --- daily_shapes ---
  const tShapes = performance.now();
  const dailyShapeIds = await db.collection('daily_trips').distinct('shape_id');
  await db.collection('daily_shapes').drop().catch(() => {});
  const dailyShapes = await db.collection('shapes').find({ shape_id: { $in: dailyShapeIds } }).toArray();
  if (dailyShapes.length > 0) {
    await db.collection('daily_shapes').insertMany(dailyShapes, { ordered: false });
  }
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

  // --- routes per fermata -> daily_stops.routes ---
  const tStopRoutes = performance.now();
  const stopRoutes = await db.collection('daily_stop_times').aggregate([
    { $group: { _id: '$stop_id', trip_ids: { $addToSet: '$trip_id' } } },
    { $lookup: { from: 'daily_trips', localField: 'trip_ids', foreignField: 'trip_id', as: 'trips' } },
    { $project: { _id: 0, stop_id: '$_id', routes: { $setUnion: '$trips.route_id' } } },
  ]).toArray();

  const bulk = stopRoutes.map((r) => ({
    updateOne: {
      filter: { stop_id: r.stop_id },
      update: { $set: { routes: r.routes } },
    },
  }));
  if (bulk.length > 0) await db.collection('daily_stops').bulkWrite(bulk);
  console.log(`[daily_stops.routes] ${bulk.length.toLocaleString()} fermate aggiornate — ${ms(tStopRoutes)}`);

  // --- closeStops: 10 fermate più vicine entro 200m ---
  const tClose = performance.now();
  const allStops = await db.collection('daily_stops')
    .find({}, { projection: { stop_id: 1, location: 1 } })
    .toArray();

  const BULK_SIZE = 500;
  let closeBulk = [];
  for (const stop of allStops) {
    if (!stop.location) continue;
    const nearby = await db.collection('daily_stops').aggregate([
      {
        $geoNear: {
          near: stop.location,
          distanceField: 'dist',
          maxDistance: 250,
          spherical: true,
          query: { stop_id: { $ne: stop.stop_id } },
        },
      },
      { $limit: 10 },
      { $project: { _id: 0, stop_id: 1 } },
    ]).toArray();

    closeBulk.push({
      updateOne: {
        filter: { stop_id: stop.stop_id },
        update: { $set: { closeStops: nearby.map((s) => s.stop_id) } },
      },
    });

    if (closeBulk.length >= BULK_SIZE) {
      await db.collection('daily_stops').bulkWrite(closeBulk);
      closeBulk = [];
    }
  }
  if (closeBulk.length > 0) await db.collection('daily_stops').bulkWrite(closeBulk);
  console.log(`[daily_stops.closeStops] ${allStops.length.toLocaleString()} fermate elaborate — ${ms(tClose)}`);

  // --- indici per l'algoritmo di ricerca ---
  const tIdx = performance.now();
  await Promise.all([
    db.collection('daily_stop_times').createIndex({ stop_id: 1, departure_time: 1 }),
    db.collection('daily_stop_times').createIndex({ trip_id: 1, stop_id: 1 }),
    db.collection('daily_trips').createIndex({ trip_id: 1 }),
    db.collection('daily_trips').createIndex({ route_id: 1 }),
    db.collection('daily_stops').createIndex({ stop_id: 1 }),
    db.collection('daily_routes').createIndex({ route_id: 1 }),
  ]);
  console.log(`[indexes] creati — ${ms(tIdx)}`);

  // --- drop collections base ---
  const tDrop2 = performance.now();
  const toDrop = ['calendar_dates', 'routes', 'shapes', 'stop_times', 'stops', 'trips'];
  for (const name of toDrop) {
    await db.collection(name).drop().catch(() => {});
  }
  console.log(`\n[drop collections] ${toDrop.join(', ')} — ${ms(tDrop2)}`);

  await findRoutesBetween(db, ORIGIN, DESTINATION);

  await client.close();
  console.log(`\n[total] ${ms(tTotal)}`);
}

main().catch((err) => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
