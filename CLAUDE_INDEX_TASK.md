# Task: Aggiungere indici mancanti post-import

## Contesto
Il repo `ImportGTFS_MONGODB` importa dati GTFS statici in MongoDB Atlas e ricrea le collection `daily_*` da zero ad ogni esecuzione. Gli indici devono quindi essere creati dentro `import.js` dopo l'inserimento dei dati.

Il file `import.js` ha **due sezioni** dove vengono creati gli indici:
1. **Locale** (riga ~319): `Promise.all([...])` dopo la build locale
2. **Remoto** (riga ~375): `Promise.all([...])` dopo il sync su Atlas

Entrambe le sezioni devono essere aggiornate con gli stessi indici.

## Indici da aggiungere

### 1. `daily_shapes` → `{ shape_id: 1 }`
**Perché:** L'endpoint `/getTripDetails` fa `find({ shape_id: trip.shape_id })` su 32.000+ documenti. Attualmente non c'è nessun indice su `shape_id` → full collection scan ad ogni chiamata.

### 2. `daily_trips` → `{ route_id: 1, trip_headsign: 1 }` (compound)
**Perché:** L'endpoint `/gtfs/vehicles/byroute?trip_headsign=...` fa `find({ route_id, trip_headsign })`. L'indice singolo `{ route_id: 1 }` esiste già ma non copre il filtro compound con `trip_headsign`.

## Come fare la modifica

In `import.js` trovare le due sezioni `Promise.all` con i `createIndex` e aggiungere le due righe mancanti in entrambe:

```js
db.collection('daily_shapes').createIndex({ shape_id: 1 }),
db.collection('daily_trips').createIndex({ route_id: 1, trip_headsign: 1 }),
```

**Attenzione:** la sezione locale usa `db` come variabile, quella remota usa `remoteDb`. Assicurarsi di usare la variabile corretta in ciascuna sezione.

## Verifica
Dopo la modifica, controllare che entrambe le sezioni contengano questi indici. Non serve eseguire l'import — il test è visivo sul codice.
