import { MongoClient } from 'mongodb';
import pkg from './package.json' with {type: 'json'};

const TxnCols = ['Inbound', 'OutBound'];

async function checkConnection(db, option) {
  if (typeof db === 'string') {
    const parseUri = new URL(db);
    const username = parseUri.username || option.user;
    const password = parseUri.password || option.pass;
    const pathname = parseUri.pathname.slice(1) || option.dbName || 'LOGS';
    db = parseUri.href;
    const mongoOptions = {};
    if (username && password) {
      mongoOptions['auth'] = { username, password };
    }
    const client = new MongoClient(parseUri.href, mongoOptions);
    await client.connect();
    db = client.db(pathname);
  }
  if (typeof db.readyState === 'number') {
    if (!db.readyState) throw 'Provided client is not active';
    if (db.useDb) db = db.useDb(option.dbName);
  } else if (!db instanceof MongoClient) {
    throw 'Unknown Mongodb client';
  } else if (db.db) {
    db = db.db(option.dbName);
  }
  return db;
}

export default async function (db, option) {
  db = await checkConnection(db, option);
  const version = await db.collection('config').findOne({ name: 'version' }, { projection: { _id: 0, value: 1 } });
  let createCol = false;
  if (!version || version.value < pkg.version) {
    await db.dropDatabase();
    await db.collection('config').insertOne({ name: 'version', value: pkg.version });
    createCol = true;
  }
  if (!option.label) {
    option.label = 'default';
  }
  return async function (collectionName, ttlOption) {
    let logger;
    const isTxnCols = TxnCols.includes(collectionName);
    if (!createCol) logger = await db.collection(collectionName);
    else {
      const timeField = ttlOption.field || 'timestamp';
      const timeseriesOpts = {
        expireAfterSeconds: ttlOption.expireAfterSeconds || 172800000,
        timeseries: { timeField, granularity: 'seconds', metaField: 'meta' }
      }

      logger = await db.createCollection(collectionName, timeseriesOpts);
      logger.createIndex({ [timeField]: -1 });
    }

    return {
      create: async (d) => {
        if(d.hasOwnProperty('_id') && d.hasOwnProperty('meta')){
          d.meta._id = d._id
        }
        const inserted = await logger.insertOne({ ...d, label: option.label });
        return inserted.insertedId;
      },
      update: (_id, d) => {
        // we know generally this only when txn is their
        const toUpdate = {};
        for (const k in d) {
          toUpdate[`meta.${k}`] = d[k]         
        }
        return logger.updateMany({'meta._id': _id}, {$set: toUpdate})
      },
      delete: (_id) => logger.deleteMany({'meta._id': _id}), // will delete only in timeseries coll, else not
      error: (d) => logger.insertOne(Object.assign(d, { label: option.label, flag: 'error' }))
    };
  };
}
