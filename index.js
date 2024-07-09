import { MongoClient } from 'mongodb';
import pkg from './package.json' with {type: 'json'};

const updateColls = ['inbound', 'axios'];

const inboundPipeline = [
  {
    $lookup: {
      from: 'inbound_u',
      localField: '_id',
      foreignField: '_id',
      as: 'res'
    }
  },
  {
    $addFields: {
      res: {
        $reduce: {
          input: '$res',
          initialValue: {},
          in: { $mergeObjects: ['$$value', '$$this'] }
        }
      }
    }
  },
  {
    $addFields: {
      error: '$res.error',
      duration: '$res.duration',
      statusCode: '$res.statusCode',
      res: '$res.res'
    }
  }
];

const axiosPipeline = [
  {
    $lookup: {
      from: 'axios_u',
      localField: '_id',
      foreignField: '_id',
      as: 'res'
    }
  },
  {
    $addFields: {
      duration: { $first: '$res.duration' },
      res: { $first: '$res.res' }
    }
  }
];

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
  return async function (collectionName, ttlfield = 'timestamp', expireAfterSeconds = 172800000, type) {
    const logger = { create: null };
    for (const a of ['create', 'update']) {
      let colName = type;
      if (!updateColls.includes(type)) {
        if (a === 'update') continue;
      } else colName += a === 'create' ? '_c' : '_u';
      if (!createCol) logger[a] = await db.collection(colName);
      else {
        logger[a] = await db.createCollection(colName, {
          expireAfterSeconds,
          timeseries: { timeField: ttlfield, granularity: 'seconds', metaField: 'label' }
        });
        await db.collection(colName).createIndex({ _id: 1 });
      }
    }

    if (createCol) {
      if (type === 'inbound') {
        await db.createCollection(collectionName, { viewOn: 'inbound_c', pipeline: inboundPipeline });
      } else if (type === 'axios') {
        await db.createCollection(collectionName, { viewOn: 'axios_c', pipeline: axiosPipeline });
      }
    }

    return {
      create: async (d) => {
        const inserted = await logger.create.insertOne({ ...d, label: option.label });
        return inserted.insertedId;
      },
      update: (_id, d) => logger.update.insertOne({ _id, ...d, label: option.label }),
      delete: (_id) => logger.create.deleteOne({ _id }),
      error: (d) => logger.create.insertOne(Object.assign(d, { label: option.label, flag: 'error' }))
    };
  };
}
