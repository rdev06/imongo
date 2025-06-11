import { MongoClient } from 'mongodb';
import pkg from './package.json' with {type: 'json'};

const updateColls = ['Inbound', 'OutBound'];

const inboundPipeline = (ttlField = 'timestamp') => [
  {
    $group: {
      _id: '$_id',
      req: {
        $mergeObjects: '$req'
      },
      txn: { $mergeObjects: '$$ROOT' }
    }
  },
  {
    $addFields: {
      'txn.req': '$req'
    }
  },
  {
    $replaceRoot: {
      newRoot: '$txn'
    }
  },
  { $sort: { [ttlField]: -1 } }
];

const outboundPipeline = (ttlField = 'timestamp') => [
  {
    $group: {
      _id: '$_id',
      txn: { $mergeObjects: '$$ROOT' }
    }
  },
  {
    $replaceRoot: {
      newRoot: '$txn'
    }
  },
  { $sort: { [ttlField]: -1 } }
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
  return async function (collectionName, ttlOption) {
    let logger;
    let colName = collectionName;
    if (updateColls.includes(collectionName)) colName += '_a';
    if (!createCol) logger = await db.collection(colName);
    else {
      const timeField = ttlOption.field || 'timestamp';
      logger = await db.createCollection(colName, {
        expireAfterSeconds: ttlOption.expireAfterSeconds || 172800000,
        timeseries: { timeField, granularity: 'seconds', metaField: 'label' }
      });
      logger.createIndex({ [timeField]: -1 });
    }

    if (createCol) {
      if (collectionName === 'Inbound') {
        await db.createCollection(collectionName, { viewOn: 'Inbound_a', pipeline: inboundPipeline(ttlOption.field) });
      } else if (collectionName === 'OutBound') {
        await db.createCollection(collectionName, { viewOn: 'OutBound_a', pipeline: outboundPipeline(ttlOption.field) });
      }
    }

    return {
      create: async (d) => {
        const inserted = await logger.insertOne({ ...d, label: option.label });
        return inserted.insertedId;
      },
      update: (_id, d) => logger.insertOne({ _id, ...d, label: option.label }),
      delete: (_id) => logger.deleteOne({ _id }),
      error: (d) => logger.insertOne(Object.assign(d, { label: option.label, flag: 'error' }))
    };
  };
}
