import { MongoClient } from 'mongodb';

async function checkConnection(db, option) {
  if (typeof db === 'string') {
    const parseUri = new URL(db);
    const username = parseUri.username || option.user;
    const password = parseUri.password || option.pass;
    const pathname = parseUri.pathname.slice(1) || option.dbName || 'LOGS';
    db = parseUri.href;
    const client = new MongoClient(parseUri.href, {username, password});
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
  if (!option.label) {
    option.label = 'default';
  }
  return async function (collectionName, ttlfield = 'timestamp', expireAfterSeconds = 172800000) {
    const logger = await db
      .createCollection(collectionName)
      .then(async (col) => {
        await col.createIndex({ [ttlfield]: 1 }, { background: true, expireAfterSeconds });
        return col;
      })
      .catch(async (err) => {
        if (err.code != 48) throw err;
        const col = db.collection(collectionName);
        const ttlIndexName = ttlfield + '_1';
        const prevTtlInfo = (await col.indexes()).find((e) => e.name === ttlIndexName);
        if (!prevTtlInfo || prevTtlInfo.expireAfterSeconds != expireAfterSeconds) {
          prevTtlInfo && (await col.dropIndex(ttlIndexName));
          await col.createIndex({ timestamp: 1 }, { background: true, expireAfterSeconds });
        }
        return col;
      });
    return {
      create: async (d) => {
        const inserted = await logger.insertOne(Object.assign(d, { label: option.label, flag: 'info' }));
        return inserted.insertedId;
      },
      update: (_id, d) => logger.updateOne({ _id }, { $set: {...d, label: option.label} }, { upsert: true }),
      delete: (_id) => logger.deleteOne({ _id }),
      error: (d) => logger.insertOne(Object.assign(d, { label: option.label, flag: 'error' }))
    };
  };
};
