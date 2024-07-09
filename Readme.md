```js
const Insight = require('@rdev06/insight');

const logger = new Insight();

const imongo = require('@rdev06/imongo');

const { MongoClient } = require('mongodb');
const express = require('express');
const client = new MongoClient('mongodb://localhost:27017');

function App() {
  const app = express();
  const inbound = logger.inbound();

  app.use(express.json({ limit: '50mb' }));
  app.use(inbound.mdw);
  app.post('/app', (req, res) => {
    // console.log(req.query);
    res.set('one-more', 'injected-headers');
    logger.customEvent('some-event', { foo: 'bar' }, 'info');
    throw 'error here';
    res.status(201).json({ foo: 'zar' });
    // res.send('hell')
  });

  // app.use(inbound.errMdw);
  return app;
}

async function server() {
  await client.connect();
  const MongoTr = await imongo(client, {user: 'mongo-user', pass: 'mongo-pass', label: 'default', dbName: 'LOG'});
  await logger.init([MongoTr]);
  logger.axios();
  logger.traceLogger();
  const app = App();
  app.listen(3001, ()=> console.log('Server is up'));
  return true;
}

server().catch(console.error);
```