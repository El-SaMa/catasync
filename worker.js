require('dotenv').config();
const amqp = require('amqplib');
const axios = require('axios');

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUES = (process.env.QUEUES || '').split(',');
const WP_CALLBACK_URL = process.env.WP_CALLBACK_URL;
const WORKER_SECRET = process.env.WORKER_SECRET;
const WORKER_NAME = process.env.WORKER_NAME || 'catasync-worker';

if (!RABBITMQ_URL || !QUEUES.length || !WP_CALLBACK_URL || !WORKER_SECRET) {
  console.error('Missing required .env config.');
  process.exit(1);
}

async function start() {
  const conn = await amqp.connect(RABBITMQ_URL);
  const ch = await conn.createChannel();
  for (const queue of QUEUES) {
    await ch.assertQueue(queue, { durable: true });
    ch.consume(queue, async (msg) => {
      if (msg !== null) {
        const content = msg.content.toString();
        console.log(`[${WORKER_NAME}] Received job from ${queue}:`, content);
        // Example: send job result to WP callback
        try {
          await axios.post(WP_CALLBACK_URL, {
            queue,
            payload: content,
            worker: WORKER_NAME,
            secret: WORKER_SECRET
          });
          ch.ack(msg);
          console.log(`[${WORKER_NAME}] Job processed and acknowledged.`);
        } catch (err) {
          console.error(`[${WORKER_NAME}] Callback failed:`, err.message);
          // Optionally: ch.nack(msg);
        }
      }
    });
    console.log(`[${WORKER_NAME}] Listening on queue: ${queue}`);
  }
  console.log(`[${WORKER_NAME}] Worker started.`);
}

start().catch(err => {
  console.error('Worker error:', err);
  process.exit(1);
});
