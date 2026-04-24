require('dotenv').config();

const amqp = require('amqplib');
const axios = require('axios');
const crypto = require('crypto');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');
const { OpenAI } = require('openai');
// OpenAI API setup
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-3.5-turbo'; // cost-effective default
let openai = null;
if (OPENAI_API_KEY) {
  openai = new OpenAI({ apiKey: OPENAI_API_KEY });
}

async function runOpenAICompletion(prompt, opts = {}) {
  if (!openai) throw new Error('OpenAI API key not set');
  const model = opts.model || OPENAI_MODEL;
  const messages = opts.messages || [
    { role: 'system', content: 'You are a helpful assistant.' },
    { role: 'user', content: prompt },
  ];
  const response = await openai.chat.completions.create({
    model,
    messages,
    max_tokens: opts.max_tokens || 512,
    temperature: opts.temperature || 0.7,
  });
  return response.choices[0]?.message?.content || '';
}

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUES = (process.env.QUEUES || '').split(',').map((q) => q.trim()).filter(Boolean);
const WP_CALLBACK_URL = process.env.WP_CALLBACK_URL;
const WP_STATUS_URL = process.env.WP_STATUS_URL || (WP_CALLBACK_URL || '').replace(/action=[^&]+/, 'action=catasync_worker_status_ping');
const WORKER_SECRET = process.env.WORKER_SECRET;
const WORKER_NAME = process.env.WORKER_NAME || os.hostname() || 'catasync-worker';
const WP_PATH = path.resolve(__dirname, process.env.WP_PATH || '../public_html');
const WP_BIN = process.env.WP_BIN || 'wp';
const IMPORT_TIMEOUT_MS = parseInt(process.env.IMPORT_TIMEOUT_MS || String(20 * 60 * 1000), 10);
const PREFETCH = Math.max(1, parseInt(process.env.PREFETCH || '1', 10));
const STATUS_PING_INTERVAL_MS = Math.max(3000, parseInt(process.env.STATUS_PING_INTERVAL_MS || '5000', 10));
let currentProcess = '';

if (!RABBITMQ_URL || !QUEUES.length || !WP_CALLBACK_URL || !WORKER_SECRET) {
  console.error('Missing required .env config: RABBITMQ_URL, QUEUES, WP_CALLBACK_URL, WORKER_SECRET.');
  process.exit(1);
}

function log(message, data) {
  const suffix = data ? ` ${JSON.stringify(data)}` : '';
  console.log(`[${WORKER_NAME}] ${message}${suffix}`);
}

function signBody(body) {
  const ts = String(Math.floor(Date.now() / 1000));
  const sig = crypto.createHmac('sha256', WORKER_SECRET).update(`${ts}.${body}`).digest('hex');
  return {
    'Content-Type': 'application/json',
    'X-Wave-Offload-Timestamp': ts,
    'X-Wave-Offload-Signature': sig,
  };
}

async function postSignedCallback(payload) {
  const body = JSON.stringify({ domain: WORKER_NAME, ...payload });
  return axios.post(WP_CALLBACK_URL, body, {
    headers: signBody(body),
    timeout: 30000,
    validateStatus: (status) => status >= 200 && status < 300,
  });
}

async function pingStatus() {
  if (!WP_STATUS_URL) {
    return;
  }
  const params = new URLSearchParams();
  params.set('worker', WORKER_NAME);
  params.set('secret', WORKER_SECRET);
  params.set('cpu', String(os.loadavg()[0] || 0));
  params.set('ram', String((os.totalmem() - os.freemem()) / Math.max(1, os.totalmem())));
  params.set('running', currentProcess ? '1' : '0');
  params.set('process', currentProcess);
  params.set('current_process', currentProcess);
  params.set('currentProcess', currentProcess);
  try {
    await axios.post(WP_STATUS_URL, params, { timeout: 10000 });
  } catch (err) {
    log('status ping failed', { error: err.message });
  }
}

function wpEval(code) {
  return new Promise((resolve, reject) => {
    execFile(
      WP_BIN,
      [`--path=${WP_PATH}`, 'eval', code],
      { timeout: IMPORT_TIMEOUT_MS, maxBuffer: 1024 * 1024 },
      (error, stdout, stderr) => {
        if (error) {
          error.stdout = stdout;
          error.stderr = stderr;
          reject(error);
          return;
        }
        resolve({ stdout, stderr });
      }
    );
  });
}

async function importStagingId(stagingId) {
  const sid = parseInt(stagingId, 10);
  if (!sid || sid <= 0) {
    throw new Error('Missing staging_id for import.');
  }

  const code = `
    $sid = ${sid};
    if ( ! class_exists( '\\CataSync\\Product_Importer' ) ) {
      fwrite( STDERR, 'CataSync Product_Importer is not loaded.' );
      exit( 10 );
    }
    $res = \\CataSync\\Product_Importer::import_staging_id( $sid );
    if ( is_wp_error( $res ) ) {
      fwrite( STDERR, $res->get_error_code() . ': ' . $res->get_error_message() );
      exit( 20 );
    }
    echo (int) $res;
  `;

  const result = await wpEval(code);
  const productId = parseInt(String(result.stdout || '').trim(), 10);
  if (!productId || productId <= 0) {
    throw new Error(`Importer returned invalid product id: ${String(result.stdout || '').trim()}`);
  }
  return productId;
}

function parseJob(content) {
  const job = JSON.parse(content);
  if (!job || typeof job !== 'object') {
    throw new Error('Job JSON is not an object.');
  }
  return job;
}

function describeJob(queue, job) {
  const feature = String((job && (job.feature_key || job.type)) || 'job');
  const stagingId = job && job.payload ? parseInt(job.payload.staging_id, 10) : 0;
  if (stagingId > 0 && (feature === 'wave.import.execute' || feature === 'waveimportexecute')) {
    return `Import staging #${stagingId}`;
  }
  return `${feature} on ${queue}`;
}

async function fetchImportedProductState(productId) {
  const pid = parseInt(productId, 10);
  if (!pid || pid <= 0) {
    throw new Error('Missing product id for post-import validation.');
  }

  const code = `
    $pid = ${pid};
    $p = get_post( $pid );
    if ( ! $p || 'product' !== $p->post_type ) {
      fwrite( STDERR, 'Imported product not found.' );
      exit( 30 );
    }
    $thumb = (int) get_post_thumbnail_id( $pid );
    $gallery_raw = (string) get_post_meta( $pid, '_product_image_gallery', true );
    $gallery = array_values( array_filter( array_map( 'intval', explode( ',', $gallery_raw ) ) ) );
    echo wp_json_encode( array(
      'title' => get_the_title( $pid ),
      'desc' => (string) $p->post_content,
      'short_desc' => (string) $p->post_excerpt,
      'thumbnail_id' => $thumb,
      'gallery_count' => count( $gallery ),
      'image_count' => ( $thumb > 0 ? 1 : 0 ) + count( $gallery ),
    ) );
  `;

  const result = await wpEval(code);
  return JSON.parse(result.stdout || '{}');
}

async function handleJob(queue, job) {
  const feature = String(job.feature_key || job.type || '');
  if (feature !== 'wave.import.execute' && feature !== 'waveimportexecute') {
    log('unsupported job acknowledged', { queue, feature });
    return;
  }

  const stagingId = job.payload && job.payload.staging_id;
  currentProcess = `Import staging #${stagingId}: enrich and create product`;
  await pingStatus();
  const productId = await importStagingId(stagingId);
  currentProcess = `Import staging #${stagingId}: validate enrichment and images`;
  await pingStatus();
  const productFields = await fetchImportedProductState(productId);

  // Check for missing fields
  const missing = [];
  if (!productFields.title || productFields.title.trim() === '') missing.push('title');
  if (!productFields.desc || productFields.desc.trim() === '') missing.push('desc');
  if (!productFields.short_desc || productFields.short_desc.trim() === '') missing.push('short_desc');
  if (!productFields.image_count || parseInt(productFields.image_count, 10) <= 0) missing.push('images');

  if (missing.length > 0) {
    throw new Error(`Import incomplete after worker run; missing ${missing.join(', ')}.`);
  }

  // Log enrichment status
  log('enriched via worker', { productId, images: productFields.image_count });

  await postSignedCallback({
    job_id: job.job_id || '',
    feature_key: feature,
    status: 'done',
    staging_id: parseInt(stagingId, 10),
    product_id: productId,
    enriched_fields: ['title', 'desc', 'short_desc'].filter((k) => {
      return productFields[k] && String(productFields[k]).trim() !== '';
    }),
    image_count: parseInt(productFields.image_count, 10) || 0,
    missing_fields: [],
  });
  log('import completed', { staging_id: parseInt(stagingId, 10), product_id: productId, images: productFields.image_count });
}

async function start() {
  const conn = await amqp.connect(RABBITMQ_URL);
  const ch = await conn.createChannel();
  await ch.prefetch(PREFETCH);

  await pingStatus();
  setInterval(pingStatus, STATUS_PING_INTERVAL_MS).unref();

  for (const queue of QUEUES) {
    await ch.assertQueue(queue, { durable: true });
    await ch.consume(queue, async (msg) => {
      if (!msg) {
        return;
      }

      let job = null;
      try {
        job = parseJob(msg.content.toString());
        currentProcess = describeJob(queue, job);
        await pingStatus();
        log('received job', { queue, job_id: job.job_id || '', feature_key: job.feature_key || '' });
        await handleJob(queue, job);
        ch.ack(msg);
      } catch (err) {
        const stagingId = job && job.payload ? parseInt(job.payload.staging_id, 10) : 0;
        log('job failed', { queue, staging_id: stagingId, error: err.message, stderr: err.stderr || '' });
        if (stagingId > 0) {
          try {
            await postSignedCallback({
              job_id: job.job_id || '',
              feature_key: job.feature_key || '',
              status: 'failed',
              staging_id: stagingId,
              error: err.stderr || err.message || 'Worker import failed.',
            });
          } catch (callbackErr) {
            log('failure callback failed', { error: callbackErr.message });
            ch.nack(msg, false, true);
            return;
          }
        }
        ch.ack(msg);
      } finally {
        currentProcess = '';
        await pingStatus();
      }
    });
    log('listening', { queue });
  }
  log('worker started', { queues: QUEUES, wp_path: WP_PATH, prefetch: PREFETCH });
}

start().catch((err) => {
  console.error(`[${WORKER_NAME}] Worker error:`, err);
  process.exit(1);
});
