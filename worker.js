require('dotenv').config();

const amqp = require('amqplib');
const axios = require('axios');
const crypto = require('crypto');
const os = require('os');

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUES = (process.env.QUEUES || '').split(',').map((q) => q.trim()).filter(Boolean);
const WP_CALLBACK_URL = process.env.WP_CALLBACK_URL;
const WP_STATUS_URL = process.env.WP_STATUS_URL || (WP_CALLBACK_URL || '').replace(/action=[^&]+/, 'action=catasync_worker_status_ping');
const WP_EXECUTE_URL = process.env.WP_EXECUTE_URL || (WP_CALLBACK_URL || '').replace(/action=[^&]+/, 'action=catasync_offload_execute');
const WORKER_SECRET = process.env.WORKER_SECRET;
const WORKER_NAME = process.env.WORKER_NAME || os.hostname() || 'catasync-worker';
const PREFETCH = Math.max(1, parseInt(process.env.PREFETCH || '1', 10));
const STATUS_PING_INTERVAL_MS = Math.max(3000, parseInt(process.env.STATUS_PING_INTERVAL_MS || '5000', 10));
const STATUS_PING_TIMEOUT_MS = Math.max(5000, parseInt(process.env.STATUS_PING_TIMEOUT_MS || '30000', 10));
const CALLBACK_TIMEOUT_MS = Math.max(5000, parseInt(process.env.CALLBACK_TIMEOUT_MS || '120000', 10));
let currentProcess = '';
let lastCpuUsage = process.cpuUsage();
let lastCpuCheckAtMs = Date.now();

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

async function postSignedJson(url, payload, timeoutMs = 30000) {
  const body = JSON.stringify({ domain: WORKER_NAME, ...payload });
  return axios.post(url, body, {
    headers: signBody(body),
    timeout: timeoutMs,
    validateStatus: (status) => status >= 200 && status < 300,
  });
}

async function postSignedCallback(payload) {
  const requested = payload && payload.callback_timeout_ms ? parseInt(String(payload.callback_timeout_ms), 10) : 0;
  const callbackTimeoutMs = Math.max(5000, requested > 0 ? requested : CALLBACK_TIMEOUT_MS);
  return postSignedJson(WP_CALLBACK_URL, payload, callbackTimeoutMs);
}

async function pingStatus() {
  if (!WP_STATUS_URL) {
    return;
  }
  const nowMs = Date.now();
  const cpuNow = process.cpuUsage();
  const deltaUser = cpuNow.user - lastCpuUsage.user;
  const deltaSystem = cpuNow.system - lastCpuUsage.system;
  const deltaCpuMicros = Math.max(0, deltaUser + deltaSystem);
  const deltaWallMs = Math.max(1, nowMs - lastCpuCheckAtMs);
  const processCpuPercent = Math.max(0, (deltaCpuMicros / (deltaWallMs * 1000)) * 100);
  lastCpuUsage = cpuNow;
  lastCpuCheckAtMs = nowMs;

  const mem = process.memoryUsage();
  const processRamMb = (mem.rss || 0) / (1024 * 1024);
  const processRamPercent = (processRamMb * 1024 * 1024 / Math.max(1, os.totalmem())) * 100;

  const params = new URLSearchParams();
  params.set('worker', WORKER_NAME);
  params.set('secret', WORKER_SECRET);
  // Backward-compatible fields kept for older plugin versions.
  params.set('cpu', String(os.loadavg()[0] || 0));
  const ramRatio = (os.totalmem() - os.freemem()) / Math.max(1, os.totalmem());
  params.set('ram', String(ramRatio));
  params.set('ram_percent', String(Math.round(ramRatio * 100)));
  // Accurate process-level metrics for worker dashboard.
  params.set('process_cpu_percent', String(processCpuPercent));
  params.set('process_ram_mb', String(processRamMb));
  params.set('process_ram_percent', String(processRamPercent));
  params.set('host_load1', String(os.loadavg()[0] || 0));
  params.set('host_ram_percent', String(Math.round(ramRatio * 100)));
  params.set('running', currentProcess ? '1' : '0');
  params.set('process', currentProcess);
  params.set('current_process', currentProcess);
  params.set('currentProcess', currentProcess);
  try {
    await axios.post(WP_STATUS_URL, params, { timeout: STATUS_PING_TIMEOUT_MS });
  } catch (err) {
    log('status ping failed', { error: err.message });
  }
}

async function importStagingId(stagingId) {
  const sid = parseInt(stagingId, 10);
  if (!sid || sid <= 0) {
    throw new Error('Missing staging_id for import.');
  }
  const response = await postSignedJson(WP_EXECUTE_URL, { staging_id: sid }, 1200000);
  const data = response && response.data && response.data.data ? response.data.data : {};
  const productId = parseInt(String(data.product_id || 0), 10);
  if (!productId || productId <= 0) {
    throw new Error('Remote execute endpoint did not return a valid product id.');
  }
  return {
    productId,
    title: String(data.title || ''),
    desc: String(data.desc || ''),
    shortDesc: String(data.short_desc || ''),
    imageCount: parseInt(String(data.image_count || 0), 10) || 0,
  };
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

function isKnownFeature(feature) {
  return [
    'wave.import.execute',
    'waveimportexecute',
    'wave.import.enrich',
    'waveimportenrich',
    'wave.sync.reprice',
    'wavesyncreprice',
    'wave.reports.generate',
    'wavereportsgenerate',
    'wave.maintenance.backfill',
    'wavemaintenancebackfill',
    'wave.search.typesense.index',
    'wavesearchtypesenseindex',
  ].includes(feature);
}

async function handleJob(queue, job) {
  const feature = String(job.feature_key || job.type || '');
  if (!isKnownFeature(feature)) {
    log('unsupported job acknowledged', { queue, feature });
    return;
  }

  if (feature !== 'wave.import.execute' && feature !== 'waveimportexecute') {
    await postSignedJson(WP_EXECUTE_URL, {
      feature_key: feature,
      payload: job.payload && typeof job.payload === 'object' ? job.payload : {},
      job_id: job.job_id || '',
      callback_timeout_ms: parseInt(String(job.callback_timeout_ms || 0), 10) || CALLBACK_TIMEOUT_MS,
    }, 1200000);

    await postSignedCallback({
      job_id: job.job_id || '',
      feature_key: feature,
      status: 'done',
      payload: job.payload && typeof job.payload === 'object' ? job.payload : {},
      callback_timeout_ms: parseInt(String(job.callback_timeout_ms || 0), 10) || CALLBACK_TIMEOUT_MS,
    });
    log('feature completed', { feature_key: feature, queue });
    return;
  }

  const stagingId = job.payload && job.payload.staging_id;
  currentProcess = `Import staging #${stagingId}: enrich and create product`;
  await pingStatus();
  const imported = await importStagingId(stagingId);
  currentProcess = `Import staging #${stagingId}: validate enrichment and images`;
  await pingStatus();

  // Check for missing fields
  const missing = [];
  if (!imported.title || imported.title.trim() === '') missing.push('title');
  if (!imported.desc || imported.desc.trim() === '') missing.push('desc');
  if (!imported.shortDesc || imported.shortDesc.trim() === '') missing.push('short_desc');
  if (!imported.imageCount || imported.imageCount <= 0) missing.push('images');

  if (missing.length > 0) {
    throw new Error(`Import incomplete after worker run; missing ${missing.join(', ')}.`);
  }

  // Log enrichment status
  log('enriched via worker', { productId: imported.productId, images: imported.imageCount });

  await postSignedCallback({
    job_id: job.job_id || '',
    feature_key: feature,
    status: 'done',
    staging_id: parseInt(stagingId, 10),
    product_id: imported.productId,
    callback_timeout_ms: parseInt(String(job.callback_timeout_ms || 0), 10) || CALLBACK_TIMEOUT_MS,
    enriched_fields: ['title', 'desc', 'short_desc'].filter((k) => {
      if (k === 'short_desc') {
        return imported.shortDesc && imported.shortDesc.trim() !== '';
      }
      return imported[k] && String(imported[k]).trim() !== '';
    }),
    image_count: imported.imageCount || 0,
    missing_fields: [],
  });
  log('import completed', { staging_id: parseInt(stagingId, 10), product_id: imported.productId, images: imported.imageCount });
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
              callback_timeout_ms: parseInt(String((job && job.callback_timeout_ms) || 0), 10) || CALLBACK_TIMEOUT_MS,
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
  log('worker started', { queues: QUEUES, execute_url: WP_EXECUTE_URL, prefetch: PREFETCH });
}

start().catch((err) => {
  console.error(`[${WORKER_NAME}] Worker error:`, err);
  process.exit(1);
});
