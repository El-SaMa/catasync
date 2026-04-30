require('dotenv').config();

const amqp = require('amqplib');
const axios = require('axios');
const crypto = require('crypto');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');

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
const STATUS_PING_TIMEOUT_MS = Math.max(5000, parseInt(process.env.STATUS_PING_TIMEOUT_MS || '30000', 10));
const CALLBACK_TIMEOUT_MS = Math.max(5000, parseInt(process.env.CALLBACK_TIMEOUT_MS || '30000', 10));
const CALLBACK_MAX_ATTEMPTS = Math.max(1, parseInt(process.env.CALLBACK_MAX_ATTEMPTS || '3', 10));
const RECONNECT_DELAY_MS = Math.max(1000, parseInt(process.env.RECONNECT_DELAY_MS || '5000', 10));
const WORKER_VERSION = '1.0.2';
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
	let lastError = null;
	for (let attempt = 1; attempt <= CALLBACK_MAX_ATTEMPTS; attempt += 1) {
		try {
			return await axios.post(url, body, {
				headers: signBody(body),
				timeout: timeoutMs,
				validateStatus: (status) => status >= 200 && status < 300,
			});
		} catch (err) {
			lastError = err;
			if (attempt >= CALLBACK_MAX_ATTEMPTS) {
				break;
			}
			await new Promise((resolve) => setTimeout(resolve, attempt * 1000));
		}
	}
	throw lastError || new Error('Signed request failed.');
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

async function postSignedCallback(payload) {
	const requested = payload && payload.callback_timeout_ms ? parseInt(String(payload.callback_timeout_ms), 10) : 0;
	const callbackTimeoutMs = Math.max(5000, requested > 0 ? requested : CALLBACK_TIMEOUT_MS);
	return postSignedJson(WP_CALLBACK_URL, payload, callbackTimeoutMs);
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

async function executeFeatureLocally(feature, payload) {
	const payloadJson = JSON.stringify(payload && typeof payload === 'object' ? payload : {});
	const payloadB64 = Buffer.from(payloadJson, 'utf8').toString('base64');
	const featureJson = JSON.stringify(String(feature || ''));
	const workerJson = JSON.stringify(WORKER_NAME);
	const code = `
		$feature = json_decode('${featureJson}', true);
		$payload_json = base64_decode('${payloadB64}');
		$payload = json_decode($payload_json, true);
		if ( ! is_array( $payload ) ) {
			$payload = array();
		}
		if ( ! class_exists( '\\CataSync\\Scheduler' ) ) {
			fwrite( STDERR, 'CataSync Scheduler is not loaded.' );
			exit( 10 );
		}
		$res = \\CataSync\\Scheduler::execute_offloaded_feature_locally( $feature, $payload, 'worker_cli', json_decode('${workerJson}', true) );
		if ( is_wp_error( $res ) ) {
			fwrite( STDERR, $res->get_error_code() . ': ' . $res->get_error_message() );
			exit( 20 );
		}
		echo wp_json_encode( $res );
	`;
	const result = await wpEval(code);
	const raw = String(result.stdout || '').trim();
	if (!raw) {
		return {};
	}
	return JSON.parse(raw);
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
  params.set('worker_version', WORKER_VERSION);
  try {
    await axios.post(WP_STATUS_URL, params, { timeout: STATUS_PING_TIMEOUT_MS });
  } catch (err) {
    log('status ping failed', { error: err.message });
  }
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

  if (feature === 'wave.import.enrich' || feature === 'waveimportenrich') {
    const payload = job.payload && typeof job.payload === 'object' ? job.payload : {};
    const productId = parseInt(payload.product_id, 10) || 0;
    const stagingId = parseInt(payload.staging_id, 10) || 0;
    currentProcess = productId > 0 ? `V2 enrich product #${productId}` : `V2 enrich staging #${stagingId}`;
    await pingStatus();
    const result = await executeFeatureLocally(feature, payload);
    await postSignedCallback({
      job_id: job.job_id || '',
      feature_key: feature,
      status: 'done',
      payload,
      product_id: productId,
      staging_id: stagingId,
      result,
      callback_timeout_ms: parseInt(String(job.callback_timeout_ms || 0), 10) || CALLBACK_TIMEOUT_MS,
    });
    log('enrichment completed locally', { feature_key: feature, product_id: productId, staging_id: stagingId });
    return;
  }

  if (feature !== 'wave.import.execute' && feature !== 'waveimportexecute') {
    await postSignedCallback({
      job_id: job.job_id || '',
      feature_key: feature,
      status: 'failed',
      payload: job.payload && typeof job.payload === 'object' ? job.payload : {},
      error: `Unsupported local-worker feature: ${feature}`,
      callback_timeout_ms: parseInt(String(job.callback_timeout_ms || 0), 10) || CALLBACK_TIMEOUT_MS,
    });
    log('unsupported local-worker feature failed', { queue, feature });
    return;
  }

  const stagingId = job.payload && job.payload.staging_id;
  currentProcess = `Import staging #${stagingId}: enrich and create product`;
  await pingStatus();
  const productId = await importStagingId(stagingId);
  currentProcess = `Import staging #${stagingId}: callback`;
  await pingStatus();

  await postSignedCallback({
    job_id: job.job_id || '',
    feature_key: feature,
    status: 'done',
    staging_id: parseInt(stagingId, 10),
    product_id: productId,
    callback_timeout_ms: parseInt(String(job.callback_timeout_ms || 0), 10) || CALLBACK_TIMEOUT_MS,
  });
  log('import completed locally', { staging_id: parseInt(stagingId, 10), product_id: productId, wp_path: WP_PATH });
}

async function start() {
  while (true) {
    let conn = null;
    let statusTimer = null;
    try {
      conn = await amqp.connect(RABBITMQ_URL);
      const ch = await conn.createChannel();
      await ch.prefetch(PREFETCH);

      const closedPromise = new Promise((resolve) => {
        conn.on('close', resolve);
      });
      conn.on('error', (err) => {
        log('amqp connection error', { error: err.message });
      });

      await pingStatus();
      statusTimer = setInterval(pingStatus, STATUS_PING_INTERVAL_MS);
      statusTimer.unref();

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
            const productId = job && job.payload ? parseInt(job.payload.product_id, 10) : 0;
            log('job failed', { queue, staging_id: stagingId, error: err.message, stderr: err.stderr || '' });
            if (stagingId > 0 || productId > 0 || (job && job.feature_key)) {
              try {
                await postSignedCallback({
                  job_id: job.job_id || '',
                  feature_key: job.feature_key || '',
                  status: 'failed',
                  staging_id: stagingId,
                  product_id: productId,
                  payload: job && job.payload && typeof job.payload === 'object' ? job.payload : {},
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
      log('worker started', { queues: QUEUES, wp_path: WP_PATH, prefetch: PREFETCH, version: WORKER_VERSION });
      await closedPromise;
    } catch (err) {
      log('worker startup failed, retrying', { error: err.message, reconnect_ms: RECONNECT_DELAY_MS });
    } finally {
      if (statusTimer) {
        clearInterval(statusTimer);
      }
      if (conn) {
        try {
          await conn.close();
        } catch (closeErr) {
          // Ignore close errors during reconnect.
        }
      }
    }
    await new Promise((resolve) => setTimeout(resolve, RECONNECT_DELAY_MS));
  }
}

start().catch((err) => {
  console.error(`[${WORKER_NAME}] Worker error:`, err);
  process.exit(1);
});
