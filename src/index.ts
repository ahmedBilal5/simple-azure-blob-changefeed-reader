import { Readable } from 'stream';
import {
  BlobServiceClient,
  StorageSharedKeyCredential,
  RestError,
} from '@azure/storage-blob';
import { DefaultAzureCredential } from '@azure/identity';

// eslint-disable-next-line @typescript-eslint/no-require-imports
const avro = require('avsc') as {
  streams: { BlockDecoder: new () => NodeJS.WritableStream & NodeJS.ReadableStream };
};

// ── Public types ─────────────────────────────────────────────────────────────

export interface ChangeFeedEvent {
  path: string;
  eventType: string;
  eventTime: string;
}

export interface ChangeFeedOptions {
  startTime?: Date;
  endTime?: Date;
  /**
   * Write step-by-step progress to stderr.
   * For Azure SDK HTTP-level logs, set AZURE_LOG_LEVEL=verbose in your environment.
   */
  verbose?: boolean;
}

// ── Client factory helpers ────────────────────────────────────────────────────

/** Create a BlobServiceClient from an Azure Storage connection string. */
export function createClientFromConnectionString(connStr: string): BlobServiceClient {
  return BlobServiceClient.fromConnectionString(connStr);
}

/**
 * Create a BlobServiceClient from an account name and account key.
 * Uses @azure/storage-blob's StorageSharedKeyCredential.
 */
export function createClientFromAccountKey(
  accountName: string,
  accountKey: string
): BlobServiceClient {
  const credential = new StorageSharedKeyCredential(accountName, accountKey);
  return new BlobServiceClient(
    `https://${accountName}.blob.core.windows.net`,
    credential
  );
}

/**
 * Create a BlobServiceClient using DefaultAzureCredential from @azure/identity.
 * Works with: Azure CLI (`az login`), Managed Identity, VS Code, environment
 * variables (AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET), and more.
 */
export function createClientFromDefaultCredential(accountName: string): BlobServiceClient {
  return new BlobServiceClient(
    `https://${accountName}.blob.core.windows.net`,
    new DefaultAzureCredential()
  );
}

// ── Internal helpers ─────────────────────────────────────────────────────────

function makeLog(verbose: boolean): (msg: string) => void {
  if (!verbose) return () => {};
  return (msg) => {
    const t = new Date().toISOString().slice(11, 19); // HH:MM:SS UTC
    process.stderr.write(`[${t}] ${msg}\n`);
  };
}

/** Formats a RestError (Azure SDK) or plain Error into a readable string. */
function formatError(err: unknown): string {
  if (err instanceof RestError) {
    const code = err.code ? ` [${err.code}]` : '';
    return `HTTP ${err.statusCode}${code}: ${err.message}`;
  }
  return err instanceof Error ? err.message : String(err);
}

/** Collects a Node.js readable stream into a Buffer. */
function streamToBuffer(stream: NodeJS.ReadableStream): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
}

/** Parses an Avro Object Container File buffer into a list of records. */
function parseAvroBuffer(buf: Buffer): Promise<Record<string, unknown>[]> {
  return new Promise((resolve, reject) => {
    const records: Record<string, unknown>[] = [];
    const decoder = new avro.streams.BlockDecoder();
    (decoder as NodeJS.EventEmitter).on('data', (r: Record<string, unknown>) => records.push(r));
    (decoder as NodeJS.EventEmitter).on('end', () => resolve(records));
    (decoder as NodeJS.EventEmitter).on('error', reject);
    const readable = new Readable({ read() {} });
    readable.push(buf);
    readable.push(null);
    readable.pipe(decoder as unknown as NodeJS.WritableStream);
  });
}

/**
 * Parses the segment time from a blob name like:
 *   idx/segments/2019/02/22/1810/meta.json  →  2019-02-22T18:10:00Z
 */
function segmentPathToDate(segPath: string): Date {
  const m = segPath.match(/idx\/segments\/(\d{4})\/(\d{2})\/(\d{2})\/(\d{2})(\d{2})\//);
  if (!m) return new Date(0);
  const [, year, month, day, hour, minute] = m;
  return new Date(`${year}-${month}-${day}T${hour}:${minute}:00Z`);
}

/**
 * Extracts "<container>/<blobpath>" from an event's subject field.
 * Subject format: /blobServices/default/containers/<container>/blobs/<path>
 */
function blobPathFromSubject(subject: string): string {
  const m = subject.match(/\/containers\/([^/]+)\/blobs\/(.+)/);
  return m ? `${m[1]}/${m[2]}` : subject;
}

// ── Main export ───────────────────────────────────────────────────────────────

/**
 * Reads the Azure Blob Change Feed and returns an array of change events.
 *
 * Pass a BlobServiceClient created via one of:
 *   createClientFromConnectionString(connStr)
 *   createClientFromAccountKey(accountName, accountKey)
 *   createClientFromDefaultCredential(accountName)   ← Managed Identity / Azure CLI
 *
 * Set AZURE_LOG_LEVEL=verbose in your environment for Azure SDK HTTP-level logs.
 */
export async function readChangeFeed(
  client: BlobServiceClient,
  options: ChangeFeedOptions = {}
): Promise<ChangeFeedEvent[]> {
  const { startTime, endTime } = options;
  const log = makeLog(options.verbose ?? false);
  const container = client.getContainerClient('$blobchangefeed');

  log(`Account   : ${client.accountName}`);
  log(`Endpoint  : ${client.url}`);
  log(`Time range: ${startTime?.toISOString() ?? '(none)'} → ${endTime?.toISOString() ?? '(none)'}`);
  log(`Tip: set AZURE_LOG_LEVEL=verbose for Azure SDK HTTP-level logs\n`);

  // ── Step 1: Read meta/segments.json ────────────────────────────────────────
  log('Step 1: Reading meta/segments.json …');

  let lastConsumable: Date;
  try {
    const blob = container.getBlobClient('meta/segments.json');
    log(`  Downloading ${blob.url}`);
    const response = await blob.download();
    const text = (await streamToBuffer(response.readableStreamBody!)).toString('utf8');
    log(`  Raw content: ${text}`);
    const meta = JSON.parse(text) as { lastConsumable: string };
    lastConsumable = new Date(meta.lastConsumable);
    log(`  lastConsumable: ${lastConsumable.toISOString()}`);
  } catch (err) {
    const msg = formatError(err);
    log(`  ERROR: ${msg}`);
    if (err instanceof RestError) {
      if (err.statusCode === 404) {
        log('  → Blob not found. Is change feed enabled on this storage account?');
        log('    Enable it in Azure Portal → Storage Account → Data Protection → Enable blob change feed');
      } else if (err.statusCode === 403) {
        log('  → Access denied. Verify your credentials have "Storage Blob Data Reader" role or use the account key.');
      } else if (err.statusCode === 401) {
        log('  → Unauthenticated. Check your connection string or account key.');
      }
    }
    throw new Error(`Failed to read segments.json: ${msg}`);
  }

  // ── Step 2: List segment manifests ─────────────────────────────────────────
  log('\nStep 2: Listing segment manifests under idx/segments/ …');

  const segmentManifests: string[] = [];
  try {
    for await (const blob of container.listBlobsFlat({ prefix: 'idx/segments/' })) {
      if (blob.name.endsWith('/meta.json')) {
        segmentManifests.push(blob.name);
        log(`  Found: ${blob.name}`);
      }
    }
  } catch (err) {
    const msg = formatError(err);
    log(`  ERROR listing blobs: ${msg}`);
    throw new Error(`Failed to list segment manifests: ${msg}`);
  }

  log(`  Total segment manifests: ${segmentManifests.length}`);

  if (segmentManifests.length === 0) {
    log('  → No segment manifests found. The change feed has no events yet.');
    return [];
  }

  // ── Step 3: Filter segments by time range ──────────────────────────────────
  log('\nStep 3: Filtering segments by time range …');

  // Per Azure docs: segment times are approximate (±15 min), so buffer ±1 h
  // to ensure all relevant events are captured, then filter precisely by eventTime.
  const rangeStart = startTime ? new Date(startTime.getTime() - 3_600_000) : null;
  const rangeEnd = endTime ? new Date(endTime.getTime() + 3_600_000) : null;
  if (rangeStart) log(`  Effective start (startTime − 1 h): ${rangeStart.toISOString()}`);
  if (rangeEnd) log(`  Effective end   (endTime   + 1 h): ${rangeEnd.toISOString()}`);

  const relevantSegments: string[] = [];
  for (const segPath of segmentManifests) {
    const t = segmentPathToDate(segPath);
    let verdict: string;

    if (t > lastConsumable) {
      verdict = `SKIP — after lastConsumable (${lastConsumable.toISOString()})`;
    } else if (rangeStart && t < rangeStart) {
      verdict = 'SKIP — before effective start';
    } else if (rangeEnd && t > rangeEnd) {
      verdict = 'SKIP — after effective end';
    } else {
      verdict = 'INCLUDE';
      relevantSegments.push(segPath);
    }

    log(`  ${segPath} [segment time: ${t.toISOString()}] → ${verdict}`);
  }

  log(`  Segments to process: ${relevantSegments.length} / ${segmentManifests.length}`);

  if (relevantSegments.length === 0) {
    log('  → All segments are outside the requested time range.');
    return [];
  }

  // ── Steps 4–6: Read manifests → list Avro files → parse records ────────────
  const events: ChangeFeedEvent[] = [];
  let totalRecords = 0;
  let skippedControl = 0;
  let skippedTimeFilter = 0;

  for (let si = 0; si < relevantSegments.length; si++) {
    const segPath = relevantSegments[si];
    log(`\nStep 4 [${si + 1}/${relevantSegments.length}]: Reading segment manifest ${segPath} …`);

    // Read segment manifest (chunkFilePaths tell us which shard directories to look in)
    let manifest: { chunkFilePaths: string[]; status: string };
    try {
      const blob = container.getBlobClient(segPath);
      const response = await blob.download();
      const text = (await streamToBuffer(response.readableStreamBody!)).toString('utf8');
      manifest = JSON.parse(text) as { chunkFilePaths: string[]; status: string };
      log(`  status        : ${manifest.status}`);
      log(`  chunkFilePaths: ${manifest.chunkFilePaths.join(', ') || '(none)'}`);
    } catch (err) {
      log(`  ERROR: ${formatError(err)} — skipping this segment`);
      continue;
    }

    for (const chunkPath of manifest.chunkFilePaths) {
      // chunkPath = "$blobchangefeed/log/00/YYYY/MM/DD/HHMM/"
      // Strip the container name prefix to get the blob prefix within the container.
      const prefix = chunkPath.replace(/^\$blobchangefeed\//, '');
      log(`\n  Step 5: Listing Avro files at prefix "${prefix}" …`);

      const avroFiles: string[] = [];
      try {
        for await (const blob of container.listBlobsFlat({ prefix })) {
          if (blob.name.endsWith('.avro')) {
            avroFiles.push(blob.name);
            log(`    Found: ${blob.name} (${blob.properties.contentLength ?? '?'} bytes)`);
          }
        }
      } catch (err) {
        log(`    ERROR listing Avro files: ${formatError(err)} — skipping this chunk`);
        continue;
      }

      log(`    Avro files in chunk: ${avroFiles.length}`);

      for (const avroPath of avroFiles) {
        log(`\n    Step 6: Downloading and parsing ${avroPath} …`);

        let records: Record<string, unknown>[];
        try {
          const blob = container.getBlobClient(avroPath);
          const response = await blob.download();
          const buf = await streamToBuffer(response.readableStreamBody!);
          log(`      Downloaded: ${buf.length} bytes`);
          records = await parseAvroBuffer(buf);
          log(`      Parsed: ${records.length} Avro record(s)`);
        } catch (err) {
          log(`      ERROR: ${formatError(err)} — skipping file`);
          continue;
        }

        totalRecords += records.length;

        for (const record of records) {
          const eventType = record['eventType'] as string;

          if (eventType === 'Control') {
            skippedControl++;
            continue;
          }

          const eventTime = record['eventTime'] as string;
          const t = new Date(eventTime);

          if (startTime && t < startTime) { skippedTimeFilter++; continue; }
          if (endTime && t > endTime) { skippedTimeFilter++; continue; }

          events.push({
            path: blobPathFromSubject(record['subject'] as string),
            eventType,
            eventTime,
          });
        }
      }
    }
  }

  log(`\nSummary:`);
  log(`  Total Avro records parsed      : ${totalRecords}`);
  log(`  Skipped (internal Control)     : ${skippedControl}`);
  log(`  Skipped (outside time range)   : ${skippedTimeFilter}`);
  log(`  Events returned                : ${events.length}`);

  return events;
}

// ── CLI entry point ───────────────────────────────────────────────────────────

function printUsage(): void {
  console.error(`
Usage:
  ts-node src/index.ts [options]

Authentication (one of):
  --connection-string <str>    Azure Storage connection string
  --account-name <name>        Storage account name
  --account-key  <key>         Storage account key (base64)
  --default-credential         Use DefaultAzureCredential (Azure CLI / Managed Identity / VS Code)
                               Requires --account-name

  Env vars: AZURE_STORAGE_CONNECTION_STRING
            AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY
            AZURE_STORAGE_ACCOUNT_NAME (alone) → uses DefaultAzureCredential

Options:
  --start <datetime>           Only return events at or after this time (ISO 8601)
  --end   <datetime>           Only return events at or before this time (ISO 8601)
  --verbose                    Print step-by-step progress to stderr
                               (Set AZURE_LOG_LEVEL=verbose for SDK HTTP logs)

Output: JSON array to stdout.
`);
}

function resolveClient(argv: string[]): BlobServiceClient {
  const get = (flag: string) => {
    const i = argv.indexOf(flag);
    return i !== -1 ? argv[i + 1] : undefined;
  };

  const connStr =
    get('--connection-string') ?? process.env['AZURE_STORAGE_CONNECTION_STRING'];
  if (connStr) return createClientFromConnectionString(connStr);

  const accountName =
    get('--account-name') ?? process.env['AZURE_STORAGE_ACCOUNT_NAME'];
  const accountKey =
    get('--account-key') ?? process.env['AZURE_STORAGE_ACCOUNT_KEY'];

  if (accountName && accountKey) return createClientFromAccountKey(accountName, accountKey);
  if (accountName && (argv.includes('--default-credential') || !accountKey)) {
    return createClientFromDefaultCredential(accountName);
  }

  printUsage();
  process.exit(1);
}

if (require.main === module) {
  const argv = process.argv.slice(2);
  const client = resolveClient(argv);

  const get = (flag: string) => {
    const i = argv.indexOf(flag);
    return i !== -1 ? argv[i + 1] : undefined;
  };
  const startArg = get('--start');
  const endArg = get('--end');

  const options: ChangeFeedOptions = {
    startTime: startArg ? new Date(startArg) : undefined,
    endTime: endArg ? new Date(endArg) : undefined,
    verbose: argv.includes('--verbose'),
  };

  readChangeFeed(client, options)
    .then((events) => console.log(JSON.stringify(events, null, 2)))
    .catch((err: Error) => {
      console.error('Error:', err.message);
      process.exit(1);
    });
}
