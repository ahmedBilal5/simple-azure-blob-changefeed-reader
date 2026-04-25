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

// ── Public types ──────────────────────────────────────────────────────────────

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
  /**
   * Maximum number of HTTP operations (manifest downloads, chunk listings,
   * avro downloads) that may be in flight at the same time.
   *
   * Raise for faster reads on a high-bandwidth connection.
   * Lower if Azure returns 429 (TooManyRequests) errors.
   * Default: 20.
   */
  concurrency?: number;
}

const DEFAULT_CONCURRENCY = 20;

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
  return new BlobServiceClient(`https://${accountName}.blob.core.windows.net`, credential);
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

// ── Semaphore ─────────────────────────────────────────────────────────────────

/**
 * Bounds the number of concurrently running async operations.
 *
 * All HTTP work (manifest downloads, chunk listings, avro downloads) shares one
 * Semaphore instance so the total number of in-flight requests never exceeds
 * `limit`, regardless of how many nested Promise.all layers are active.
 */
class Semaphore {
  private slots: number;
  private readonly waiters: Array<() => void> = [];

  constructor(limit: number) {
    this.slots = limit;
  }

  async run<T>(fn: () => Promise<T>): Promise<T> {
    // Block until a slot is free.
    await new Promise<void>((resolve) => {
      if (this.slots > 0) {
        this.slots--;
        resolve();
      } else {
        this.waiters.push(resolve);
      }
    });
    try {
      return await fn();
    } finally {
      // Release the slot; wake the oldest waiter if any.
      if (this.waiters.length > 0) {
        this.waiters.shift()!();
      } else {
        this.slots++;
      }
    }
  }
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function makeLog(verbose: boolean): (msg: string) => void {
  if (!verbose) return () => {};
  return (msg) => {
    const t = new Date().toISOString().slice(11, 19); // HH:MM:SS UTC
    process.stderr.write(`[${t}] ${msg}\n`);
  };
}

function formatError(err: unknown): string {
  if (err instanceof RestError) {
    const code = err.code ? ` [${err.code}]` : '';
    return `HTTP ${err.statusCode}${code}: ${err.message}`;
  }
  return err instanceof Error ? err.message : String(err);
}

/** Collects a Node.js readable stream into a Buffer (used for small JSON files). */
function streamToBuffer(stream: NodeJS.ReadableStream): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
}

/**
 * Pipes the download stream directly into the Avro BlockDecoder.
 *
 * Faster than the buffer-then-parse approach because:
 *   - Decoding starts as soon as the first bytes arrive over the network.
 *   - No intermediate Buffer allocation for the entire file contents.
 */
function parseAvroStream(stream: NodeJS.ReadableStream): Promise<Record<string, unknown>[]> {
  return new Promise((resolve, reject) => {
    const records: Record<string, unknown>[] = [];
    const decoder = new avro.streams.BlockDecoder();
    (decoder as NodeJS.EventEmitter).on('data', (r: Record<string, unknown>) => records.push(r));
    (decoder as NodeJS.EventEmitter).on('end', () => resolve(records));
    (decoder as NodeJS.EventEmitter).on('error', reject);
    stream.on('error', reject);
    stream.pipe(decoder as unknown as NodeJS.WritableStream);
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
 * Returns one `idx/segments/YYYY/MM/DD/` prefix per calendar day in [start, end].
 * Returns [] when start > end (e.g. startTime is beyond lastConsumable).
 * Date.UTC arithmetic handles month/year roll-overs automatically.
 */
function enumerateDayPrefixes(start: Date, end: Date): string[] {
  const prefixes: string[] = [];
  const cur = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate()));
  const last = new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate()));
  while (cur <= last) {
    const y = cur.getUTCFullYear().toString().padStart(4, '0');
    const mo = (cur.getUTCMonth() + 1).toString().padStart(2, '0');
    const d = cur.getUTCDate().toString().padStart(2, '0');
    prefixes.push(`idx/segments/${y}/${mo}/${d}/`);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return prefixes;
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
 *   createClientFromDefaultCredential(accountName)  ← Managed Identity / Azure CLI
 *
 * Set AZURE_LOG_LEVEL=verbose in your environment for Azure SDK HTTP-level logs.
 */
export async function readChangeFeed(
  client: BlobServiceClient,
  options: ChangeFeedOptions = {}
): Promise<ChangeFeedEvent[]> {
  const { startTime, endTime } = options;
  const concurrency = options.concurrency ?? DEFAULT_CONCURRENCY;
  const log = makeLog(options.verbose ?? false);
  const container = client.getContainerClient('$blobchangefeed');
  const startedAt = Date.now();

  log(`Account     : ${client.accountName}`);
  log(`Endpoint    : ${client.url}`);
  log(`Time range  : ${startTime?.toISOString() ?? '(none)'} → ${endTime?.toISOString() ?? '(none)'}`);
  log(`Concurrency : ${concurrency} simultaneous HTTP operations`);
  log(`Tip: set AZURE_LOG_LEVEL=verbose for Azure SDK HTTP-level logs\n`);

  // ── Step 1: Read meta/segments.json ──────────────────────────────────────────
  // Must be done first — gives us lastConsumable, the boundary beyond which
  // segments are still being written and must not be consumed yet.
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
        log('    Enable it: Azure Portal → Storage Account → Data Protection → Enable blob change feed');
      } else if (err.statusCode === 403) {
        log('  → Access denied. Verify credentials have "Storage Blob Data Reader" role or use the account key.');
      } else if (err.statusCode === 401) {
        log('  → Unauthenticated. Check your connection string or account key.');
      }
    }
    throw new Error(`Failed to read segments.json: ${msg}`);
  }

  // Semaphore is created here (before Step 2) so it is shared across both the
  // parallel segment-manifest listings (Step 2) and the parallel pipeline (Steps 4–6).
  const sem = new Semaphore(concurrency);

  // ── Step 2: List segment manifests ───────────────────────────────────────────
  // When startTime is provided we enumerate only the relevant day-level prefixes
  // (e.g. `idx/segments/2024/01/15/`) instead of scanning the entire tree.
  // Each day holds at most ~96 entries (one per 15-min segment window), so total
  // listing work is proportional to the query range, not the account's full history.
  // Widening by 1 day on each side guarantees we cover the ±1 h buffer used in
  // Step 3 (1 day >> 1 hour). All day-prefix listings run in parallel via the
  // shared semaphore.
  //
  // When no startTime is given there is no lower-bound prefix to start from,
  // so we fall back to a single full listing under idx/segments/.
  log('\nStep 2: Listing segment manifests …');

  let listPrefixes: string[];
  if (startTime) {
    const endBound = endTime ?? lastConsumable;
    // Date.UTC handles roll-overs: day 0 → last day of previous month, etc.
    const dateStart = new Date(Date.UTC(
      startTime.getUTCFullYear(), startTime.getUTCMonth(), startTime.getUTCDate() - 1
    ));
    const dateEnd = new Date(Date.UTC(
      endBound.getUTCFullYear(), endBound.getUTCMonth(), endBound.getUTCDate() + 1
    ));
    listPrefixes = enumerateDayPrefixes(dateStart, dateEnd);
    log(`  startTime provided — ${listPrefixes.length} day-level prefix(es) listed in parallel`);
    log(`  Date window : ${dateStart.toISOString().slice(0, 10)} → ${dateEnd.toISOString().slice(0, 10)}`);
  } else {
    listPrefixes = ['idx/segments/'];
    log('  No startTime — listing full idx/segments/ tree');
  }

  const segmentManifests: string[] = [];
  await Promise.all(
    listPrefixes.map((prefix) =>
      sem.run(async () => {
        try {
          for await (const blob of container.listBlobsFlat({ prefix })) {
            if (blob.name.endsWith('/meta.json')) {
              segmentManifests.push(blob.name);
              log(`  Found: ${blob.name}`);
            }
          }
        } catch (err) {
          throw new Error(`Failed to list segment manifests for ${prefix}: ${formatError(err)}`);
        }
      })
    )
  );

  log(`  Total: ${segmentManifests.length} segment manifest(s)`);
  if (segmentManifests.length === 0) {
    log('  → No segment manifests found. The change feed has no events yet.');
    return [];
  }

  // ── Step 3: Filter segments by time range ────────────────────────────────────
  // Per Azure docs: segment times are approximate (±15 min), so we widen the
  // filter by ±1 h and then apply the precise filter per-record by eventTime.
  log('\nStep 3: Filtering segments by time range …');

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

    log(`  ${segPath} [${t.toISOString()}] → ${verdict}`);
  }

  log(`  Segments to process: ${relevantSegments.length} / ${segmentManifests.length}`);
  if (relevantSegments.length === 0) {
    log('  → All segments are outside the requested time range.');
    return [];
  }

  // ── Steps 4–6: Parallel pipeline ─────────────────────────────────────────────
  //
  // BEFORE (sequential): the three inner steps ran one after another — each
  // segment waited for the previous, each chunk waited for its segment, etc.
  //
  // NOW (parallel pipeline):
  //   • All segments launch simultaneously via Promise.all.
  //   • As soon as a segment manifest is downloaded, its chunks start listing
  //     immediately — without waiting for other manifests to finish.
  //   • As soon as a chunk's avro list is ready, downloads start immediately —
  //     without waiting for other chunks.
  //   • A single Semaphore shared across all three levels caps the total number
  //     of in-flight HTTP requests at `concurrency` at any moment.
  //   • Array.push() is safe to call from multiple concurrent async tasks because
  //     Node.js is single-threaded; awaits never run truly in parallel.
  //
  log(`\nSteps 4–6: Parallel pipeline — ${relevantSegments.length} segment(s), concurrency: ${concurrency} …`);

  const events: ChangeFeedEvent[] = [];
  const stats = { records: 0, control: 0, filtered: 0, errors: 0 };

  await Promise.all(
    relevantSegments.map(async (segPath) => {

      // ── Step 4: Download segment manifest ──────────────────────────────────
      // sem.run returns the value directly so TypeScript can narrow the type.
      // (Assigning to a `let` inside a closure prevents TypeScript from tracking
      // the narrowed type, causing it to infer `never` after a null-check.)
      const manifest = await sem.run(async () => {
        try {
          const response = await container.getBlobClient(segPath).download();
          const text = (await streamToBuffer(response.readableStreamBody!)).toString('utf8');
          const m = JSON.parse(text) as { chunkFilePaths: string[]; status: string };
          log(`  ✓ Manifest ${segPath} → status: ${m.status}, chunks: ${m.chunkFilePaths.length}`);
          return m;
        } catch (err) {
          stats.errors++;
          log(`  ✗ Manifest ${segPath}: ${formatError(err)} — skipping segment`);
          return null;
        }
      });

      if (!manifest) return;

      // All chunks within this segment are processed in parallel.
      await Promise.all(
        manifest.chunkFilePaths.map(async (chunkPath) => {
          // Strip "$blobchangefeed/" to get the blob name prefix inside the container.
          const prefix = chunkPath.replace(/^\$blobchangefeed\//, '');

          // ── Step 5: List Avro files for this chunk ───────────────────────
          // The entire listBlobsFlat call (all pages) runs as one throttled
          // operation so the slot is not released between pages.
          const avroFiles = await sem.run(async () => {
            const files: string[] = [];
            try {
              for await (const blob of container.listBlobsFlat({ prefix })) {
                if (blob.name.endsWith('.avro')) files.push(blob.name);
              }
              log(`    ✓ Chunk ${prefix} → ${files.length} avro file(s)`);
            } catch (err) {
              stats.errors++;
              log(`    ✗ Chunk ${prefix}: ${formatError(err)} — skipping chunk`);
            }
            return files;
          });

          // All avro files in this chunk are processed in parallel.
          await Promise.all(
            avroFiles.map(async (avroPath) => {

              // ── Step 6: Download and parse Avro file ────────────────────
              // The download stream is piped directly into the Avro decoder
              // (parseAvroStream) rather than buffered first — decoding starts
              // as the first bytes arrive over the network.
              const records = await sem.run(async () => {
                try {
                  const response = await container.getBlobClient(avroPath).download();
                  const recs = await parseAvroStream(response.readableStreamBody!);
                  log(`      ✓ ${avroPath} → ${recs.length} record(s)`);
                  return recs;
                } catch (err) {
                  stats.errors++;
                  log(`      ✗ ${avroPath}: ${formatError(err)} — skipping file`);
                  return [] as Record<string, unknown>[];
                }
              });

              // Filter and collect — pure CPU, not throttled by the semaphore.
              for (const record of records) {
                stats.records++;
                const eventType = record['eventType'] as string;
                if (eventType === 'Control') { stats.control++; continue; }

                const eventTime = record['eventTime'] as string;
                const t = new Date(eventTime);
                if (startTime && t < startTime) { stats.filtered++; continue; }
                if (endTime && t > endTime) { stats.filtered++; continue; }

                events.push({
                  path: blobPathFromSubject(record['subject'] as string),
                  eventType,
                  eventTime,
                });
              }
            })
          );
        })
      );
    })
  );

  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
  log(`\nSummary (${elapsed}s elapsed):`);
  log(`  Total Avro records parsed      : ${stats.records}`);
  log(`  Skipped (internal Control)     : ${stats.control}`);
  log(`  Skipped (outside time range)   : ${stats.filtered}`);
  log(`  Errors (segments/files skipped): ${stats.errors}`);
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
  --concurrency <n>            Max simultaneous HTTP operations (default: ${DEFAULT_CONCURRENCY})
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

  const options: ChangeFeedOptions = {
    startTime: get('--start') ? new Date(get('--start')!) : undefined,
    endTime: get('--end') ? new Date(get('--end')!) : undefined,
    concurrency: get('--concurrency') ? parseInt(get('--concurrency')!, 10) : undefined,
    verbose: argv.includes('--verbose'),
  };

  readChangeFeed(client, options)
    .then((events) => console.log(JSON.stringify(events, null, 2)))
    .catch((err: Error) => {
      console.error('Error:', err.message);
      process.exit(1);
    });
}
