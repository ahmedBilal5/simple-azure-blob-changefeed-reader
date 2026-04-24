# simple-azure-blob-changefeed-reader

A lightweight TypeScript library and CLI that reads the **Azure Blob Storage Change Feed** without the official `@azure/storage-blob-changefeed` package.

## Why This Package?

The official `@azure/storage-blob-changefeed` SDK package is still in public preview and was throwing runtime errors in practice. This library reimplements the change-feed traversal logic from scratch using only the stable `@azure/storage-blob` SDK (for authentication and HTTP) and the `avsc` package (for Avro decoding). No preview packages, no unstable APIs.

## How Azure Blob Change Feed Works

The change feed is a log of every create, update, and delete operation on blobs in your storage account. Azure writes it as a tree of files inside a special container called `$blobchangefeed`:

```
$blobchangefeed/
  meta/
    segments.json          ← index: lists all segments + lastConsumable timestamp
  idx/
    segments/
      YYYY/MM/DD/HHMM/
        meta.json          ← segment manifest: lists chunk prefixes for this time window
  log/
    <shardId>/
      YYYY/MM/DD/HHMM/
        <n>.avro           ← actual event records in Apache Avro OCF format
```

**Reading the feed requires three traversal steps:**

1. **`meta/segments.json`** — fetch `lastConsumable`. Segments after this timestamp are still being written by Azure and must not be consumed.
2. **Segment manifests** (`idx/segments/.../meta.json`) — each manifest contains a list of `chunkFilePaths` pointing to directories of `.avro` files.
3. **Avro files** (`log/.../N.avro`) — each file contains batches of change events in [Apache Avro OCF](https://avro.apache.org/docs/current/spec.html#Object+Container+Files) format.

> **Note on time filtering:** Per Azure documentation, segment timestamps are approximate (±15 minutes). This library widens the filter by ±1 hour when selecting segments, then applies a precise per-record filter using the `eventTime` field inside each Avro record.

## Installation

```bash
npm install simple-azure-blob-changefeed-reader
```

## Quick Start

### Programmatic

```typescript
import {
  createClientFromConnectionString,
  readChangeFeed,
} from 'simple-azure-blob-changefeed-reader';

const client = createClientFromConnectionString(process.env.AZURE_STORAGE_CONNECTION_STRING!);

const events = await readChangeFeed(client, {
  startTime: new Date('2024-01-01T00:00:00Z'),
  endTime:   new Date('2024-01-02T00:00:00Z'),
  verbose:   true,   // logs step-by-step progress to stderr
});

console.log(JSON.stringify(events, null, 2));
```

Each event looks like:

```json
{
  "path":      "my-container/path/to/blob.txt",
  "eventType": "BlobCreated",
  "eventTime": "2024-01-01T12:34:56.000Z",
}
```

### CLI

```bash
ts-node src/index.ts \
  --connection-string "$AZURE_STORAGE_CONNECTION_STRING" \
  --start 2024-01-01T00:00:00Z \
  --end   2024-01-02T00:00:00Z \
  --verbose
```

Output is a JSON array on stdout; progress logs go to stderr.

## Authentication

Three methods are supported. Pass credentials via CLI flags or environment variables.

### Option A — Connection String (highest priority)

```bash
# env
AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=..."

# CLI flag
--connection-string "..."
```

```typescript
createClientFromConnectionString(connStr)
```

### Option B — Account Name + Key

```bash
AZURE_STORAGE_ACCOUNT_NAME=mystorageaccount
AZURE_STORAGE_ACCOUNT_KEY=base64key==
```

```typescript
createClientFromAccountKey(accountName, accountKey)
```

### Option C — DefaultAzureCredential (Azure CLI / Managed Identity / VS Code)

Set only `AZURE_STORAGE_ACCOUNT_NAME` and leave the key blank. The library will use `DefaultAzureCredential` from `@azure/identity`, which tries (in order): environment variables → workload identity → Azure CLI → VS Code.

```bash
AZURE_STORAGE_ACCOUNT_NAME=mystorageaccount
az login   # for Azure CLI credential
```

```typescript
createClientFromDefaultCredential(accountName)
```

The storage account's service principal needs the **Storage Blob Data Reader** role on the `$blobchangefeed` container (or the account).

## API Reference

### `readChangeFeed(client, options?)`

```typescript
readChangeFeed(
  client:  BlobServiceClient,
  options: ChangeFeedOptions = {}
): Promise<ChangeFeedEvent[]>
```

#### `ChangeFeedOptions`

| Field         | Type      | Default | Description                                             |
|---------------|-----------|---------|---------------------------------------------------------|
| `startTime`   | `Date`    | —       | Only return events at or after this time.               |
| `endTime`     | `Date`    | —       | Only return events at or before this time.              |
| `verbose`     | `boolean` | `false` | Write step-by-step progress to stderr.                  |
| `concurrency` | `number`  | `20`    | Max simultaneous HTTP requests (see Performance below). |

#### `ChangeFeedEvent`

| Field       | Type                       | Description                                          |
|-------------|----------------------------|------------------------------------------------------|
| `path`      | `string`                   | `<container>/<blobpath>` extracted from the subject. |
| `eventType` | `string`                   | e.g. `BlobCreated`, `BlobDeleted`, `BlobMetadataUpdated`. |
| `eventTime` | `string`                   | ISO 8601 timestamp from the event record.            |
| `metadata`  | `Record<string, string>?`  | New metadata key-value pairs. Only present for `BlobMetadataUpdated` events, and only when the Avro record includes them. |

### Client factories

```typescript
createClientFromConnectionString(connStr: string): BlobServiceClient
createClientFromAccountKey(accountName: string, accountKey: string): BlobServiceClient
createClientFromDefaultCredential(accountName: string): BlobServiceClient
```

## CLI Reference

```
Usage:
  ts-node src/index.ts [options]

Authentication (one of):
  --connection-string <str>    Azure Storage connection string
  --account-name <name>        Storage account name
  --account-key  <key>         Account key (base64)
  --default-credential         Use DefaultAzureCredential (requires --account-name)

Options:
  --start <datetime>           Only return events at or after this time (ISO 8601)
  --end   <datetime>           Only return events at or before this time (ISO 8601)
  --concurrency <n>            Max simultaneous HTTP operations (default: 20)
  --verbose                    Print step-by-step progress to stderr
```

## Performance

By default the library runs all three traversal steps in a parallel pipeline:

- All relevant segments are processed concurrently via `Promise.all`.
- As soon as a segment manifest is downloaded, its chunk listings start immediately — without waiting for other segments to finish.
- As soon as a chunk's Avro list is ready, downloads start — without waiting for other chunks.
- A single `Semaphore` shared across all levels caps the total number of in-flight HTTP requests at `concurrency` (default: 20) at any moment.
- Avro files are streamed directly into the `BlockDecoder` as bytes arrive over the network — no intermediate buffering.

If Azure returns `429 TooManyRequests` errors, reduce `--concurrency`. On a high-bandwidth connection, raising it above 20 may further improve throughput.

## Running the Test Script

Copy `.env.example` to `.env`, fill in your credentials, then:

```bash
npm run test:feed
```

This runs `src/test.ts` which reads credentials from `.env`, calls `readChangeFeed` with `verbose: true`, and prints all events as JSON.

## SDK-Level HTTP Logging

Set `AZURE_LOG_LEVEL=verbose` (or `info` / `warning` / `error`) in your environment to see detailed HTTP request/response logs from the Azure SDK:

```bash
AZURE_LOG_LEVEL=verbose npm run test:feed
```

## Development

```bash
npm install
npm run build       # tsc → dist/
npm run test:feed   # run against a real storage account
```

TypeScript type-checks only:

```bash
npx tsc --noEmit
```

## Publishing

This package is published to the npm public registry automatically on every merge to `main` via [semantic-release](https://semantic-release.gitbook.io/semantic-release/).

Version bumps are driven by the **PR title** using [Conventional Commits](https://www.conventionalcommits.org/):

| PR title prefix     | Version bump |
|---------------------|--------------|
| `fix: ...`          | patch        |
| `feat: ...`         | minor        |
| `feat!: ...`        | major        |
| `chore:`, `docs:`, `refactor:`, `test:`, `ci:` | no release |

> **GitHub setup:** In your repository settings → **General** → *Pull Requests*, enable **"Allow squash merging"** and set the default commit message to **"Pull request title and description"**. This ensures the PR title (in conventional commit format) becomes the squash-merge commit message that semantic-release reads.

Required repository secrets:
- `NPM_TOKEN` — a granular npm access token with publish permission for this package.

`GITHUB_TOKEN` is provided automatically by GitHub Actions.

## License

MIT
