> NOTE: This package was created with heavy usage of Claude Code. While the information below has been checked for correctness, do read with caution.  

# simple-azure-blob-changefeed-reader

A lightweight TypeScript library and CLI that reads the **Azure Blob Storage Change Feed** without the official `@azure/storage-blob-changefeed` package.

## Why This Package?

The official `@azure/storage-blob-changefeed` SDK package is in preview and was throwing runtime errors in practice. This library is an alternative to that library.

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
        <n>.avro           ← actual event records in Apache Avro format
```

**Reading the feed requires three traversal steps:**

1. **`meta/segments.json`** — fetch `lastConsumable`. Segments after this timestamp are still being written by Azure and must not be consumed.
2. **Segment manifests** (`idx/segments/.../meta.json`) — each manifest contains a list of `chunkFilePaths` pointing to directories of `.avro` files.
3. **Avro files** (`log/.../N.avro`) — each file contains batches of change events in [Apache Avro OCF](https://avro.apache.org/docs/current/spec.html#Object+Container+Files) format.

> **Note on time filtering:** Per Azure documentation, segment timestamps are approximate (±15 minutes). This library widens the filter by ±1 hour when selecting segments, then applies a per-record filter using the `eventTime` field inside each Avro record.

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

> Note that Auth Options B and C, while supported, have not been tested in practice and may or may not work. Option A has been tested

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
