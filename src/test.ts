import 'dotenv/config';
import { BlobServiceClient } from '@azure/storage-blob';
import {
  readChangeFeed,
  createClientFromConnectionString,
  createClientFromAccountKey,
  createClientFromDefaultCredential,
} from './index';

function resolveClient(): BlobServiceClient {
  const connStr = process.env['AZURE_STORAGE_CONNECTION_STRING'];
  if (connStr) {
    console.log('Auth: connection string');
    return createClientFromConnectionString(connStr);
  }

  const accountName = process.env['AZURE_STORAGE_ACCOUNT_NAME'];
  const accountKey = process.env['AZURE_STORAGE_ACCOUNT_KEY'];

  if (accountName && accountKey) {
    console.log(`Auth: account key (account: ${accountName})`);
    return createClientFromAccountKey(accountName, accountKey);
  }

  if (accountName) {
    console.log(`Auth: DefaultAzureCredential (account: ${accountName})`);
    console.log('      → Uses Azure CLI / Managed Identity / VS Code in that order');
    return createClientFromDefaultCredential(accountName);
  }

  console.error(
    'Error: no credentials found in .env\n' +
    '  Set AZURE_STORAGE_CONNECTION_STRING, or\n' +
    '  Set AZURE_STORAGE_ACCOUNT_NAME + AZURE_STORAGE_ACCOUNT_KEY, or\n' +
    '  Set AZURE_STORAGE_ACCOUNT_NAME alone to use DefaultAzureCredential'
  );
  process.exit(1);
}

async function main() {
  const client = resolveClient();

  const startEnv = process.env['CHANGEFEED_START'];
  const endEnv = process.env['CHANGEFEED_END'];
  const startTime = startEnv ? new Date(startEnv) : undefined;
  const endTime = endEnv ? new Date(endEnv) : undefined;

  console.log(`Start     : ${startTime?.toISOString() ?? '(none — reading all events)'}`);
  console.log(`End       : ${endTime?.toISOString() ?? '(none — reading all events)'}`);
  if (process.env['AZURE_LOG_LEVEL']) {
    console.log(`SDK logs  : AZURE_LOG_LEVEL=${process.env['AZURE_LOG_LEVEL']}`);
  }
  console.log('');

  const events = await readChangeFeed(client, { startTime, endTime, verbose: true });

  console.log('');
  if (events.length === 0) {
    console.log('No events found in the specified range.');
    return;
  }

  console.log(`Found ${events.length} event(s):\n`);
  console.log(JSON.stringify(events, null, 2));
  console.log("Total Events: ", events.length)
}

main().catch((err: Error) => {
  console.error('\nFatal:', err.message);
  process.exit(1);
});
