#!/usr/bin/env node
import { generatePostgresMigration } from '../cli/migration';

const args = process.argv.slice(2);
const command = args[0];

if (!command || command === '--help' || command === '-h') {
  console.log(`
Airlock CLI

Commands:
  migration:generate    Generate the raw SQL migration for the Airlock schema.
  `);
  process.exit(0);
}

if (command === 'migration:generate') {
  console.log(generatePostgresMigration());
} else {
  console.error(`Unknown command: ${command}`);
  process.exit(1);
}
