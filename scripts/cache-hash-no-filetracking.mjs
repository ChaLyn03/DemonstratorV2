#!/usr/bin/env node
import fs from 'node:fs';
import crypto from 'node:crypto';

const [, , inputPath] = process.argv;

if (!inputPath) {
  console.error('Usage: node scripts/cache-hash-no-filetracking.mjs <cache.json>');
  process.exit(1);
}

const sortDeep = (value) => {
  if (Array.isArray(value)) {
    return value.map(sortDeep);
  }
  if (value && typeof value === 'object') {
    const sorted = {};
    Object.keys(value).sort().forEach((key) => {
      sorted[key] = sortDeep(value[key]);
    });
    return sorted;
  }
  return value;
};

const raw = fs.readFileSync(inputPath, 'utf8');
const data = JSON.parse(raw);
const { fileTracking, ...withoutFileTracking } = data;
void fileTracking;

const canonical = JSON.stringify(sortDeep(withoutFileTracking));
const hash = crypto.createHash('sha256').update(canonical).digest('hex');

console.log(hash);
