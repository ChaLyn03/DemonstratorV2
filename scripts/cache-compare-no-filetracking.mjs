#!/usr/bin/env node
import fs from 'node:fs';
import crypto from 'node:crypto';

const [, , leftPath, rightPath] = process.argv;

if (!leftPath || !rightPath) {
  console.error('Usage: node scripts/cache-compare-no-filetracking.mjs <left.json> <right.json>');
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

const canonicalWithoutFileTracking = (filePath) => {
  const raw = fs.readFileSync(filePath, 'utf8');
  const data = JSON.parse(raw);
  const { fileTracking, ...withoutFileTracking } = data;
  void fileTracking;
  return JSON.stringify(sortDeep(withoutFileTracking));
};

const leftCanonical = canonicalWithoutFileTracking(leftPath);
const rightCanonical = canonicalWithoutFileTracking(rightPath);

const leftHash = crypto.createHash('sha256').update(leftCanonical).digest('hex');
const rightHash = crypto.createHash('sha256').update(rightCanonical).digest('hex');
const equal = leftHash === rightHash;

console.log(`equal: ${equal}`);
console.log(`leftHash:  ${leftHash}`);
console.log(`rightHash: ${rightHash}`);

process.exit(equal ? 0 : 2);
