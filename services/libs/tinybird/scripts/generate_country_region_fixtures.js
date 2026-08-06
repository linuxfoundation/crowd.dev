#!/usr/bin/env node
// Copyright (c) 2026 The Linux Foundation and each contributor.
// SPDX-License-Identifier: MIT
//
// Reads mledoze_countries_snapshot.json, a checked-in snapshot of countries.json
// pinned at https://github.com/mledoze/countries/blob/09b28e3d03e6ca3fbbac996d716a50d929781e8c/countries.json
// so re-running this script later doesn't depend on network access or pick up upstream drift.

const fs = require('fs');
const path = require('path');

const PIPE_PATH = path.join(__dirname, '../pipes/country_mapping.pipe');
const FIXTURES_DIR = path.join(__dirname, '../datasources/fixtures');
const MLEDOZE_SNAPSHOT_PATH = path.join(__dirname, 'mledoze_countries_snapshot.json');

const UNKNOWN = 'Unknown';

// Sentinel row already present in the live country_mapping_ds/country_mapping_no_flags_ds
// datasources (country_code 'XX') for unmapped/fuzzy-match-fallback locations. Not present
// in country_mapping.pipe's literal, so it must be added explicitly to avoid dropping it
// when fixtures are used to reload the live datasources.
const UNKNOWN_COUNTRY_ROW = { country: 'Unknown', flag: '❓', country_code: 'XX', timezone_offset: 0 };

// Extracts the ('Country', 'flag', 'CODE', offset) tuples from the arrayJoin([...]) literal
// in country_mapping.pipe.
function parsePipeCountries(pipeContent) {
  const tupleRegex = /\(\s*'((?:[^'\\]|\\.|'')*)'\s*,\s*'((?:[^'\\]|\\.|'')*)'\s*,\s*'([A-Z]{2})'\s*,\s*(-?\d+)\s*\)/g;
  const countries = [];
  let match;
  while ((match = tupleRegex.exec(pipeContent)) !== null) {
    const unescape = (s) => s.replace(/''/g, "'");
    countries.push({
      country: unescape(match[1]),
      flag: unescape(match[2]),
      country_code: match[3],
      timezone_offset: parseInt(match[4], 10),
    });
  }
  return countries;
}

function main() {
  const pipeContent = fs.readFileSync(PIPE_PATH, 'utf8');
  const countries = parsePipeCountries(pipeContent);
  countries.push(UNKNOWN_COUNTRY_ROW);
  console.error(`Parsed ${countries.length - 1} countries from country_mapping.pipe, plus the 'XX' unknown-location sentinel row`);

  const mledoze = JSON.parse(fs.readFileSync(MLEDOZE_SNAPSHOT_PATH, 'utf8'));

  const byCca2 = new Map();
  for (const entry of mledoze) {
    if (entry.cca2) byCca2.set(entry.cca2, entry);
  }

  const rows = countries.map(({ country, flag, country_code, timezone_offset }) => {
    const match = byCca2.get(country_code);
    let region = UNKNOWN;
    let subregion = UNKNOWN;
    if (match) {
      region = match.region || UNKNOWN;
      subregion = match.subregion || UNKNOWN;
    } else {
      console.error(`WARNING: no mledoze match for country_code=${country_code} (${country}) — defaulting region/subregion to "${UNKNOWN}"`);
    }
    return { country, flag, country_code, timezone_offset, region, subregion };
  });

  fs.mkdirSync(FIXTURES_DIR, { recursive: true });

  const withFlags = rows.map((row) => JSON.stringify(row));
  fs.writeFileSync(path.join(FIXTURES_DIR, 'country_mapping_ds.ndjson'), withFlags.join('\n') + '\n');

  const noFlags = rows.map(({ country, country_code, timezone_offset, region, subregion }) =>
    JSON.stringify({ country, country_code, timezone_offset, region, subregion }),
  );
  fs.writeFileSync(path.join(FIXTURES_DIR, 'country_mapping_no_flags_ds.ndjson'), noFlags.join('\n') + '\n');

  console.error(`Wrote ${rows.length} rows to country_mapping_ds.ndjson and country_mapping_no_flags_ds.ndjson`);
}

try {
  main();
} catch (err) {
  console.error(err);
  process.exit(1);
}
