/*
 * ============================================================================
 * WORK PACK DATA CACHE GENERATION (PIPE SHOP)
 * ============================================================================
 *
 * Purpose
 * -------
 * This worker builds a consolidated JSON cache used by the Pipe Shop area of
 * the application. It ingests multiple upstream exports, normalises and joins
 * them, enriches core entities, and writes a single cache file for faster
 * downstream consumption.
 *
 * Core output structure
 * ---------------------
 * {
 *   version,
 *   pipeDatabase,
 *   spoolDatabase,
 *   iBomRoutedSystems,
 *   cognosActivityDetails,
 *   aveva: {
 *     workpacks,
 *     jobs,
 *     activities,
 *     drls
 *   },
 *   fileTracking
 * }
 *
 * Data domains handled
 * --------------------
 * 1. MCE / master spool data
 * 2. iBOM routed systems / parts and pipe data
 * 3. Cognos activity and DRL data
 * 4. Drawing metadata
 * 5. Bend-ratio data
 * 6. Extended pipe data
 * 7. DOSR install data
 * 8. Spool block / location metadata
 * 9. AVEVA schedule / workpack / activity / job data
 * 10. Windchill pressure-testing data
 *
 * Processing strategy
 * -------------------
 * 1. Load source files and matching mapping configs
 * 2. Read all upstream datasets
 * 3. Remove duplicates / unusable rows
 * 4. Build pipe-only and part-only subsets
 * 5. Derive AVEVA activity / job / workpack / DRL views
 * 6. Enrich pipe and spool records with related metadata
 * 7. Write a single cache JSON file plus provenance tracking
 */

import fs from 'node:fs';
import { workerData } from 'node:worker_threads';
import { readFile, dataToSpreadsheet } from '../utils/FileSystem.js';
import Logging from '../utils/Logging.js';
import { progress } from './Worker.js';


/**
 * Stage 0
 * -------
 * Start-of-job logging so the worker status is visible in console / job logs.
 */
Logging.log('[Pipe Shop] {Starting work pack data creation...|yellow}');

/* ============================================================================
 * OPTIMISATION ROI NOTES
 * ============================================================================
 * These comments are intentionally separate from the documentation comments.
 * They describe the highest-return runtime improvements to implement later,
 * without changing current behaviour in this version.
 *
 * Highest ROI items in this file:
 *
 * 1. Replace repeated Array.find / Array.filter scans with prebuilt Maps.
 *    This file repeatedly scans large arrays by spoolID, activityID, workpackID,
 *    userCode, and DRL. That creates a large amount of avoidable O(n*m) work.
 *
 * 2. Replace duplicate detection arrays with Set-based membership checks.
 *    usedUids.includes(uid) is linear for every row. A Set would reduce that
 *    duplicate pass to near O(n).
 *
 * 3. Pre-group related datasets once, then reuse.
 *    Examples:
 *    - drawings by spoolID
 *    - activities by spoolID
 *    - pressure-testing rows by spoolID
 *    - DOSR rows by spoolID
 *    - Cognos DRLs by activityID
 *    - AVEVA rows by workpackID
 *    These are currently rebuilt through repeated filters during enrichment.
 *
 * 4. Convert existence checks into Set lookups.
 *    spoolDatabase.find(spool => spool.spoolID === x) is used many times only
 *    to answer "does this spool exist?". A Set of spoolIDs is the cheapest fix.
 *
 * 5. Reduce repeated date formatting work with a small helper.
 *    Not the biggest cost, but it removes duplicated logic and repeated checks.
 *
 * 6. Avoid repeated .at(-1) and repeated array indexing after filter passes.
 *    Store the selected object once where the same last or first item is reused.
 *
 * Order of implementation if runtime is the priority:
 *   a) spoolID Set + key Maps
 *   b) grouped lookups for enrichment
 *   c) Set-based duplicate handling
 *   d) grouped AVEVA/Cognos derivation
 *   e) minor cleanup helpers
 *
 * Most of the runtime cost in this worker is lookup strategy, not parsing.
 * ============================================================================
 */

/**
 * Static path configuration
 * -------------------------
 * loadFolder:
 *   Base path containing JSON mapping/config files used during import.
 *
 * outputFolder:
 *   Destination folder for the generated consolidated cache.
 *
 * output:
 *   Final output file path.
 */
const loadFolder = './app/data/pipeshop/';
const outputFolder = './cache/';
const output = `${outputFolder}workpack-data.json`;

/**
 * Source file registry and import config registry
 * -----------------------------------------------
 * files:
 *   Maps internal dataset keys to the physical source file path used for that run.
 *   This is later reused for file-tracking / provenance metadata.
 *
 * config:
 *   Maps internal dataset keys to their corresponding config JSON files.
 *   These configs define field mappings / parsing behaviour for imports.
 *
 * Note:
 *   These are populated dynamically from workerData.data.dataMap so the worker
 *   can operate against varying source-file payloads without hard-coding each
 *   input path individually.
 */
// List of Weekly updated files.
const files = {};
const config = {};

/**
 * Initial job progress
 * --------------------
 * Arguments are consistent with the worker progress API already used elsewhere.
 * This marks the job as started at 0/99.
 */
progress(false, false, 0, 99);

/**
 * Build file and config lookup tables
 * -----------------------------------
 * For each entry in the incoming data map:
 * - files[data.key] becomes the concrete source file path
 * - config[data.key] becomes the JSON config file path for that dataset
 *
 * Example intent:
 *   config.spoolDatabase -> ./app/data/pipeshop/<file>.json
 *   files.spoolDatabase  -> <storageFolder>/<file>.<format>
 */
Object.values(workerData.data.dataMap).forEach((data) => {
  files[data.key] = `${workerData.data.storageFolder}${data.file}.${data.format}`;
  config[data.key] = {
    file: `${loadFolder}${data.file}.json`
  };
});

//////////////////////
// Read report data //
//////////////////////

/**
 * Stage 1A - Core spool master data
 * ---------------------------------
 * Source:
 *   mce-2564
 *
 * Purpose:
 *   This is the canonical spool dataset used as the primary reference set for
 *   spool existence and later spool enrichment.
 *
 * Parsing notes:
 * - Imported as Excel
 * - Reads from sheet 'Excel_2'
 * - cache disabled to force fresh parsing
 * - mapped through config.spoolDatabase
 */
let spoolDatabase = dataToSpreadsheet(workerData.data.fileData['mce-2564'], {
  format: 'excel',
  sheet: 'Excel_2',
  cache: false,
  config: config.spoolDatabase
  // It should now automatically support a file of the same name, but with .json ext as the config file (xlsx and csv)
  // config: {
  //   file: './data/cache-generation/MCE2564 - User Attributes for Spools.json'
  // }
});

/**
 * Visibility log:
 *   Confirms initial spool count after import.
 */
Logging.warn(`[Pipe Shop] spoolDatabase: ${spoolDatabase.length}`);

/**
 * Progress marker after spool import.
 */
progress(false, false, 5);

/**
 * Stage 1B - iBOM routed systems
 * ------------------------------
 * Source:
 *   ibom-pipes
 *
 * Purpose:
 *   Mixed dataset containing both actual pipe rows and non-pipe routed-system
 *   parts. This later gets split into:
 *   - pipeDatabase
 *   - iBomRoutedSystems (non-pipe components only)
 *
 * Parsing notes:
 * - CSV input
 * - autoNumberDetection disabled to preserve identifiers as strings
 * - mapped through config.iBomRoutedSystems
 */
let iBomRoutedSystems = readFile(workerData.data.fileData['ibom-pipes'], {
  format: 'csv',
  cache: false,
  autoNumberDetection: false,
  config: config.iBomRoutedSystems
});

progress(false, false, 10);

/**
 * Stage 1C - Cognos activity detail data
 * --------------------------------------
 * Source:
 *   cognos-ermh5008
 *
 * Purpose:
 *   Provides additional metadata for activities, including revision details
 *   later used to derive red-line / blue-line flags on AVEVA activities.
 */
let cognosActivityDetails = dataToSpreadsheet(workerData.data.fileData['cognos-ermh5008'], {
  format: 'excel',
  sheet: 'ExcelEQLALookahead_2',
  cache: false,
  config: config.cognosActivityDetails
});

progress(false, false, 15);

/**
 * Stage 1D - Cognos DRL detail data
 * ---------------------------------
 * Source:
 *   cognos-ermh5012
 *
 * Purpose:
 *   Used to derive DRL aggregates and determine whether DRLs relate to pipes
 *   or parts, plus linkages to activities and spools.
 *
 * Parsing notes:
 * - raw: true preserves lower-level source values where needed
 * - filtered immediately to the target delivery location only
 *   ('BLD02_PIPE'), which narrows the dataset to Pipe Shop-relevant entries
 */
let cognosDrlDetails = dataToSpreadsheet(workerData.data.fileData['cognos-ermh5012'], {
  format: 'excel',
  cache: false,
  raw: true,
  config: config.cognosDrlDetails
}).filter(row => row.deliveryLocation === 'BLD02_PIPE');

progress(false, false, 20);

/**
 * Stage 1E - Drawing metadata
 * ---------------------------
 * Source:
 *   mce-2693
 *
 * Purpose:
 *   Provides drawing number / revision data to be attached to each spool.
 */
let drawings = dataToSpreadsheet(workerData.data.fileData['mce-2693'], {
  format: 'excel',
  sheet: 'Excel_2',
  cache: false,
  config: config.drawings
});

progress(false, false, 21);

/**
 * Stage 1F - Bend-ratio data
 * --------------------------
 * Source:
 *   mce-2582
 *
 * Purpose:
 *   Provides bend-ratio values later attached to pipe records.
 */
let bendRatioDatabase = dataToSpreadsheet(workerData.data.fileData['mce-2582'], {
  format: 'excel',
  sheet: 'Excel_2',
  cache: false,
  config: config.bendRatioDatabase
});

progress(false, false, 22);

/**
 * Stage 1G - Extended pipe dataset
 * --------------------------------
 * Source:
 *   mce-2691
 *
 * Purpose:
 *   Supplements missing pipe records not present in the iBOM-derived pipe set.
 *   This acts as a recovery / completion source for incomplete upstream data.
 */
let extendedPipeDatabase = dataToSpreadsheet(workerData.data.fileData['mce-2691'], {
  format: 'excel',
  sheet: 'Excel_2',
  cache: false,
  config: config.pipeDatabase
});

progress(false, false, 23);

/**
 * Stage 1H - DOSR install data
 * ----------------------------
 * Source:
 *   mce-0320
 *
 * Purpose:
 *   Provides installation-related metadata later appended to spool records:
 *   - installID
 *   - installDescription
 *   - installStart
 *   - makeOrBuy
 */
let dosr = dataToSpreadsheet(workerData.data.fileData['mce-0320'], {
  format: 'excel',
  sheet: 'Excel_2',
  cache: false,
  config: config.dosr
});

progress(false, false, 24);

/**
 * Stage 1I - Spool block / location data
 * --------------------------------------
 * Source:
 *   ibom-spools
 *
 * Purpose:
 *   Extends spool records with planning and physical-location metadata such as:
 *   - block
 *   - unit
 *   - deck
 *   - compartment
 *   - plannable
 *   - wpName / wpState
 *   - bmodNumber
 *   - fittedDate
 *   - childObsolete
 */
let spoolBlocks = readFile(workerData.data.fileData['ibom-spools'], {
  format: 'csv',
  cache: false,
  config: config.spoolBlocks
});

progress(false, false, 25);

/**
 * Stage 1J - AVEVA schedule / gantt data
 * --------------------------------------
 * Source:
 *   aveva
 *
 * Purpose:
 *   Drives the derived workpack / job / activity model used in the cache.
 *
 * Config note:
 *   Uses an explicit config file rather than config.avevaDataDump because the
 *   gantt-format mapping is fixed to a specific config file name.
 */
let avevaDataDump = dataToSpreadsheet(workerData.data.fileData['aveva'], {
  format: 'excel',
  cache: false,
  config: {
    file: `${loadFolder}aveva-gant-config.json`
  }
});

progress(false, false, 26);

/**
 * Stage 1K - Pressure testing data
 * --------------------------------
 * Source:
 *   windchill-hcf-0000106140
 *
 * Purpose:
 *   Adds pressure-testing requirements and derived pressure-section text to
 *   spool records.
 */
let spoolPressureTesting = dataToSpreadsheet(workerData.data.fileData['windchill-hcf-0000106140'], {
  format: 'excel',
  sheet: '1.1 - Master List',
  cache: false,
  config: config.spoolPressureTesting
});

////////////////////////////
// Filter out unused data //
////////////////////////////

/**
 * Progress marker before cleanup / filtering stage.
 */
progress(false, false, 27);

/* OPTIMISATION ROI:
 * After all source reads complete, this is the best place to build shared
 * lookup structures once, before heavy filtering and enrichment begins.
 *
 * Highest-value candidates:
 * - const spoolIds = new Set(spoolDatabase.map(spool => spool.spoolID));
 * - const spoolById = new Map(spoolDatabase.map(spool => [spool.spoolID, spool]));
 * - const drawingsBySpoolId = Map<spoolID, drawing[]>
 * - const spoolBlocksBySpoolId = Map<spoolID, spoolBlock>
 * - const pressureTestingBySpoolId = Map<spoolID, pt[]>
 * - const dosrBySpoolId = Map<spoolID, dosr[]>
 * - const cognosByActivityId = Map<activityID, cognosActivity>
 * - const cognosDrlsByActivityId = Map<activityID, drl[]>
 * - const avevaRowsByWorkpackId = Map<workpackID, row[]>
 *
 * Building these once removes most repeated full-array scans below.
 */

/**
 * Stage 2A - Duplicate flagging in iBomRoutedSystems
 * --------------------------------------------------
 * Goal:
 *   Detect duplicate routed-system rows using the composite business key:
 *   spoolID + pipeID + userCode
 *
 * Behaviour:
 * - First occurrence is retained
 * - Subsequent duplicates are replaced with undefined in-place
 *
 * Why undefined first, instead of filter immediately:
 *   This preserves index-position mutation during the pass, then later filter
 *   stages remove undefined entries cleanly.
 *
 * Implementation note:
 *   usedUids is an array, so includes(uid) is linear-time lookup.
 *   That is fine here because the task requested comments only, not changes.
 */
// OPTIMISATION NOTES (runtime, highest ROI)
// - Highest ROI here: replace usedUids array + includes() with a Set.
//   Current behaviour performs an O(n) lookup for every row, making the
//   duplicate-detection pass trend toward O(n^2) as the dataset grows.
// - Same output, much lower runtime: Set.has(uid) / Set.add(uid).
// - This is one of the cheapest changes to implement and one of the biggest
//   wins in the whole file because this pass touches the full routed-systems set.
Logging.log(`[Pipe Shop] {Flagging duplicates in iBomRoutedSystems...|yellow} [length: ${iBomRoutedSystems.length}]`);
let usedUids = [];
iBomRoutedSystems.forEach((pipe, index) => {
  let uid = `${pipe.spoolID}_${pipe.pipeID}_${pipe.userCode}`;
  if (usedUids.includes(uid)) {
    iBomRoutedSystems[index] = undefined;
    return false;
  }
  usedUids.push(uid);
  return true;
});
Logging.warn(`[Pipe Shop] Finished flagging duplicates in iBomRoutedSystems, found ${usedUids.length} entires with 1 or more duplicates.`);

/* OPTIMISATION ROI:
 * This is one of the clearest wins in the whole file.
 * Current complexity is effectively O(n^2) because includes() scans usedUids.
 * A Set-based seen-UID lookup preserves behaviour and drops this pass to O(n).
 *
 * Safe future shape:
 *   const usedUids = new Set();
 *   if (usedUids.has(uid)) ...
 *   usedUids.add(uid);
 */

progress(false, false, 50);

/**
 * Stage 2B - Build initial pipeDatabase from mixed iBOM routed systems data
 * -------------------------------------------------------------------------
 * Goal:
 *   Extract rows representing actual pipes only.
 *
 * Rule:
 *   pipe.pipeID must start with 'PIPE'
 *
 * Reason:
 *   iBomRoutedSystems contains both pipes and parts/non-pipe items.
 *   This step isolates the pipe subset for later pipe-specific enrichment.
 */
// Create Pipe Database from DataPORT data (to fix issues with missing MCE data, e.g. CCLZ-0060-010305 contains no pipe
Logging.log('[Pipe Shop] {Filtering iBomRoutedSystems to create pipeDatabase...|yellow}');
let pipeDatabase = iBomRoutedSystems.filter(pipe => {
  if (!pipe) { return false; }
  if (pipe.pipeID.startsWith('PIPE')) { return true; }
  return false;
});
Logging.warn(`[Pipe Shop] New pipeDatabase... [length: ${pipeDatabase.length}]`);

progress(false, false, 51);

/**
 * Stage 2C - Backfill missing pipes from extended pipe source
 * -----------------------------------------------------------
 * Goal:
 *   Add pipe records from extendedPipeDatabase when no existing pipeDatabase
 *   record has the same userCode.
 *
 * Matching key used:
 *   userCode
 *
 * Outcome:
 *   tempEx contains missing extended records and is concatenated into the
 *   active pipeDatabase.
 */
// OPTIMISATION NOTES (runtime, highest ROI)
// - Highest ROI here: pre-build a Set of pipeDatabase userCode values before
//   scanning extendedPipeDatabase.
// - Current behaviour does a full pipeDatabase.find(...) for each extended row,
//   which is another repeated O(n*m) pattern.
// - A Set-backed membership check would reduce this stage to near-linear time
//   while preserving the exact same matching rule (userCode).
let tempEx = extendedPipeDatabase.filter(exPipe => !pipeDatabase.find(pipe => pipe.userCode === exPipe.userCode));
pipeDatabase = pipeDatabase.concat(tempEx);
Logging.warn(`[Pipe Shop] Adding missing pipe to pipeDatabase, added: ${tempEx.length}`);
Logging.warn(`[Pipe Shop] Updated pipeDatabase... [length: ${pipeDatabase.length}]`);

/* OPTIMISATION ROI:
 * This backfill currently does a full pipeDatabase scan for every extended pipe.
 * A Set of existing userCode values gives a major speedup on large pipe sets.
 *
 * Best later approach:
 * - build const pipeUserCodes = new Set(pipeDatabase.map(pipe => pipe.userCode))
 * - filter extendedPipeDatabase using pipeUserCodes.has(exPipe.userCode)
 * - if concatenating, update the Set as records are accepted
 */

progress(false, false, 55);

/**
 * Stage 2D - Validate and initialise pipeDatabase
 * -----------------------------------------------
 * Validation rules:
 * - spoolID must exist
 * - pipeID must exist
 * - referenced spool must exist in spoolDatabase
 *
 * Enrichment performed during same pass:
 * - initialise pipe.bendRatio = 0
 *
 * Reason for setting bendRatio here:
 *   Ensures every surviving pipe has a default value before later bend-ratio
 *   enrichment is applied.
 */
// Pipe Database
Logging.log(`[Pipe Shop] {Filtering & Extending pipeDatabase...|yellow} [length: ${pipeDatabase.length}]`);
// OPTIMISATION NOTES (runtime, highest ROI)
// - Highest ROI here: build a spool lookup structure once, before the major
//   filtering/enrichment stages:
//     1) Set for existence checks by spoolID
//     2) Map for direct spool retrieval by spoolID
// - Current code repeatedly scans spoolDatabase with .find(...) inside many
//   loops. That creates a large amount of avoidable repeated work.
// - This single change would accelerate:
//   * pipeDatabase validation
//   * bendRatioDatabase validation
//   * iBomRoutedSystems validation
//   * AVEVA activity filtering
//   * bend-ratio enrichment
//   * any later spool-based lookups
pipeDatabase = pipeDatabase.filter((pipe) => {
  if (!pipe.spoolID) { return false; }
  if (!pipe.pipeID) { return false; }
  if (!spoolDatabase.find(spool => spool.spoolID === pipe.spoolID)) { return false; }

  // Use the filter pass to extend pipe properties
  pipe.bendRatio = 0;

  return true;
});
Logging.warn(`[Pipe Shop] Filtered pipeDatabase: ${pipeDatabase.length}`);

/* OPTIMISATION ROI:
 * Another very high-return fix.
 * The spool existence check currently scans spoolDatabase for every pipe row.
 * Replacing this with a prebuilt Set of spoolIDs removes a major repeated cost.
 */

progress(false, false, 60);

/**
 * Stage 2E - Validate bend-ratio dataset
 * --------------------------------------
 * Validation rules:
 * - spoolID must exist
 * - spool must exist in spoolDatabase
 *
 * Goal:
 *   Remove bend entries that cannot be reliably linked to valid spools.
 */
// Pipe Bend Data
Logging.log(`[Pipe Shop] {Filtering bendRatioDatabase...|yellow} [length: ${bendRatioDatabase.length}]`);
// OPTIMISATION NOTES (runtime, highest ROI)
// - This stage would benefit directly from the same precomputed spoolID Set
//   mentioned above.
// - Without that, each retained bend-ratio row performs a linear scan across
//   spoolDatabase just to answer an existence question.
bendRatioDatabase = bendRatioDatabase.filter((pipe) => {
  if (!pipe.spoolID) { return false; }
  if (!spoolDatabase.find(spool => spool.spoolID === pipe.spoolID)) { return false; }
  return true;
});
Logging.warn(`[Pipe Shop] Filtered bendRatioDatabase: ${bendRatioDatabase.length}`);

/* OPTIMISATION ROI:
 * Same issue as above. This should also use the same shared spoolID Set.
 * Reusing one lookup object across all spool existence checks is cheap and
 * removes repeated full scans in multiple stages.
 */

progress(false, false, 65);

/**
 * Stage 2F - Validate drawing records
 * -----------------------------------
 * Rule:
 *   Keep only drawing rows with a spoolID.
 *
 * Reason:
 *   Drawings are appended to spools by spoolID; records without spoolID are
 *   unusable for that linkage.
 */
// MCE Drawing Details
Logging.log(`[Pipe Shop] {Filtering drawings...|yellow} [length: ${drawings.length}]`);
drawings = drawings.filter(drawing => drawing.spoolID);
Logging.warn(`[Pipe Shop] Filtered drawings: ${drawings.length}`);

/**
 * Stage 2G - Reduce iBomRoutedSystems to non-pipe routed-system items only
 * ------------------------------------------------------------------------
 * Validation rules:
 * - row must exist
 * - spoolID must exist
 * - pipeID must exist
 * - pipeID must NOT start with 'PIPE'
 * - referenced spool must exist in spoolDatabase
 *
 * Result:
 *   After this pass, iBomRoutedSystems no longer contains actual pipe rows.
 *   It becomes the non-pipe component / parts subset only.
 */
// iBom > Routed Systems (e.g. Pipes and Parts)
Logging.log(`[Pipe Shop] {Filtering iBomRoutedSystems...|yellow} [length: ${iBomRoutedSystems.length}]`);
// OPTIMISATION NOTES (runtime, highest ROI)
// - Same issue again: repeated spoolDatabase.find(...) inside a full-dataset
//   filter pass.
// - Consolidating spool lookups behind a Set/Map would remove another major
//   source of repeated linear scans.
// - Because this file validates several large datasets against the same spool
//   identity set, shared indexes have a compounding payoff.
iBomRoutedSystems = iBomRoutedSystems.filter((pipe) => {
  if (!pipe) { return false; }
  if (!pipe.spoolID) { return false; }
  if (!pipe.pipeID) { return false; }
  if (pipe.pipeID.startsWith('PIPE')) { return false; }
  if (!spoolDatabase.find(spool => spool.spoolID === pipe.spoolID)) { return false; }
  return true;
});
Logging.warn(`[Pipe Shop] Filtered iBomRoutedSystems: ${iBomRoutedSystems.length}`);

/* OPTIMISATION ROI:
 * This is the third separate pass using spoolDatabase.find for existence only.
 * A shared Set would remove all three repeated scans together.
 */

progress(false, false, 70);

////////////////////////////////
// Process AVEVA Data for use //
////////////////////////////////

/**
 * Stage 3A - Derive AVEVA activity records
 * ----------------------------------------
 * Goal:
 *   Build a clean activity list from AVEVA rows marked as activities and tied
 *   to valid spools.
 *
 * Inclusion criteria:
 * - row.isActivity
 * - row.spoolID exists
 * - row.spoolID exists in spoolDatabase
 *
 * Derived fields include:
 * - normalised date strings
 * - DRL links for parts and pipes
 * - inferred job linkage
 * - Cognos revision / red-line / blue-line metadata
 *
 * Important behavioural note:
 *   This code intentionally mutates AVEVA rows:
 *   - row.requestedSpools is temporarily cleared
 *   - row.isJob may be forced true in the no-parent-job fallback case
 *
 * That mutation is part of the original logic and affects downstream job
 * processing, so it is preserved exactly.
 */

// e.g. activity number and DRLs
Logging.log('[Pipe Shop] {Process AVEVA activities...|yellow}');
// OPTIMISATION NOTES (runtime, highest ROI)
// - Highest ROI area in the AVEVA stage: pre-index dependent datasets before
//   iterating the full avevaDataDump.
// - Current per-row work repeatedly scans:
//   * spoolDatabase            -> spool existence
//   * cognosDrlDetails         -> activity-linked DRLs
//   * avevaDataDump            -> parent job resolution
//   * cognosActivityDetails    -> matching activity revision
// - Best payoff:
//   * Map<spoolID, spool>
//   * Map<activityID, cognos activity>
//   * Map<activityID, cognos DRL rows[]>
//   * Map<workpackID, candidate parent-job rows[]>
// - That would convert several nested scans into direct lookups and materially
//   reduce runtime in the heaviest derived-data section of the file.
const activities = [];
avevaDataDump.forEach((row) => {
  if (row.isActivity && row.spoolID) {
    if (spoolDatabase.find(spool => spool.spoolID === row.spoolID)) {

      // Remove errors in requestedSpools
      /* let uniqueRequestedSpools = [...new Set(row.requestedSpools)];
      let index = uniqueRequestedSpools.indexOf(row.spoolID);
      if (index >= 0) { uniqueRequestedSpools.splice(index, 1); }
      row.requestedSpools = uniqueRequestedSpools;
      if (row.requestedSpools.length === 0) { row.isActivity = true; } */

      /**
       * Base activity projection
       * ------------------------
       * Extracts only the fields required by downstream consumers and converts
       * Date objects to YYYY-MM-DD strings where present.
       */
      const activity = {
        row: row.rowIndex,
        spoolID: row.spoolID,
        activityID: row.activityID,
        workpackID: row.workpackID,
        // drlParts: [],
        status: row.status || '',
        description: row.description,
        plannedStart: row.plannedStart?.constructor === Date ? row.plannedStart.toISOString().split('T')[0] : '',
        startDate: row.startDate?.constructor === Date ? row.startDate.toISOString().split('T')[0] : '',
        finishDate: row.finishDate?.constructor === Date ? row.finishDate.toISOString().split('T')[0] : '',
        sequenceComments: row.sequenceComments,
        manufacture: row.manufacture,
        manSetDate: row.manSetDate?.constructor === Date ? row.manSetDate.toISOString().split('T')[0] : '',
        manIssueForecast: row.manIssueForecast?.constructor === Date ? row.manIssueForecast.toISOString().split('T')[0] : '',
        manComments: row.manComments,
        scopeComments: row.scopeComments,
        releasedDate: row.releasedDate?.constructor === Date ? row.releasedDate.toISOString().split('T')[0] : '',
        nc: row.nc,
        printIssueDate: row.printIssueDate?.constructor === Date ? row.printIssueDate.toISOString().split('T')[0] : '',
        prodJobPlanner: row.prodJobPlanner,
        releaseHoldUp: row.releaseHoldUp,
        planningReadyDate: row.planningReadyDate?.constructor === Date ? row.planningReadyDate.toISOString().split('T')[0] : '',
        revisionReason: row.revisionReason,
        workDescription: row.workDescription
      };

      /**
       * Related DRL extraction
       * ----------------------
       * Pull all Cognos DRL rows for the current activity, then separate into:
       * - drlParts: DRLs linked to non-pipe items
       * - drlPipes: DRLs linked to pipe items
       *
       * Sets are used to enforce uniqueness.
       */
            // OPTIMISATION NOTES (runtime)
      // - This filter runs for every qualifying AVEVA activity.
      // - Pre-grouping Cognos DRLs by activityID once upfront would remove this
      //   repeated full scan and turn it into a direct lookup.
      let relatedDrls = cognosDrlDetails.filter(entry => entry.activityID === row.activityID);

      let drlParts = [...new Set(relatedDrls.filter(entry => !entry.pipeID.startsWith('PIPE')).map(entry => entry.drl))];
      let drlPipes = [...new Set(relatedDrls.filter(entry => entry.pipeID.startsWith('PIPE')).map(entry => entry.drl))];

      activity.drlParts = drlParts;

      /**
       * Parent job resolution
       * ---------------------
       * To determine the job that owns the current spool activity:
       * 1. Temporarily clear row.requestedSpools
       * 2. Search AVEVA for another item in same workpack whose requestedSpools
       *    includes this activity's spoolID
       *
       * If found:
       *   - that item's activityID becomes activity.jobID
       *   - job DRL plus pipe DRLs are combined
       *
       * If not found:
       *   - this activity is promoted to job status
       *   - row.isJob is set true
       *   - requestedSpools restored to the current spool
       *   - activity links to itself as jobID
       *
       * This fallback preserves downstream integrity when parent job structure is
       * absent or inconsistent in AVEVA source data.
       */

      // Clear the spools so that this activity is detected as a job
      row.requestedSpools = [];
            // OPTIMISATION NOTES (runtime)
      // - Parent-job resolution is currently a scan across the whole AVEVA dump
      //   for every activity.
      // - A prebuilt index keyed by workpackID, and ideally also by requested
      //   spool membership, would remove one of the most expensive repeated
      //   searches in the file.
      let job = avevaDataDump.find(item => (item.workpackID === row.workpackID) && item.requestedSpools.includes(row.spoolID));
      if (job) {
        activity.jobID = job.activityID;
        activity.drlPipe = [...new Set([job.drl, ...drlPipes])];
      } else {
        row.isJob = true;
        row.requestedSpools = [row.spoolID];

        activity.jobID = row.activityID;
        activity.drlPipe = drlPipes;
      }

      /**
       * Cognos activity augmentation
       * ----------------------------
       * Adds revision metadata when a matching Cognos activity record exists.
       *
       * Derived flags:
       * - redLine: revision contains 'RL'
       * - blueLine: revision contains 'BL'
       */
            // OPTIMISATION NOTES (runtime)
      // - A Map keyed by activityID would be the highest-ROI replacement for
      //   this repeated find().
      let cognos = cognosActivityDetails.find(item => item.activityID === row.activityID);
      if (cognos) {
        activity.revision = cognos.revision;
        activity.redLine = typeof cognos.revision === 'string' ? cognos.revision.includes('RL') : false;
        activity.blueLine = typeof cognos.revision === 'string' ? cognos.revision.includes('BL') : false;
      }

      /**
       * Commit the fully derived activity record.
       */
      activities.push(activity);
    }
  }
});

/* OPTIMISATION ROI:
 * This activity derivation block is one of the biggest runtime hotspots.
 *
 * Current repeated costs inside the loop:
 * - spoolDatabase.find(...)                     -> spool existence scan
 * - cognosDrlDetails.filter(...)               -> full DRL scan per activity
 * - avevaDataDump.find(...)                    -> full AVEVA scan per activity
 * - cognosActivityDetails.find(...)            -> full Cognos scan per activity
 *
 * Best future optimisation strategy:
 * 1. Use spoolID Set for existence checks.
 * 2. Pre-group cognosDrlDetails by activityID.
 * 3. Pre-map cognosActivityDetails by activityID.
 * 4. Pre-group avevaDataDump by workpackID so the parent-job search is local
 *    to the workpack instead of scanning the entire AVEVA dataset each time.
 *
 * This single section likely offers the largest overall speedup after the
 * shared lookup structures are introduced.
 */

/**
 * validWorkpacks
 * --------------
 * Used later to ensure only workpacks that actually own valid activities are
 * included in the exported workpack list.
 */
let validWorkpacks = new Set(activities.map(activity => activity.workpackID));

Logging.warn(`[Pipe Shop] AVEVA activities: ${activities.length}`);

progress(false, false, 85);

/**
 * Stage 3B - Derive DRL aggregates
 * --------------------------------
 * Goal:
 *   Collapse raw Cognos DRL rows into a grouped DRL structure:
 *   {
 *     id,
 *     pipes: [],
 *     parts: [],
 *     mvr,
 *     nc,
 *     links: { activityID, spoolID }
 *   }
 *
 * Inclusion rule:
 *   Rows with pickQty === 0 are ignored, because nothing was actually picked.
 *
 * Pipe grouping logic:
 * - Match by line if available
 * - otherwise by (pipeID + occNumber)
 *
 * Part grouping logic:
 * - same dedupe rule as above
 * - if occNumber missing and pickQty > 1, synthetic duplicate part entries are
 *   generated with derived line numbers so quantity is represented explicitly
 *   in the exported structure
 */
Logging.log('[Pipe Shop] {Process AVEVA DRLs...|yellow}');
const drls = [];
// OPTIMISATION NOTES (runtime, medium-high ROI)
// - DRL aggregation currently searches drls with .find(...) on every Cognos
//   entry, so lookup cost grows as drls grows.
// - A Map keyed by drl id would keep grouping behaviour identical while making
//   insertion/lookup effectively constant time.
// - Lower ROI than the shared spool indexes and AVEVA indexes, but still a
//   worthwhile optimisation because this loop can be large.
cognosDrlDetails.forEach(entry => {
  // If no pipe length or part items were picked, skip this entry
  if (entry.pickQty === 0) { return false; }

  let drl = drls.find(drlRow => drlRow.id === entry.drl);

  // If DRL entry is missing, add to array and set drl to the new Obj
  if (!drl) {
    drl = {
      id: entry.drl,
      pipes: [],
      parts: [],
      mvr: (entry.note || '').includes('MVR'),
      nc: entry.nc,
      links: {
        activityID: entry.activityID,
        spoolID: entry.spoolID
      }
    };
    drls.push(drl);
  }

  /**
   * Pipe entries
   * ------------
   * Stores line-level length and supplied-length detail.
   */
  if (entry.pipeID.startsWith('PIPE')) {
    let foundPipe = drl.pipes.find(pipe => {
      if (entry.line === pipe.line) { return true; }
      if (entry.occNumber && (entry.pipeID === pipe.pipeID) && (entry.occNumber === pipe.occNumber)) { return true; }
      return false;
    });

    if (!foundPipe) {
      drl.pipes.push({
        line: entry.line,
        pipeID: entry.pipeID,
        occNumber: entry.occNumber,
        length: entry.requiredQty,
        suppliedLength: entry.pickQty
      });
    }
  } else {
    /**
     * Part entries
     * ------------
     * Stores identity-level part records. When occNumber is absent and quantity
     * is greater than one, the additional quantity is expanded into synthetic
     * separate entries to preserve count semantics.
     */
    let foundParts = drl.parts.find(part => {
      if (entry.line === part.line) { return true; }
      if (entry.occNumber && (entry.pipeID === part.pipeID) && (entry.occNumber === part.occNumber)) { return true; }
      return false;
    });

    if (!foundParts) {
      drl.parts.push({
        line: entry.line,
        pipeID: entry.pipeID,
        occNumber: entry.occNumber
      });

      if (!entry.occNumber && (entry.pickQty > 1)) {
        for (let index = 1; index < entry.pickQty; index++) {
          drl.parts.push({
            line: (entry.line * 10000) + index,
            pipeID: entry.pipeID,
            occNumber: entry.occNumber
          });
        }
      }
    }
  }
  
});

Logging.warn(`[Pipe Shop] AVEVA DRLs: ${drls.length}`);

/* OPTIMISATION ROI:
 * drls.find(drlRow => drlRow.id === entry.drl) scans the current DRL output
 * array for every Cognos DRL row. As DRL count grows, this becomes expensive.
 *
 * High-return future fix:
 * - maintain a Map keyed by DRL id during construction
 * - keep the output array for ordering/serialisation if needed
 *
 * That preserves external structure while removing repeated group lookup scans.
 */

progress(false, false, 88);

/**
 * Stage 3C - Derive job records
 * -----------------------------
 * Goal:
 *   Export AVEVA rows currently marked as jobs into a simplified job structure.
 *
 * DRL enrichment:
 *   Related pipe DRLs from Cognos are merged with the AVEVA job's direct drl
 *   field, then uniqued.
 */
Logging.log('[Pipe Shop] {Process AVEVA jobs...|yellow}');
// OPTIMISATION NOTES (runtime, medium-high ROI)
// - Job derivation repeats the same Cognos DRL filtering pattern already used
//   in activities.
// - Reusing a pre-grouped Map<activityID, drl rows[]> would optimise both
//   stages at once and avoid duplicated full scans of cognosDrlDetails.
const jobs = avevaDataDump.filter(activity => activity.isJob).map((job) => {
  let relatedDrls = cognosDrlDetails.filter(entry => entry.activityID === job.activityID);
  let drlPipes = [...new Set(relatedDrls.filter(entry => entry.pipeID.startsWith('PIPE')).map(entry => entry.drl))];

  return {
    row: job.rowIndex,
    jobID: job.activityID,
    workpackID: job.workpackID,
    status: job.status,
    manufacture: job.manufacture,
    manComments: job.manComments,
    scopeComments: job.scopeComments,
    drlPipe: [...new Set([job.drl, ...drlPipes])],
    spools: job.requestedSpools,
    description: job.description
  };
});
Logging.warn(`[Pipe Shop] AVEVA jobs: ${jobs.length}`);

/* OPTIMISATION ROI:
 * The repeated cognosDrlDetails.filter(entry => entry.activityID === job.activityID)
 * should reuse the same activityID-grouped DRL lookup suggested for activities.
 * That gives a shared optimisation across both Stage 3A and 3C.
 */

progress(false, false, 90);

/**
 * Stage 3D - Derive workpack records
 * ----------------------------------
 * Inclusion rule:
 *   Export only AVEVA rows marked isWorkpack where the workpackID exists in
 *   validWorkpacks.
 *
 * Hours model:
 *   1. Prefer explicit isHours rows within the workpack
 *   2. Fallback to the workpack's own hours value if > 0
 *
 * Output includes:
 * - workpack identity and status
 * - list of child job IDs
 * - selected hours breakdown
 */
Logging.log('[Pipe Shop] {Process AVEVA workpacks...|yellow}');
// OPTIMISATION NOTES (runtime, medium-high ROI)
// - Workpack derivation repeatedly rescans avevaDataDump for:
//   * hours rows per workpack
//   * job rows per workpack
// - Pre-grouping rows by workpackID once would let this stage reuse those
//   arrays directly instead of paying for repeated full-dataset filters.
const workpacks = avevaDataDump.filter(activity => activity.isWorkpack && validWorkpacks.has(activity.workpackID)).map((workpack) => {
  let selectedHours = avevaDataDump.filter(activity => activity.isHours && (workpack.workpackID === activity.workpackID)).map((activity) => {
    return {
      activityID: activity.activityID,
      status: activity.status,
      hours: activity.hours,
      description: activity.description
    };
  });

  /**
   * Fallback hours population
   * -------------------------
   * If no explicit hours rows exist, reuse the workpack's own hours field.
   */
  if (selectedHours.length === 0) {
    if (workpack.hours > 0) {
      selectedHours.push({
        activityID: workpack.workpackID,
        status: workpack.status,
        hours: workpack.hours,
        description: workpack.description
      });
    }
  }

  return {
    row: workpack.rowIndex,
    projectID: workpack.projectID,
    workpackID: workpack.workpackID,
    description: workpack.description,
    status: workpack.status,
    jobs: avevaDataDump.filter(activity => activity.isJob && (workpack.workpackID === activity.workpackID)).map(activity => activity.activityID),
    hours: selectedHours
  };
});
Logging.warn(`[Pipe Shop] AVEVA workpacks: ${workpacks.length}`);

/* OPTIMISATION ROI:
 * Workpack derivation currently re-scans avevaDataDump multiple times per
 * workpack:
 * - once for hours rows
 * - once for job rows
 *
 * Best future fix:
 * pre-group AVEVA rows by workpackID once, then derive hours/jobs from the
 * small local group instead of scanning the full dataset for every workpack.
 */

// Check for orphan activities
/* activities.filter((activity) => {
  let parentJobs = jobs.filter((job) => {
    return job.spools.includes(activity.spoolID);
  });
  
  if (parentJobs.length === 0) {
    console.log(activity)
  }
}); */


///////////////////////////
// Extend core data sets //
///////////////////////////

progress(false, false, 93);

/**
 * Stage 4A - Apply bend-ratio values to pipeDatabase
 * --------------------------------------------------
 * Goal:
 *   Find the matching pipe by (userCode + spoolID) and assign bendRatio.
 *
 * Validation / diagnostic logic:
 * - parse bendRatio as float
 * - if integer base is not 2 or 3, perform additional validation
 * - values are accepted if they are in validBendRatios
 * - otherwise they are tolerated only when spool.heijunka is one of the
 *   invalidHeijunka exception codes
 *
 * Logging:
 *   Any suspicious / invalid bend ratio is logged as an error with spool ID
 *   and heijunka code for investigation.
 */
Logging.log('[Pipe Shop] {Extending pipeDatabase...|yellow} [adding bend information]');
const validBendRatios = [2, 3, 11.8, 14.9];
const invalidHeijunka = ['MAS', 'N/A', 'CHOOSE THE APPLICABLE CODE'];
// OPTIMISATION NOTES (runtime, highest ROI)
// - Highest ROI here: build a composite-key Map for pipes, e.g.
//   `${userCode}__${spoolID}` -> pipe row.
// - Current behaviour scans pipeDatabase for every bend item, which becomes
//   expensive at scale.
// - This same pipe index could also support other future pipe-level enrichments
//   without additional search cost.
bendRatioDatabase.forEach((bendItem) => {
  let pipe = pipeDatabase.find(pipe => (pipe.userCode === bendItem.userCode) && (pipe.spoolID === bendItem.spoolID));
  if (pipe) {
    pipe.bendRatio = bendItem.bendRatio; 
    let value = parseFloat(bendItem.bendRatio);
    let baseValue = Math.floor(value);
    if (baseValue !== 2 && baseValue !== 3) {
      let spool = spoolDatabase.find(spool => spool.spoolID === pipe.spoolID);
      if (spool && !validBendRatios.includes(value) && !invalidHeijunka.includes(spool.heijunka)) {
        Logging.error(`[Pipe Shop] [${pipe.spoolID}] Invalid Bend Ratio: ${value} [${spool.heijunka}]`);
      }
    }
  }
});

/* OPTIMISATION ROI:
 * This loop should eventually use:
 * - a pipe lookup keyed by `${userCode}__${spoolID}`
 * - a spool lookup keyed by spoolID
 *
 * Right now every bend item may scan both pipeDatabase and spoolDatabase.
 * That makes this enrichment much more expensive than necessary.
 */

progress(false, false, 95);

/**
 * Stage 4B - Enrich spoolDatabase
 * -------------------------------
 * For each spool:
 * 1. Find matching spool block record
 * 2. Copy block / unit / deck / compartment and planning metadata
 * 3. Attach drawing list
 * 4. Attach AVEVA activity list
 * 5. Attach pressure-testing metadata
 * 6. Attach DOSR installation metadata
 *
 * Important behavioural note:
 *   Enrichment only occurs when a spoolBlock exists for the spool. If no
 *   spoolBlock is found, none of the following assignments are performed for
 *   that spool.
 */
Logging.log('[Pipe Shop] {Extending spoolDatabase...|yellow}');
// OPTIMISATION NOTES (runtime, highest ROI)
// - This is the other major hotspot.
// - For each spool, the current code performs repeated .find() / .filter()
//   scans across multiple full datasets:
//   * spoolBlocks
//   * drawings
//   * activities
//   * spoolPressureTesting
//   * dosr
// - Highest ROI improvement: pre-group each of those datasets by spoolID once,
//   then enrich each spool via direct lookup.
// - This would drastically reduce total runtime because spool enrichment is
//   currently a nested-scan fan-out stage over several collections.
spoolDatabase.forEach((spool) => {
  let spoolBlock = spoolBlocks.find(spoolBlock => spoolBlock.spoolID === spool.spoolID);
  if (spoolBlock) {
    /**
     * 4B.1 - Block / physical location / planning metadata
     * ----------------------------------------------------
     * compartment is normalised to number where possible.
     */
    spool.block = spoolBlock.block;
    spool.unit = spoolBlock.unit;
    spool.deck = spoolBlock.deck;
    spool.compartment = (typeof spoolBlock.compartment === 'number' ? spoolBlock.compartment : parseInt(spoolBlock.compartment));

    spool.plannable = spoolBlock.plannable;
    spool.wpName = spoolBlock.wpName;
    spool.wpState = spoolBlock.wpState;
    spool.bmodNumber = spoolBlock.bmodNumber;
    spool.fittedDate = spoolBlock.fittedDate;
    spool.childObsolete = spoolBlock.childObsolete;

    /**
     * 4B.2 - Drawing attachment
     * -------------------------
     * Adds all drawings matching the spool, with a derived pmf boolean flag.
     */
    // Append drawing information, as it came from Foran
    spool.drawings = drawings.filter(drawing => drawing.spoolID === spool.spoolID).map((drawing) => {
      return {
        pmf: drawing.drawingNum.startsWith('HCF-PMF'),
        number: drawing.drawingNum,
        revision: drawing.drawingRev
      };
    });

    /**
     * 4B.3 - Activity attachment
     * --------------------------
     * Appends all derived AVEVA activities linked to this spool.
     */
    // Append the activity details from AVEVA
    spool.activities = activities.filter(activity => activity.spoolID === spool.spoolID);

    /**
     * 4B.4 - Pressure-testing enrichment
     * ----------------------------------
     * Rules:
     * - If multiple pressure-testing records exist, warn and use the first
     * - If none exist, populate explicit empty defaults
     * - pressureTesting is true when ptRequired starts with 'Yes' or includes 'HOLD'
     * - pressureSection is a formatted string derived from subsystem + lrClass
     */
    // Append the pressure testing information
    // OPTIMISATION NOTES (runtime)
    // - Pre-group pressure-testing rows by spoolID to avoid filtering the full
    //   pressure-testing dataset for every spool.
    let ptSpoolList = spoolPressureTesting.filter(ptSpool => ptSpool.spoolID === spool.spoolID);
    if (ptSpoolList.length > 1) {
      Logging.warn(`[Pipe Shop] Multiple pressure testing data entries found for spool {${spool.spoolID}|cyan}. The first entry will be used.`);
    }
    
    if (ptSpoolList.length === 0) {
      spool.pressureTesting = false;
      spool.pressureValue = '';
      spool.pressureSection = '';
      spool.pressureMedium = '';
    } else {
      let ptSpool = ptSpoolList[0];
      spool.pressureTesting = ptSpool.ptRequired.startsWith('Yes') || ptSpool.ptRequired.includes('HOLD');
      spool.pressureMedium = spool.pressureTesting ? ptSpool.ptMedium : '';
      spool.pressureValue = spool.pressureTesting ? ptSpool.testValue : '';
      spool.pressureSection = spool.pressureTesting ? `${ptSpool.subsystem.slice(7).replace(/\(Except Below\):$/, '').trim()} (Lloyds Class ${ptSpool.lrClass})` : '';
    }

    /**
     * 4B.5 - DOSR enrichment
     * ----------------------
     * Uses all DOSR records matching the spoolID.
     *
     * Behaviour:
     * - If none exist, assign empty defaults
     * - If one or more exist, use the LAST entry in the list
     *
     * Note:
     *   The duplicate-warning block is intentionally commented out in the
     *   original source and remains unchanged.
     */
    // OPTIMISATION NOTES (runtime)
    // - Same pattern as pressure testing: pre-group DOSR rows by spoolID once.
    let dosrSpoolList = dosr.filter(dosrSpool => dosrSpool.spoolID === spool.spoolID);

    // if (dosrSpoolList.length > 1) {
    //   Logging.warn(`[Pipe Shop] Multiple DOSR entries found for spool {${spool.spoolID}|cyan}. The last entry will be used.`);
    // }
    
    if (dosrSpoolList.length === 0) {
      spool.installID = '';
      spool.installDescription = '';
      spool.installStart = '';
      spool.makeOrBuy = '';
    } else {
      spool.installID = dosrSpoolList.at(-1).installID;
      spool.installDescription = dosrSpoolList.at(-1).installDescription;
      spool.installStart = dosrSpoolList.at(-1)?.installStart.toISOString().split('T')[0] || '';
      spool.makeOrBuy = dosrSpoolList.at(-1).makeOrBuy;
    }

    // if (spool.activities.length) { console.log(spool) }

    // Enable to get inTank value check logging
    /* if (pipeConfig.tanks.includes(spool.compartment)) {
      //console.log('in tank...')
      if (!spool.materialFinish.includes('NOT PAINTED') && spool.materialFinish.includes('PAINTED')) {
        //if ([5073, 5095].includes(spool.compartment)) {
        Logging.log(`{Painted Pipe in Tank.|yellow} spool: {${spool.spoolID}|magenta}, compartment: {${spool.compartment}|magenta}, finish: {${spool.materialFinish}|magenta}`);
        //console.log(`spool: ${spool.spoolID}, compartment: ${spool.compartment}, finish: ${spool.materialFinish}`);
        //}
      }
    }  */   
  }
});

/* OPTIMISATION ROI:
 * This spool enrichment loop is probably the single worst repeated-scan section
 * in the file after AVEVA activity derivation.
 *
 * For every spool it currently does:
 * - spoolBlocks.find(...)
 * - drawings.filter(...)
 * - activities.filter(...)
 * - spoolPressureTesting.filter(...)
 * - dosr.filter(...)
 *
 * That means multiple full-array scans per spool.
 *
 * Best future fix:
 * pre-group each related dataset by spoolID once:
 * - spoolBlockBySpoolId        -> Map<spoolID, spoolBlock>
 * - drawingsBySpoolId          -> Map<spoolID, drawing[]>
 * - activitiesBySpoolId        -> Map<spoolID, activity[]>
 * - pressureTestingBySpoolId   -> Map<spoolID, pt[]>
 * - dosrBySpoolId              -> Map<spoolID, dosr[]>
 *
 * Then this loop becomes almost entirely constant-time lookup work.
 *
 * That change is high ROI and low semantic risk if implemented carefully.
 */


/////////////////////////////////////////////////////////////
// Generate file format, convert to JSON and write to disk //
/////////////////////////////////////////////////////////////

/**
 * Progress marker before final serialisation.
 */
progress(false, false, 98);

/**
 * Stage 5A - Final cache assembly
 * -------------------------------
 * version:
 *   Schema/version marker for downstream consumers.
 *
 * fileTracking:
 *   Begins with a synthetic entry describing the cache build timestamp itself.
 */
Logging.debug(`[Pipe Shop] Writing cache file: {${output}|cyan} (json:utf8)`);
const cacheData = {
  version: 2,
  pipeDatabase: pipeDatabase,
  spoolDatabase: spoolDatabase,
  iBomRoutedSystems: iBomRoutedSystems,
  cognosActivityDetails: cognosActivityDetails,
  aveva: {
    workpacks,
    jobs,
    activities,
    drls
  },
  fileTracking: [
    {
      id: 'dataCache',
      name: 'Cache File',
      date: new Date().toISOString()
    }
  ]
};

/**
 * Stage 5B - Append source provenance metadata
 * --------------------------------------------
 * For each input source file, add:
 * - internal dataset key
 * - source filename without extension
 * - source timestamp from workerData.data.timestamps
 *
 * This supports cache validation, staleness checks, and auditability.
 */
// Add file stats for data validation
Object.keys(files).forEach((key) => {
  cacheData.fileTracking.push({
    id: key,
    name: files[key].split('/').at(-1).split('.')[0],
    date: workerData.data.timestamps[key]
  });
});

/**
 * Stage 5C - Write final cache file
 * ---------------------------------
 * Current behaviour writes compact JSON.
 *
 * The pretty-printed alternative remains commented out for optional debugging.
 */
// if (!fs.existsSync(outputFolder)) { fs.mkdirSync(outputFolder, {recursive: true}); }
if (!fs.existsSync(outputFolder)) {
  fs.mkdirSync(outputFolder, { recursive: true });
}

fs.writeFileSync(output, JSON.stringify(cacheData));
// fs.writeFileSync(output, JSON.stringify(cacheData, null, '  '));

/**
 * Final progress and completion log.
 */
progress(false, false, 99);

Logging.log('[Pipe Shop] {Finished work pack Data creation.|green}');