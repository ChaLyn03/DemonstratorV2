import fs from 'node:fs';
import { workerData } from 'node:worker_threads';
import { readFile, dataToSpreadsheet } from '../utils/FileSystem.js';
import Logging from '../utils/Logging.js';
import { progress } from './Worker.js';

Logging.log('[Pipe Shop] {Starting work pack data creation...|yellow}');

const loadFolder = './app/data/pipeshop/';
const outputFolder = './cache/';
const output = `${outputFolder}workpack-data.json`;

// List of Weekly updated files.
const files = {};
const config = {};

progress(false, false, 0, 99);

Object.values(workerData.data.dataMap).forEach((data) => {
  files[data.key] = `${workerData.data.storageFolder}${data.file}.${data.format}`;
  config[data.key] = {
    file: `${loadFolder}${data.file}.json`
  };
});

//////////////////////
// Read report data //
//////////////////////

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

Logging.warn(`[Pipe Shop] spoolDatabase: ${spoolDatabase.length}`);

progress(false, false, 5);

let iBomRoutedSystems = readFile(workerData.data.fileData['ibom-pipes'], {
  format: 'csv',
  cache: false,
  autoNumberDetection: false,
  config: config.iBomRoutedSystems
});

progress(false, false, 10);

let cognosActivityDetails = dataToSpreadsheet(workerData.data.fileData['cognos-ermh5008'], {
  format: 'excel',
  sheet: 'ExcelEQLALookahead_2',
  cache: false,
  config: config.cognosActivityDetails
});

progress(false, false, 15);

let cognosDrlDetails = dataToSpreadsheet(workerData.data.fileData['cognos-ermh5012'], {
  format: 'excel',
  cache: false,
  raw: true,
  config: config.cognosDrlDetails
}).filter(row => row.deliveryLocation === 'BLD02_PIPE');

progress(false, false, 20);

let drawings = dataToSpreadsheet(workerData.data.fileData['mce-2693'], {
  format: 'excel',
  sheet: 'Excel_2',
  cache: false,
  config: config.drawings
});

progress(false, false, 21);

let bendRatioDatabase = dataToSpreadsheet(workerData.data.fileData['mce-2582'], {
  format: 'excel',
  sheet: 'Excel_2',
  cache: false,
  config: config.bendRatioDatabase
});

progress(false, false, 22);

let extendedPipeDatabase = dataToSpreadsheet(workerData.data.fileData['mce-2691'], {
  format: 'excel',
  sheet: 'Excel_2',
  cache: false,
  config: config.pipeDatabase
});

progress(false, false, 23);

let dosr = dataToSpreadsheet(workerData.data.fileData['mce-0320'], {
  format: 'excel',
  sheet: 'Excel_2',
  cache: false,
  config: config.dosr
});

progress(false, false, 24);

let spoolBlocks = readFile(workerData.data.fileData['ibom-spools'], {
  format: 'csv',
  cache: false,
  config: config.spoolBlocks
});

progress(false, false, 25);

let avevaDataDump = dataToSpreadsheet(workerData.data.fileData['aveva'], {
  format: 'excel',
  cache: false,
  config: {
    file: `${loadFolder}aveva-gant-config.json`
  }
});

progress(false, false, 26);

let spoolPressureTesting = dataToSpreadsheet(workerData.data.fileData['windchill-hcf-0000106140'], {
  format: 'excel',
  sheet: '1.1 - Master List',
  cache: false,
  config: config.spoolPressureTesting
});

////////////////////////////
// Filter out unused data //
////////////////////////////

progress(false, false, 27);

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

progress(false, false, 50);

// Create Pipe Database from DataPORT data
Logging.log('[Pipe Shop] {Filtering iBomRoutedSystems to create pipeDatabase...|yellow}');

let pipeDatabase = iBomRoutedSystems.filter(pipe => {
  if (!pipe) {
    return false;
  }

  if (pipe.pipeID.startsWith('PIPE')) {
    return true;
  }

  return false;
});

Logging.warn(`[Pipe Shop] New pipeDatabase... [length: ${pipeDatabase.length}]`);

progress(false, false, 51);

let tempEx = extendedPipeDatabase.filter(
  exPipe => !pipeDatabase.find(pipe => pipe.userCode === exPipe.userCode)
);

pipeDatabase = pipeDatabase.concat(tempEx);

Logging.warn(`[Pipe Shop] Adding missing pipe to pipeDatabase, added: ${tempEx.length}`);
Logging.warn(`[Pipe Shop] Updated pipeDatabase... [length: ${pipeDatabase.length}]`);

progress(false, false, 55);

// Pipe Database
Logging.log(`[Pipe Shop] {Filtering & Extending pipeDatabase...|yellow} [length: ${pipeDatabase.length}]`);

pipeDatabase = pipeDatabase.filter((pipe) => {
  if (!pipe.spoolID) {
    return false;
  }

  if (!pipe.pipeID) {
    return false;
  }

  if (!spoolDatabase.find(spool => spool.spoolID === pipe.spoolID)) {
    return false;
  }

  pipe.bendRatio = 0;
  return true;
});

Logging.warn(`[Pipe Shop] Filtered pipeDatabase: ${pipeDatabase.length}`);

progress(false, false, 60);

// Pipe Bend Data
Logging.log(`[Pipe Shop] {Filtering bendRatioDatabase...|yellow} [length: ${bendRatioDatabase.length}]`);

bendRatioDatabase = bendRatioDatabase.filter((pipe) => {
  if (!pipe.spoolID) {
    return false;
  }

  if (!spoolDatabase.find(spool => spool.spoolID === pipe.spoolID)) {
    return false;
  }

  return true;
});

Logging.warn(`[Pipe Shop] Filtered bendRatioDatabase: ${bendRatioDatabase.length}`);

progress(false, false, 65);

// MCE Drawing Details
Logging.log(`[Pipe Shop] {Filtering drawings...|yellow} [length: ${drawings.length}]`);

drawings = drawings.filter(drawing => drawing.spoolID);

Logging.warn(`[Pipe Shop] Filtered drawings: ${drawings.length}`);

// iBom Routed Systems
Logging.log(`[Pipe Shop] {Filtering iBomRoutedSystems...|yellow} [length: ${iBomRoutedSystems.length}]`);

iBomRoutedSystems = iBomRoutedSystems.filter((pipe) => {
  if (!pipe) return false;
  if (!pipe.spoolID) return false;
  if (!pipe.pipeID) return false;
  if (pipe.pipeID.startsWith('PIPE')) return false;

  if (!spoolDatabase.find(spool => spool.spoolID === pipe.spoolID)) {
    return false;
  }

  return true;
});

Logging.warn(`[Pipe Shop] Filtered iBomRoutedSystems: ${iBomRoutedSystems.length}`);

progress(false, false, 70);

////////////////////////////////
// Process AVEVA Data for use //
////////////////////////////////

Logging.log('[Pipe Shop] {Process AVEVA activities...|yellow}');

const activities = [];

avevaDataDump.forEach((row) => {

  if (row.isActivity && row.spoolID) {

    if (spoolDatabase.find(spool => spool.spoolID === row.spoolID)) {

      const activity = {
        row: row.rowIndex,
        spoolID: row.spoolID,
        activityID: row.activityID,
        workpackID: row.workpackID,
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

      let relatedDrls = cognosDrlDetails.filter(entry => entry.activityID === row.activityID);

      let drlParts = [...new Set(
        relatedDrls
          .filter(entry => !entry.pipeID.startsWith('PIPE'))
          .map(entry => entry.drl)
      )];

      let drlPipes = [...new Set(
        relatedDrls
          .filter(entry => entry.pipeID.startsWith('PIPE'))
          .map(entry => entry.drl)
      )];

      activity.drlParts = drlParts;

      row.requestedSpools = [];

      let job = avevaDataDump.find(item =>
        (item.workpackID === row.workpackID) &&
        item.requestedSpools.includes(row.spoolID)
      );

      if (job) {
        activity.jobID = job.activityID;
        activity.drlPipe = [...new Set([job.drl, ...drlPipes])];
      } else {
        row.isJob = true;
        row.requestedSpools = [row.spoolID];
        activity.jobID = row.activityID;
        activity.drlPipe = drlPipes;
      }

      let cognos = cognosActivityDetails.find(item => item.activityID === row.activityID);

      if (cognos) {
        activity.revision = cognos.revision;
        activity.redLine = typeof cognos.revision === 'string' ? cognos.revision.includes('RL') : false;
        activity.blueLine = typeof cognos.revision === 'string' ? cognos.revision.includes('BL') : false;
      }

      activities.push(activity);
    }
  }
});

let validWorkpacks = new Set(activities.map(activity => activity.workpackID));

Logging.warn(`[Pipe Shop] AVEVA activities: ${activities.length}`);

progress(false, false, 85);

Logging.log('[Pipe Shop] {Process AVEVA DRLs...|yellow}');

const drls = [];

cognosDrlDetails.forEach(entry => {

  if (entry.pickQty === 0) {
    return false;
  }

  let drl = drls.find(drlRow => drlRow.id === entry.drl);

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

  if (entry.pipeID.startsWith('PIPE')) {

    let foundPipe = drl.pipes.find(pipe => {
      if (entry.line === pipe.line) return true;

      if (entry.occNumber &&
          entry.pipeID === pipe.pipeID &&
          entry.occNumber === pipe.occNumber) {
        return true;
      }

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

    let foundParts = drl.parts.find(part => {
      if (entry.line === part.line) return true;

      if (entry.occNumber &&
          entry.pipeID === part.pipeID &&
          entry.occNumber === part.occNumber) {
        return true;
      }

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

progress(false, false, 88);

Logging.log('[Pipe Shop] {Process AVEVA jobs...|yellow}');

const jobs = avevaDataDump
  .filter(activity => activity.isJob)
  .map((job) => {

    let relatedDrls = cognosDrlDetails.filter(entry => entry.activityID === job.activityID);

    let drlPipes = [...new Set(
      relatedDrls
        .filter(entry => entry.pipeID.startsWith('PIPE'))
        .map(entry => entry.drl)
    )];

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

progress(false, false, 90);

Logging.log('[Pipe Shop] {Process AVEVA workpacks...|yellow}');

const workpacks = avevaDataDump
  .filter(activity => activity.isWorkpack && validWorkpacks.has(activity.workpackID))
  .map((workpack) => {

    let selectedHours = avevaDataDump
      .filter(activity =>
        activity.isHours &&
        workpack.workpackID === activity.workpackID
      )
      .map(activity => ({
        activityID: activity.activityID,
        status: activity.status,
        hours: activity.hours,
        description: activity.description
      }));

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
      jobs: avevaDataDump
        .filter(activity =>
          activity.isJob &&
          workpack.workpackID === activity.workpackID
        )
        .map(activity => activity.activityID),
      hours: selectedHours
    };
  });

Logging.warn(`[Pipe Shop] AVEVA workpacks: ${workpacks.length}`);

///////////////////////////
// Extend core data sets //
///////////////////////////

progress(false, false, 93);

Logging.log('[Pipe Shop] {Extending pipeDatabase...|yellow} [adding bend information]');

const validBendRatios = [2, 3, 11.8, 14.9];
const invalidHeijunka = ['MAS', 'N/A', 'CHOOSE THE APPLICABLE CODE'];

bendRatioDatabase.forEach((bendItem) => {

  let pipe = pipeDatabase.find(pipe =>
    pipe.userCode === bendItem.userCode &&
    pipe.spoolID === bendItem.spoolID
  );

  if (pipe) {

    pipe.bendRatio = bendItem.bendRatio;

    let value = parseFloat(bendItem.bendRatio);
    let baseValue = Math.floor(value);

    if (baseValue !== 2 && baseValue !== 3) {

      let spool = spoolDatabase.find(spool => spool.spoolID === pipe.spoolID);

      if (spool &&
          !validBendRatios.includes(value) &&
          !invalidHeijunka.includes(spool.heijunka)) {

        Logging.error(`[Pipe Shop] [${pipe.spoolID}] Invalid Bend Ratio: ${value} [${spool.heijunka}]`);
      }
    }
  }
});

progress(false, false, 95);

Logging.log('[Pipe Shop] {Extending spoolDatabase...|yellow}');

spoolDatabase.forEach((spool) => {

  let spoolBlock = spoolBlocks.find(spoolBlock => spoolBlock.spoolID === spool.spoolID);

  if (spoolBlock) {

    spool.block = spoolBlock.block;
    spool.unit = spoolBlock.unit;
    spool.deck = spoolBlock.deck;
    spool.compartment = (typeof spoolBlock.compartment === 'number'
      ? spoolBlock.compartment
      : parseInt(spoolBlock.compartment));

    spool.plannable = spoolBlock.plannable;
    spool.wpName = spoolBlock.wpName;
    spool.wpState = spoolBlock.wpState;
    spool.bmodNumber = spoolBlock.bmodNumber;
    spool.fittedDate = spoolBlock.fittedDate;
    spool.childObsolete = spoolBlock.childObsolete;

    spool.drawings = drawings
      .filter(drawing => drawing.spoolID === spool.spoolID)
      .map((drawing) => ({
        pmf: drawing.drawingNum.startsWith('HCF-PMF'),
        number: drawing.drawingNum,
        revision: drawing.drawingRev
      }));

    spool.activities = activities.filter(activity => activity.spoolID === spool.spoolID);

    let ptSpoolList = spoolPressureTesting.filter(
      ptSpool => ptSpool.spoolID === spool.spoolID
    );

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

      spool.pressureTesting =
        ptSpool.ptRequired.startsWith('Yes') ||
        ptSpool.ptRequired.includes('HOLD');

      spool.pressureMedium = spool.pressureTesting ? ptSpool.ptMedium : '';
      spool.pressureValue = spool.pressureTesting ? ptSpool.testValue : '';

      spool.pressureSection = spool.pressureTesting
        ? `${ptSpool.subsystem.slice(7).replace(/\(Except Below\):$/, '').trim()} (Lloyds Class ${ptSpool.lrClass})`
        : '';
    }

    let dosrSpoolList = dosr.filter(
      dosrSpool => dosrSpool.spoolID === spool.spoolID
    );

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
  }
});

/////////////////////////////////////////////////////////////
// Generate file format, convert to JSON and write to disk //
/////////////////////////////////////////////////////////////

progress(false, false, 98);

Logging.debug(`[Pipe Shop] Writing cache file: {${output}|cyan} (json:utf8)`);

const cacheData = {
  version: 2,
  pipeDatabase: pipeDatabase,
  spoolDatabase: spoolDatabase,
  iBomRoutedSystems: iBomRoutedSystems,
  cognosActivityDetails: cognosActivityDetails,
  aveva: { workpacks, jobs, activities, drls },
  fileTracking: [
    {
      id: 'dataCache',
      name: 'Cache File',
      date: new Date().toISOString()
    }
  ]
};

Object.keys(files).forEach((key) => {
  cacheData.fileTracking.push({
    id: key,
    name: files[key].split('/').at(-1).split('.')[0],
    date: workerData.data.timestamps[key]
  });
});

if (!fs.existsSync(outputFolder)) {
  fs.mkdirSync(outputFolder, { recursive: true });
}

fs.writeFileSync(output, JSON.stringify(cacheData));

progress(false, false, 99);

Logging.log('[Pipe Shop] {Finished work pack Data creation.|green}');