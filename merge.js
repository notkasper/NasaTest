// imports
const path = require("path");
const fs = require("fs");
const { parse: toCsv } = require("json2csv");
const csvToJson = require("csvToJson");
const cliProgress = require("cli-progress");

const OUTPUT_BASE_DIR = path.join(__dirname, "./output");
const MERGE_OUTPUT = `${OUTPUT_BASE_DIR}/merged`;
const INGEST_OUTPUT = `${OUTPUT_BASE_DIR}/csvPerCountry`;

const wait = async (timeout) => {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, timeout);
  });
};

const mergeFolder = async (directory, country) => {
  const folder = await fs.promises.opendir(directory);
  const amountOfFiles = (await fs.promises.readdir(directory)).length;

  let buffer = [];
  const fields = [
    "YEAR",
    "DOY",
    "T2M",
    "PRECTOTCORR",
    "RH2M",
    "GWETROOT",
    "GWETPROF",
    "T2MWET",
    "zipcode",
    "city",
    "country",
    "latitude",
    "longitude",
  ];
  const options = { fields };

  const progressBar = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_classic
  );
  progressBar.start(amountOfFiles, 0);

  for await (const file of folder) {
    const toJson = csvToJson();
    const filePath = path.join(folder.path, file.name);
    const data = await toJson.fromFile(filePath);
    buffer = buffer.concat(data);
    await wait(1000);
    progressBar.increment();
  }

  const csv = toCsv(buffer, options);
  const filepath = path.join(MERGE_OUTPUT, `${country}.csv`);
  await fs.promises.writeFile(filepath, csv);
  progressBar.stop();
};

const mergeAll = async () => {
  //   make sure output folder exists
  if (!fs.existsSync(MERGE_OUTPUT)) {
    fs.mkdirSync(MERGE_OUTPUT);
  }

  const root = await fs.promises.opendir(INGEST_OUTPUT);

  for await (const directory of root) {
    const subDirectory = path.join(root.path, directory.name);

    await mergeFolder(subDirectory, directory.name);
  }
};

const start = async () => {
  await mergeAll();
};

start();
