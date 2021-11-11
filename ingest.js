const fs = require("fs");
const path = require("path");
const uuid = require("uuid");
const request = require("superagent");
const csvToJson = require("csvToJson");
const { parse: toCsv } = require("json2csv");
const cliProgress = require("cli-progress");

const RATE_LIMIT = 50; // Allowed amount of calls per minute
const OUTPUT_BASE_DIR = path.join(__dirname, "./output");
const INGEST_OUTPUT = `${OUTPUT_BASE_DIR}/csvPerCountry`;
const TEMP_PATH = `${OUTPUT_BASE_DIR}/temp`;
const START_DATE = "20210101";
const END_DATE = "20210103";

const progressBar = new cliProgress.SingleBar(
  {},
  cliProgress.Presets.shades_classic
);

const sortByCountry = (jsonZipcodes) => {
  const locationsPerCountry = {};

  for (const index in jsonZipcodes) {
    const row = jsonZipcodes[index];
    const { country } = row;

    if (!locationsPerCountry[country]) {
      locationsPerCountry[country] = [];
    }
    locationsPerCountry[country].push(row);
  }

  return locationsPerCountry;
};

// const transformResponse = (response, parameters) => {
//     const parameterResponses = response.body.properties.parameter
//     const dates = parameterResponses[parameters[0]] // Bit ugly, but now we can loo
//     return {}
// }

const getLocation = async (location) => {
  const { longitude, latitude, zipcode, country, city } = location;
  const baseUrl = "https://power.larc.nasa.gov/api/temporal/daily/point";
  const parameters = [
    "T2M",
    "PRECTOTCORR",
    "RH2M",
    "GWETROOT",
    "GWETPROF",
    "T2MWET",
  ];
  const query = {
    start: START_DATE,
    end: END_DATE,
    community: "AG",
    parameters: parameters.join(","),
    longitude,
    latitude,
    format: "CSV",
  };

  const response = await request.get(baseUrl).query(query);
  const csvData = response.text.split("-END HEADER-")[1].trim(); // Remove meta data located above headers

  const tempFilename = `${country}-${zipcode}-${city}-${START_DATE}-${END_DATE}-RAW`;
  const tempFilePath = `${TEMP_PATH}/${country}/${tempFilename}.csv`;
  await fs.promises.writeFile(tempFilePath, csvData);

  const toJson = csvToJson();
  let csvJson = await toJson.fromFile(tempFilePath);
  const enrichedResponse = csvJson.map((row) => ({
    ...row,
    longitude,
    latitude,
    zipcode,
    country,
    city,
  }));
  const filename = `${country}-${zipcode}-${city}-${START_DATE}-${END_DATE}`;
  const filepath = `${INGEST_OUTPUT}/${country}/${filename}.csv`;
  await saveCountry(enrichedResponse, filepath);
};

const getBatch = (locationBatch) => Promise.all(locationBatch.map(getLocation));

const wait = async (timeout) => {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, timeout);
  });
};

const batchify = (arr, batchSize) => {
  const batches = [];
  for (let i = 0; i < arr.length; i += batchSize) {
    const batch = arr.slice(i, i + batchSize);
    batches.push(batch);
  }
  return batches;
};

const ingestCountry = async (country, locations, tries = 0) => {
  const locationBatches = batchify(locations, RATE_LIMIT);

  // make sure country folder exists
  if (!fs.existsSync(`${INGEST_OUTPUT}/${country}`)) {
    fs.mkdirSync(`${INGEST_OUTPUT}/${country}`);
  }

  // make sure country folder exists for raw output
  if (!fs.existsSync(`${TEMP_PATH}/${country}`)) {
    fs.mkdirSync(`${TEMP_PATH}/${country}`);
  }

  try {
    for (const locationBatch of locationBatches) {
      const startTime = new Date(); // keep track of time to avoid rate limit
      await getBatch(locationBatch);
      const endTime = new Date();
      const elapsedTime = endTime.getTime() - startTime.getTime();
      const timeToWaitMs = RATE_LIMIT - elapsedTime + 5 * 1000;

      if (timeToWaitMs > 0) {
        await wait(timeToWaitMs);
      }
    }
  } catch (error) {
    if (tries <= 3) {
      tries += 1;
      await wait(60 * 1000);
      await ingestCountry(country, locations, tries);
    } else {
      return;
    }
  }
};

const saveCountry = async (data, filepath) => {
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

  try {
    const csv = toCsv(data, options);
    await fs.promises.writeFile(filepath, csv);
  } catch (err) {
    console.error(err);
  }
};

const ingestAll = async (locationsPerCountry) => {
  const countries = Object.keys(locationsPerCountry);
  progressBar.start(countries.length, 0);
  for (const country of countries) {
    const locations = locationsPerCountry[country];
    await ingestCountry(country, locations);
    progressBar.increment();
  }
  progressBar.stop();
};

const start = async () => {
  //   settings
  const sourceFile = path.join(__dirname, "./source.csv");

  //   Load zipcodes
  const toJson = csvToJson();
  const jsonZipcodes = await toJson.fromFile(sourceFile);

  //   sort locations per country
  const locationsPerCountry = sortByCountry(jsonZipcodes);

  //   make sure output folder exists
  if (!fs.existsSync(OUTPUT_BASE_DIR)) {
    fs.mkdirSync(OUTPUT_BASE_DIR);
  }

  // Place where we store individual API responses, to be processed later
  if (!fs.existsSync(TEMP_PATH)) {
    await fs.promises.mkdir(TEMP_PATH);
  }

  //   make sure output folder exists
  if (!fs.existsSync(INGEST_OUTPUT)) {
    fs.mkdirSync(INGEST_OUTPUT);
  }

  //   ingest all data
  await ingestAll(locationsPerCountry);
};

start();
