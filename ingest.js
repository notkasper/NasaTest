const { parse: toCsv } = require("json2csv");
const request = require("superagent");
const path = require("path");
const fs = require("fs");
const csvToJson = require("csvToJson");

const sortByCountry = (jsonZipcodes) => {
  const locationsPerCountry = {};

  for (const index in jsonZipcodes) {
    const row = jsonZipcodes[index];
    const { zipcode, city, country, latitude, longitude } = row;

    if (!locationsPerCountry[country]) {
      locationsPerCountry[country] = [];
    }
    locationsPerCountry[country].push(row);
  }

  return locationsPerCountry;
};

const getLocation = async (latitude, longitude) => {
  const baseUrl = "https://power.larc.nasa.gov/api/temporal/daily/point";
  const query = {
    start: "20010101",
    end: "20210101",
    community: "AG",
    parameters: "T2M,PRECTOTCORR",
    longitude,
    latitude,
  };

  console.info(`Awaiting response...`);
  const res = await request.get(baseUrl).query(query);
  console.info("Response received");
  return res.body.properties.parameter;
};

const getBatch = async (locationBatch) => {
  const responses = Promise.all(
    locationBatch.map((location) =>
      getLocation(location.latitude, location.longitude)
    )
  );
  return responses;
};

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

const ingestCountry = async (country, locations) => {
  const responses = [];
  const locationBatches = batchify(locations);
  for (const locationBatch of locationBatches) {
    const startTime = new Date(); // keep track of time to avoid rate limit
    const batchResponses = await getBatch(locationBatch);
    responses.concat(batchResponses);
    const endTime = new Date();
    const timeToWaitMs = endTime.getTime() - startTime.getTime();
    if (timeToWaitMs > 0) {
      console.log(`Gonna wait ${Math.ceil(timeToWaitMs / 1000)} seconds`);
      await wait(timeToWaitMs);
    }
  }
  return responses;
};

const saveCountry = (data, filepath) => {
  const fields = [
    "dayOfYear",
    "zipcode",
    "city",
    "country",
    "latitude",
    "longitude",
    "temperature",
    "precipitation",
  ];
  const options = { fields };

  try {
    const csv = toCsv(data, options);
    fs.writeFileSync(filepath, csv);
  } catch (err) {
    console.error(err);
  }
};

const ingestAll = async (locationsPerCountry, outputDir) => {
  const countries = Object.keys(locationsPerCountry);
  for (const country of countries) {
    const locations = locationsPerCountry[country];
    const countryResponses = await ingestCountry(country, locations);
    const filepath = `${outputDir}/${country}.csv`;
    saveCountry(countryResponses, filepath);
  }
};

const start = async () => {
  //   settings
  const outputDir = "./output";
  const sourceFile = path.join(__dirname, "./source.csv");

  //   Load zipcodes
  const toJson = csvToJson();
  const jsonZipcodes = await toJson.fromFile(sourceFile);

  //   sort locations per country
  const locationsPerCountry = sortByCountry(jsonZipcodes);

  //   make sure country folder exists
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
  }

  //   ingest all data
  await ingestAll(locationsPerCountry, outputDir);
};

start();
