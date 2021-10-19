// imports
const request = require('superagent');
const path = require('path');
const fs = require('fs');
const { parse: toCsv } = require('json2csv');
const csvToJson = require('csvToJson');

// settings
const csvFilePath = path.join(__dirname, './ZipCodes.csv');
const baseUrl = 'https://power.larc.nasa.gov/api/temporal/daily/point';
const baseQuery = {
  start: '20010101',
  end: '20210101',
  community: 'AG',
  parameters: 'T2M,PRECTOTCORR',
  longitude: null,
  latitude: null,
};
const outputDir = './output';

const toJson = csvToJson({ delimiter: ';' });

const getWeatherData = async (lat, lon) => {
  const query = { ...baseQuery, latitude: lat, longitude: lon };
  console.log(`Awaiting response...`);
  const res = await request.get(baseUrl).query(query);
  console.log('Response received');
  return res.body.properties.parameter;
};

const dateToDayOfYear = (date) => {
  const start = new Date(date.getFullYear(), 0, 0);
  const diff = date - start + (start.getTimezoneOffset() - date.getTimezoneOffset()) * 60 * 1000;
  const oneDay = 1000 * 60 * 60 * 24;
  const day = Math.floor(diff / oneDay);
  return day;
};

// For one zipcode, create a csv and fill it with temperature data
const generateOne = async (zipcode, lat, lon, city, office) => {
  const buffer = [];
  const { T2M: temperatureData, PRECTOTCORR: precipitationData } = await getWeatherData(lat, lon);
  Object.keys(temperatureData).forEach((dateString) => {
    const temperature = temperatureData[dateString];
    const precipitation = precipitationData[dateString];

    const year = dateString.slice(0, 4);
    const month = dateString.slice(4, 6);
    const day = dateString.slice(6, 8);

    const dateObject = new Date();
    dateObject.setFullYear(year);
    dateObject.setMonth(month - 1); // JS works with months from 0-11
    dateObject.setDate(day);

    const dayOfYear = dateToDayOfYear(dateObject);

    buffer.push({
      dayOfYear: dayOfYear,
      zipcode,
      city: city || 'N/A',
      office: office || 'N/A',
      latitude: lat,
      longitude: lon,
      temperature,
      precipitation,
    });
  });

  const fields = ['dayOfYear', 'zipcode', 'city', 'office', 'latitude', 'longitude', 'temperature', 'precipitation'];
  const options = { fields };

  try {
    const csv = toCsv(buffer, options);
    const filepath = `${outputDir}/${zipcode}.csv`;
    fs.writeFileSync(filepath, csv);
  } catch (err) {
    console.error(err);
  }
};

const generate = async () => {
  // make sure output dir exists, if not, create it
  const exists = fs.existsSync(outputDir);
  if (!exists) {
    fs.mkdirSync(outputDir);
  }

  // Load zipcodes
  const jsonZipcodes = await toJson.fromFile(csvFilePath);

  // loop over zipcodes and fill buffer
  for (const index in jsonZipcodes) {
    const row = jsonZipcodes[index];
    const { ZipCode: zipcode, Latitude: lat, Longitude: lon, City: city, Office: office } = row;

    await generateOne(zipcode, lat, lon, city, office);
  }

  console.info("Done generating csv's");
};

generate();
