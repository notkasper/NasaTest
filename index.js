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
  start: '20160101',
  end: '20210101',
  community: 'AG',
  parameters: 'T2M',
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
  return res.body.properties.parameter.T2M;
};

// For one zipcode, create a csv and fill it with temperature data
const generateOne = async (zipcode, lat, lon) => {
  const buffer = [];
  const weatherData = await getWeatherData(lat, lon);
  Object.keys(weatherData).forEach((dateString) => {
    const temperature = weatherData[dateString];
    buffer.push({
      date: dateString,
      zipcode,
      latitude: lat,
      longitude: lon,
      temperature,
    });
  });

  const fields = ['date', 'zipcode', 'latitude', 'longitude', 'temperature'];
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
    const { ZipCode: zipcode, Latitude: lat, Longitude: lon } = row;

    await generateOne(zipcode, lat, lon);
  }

  console.info("Done generating csv's");
};

generate();
