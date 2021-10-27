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
  console.info(`Awaiting response...`);
  const res = await request.get(baseUrl).query(query);
  console.info('Response received');
  return res.body.properties.parameter;
};

const dateToDayOfYear = (date) => {
  const start = new Date(date.getFullYear(), 0, 0);
  const diff = date - start + (start.getTimezoneOffset() - date.getTimezoneOffset()) * 60 * 1000;
  const oneDay = 1000 * 60 * 60 * 24;
  const day = Math.floor(diff / oneDay);
  return day;
};

const geocode = async (office, city, zipcode) => {
  const googleGeocodeApiBaseUrl = 'http://api.positionstack.com/v1/forward';
  const key = 'fb1c96a991dc27bf030202914c73819f';
  const address = `${zipcode}, ${city}, ${office}`;

  let response = null;
  try {
    response = await request.get(googleGeocodeApiBaseUrl).query({ query: address, access_key: key });
  } catch (error) {
    console.error(error.response.body);
    return null;
  }

  // in case geocoding did not give any results
  if (!response.body.data.length) {
    return null;
  }

  let best = response.body.data[0];
  response.body.data.forEach((option) => {
    if (option.confidence > best.confidence) {
      best = option;
    }
  });
  return [best.latitude, best.longitude];
};

// For one zipcode, create a csv and fill it with temperature data
const generateOne = async (zipcode, lat, lon, city, office) => {
  const buffer = [];
  let response = null;
  try {
    response = await getWeatherData(lat, lon);
  } catch (error) {
    console.error(error.response.body);
    return;
  }
  const { T2M: temperatureData, PRECTOTCORR: precipitationData } = response;
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
      country: office || 'N/A',
      latitude: lat,
      longitude: lon,
      temperature,
      precipitation,
    });
  });

  const fields = ['dayOfYear', 'zipcode', 'city', 'country', 'latitude', 'longitude', 'temperature', 'precipitation'];
  const options = { fields };

  try {
    const csv = toCsv(buffer, options);

    // make sure country folder exists
    if (!fs.existsSync(`${outputDir}/${office}`)) {
      fs.mkdirSync(`${outputDir}/${office}`);
    }

    const filepath = `${outputDir}/${office}/[${latitude}][${longitude}].csv`;
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
    const { ZipCode: zipcode, City: city, Office: office } = row;
    const geocodeResult = await geocode(office, city, zipcode);

    if (!geocodeResult) {
      console.info(`Could not geocode:\n ${JSON.stringify(row)}`);
      continue;
    }

    const [latitude, longitude] = geocodeResult;
    await generateOne(zipcode, latitude, longitude, city, office);
  }

  console.info("Done generating csv's");
};

generate();
