// imports
const request = require('superagent');
const path = require('path');
const fs = require('fs');
const { parse: toCsv } = require('json2csv');
const csvToJson = require('csvToJson');

// settings
const csvFilePath = path.join(__dirname, './ZipCodes.csv');
const toJson = csvToJson({ delimiter: ';' });

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

const generate = async () => {
  // Load zipcodes
  const jsonZipcodes = await toJson.fromFile(csvFilePath);
  const buffer = [];
  const failedBuffer = [];
  let failStreak = 0;
  let lastIndex = 0;

  // loop over zipcodes and fill buffer
  for (const index in jsonZipcodes) {
    if (failStreak > 10) {
      // terminate the program gracefully and write a log
      continue;
    }
    lastIndex = index;
    const row = jsonZipcodes[index];
    const { ZipCode: zipcode, City: city, Office: office } = row;
    const geocodeResult = await geocode(office, city, zipcode);

    if (!geocodeResult) {
      console.info(`Could not geocode:\n ${JSON.stringify(row)}`);
      failedBuffer.push({ zipcode, city, office });
      failStreak += 1;
      continue;
    }
    failStreak = 0;

    const [latitude, longitude] = geocodeResult;
    buffer.push({ zipcode, city, office, latitude, longitude });
  }

  const options = { fields: ['zipcode', 'city', 'office', 'latitude', 'longitude'] };
  const failedOptions = { fields: ['zipcode', 'city', 'office'] };

  try {
    const csv = toCsv(buffer, options);
    const failedCsv = toCsv(failedBuffer, failedOptions);

    const filepath = 'enrichedZipcodes.csv';
    const failedFilepath = 'failedZipcodes.csv';
    fs.writeFileSync(filepath, csv);
    fs.writeFileSync(failedFilepath, failedCsv);
    fs.writeFileSync(path.join(__dirname, 'log.txt'), lastIndex);
  } catch (err) {
    console.error(err);
  }

  console.info("Done generating csv's");
};

generate();
