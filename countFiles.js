const fs = require("fs");
const path = require("path");

const rootDir = path.join(__dirname, "./output/csvPerCountry");

async function ls(thePath) {
  let fileCounter = 0;
  const dir = await fs.promises.opendir(thePath);
  for await (const dirent of dir) {
    const nested = path.join(rootDir, dirent.name);
    const countryFolder = await fs.promises.opendir(nested);
    for await (const countryFiles of countryFolder) {
      fileCounter += 1;
    }
  }
  console.log(`Total files: ${fileCounter}`);
}

ls(rootDir);
