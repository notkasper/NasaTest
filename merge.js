// imports
const path = require("path");
const fs = require("fs");
const readline = require("readline");
const cliProgress = require("cli-progress");

const OUTPUT_BASE_DIR = path.join(__dirname, "./output");
const MERGE_OUTPUT = `${OUTPUT_BASE_DIR}/merged`;
const INGEST_OUTPUT = `${OUTPUT_BASE_DIR}/csvPerCountry`;

const mergeFolder = async (directory, country) => {
  const folder = await fs.promises.opendir(directory);
  const amountOfFiles = (await fs.promises.readdir(directory)).length;

  const progressBar = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_classic
  );
  progressBar.start(amountOfFiles, 0);

  const filepath = path.join(MERGE_OUTPUT, `${country}.csv`);
  const writer = fs.createWriteStream(filepath);

  const readFile = async (filePath, firstFile, writer) => {
    return new Promise((resolve, reject) => {
      const readStream = fs.createReadStream(filePath);
      const reader = readline.createInterface(readStream, writer);
      let firstRow = true;
      reader.on("line", (line) => {
        if (firstRow && firstFile) {
          firstRow = false;
          writer.write(line);
          writer.write("\n");
        } else if (!firstRow) {
          writer.write(line);
          writer.write("\n");
        } else if (!firstFile && firstRow) {
          firstRow = false;
        }
      });
      reader.on("close", () => {
        resolve();
      });
    });
  };

  let firstFile = true;
  for await (const file of folder) {
    const fileToRead = path.join(folder.path, file.name);
    await readFile(fileToRead, firstFile, writer);
    firstFile = false;
    progressBar.increment();
  }
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
