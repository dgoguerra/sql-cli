const os = require("os");
const fs = require("fs");
const { exec } = require("child_process");
const Knex = require("knex");

const TEST_DB_DIR = `${process.env.PWD}/.tmp`;
const TEST_DB_FILE = `${TEST_DB_DIR}/test-${process.env.JEST_WORKER_ID}.db`;
const TEST_CONF_DIR = `${os.tmpdir()}/test-${process.env.JEST_WORKER_ID}`;

const runCli = (args, { stdin = null, debug = false } = {}) =>
  new Promise((resolve, reject) => {
    let command = `${process.env.PWD}/src/index.js ${args}`;
    let stdout = "";
    let stderr = "";
    let all = "";

    if (stdin) {
      command = `echo "${stdin}" | ${command}`;
    }

    const proc = exec(command, {
      env: {
        ...process.env,
        // Set a custom config directory, to avoid using the user system's config
        SQL_CONF_DIR: TEST_CONF_DIR,
        // Force default date of stringDate() to generate migrations filenames
        SQL_DUMP_DATE: "2020-07-22T18:22:50.732Z",
        // Disable importing aliases from the user's TablePlus and Sequel Pro
        SQL_NO_IMPORT_ALIASES: 1,
        // Increase max width of output tables
        SQL_LINE_WIDTH: 1000,
        // Disable chalk's coloring of outputs
        FORCE_COLOR: 0,
      },
    });
    if (debug) {
      console.log(`Running: ${command}`);
    }
    proc.stdout.on("data", (data) => {
      debug && console.log(`stdout: ${data}`);
      all += data;
      stdout += data;
    });
    proc.stderr.on("data", (data) => {
      debug && console.log(`stderr: ${data}`);
      all += data;
      stderr += data;
    });
    proc.on("close", (code) =>
      code ? reject(new Error(all)) : resolve(stdout)
    );
  });

const getTestKnex = () => {
  fs.mkdirSync(TEST_DB_DIR, { recursive: true });
  if (fs.existsSync(TEST_DB_FILE)) {
    fs.unlinkSync(TEST_DB_FILE);
  }
  return Knex({
    client: "sqlite3",
    connection: { filename: TEST_DB_FILE },
    useNullAsDefault: true,
  });
};

module.exports = { runCli, getTestKnex };
