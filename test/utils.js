const os = require("os");
const fs = require("fs");
const { exec } = require("child_process");
const Knex = require("knex");

const TEST_DB_DIR = `${process.env.PWD}/.tmp`;
const TEST_DB_FILE = `${TEST_DB_DIR}/test-${process.env.JEST_WORKER_ID}.db`;
const TEST_CONF_DIR = os.tmpdir();

const runCli = (args) =>
  new Promise((resolve, reject) => {
    let stdout = "";
    let stderr = "";
    let all = "";
    const proc = exec(`${process.env.PWD}/cli.js ${args}`, {
      env: {
        ...process.env,
        // Setting a custom config directory
        SQL_CONF_DIR: TEST_CONF_DIR,
        // Force default date of stringDate() to generate migrations filenames
        SQL_DUMP_DATE: "2020-07-22T18:22:50.732Z",
        // Disable chalk's coloring of outputs
        FORCE_COLOR: 0,
      },
    });
    proc.stdout.on("data", (data) => {
      all += data;
      stdout += data;
    });
    proc.stderr.on("data", (data) => {
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

const getKnexUri = (knex) => {
  const { client, connection: conn } = knex.client.config;
  if (client === "sqlite3") {
    return `${client}://${conn.filename}`;
  }
  const host = conn.server || conn.host;
  return `${client}://${conn.user}:${conn.password}@${host}:${conn.port}/${conn.database}`;
};

module.exports = { runCli, getTestKnex, getKnexUri };