const { spawn } = require("child_process");
const fs = require("fs");
const Knex = require("knex");

const TEST_DB_DIR = `${process.env.PWD}/.tmp`;
const TEST_DB_FILE = `${TEST_DB_DIR}/test-${process.env.JEST_WORKER_ID}.db`;

const runCli = (cmd, args = []) =>
  new Promise((resolve, reject) => {
    let stdout = "";
    let stderr = "";
    let all = "";
    // Add SQL_DUMP_DATE env var to force the default date used
    // by stringDate() to generate filenames of knex migrations.
    const proc = spawn(`${process.env.PWD}/cli.js`, [cmd, ...args], {
      env: { ...process.env, SQL_DUMP_DATE: "2020-07-22T18:22:50.732Z" },
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
  const { client, connection } = knex.client.config;
  if (client === "sqlite3") {
    return `${client}://${connection.filename}`;
  }
  throw new Error(`getKnexUri() not configured for client '${client}'`);
};

module.exports = { runCli, getTestKnex, getKnexUri };
