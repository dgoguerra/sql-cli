#!/usr/bin/env node

const fs = require("fs");
const path = require("path");
const rimraf = require("rimraf");
const { execSync } = require("child_process");
const { migrateTestTables } = require("../test/cli-test-suite");
const CliApp = require("../src/CliApp");

const PROJ_DIR = path.resolve(__dirname, "..");
const CLI_BIN = `node ${PROJ_DIR}/src/index.js`;
const CONN_URI = `mysql://root:Secret123@127.0.0.1:23306`;

// Use a custom fresh config directory for sql-cli,
// to avoid using the user system's config.
const SQL_CONF_DIR = `${PROJ_DIR}/.tmp/test-config`;
if (fs.existsSync(SQL_CONF_DIR)) {
  rimraf.sync(SQL_CONF_DIR);
}

const runCmd = (cmd, { env = {}, debug = false } = {}) => {
  if (debug) {
    console.log(`Running: ${cmd}`);
  }
  const stdall = execSync(cmd, {
    env: { ...process.env, ...env, SQL_CONF_DIR, SQL_NO_IMPORT_ALIASES: 1 },
  });
  if (debug) {
    console.log(stdall.toString());
  }
};

process.chdir(`${PROJ_DIR}/.tmp`);

(async () => {
  execSync(`
    docker-compose up --detach mysql
    docker-compose run wait-all -wait tcp://mysql:3306 -timeout 30s
  `);

  process.on("exit", () => {
    execSync("docker-compose down");
  });

  await CliApp.initLib(CONN_URI).then(async (knex) => {
    await knex.raw("create schema test1_db");
    await knex.raw("create schema test2_db");
    await knex.raw("create schema loaded_db");
    await knex.destroy();
  });

  console.log("building schema test1_db ...");
  await CliApp.initLib(`${CONN_URI}/test1_db`).then(async (knex) => {
    await migrateTestTables(knex);
    await knex.destroy();
  });

  console.log("building schema test2_db ...");
  await CliApp.initLib(`${CONN_URI}/test2_db`).then(async (knex) => {
    await migrateTestTables(knex);
    await knex("table_2").where({ id: 2 }).delete();
    await knex.schema.table("table_3", (t) => {
      t.dropColumn("id_field");
      t.integer("other_field_1");
      t.text("other_field_2");
    });
    await knex.schema.dropTable("table_1");
    await knex.schema.createTable("table_4", (t) => {
      t.integer("id");
    });
    await knex.destroy();
  });

  // Add some aliases to show as available
  runCmd(`
    ${CLI_BIN} alias add local-sqlite sqlite:///path/to/file1.db
    ${CLI_BIN} alias add local-pg postgres://127.0.0.1:5432/mydb
    ${CLI_BIN} alias add project-dev mysql://12.12.12.12/dev_db
    ${CLI_BIN} alias add project-prod mysql://34.34.34.34/prod_db
  `);

  const images = [
    {
      key: "show",
      height: 20,
      cmds: [`list ${CONN_URI}/test1_db`, `show ${CONN_URI}/test1_db/table_1`],
    },
    {
      key: "aliases",
      height: 36,
      cmds: [
        `alias add test-1 ${CONN_URI}/test1_db`,
        `alias add test-2 ${CONN_URI}/test2_db`,
        "alias ls",
        "ls test-1",
        "show test-1/table_1",
      ],
    },
    {
      key: "diff",
      height: 29,
      env: { SQL_NO_STRIPED_TABLES: 1 },
      cmds: [
        `diff test-1 test-2`,
        `diff test-1/table_1 test-2/table_2`,
        `diff test-1/table_1 test-2/table_2 --data --rows=20`,
      ],
    },
    {
      key: "dump",
      env: { SQL_NO_STRIPED_TABLES: 1 },
      cmds: [
        `dump create test-1 dump-mydb`,
        `dump load ${CONN_URI}/loaded_db dump-mydb.tgz`,
        `diff test-1 ${CONN_URI}/loaded_db --all`,
      ],
    },
    {
      key: "dump-file",
      height: 26,
      cmds: [
        `ls dump-mydb.tgz`,
        `show dump-mydb.tgz/table_1`,
        `shell dump-mydb.tgz 'select * from table_1;'`,
      ],
    },
  ];

  images.forEach((im) => {
    const castFile = `${PROJ_DIR}/.tmp/${im.key}.cast`;
    const svgFile = `${PROJ_DIR}/img/${im.key}.svg`;

    console.log(`creating img/${im.key}.svg ...`);

    const buildCmd = (cmd) => {
      const escaped = cmd.replace(/'/g, '\\"');
      return `echo '$ sql ${escaped}'; ${CLI_BIN} ${cmd}; echo`;
    };

    const commands = im.cmds.map(buildCmd).join("; ");
    runCmd(
      `asciinema rec --command "${commands}" --overwrite ${castFile}
      svg-term --in ${castFile} --out ${svgFile} \
        --at 100000 ${im.height ? `--height ${im.height}` : ""} \
        --term iterm2 --profile unused --window --no-cursor`,
      { env: im.env || {} }
    );
    //runCmd(`qlmanage -t -s 1000 -o ${PROJ_DIR}/img ${svgFile}`);
  });
})();
