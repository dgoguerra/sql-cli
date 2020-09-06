#!/usr/bin/env node

const fs = require("fs");
const os = require("os");
const Knex = require("knex");
const rimraf = require("rimraf");
const { execSync } = require("child_process");
const { migrateTestTables } = require("../test/cli-test-suite");

const TMP_DIR = `${__dirname}/../.tmp`;
const IMG_DIR = `${__dirname}/../img`;

// Use a custom fresh config directory for sql-cli,
// to avoid using the user system's config.
const SQL_CONF_DIR = `${os.tmpdir()}/test-config`;
if (fs.existsSync(SQL_CONF_DIR)) {
  rimraf.sync(SQL_CONF_DIR);
}

const runCmd = (cmd, { debug = false } = {}) => {
  if (debug) {
    console.log(`Running: ${cmd}`);
  }
  const stdall = execSync(cmd, { env: { ...process.env, SQL_CONF_DIR } });
  if (debug) {
    console.log(stdall.toString());
  }
};

process.chdir(TMP_DIR);

(async () => {
  const initDatabase = (name, knexCallback) => {
    const dbFile = `${TMP_DIR}/${name}`;
    if (fs.existsSync(dbFile)) {
      fs.unlinkSync(dbFile);
    }
    const knex = Knex({
      client: "sqlite3",
      connection: { filename: dbFile },
      useNullAsDefault: true,
    });
    return knexCallback(knex).then(() => knex.destroy());
  };

  console.log("building example file1.db ...");
  await initDatabase("file1.db", async (knex) => {
    await migrateTestTables(knex);
  });

  console.log("building example file2.db ...");
  await initDatabase("file2.db", async (knex) => {
    await migrateTestTables(knex);

    await knex("table_2").where({ id: 2 }).delete();
    await knex.schema.table("table_3", (t) => {
      t.dropColumn("idField");
      t.integer("other_field_1");
      t.text("other_field_2");
    });
    await knex.schema.dropTable("table_1");
    await knex.schema.createTable("table_4", (t) => {
      t.integer("id");
    });
  });

  // Add some aliases to show as available
  runCmd(`node ../cli.js alias add local-pg postgres://127.0.0.1:5432/mydb`);
  runCmd(`node ../cli.js alias add local-my mysql://127.0.0.1:3306/mydb`);
  runCmd(`node ../cli.js alias add project-dev mysql://12.12.12.12/dev_db`);
  runCmd(`node ../cli.js alias add project-prod mysql://34.34.34.34/prod_db`);

  const images = [
    {
      key: "diff",
      cmds: [
        "diff sqlite://file1.db sqlite://file2.db",
        "diff sqlite://file1.db/table_1 sqlite://file2.db/table_2",
        "diff sqlite://file1.db/table_1 sqlite://file2.db/table_2 --data --rows=20",
      ],
    },
    {
      key: "aliases",
      height: 27,
      cmds: [
        "alias add mydb sqlite://file1.db",
        "alias ls",
        "ls mydb",
        "show mydb/table_1",
      ],
    },
  ];

  images.forEach((im) => {
    console.log(`creating ${im.key}.svg ...`);
    const command = im.cmds
      .map((cmd) => `echo $ sql ${cmd}; node ../cli.js ${cmd}; echo`)
      .join("; ");
    runCmd(`asciinema rec --command "${command}" --overwrite ${im.key}.cast`);
    runCmd(`
      svg-term --in ${im.key}.cast \
        --out ${IMG_DIR}/${im.key}.svg \
        --at 100000 ${im.height ? `--height ${im.height}` : ''} \
        --window --no-cursor
    `);
    //runCmd(`qlmanage -t -s 1000 -o ${IMG_DIR} ${IMG_DIR}/${im.key}.svg`);
  });
})();
