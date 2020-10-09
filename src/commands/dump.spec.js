const fs = require("fs");
const path = require("path");
const tar = require("tar");
const rimraf = require("rimraf");
const { getTestKnex, runCli } = require("../../test/utils");
const { hydrateKnex } = require("../knexUtils");

const TEST_DUMP_NAME = "my_dump";
const TEST_DUMP_PATH = `${process.env.PWD}/.tmp/${TEST_DUMP_NAME}.tgz`;
const TEST_EXTRACTED_PATH = `${process.env.PWD}/.tmp/${TEST_DUMP_NAME}`;
const TEST_MIGRATIONS_TABLE = `migrations_${TEST_DUMP_NAME}`;

const TEST_DATETIME_1 = "2020-07-24 18:34:00";
const TEST_DATETIME_2 = "2020-07-24 19:25:00";

const TEST_TABLE1_CONTENT = [
  { field_1: 12, field_2: "foo", created_at: TEST_DATETIME_1 },
  { field_1: 30, field_2: "bar", created_at: TEST_DATETIME_1 },
];

const TEST_TABLE2_CONTENT = [
  { field_1: 12.3, field_2: "foo", created_at: TEST_DATETIME_1 },
  { field_1: 30.45, field_2: "bar", created_at: TEST_DATETIME_2 },
];

describe("dump", () => {
  let knex;
  let connUri;

  let knex2;
  let connUri2;

  beforeAll(async () => {
    knex = hydrateKnex(getTestKnex());
    connUri = knex.getUri();

    knex2 = hydrateKnex(getTestKnex("test-load.db"));
    connUri2 = knex2.getUri();

    await knex.schema.createTable("table_1", (t) => {
      t.increments("id");
      t.integer("field_1");
      t.text("field_2");
      t.timestamps();
      t.index(["field_1"]);
    });
    await knex.schema.createTable("table_2", (t) => {
      t.increments("id");
      t.decimal("field_1").notNullable();
      t.text("field_2").defaultTo("default text");
      t.timestamps();
      t.unique(["field_1"]);
    });
    await knex.schema.createTable("table_3", (t) => {
      t.bigIncrements("id_field");
      t.bigInteger("field_1");
      t.timestamps();
    });

    await knex("table_1").insert(TEST_TABLE1_CONTENT);
    await knex("table_2").insert(TEST_TABLE2_CONTENT);

    rimraf.sync(TEST_DUMP_PATH);
    rimraf.sync(TEST_EXTRACTED_PATH);
  });

  it("can create database dump", async () => {
    expect(fs.existsSync(TEST_DUMP_PATH)).toBeFalsy();
    await runCli(`dump create ${connUri} ${TEST_DUMP_PATH}`);
    expect(fs.existsSync(TEST_DUMP_PATH)).toBeTruthy();
  });

  it("dumpfile contents are valid", async () => {
    expect(fs.existsSync(TEST_EXTRACTED_PATH)).toBeFalsy();

    await tar.extract({
      file: TEST_DUMP_PATH,
      cwd: path.dirname(TEST_DUMP_PATH),
    });
    expect(fs.existsSync(TEST_EXTRACTED_PATH)).toBeTruthy();

    const files = [
      "migrations/20200722182250-table_1.js",
      "migrations/20200722182250-table_2.js",
      "migrations/20200722182250-table_3.js",
      "data/table_1.jsonl",
      "data/table_2.jsonl",
    ];
    files.forEach((file) => {
      const filePath = `${TEST_EXTRACTED_PATH}/${file}`;
      expect(fs.existsSync(filePath)).toBeTruthy();
      expect(fs.readFileSync(filePath).toString()).toMatchSnapshot();
    });
  });

  it("can see dumpfile contents", async () => {
    expect(await runCli(`ls ${TEST_DUMP_PATH}`)).toMatchSnapshot();

    expect(await runCli(`show ${TEST_DUMP_PATH}/table_1`)).toMatchSnapshot();

    expect(await runCli(`diff ${connUri} ${TEST_DUMP_PATH}`)).toMatchSnapshot();

    expect(
      await runCli(`diff ${connUri}/table_1 ${TEST_DUMP_PATH}/table_1`)
    ).toMatchSnapshot();
  });

  it("can load dump to database", async () => {
    expect(await knex2.schema.listTables()).toMatchObject([]);

    await runCli(`dump load ${connUri2} ${TEST_DUMP_PATH}`);

    expect(await knex2.schema.listTables()).toMatchObject([
      { table: TEST_MIGRATIONS_TABLE },
      { table: `${TEST_MIGRATIONS_TABLE}_lock` },
      { table: "table_1" },
      { table: "table_2" },
      { table: "table_3" },
    ]);
  });

  it("loaded database has expected content", async () => {
    expect(await knex2(TEST_MIGRATIONS_TABLE)).toMatchObject([
      { id: 1, batch: 1, name: "20200722182250-table_1.js" },
      { id: 2, batch: 1, name: "20200722182250-table_2.js" },
      { id: 3, batch: 1, name: "20200722182250-table_3.js" },
    ]);
    expect(await knex2(`${TEST_MIGRATIONS_TABLE}_lock`)).toMatchObject([
      { index: 1, is_locked: 0 },
    ]);

    expect(await runCli(`show ${connUri2}/table_1`)).toMatchSnapshot();
    expect(await runCli(`show ${connUri2}/table_2`)).toMatchSnapshot();
    expect(await runCli(`show ${connUri2}/table_3`)).toMatchSnapshot();

    // sqlite returns dates as their numeric value in milliseconds
    const toSqliteRow = (row) => {
      row.created_at = new Date(row.created_at).getTime();
      return row;
    };

    expect(await knex2("table_1")).toMatchObject(
      TEST_TABLE1_CONTENT.map(toSqliteRow)
    );
    expect(await knex2("table_2")).toMatchObject(
      TEST_TABLE2_CONTENT.map(toSqliteRow)
    );
    expect(await knex2("table_3")).toMatchObject([]);
  });

  it("can diff against original database", async () => {
    expect(await runCli(`diff ${connUri} ${connUri2}`)).toMatchSnapshot();

    expect(
      await runCli(`diff ${connUri}/table_1 ${connUri2}/table_1`)
    ).toMatchSnapshot();
  });
});
