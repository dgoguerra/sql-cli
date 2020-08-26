const fs = require("fs");
const tar = require("tar");
const rimraf = require("rimraf");
const { runCli, getTestKnex, getKnexUri } = require("./utils");
const { listTables } = require("../src/knexUtils");

const TEST_DUMP_NAME = ".tmp/dump-test";
const TEST_DUMP_PATH = `${process.env.PWD}/${TEST_DUMP_NAME}.tgz`;
const TEST_EXTRACTED_PATH = `${process.env.PWD}/${TEST_DUMP_NAME}`;
const TEST_DATETIME_1 = "2020-07-24 18:34:00";
const TEST_DATETIME_2 = "2020-07-24 19:25:00";

describe("CLI dump and load commands", () => {
  let knex;

  beforeAll(async () => {
    knex = getTestKnex();

    await knex.schema.createTable("table_1", (t) => {
      t.increments("id");
      t.string("field_1");
      t.integer("field_2");
      t.timestamps();
    });
    await knex.schema.createTable("table_2", (t) => {
      t.increments("id");
      t.decimal("field_1").notNullable().defaultTo(23.56);
      t.text("field_2").defaultTo("default text");
      t.timestamps();
    });
    await knex("table_2").insert([
      {
        field_1: 12.3,
        field_2: "foo",
        created_at: TEST_DATETIME_1,
        updated_at: TEST_DATETIME_1,
      },
      {
        field_1: 30.45,
        field_2: "bar",
        created_at: TEST_DATETIME_2,
        updated_at: TEST_DATETIME_2,
      },
    ]);

    rimraf.sync(TEST_DUMP_PATH);
    rimraf.sync(TEST_EXTRACTED_PATH);
  });

  it("can create database dump", async () => {
    expect(fs.existsSync(TEST_DUMP_PATH)).toBeFalsy();
    await runCli(`dump create ${getKnexUri(knex)} ${TEST_DUMP_NAME}`);
    expect(fs.existsSync(TEST_DUMP_PATH)).toBeTruthy();
  });

  it("database dump contents are valid", async () => {
    expect(fs.existsSync(TEST_EXTRACTED_PATH)).toBeFalsy();

    await tar.extract({ file: TEST_DUMP_PATH });
    expect(fs.existsSync(TEST_EXTRACTED_PATH)).toBeTruthy();

    const files = [
      "migrations/20200722182250-table_1.js",
      "migrations/20200722182250-table_2.js",
      "data/table_2.jsonl",
    ];

    files.forEach((file) => {
      const filePath = `${TEST_EXTRACTED_PATH}/${file}`;
      expect(fs.existsSync(filePath)).toBeTruthy();
      expect(fs.readFileSync(filePath).toString()).toMatchSnapshot();
    });
  });

  it("can load database dump", async () => {
    await knex.schema.dropTable("table_1");
    await knex.schema.dropTable("table_2");

    expect(await listTables(knex)).toMatchObject([]);

    await runCli(`dump load ${getKnexUri(knex)} ${TEST_DUMP_PATH}`);

    expect(await listTables(knex)).toMatchObject([
      { table: "dump_knex_migrations" },
      { table: "dump_knex_migrations_lock" },
      { table: "table_1" },
      { table: "table_2" },
    ]);

    expect(await knex("dump_knex_migrations")).toMatchObject([
      { id: 1, batch: 1, name: "20200722182250-table_1.js" },
      { id: 2, batch: 1, name: "20200722182250-table_2.js" },
    ]);
    expect(await knex("dump_knex_migrations_lock")).toMatchObject([
      { index: 1, is_locked: 0 },
    ]);
    expect(await knex("table_1")).toMatchObject([]);
    expect(await knex("table_2")).toMatchObject([
      {
        id: 1,
        field_1: 12.3,
        field_2: "foo",
        created_at: TEST_DATETIME_1,
        updated_at: TEST_DATETIME_1,
      },
      {
        id: 2,
        field_1: 30.45,
        field_2: "bar",
        created_at: TEST_DATETIME_2,
        updated_at: TEST_DATETIME_2,
      },
    ]);
  });
});
