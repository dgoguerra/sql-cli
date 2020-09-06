const fs = require("fs");
const tar = require("tar");
const rimraf = require("rimraf");
const { runCli, getKnexUri } = require("./utils");
const { listTables } = require("../src/knexUtils");

const TEST_DATETIME_1 = "2020-07-24 18:34:00";
const TEST_DATETIME_2 = "2020-07-24 19:25:00";
const TEST_DUMP_NAME = `.tmp/dump-test-${process.env.JEST_WORKER_ID}`;
const TEST_DUMP_PATH = `${process.env.PWD}/${TEST_DUMP_NAME}.tgz`;
const TEST_EXTRACTED_PATH = `${process.env.PWD}/${TEST_DUMP_NAME}`;

const TEST_TABLE1_CONTENT = [
  {
    field_1: 12,
    field_2: "foo",
    created_at: TEST_DATETIME_1,
  },
  {
    field_1: 30,
    field_2: "bar",
    created_at: TEST_DATETIME_1,
    updated_at: TEST_DATETIME_1,
  },
];

const TEST_TABLE2_CONTENT = [
  {
    field_1: 12.3,
    field_2: "foo",
    created_at: TEST_DATETIME_1,
    updated_at: TEST_DATETIME_1,
  },
  {
    field_1: 30.45,
    field_2: "bar",
    created_at: TEST_DATETIME_1,
    updated_at: TEST_DATETIME_2,
  },
];

const cliTestSuite = (name, knexFactory) => {
  jest.setTimeout(15000);

  describe(`CLI test: ${name}`, () => {
    let knex;
    let connUri;

    beforeAll(async () => {
      knex = await knexFactory();
      connUri = getKnexUri(knex);

      await migrateTestTables(knex);

      rimraf.sync(TEST_DUMP_PATH);
      rimraf.sync(TEST_EXTRACTED_PATH);
    });

    afterAll(async () => {
      await knex.destroy();
    });

    it("can list tables", async () => {
      expect(await runCli(`list ${connUri}`)).toMatchSnapshot();
    });

    it("can show table", async () => {
      expect(await runCli(`show ${connUri}/table_2`)).toMatchSnapshot();
    });

    it("can diff tables", async () => {
      expect(
        await runCli(`diff ${connUri}/table_1 ${connUri}/table_2`)
      ).toMatchSnapshot();
    });

    it("can diff tables data", async () => {
      expect(
        await runCli(`diff ${connUri}/table_1 ${connUri}/table_2 --data`)
      ).toMatchSnapshot();
    });

    it("can diff schemas", async () => {
      expect(await runCli(`diff ${connUri} ${connUri}`)).toMatchSnapshot();
    });

    it("can create conn aliases", async () => {
      await runCli("alias add alias1 mysql://app:secret@127.0.0.1:1234/dbname");
      await runCli("alias add alias2 sqlite3:///path/to/file.db");

      // Alias has been created
      expect(await runCli("alias ls")).toMatchSnapshot();
    });

    it("can use a created alias", async () => {
      await runCli(`alias add test-alias ${connUri}`);
      expect(await runCli("list test-alias")).toMatchSnapshot();
    });

    it("can delete conn aliases", async () => {
      await runCli("alias rm test-alias");
      await runCli("alias rm alias1");

      // Only alias2 still exists
      expect(await runCli("alias ls")).toMatchSnapshot();

      await runCli("alias rm alias2");
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

    it("can load database dump", async () => {
      await knex.schema.dropTable("table_1");
      await knex.schema.dropTable("table_2");
      await knex.schema.dropTable("table_3");

      expect(await listTables(knex)).toMatchObject([]);

      await runCli(`dump load ${getKnexUri(knex)} ${TEST_DUMP_PATH}`);

      expect(await listTables(knex)).toMatchObject([
        { table: "dump_knex_migrations" },
        { table: "dump_knex_migrations_lock" },
        { table: "table_1" },
        { table: "table_2" },
        { table: "table_3" },
      ]);

      expect(await knex("dump_knex_migrations")).toMatchObject([
        { id: 1, batch: 1, name: "20200722182250-table_1.js" },
        { id: 2, batch: 1, name: "20200722182250-table_2.js" },
        { id: 3, batch: 1, name: "20200722182250-table_3.js" },
      ]);
      expect(await knex("dump_knex_migrations_lock")).toMatchObject([
        { index: 1, is_locked: 0 },
      ]);

      expect(
        await runCli(`show ${getKnexUri(knex)}/table_1`)
      ).toMatchSnapshot();
      expect(
        await runCli(`show ${getKnexUri(knex)}/table_2`)
      ).toMatchSnapshot();
      expect(
        await runCli(`show ${getKnexUri(knex)}/table_3`)
      ).toMatchSnapshot();

      expect(await getTable(knex, "table_1")).toMatchObject(
        TEST_TABLE1_CONTENT
      );
      expect(await getTable(knex, "table_2")).toMatchObject(
        TEST_TABLE2_CONTENT
      );
      expect(await getTable(knex, "table_3")).toMatchObject([]);
    });
  });
};

const getTable = (knex, table) => {
  // Convert a date to format YYYY-MM-DD HH:mm:ss
  const toDateString = (str) => {
    if (!str) {
      return str;
    }
    return new Date(str)
      .toISOString()
      .replace("T", " ")
      .replace(/\.\d+Z$/, "");
  };

  const isNumeric = (num) => {
    if (typeof num === "string") {
      num = Number(num);
    }
    return typeof num === "number" && Number.isFinite(num);
  };

  return knex(table).then((rows) =>
    rows.map((row) => {
      for (const key in row) {
        if (isNumeric(row[key])) {
          row[key] = Number(row[key]);
        }
      }
      row.created_at = toDateString(row.created_at);
      row.updated_at = toDateString(row.updated_at);
      return row;
    })
  );
};

const migrateTestTables = async (knex) => {
  await knex.schema.createTable("table_1", (t) => {
    t.increments("id");
    t.integer("field_1");
    t.text("field_2");
    t.timestamps();
  });
  await knex.schema.createTable("table_2", (t) => {
    t.increments("id");
    t.decimal("field_1").notNullable();
    t.text("field_2").defaultTo("default text");
    t.timestamps();
  });
  await knex.schema.createTable("table_3", (t) => {
    t.bigIncrements("idField");
    t.bigInteger("field_1");
    t.timestamps();
  });
  await knex("table_1").insert(TEST_TABLE1_CONTENT);
  await knex("table_2").insert(TEST_TABLE2_CONTENT);
};

module.exports = { cliTestSuite, migrateTestTables };
