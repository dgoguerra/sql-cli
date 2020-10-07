const fs = require("fs");
const path = require("path");
const tar = require("tar");
const _ = require("lodash");
const rimraf = require("rimraf");
const { runCli } = require("./utils");
const { hydrateKnex } = require("../src/knexUtils");
const { stringifyConn } = require("../src/connUtils");

const TEST_DATETIME_1 = "2020-07-24 18:34:00";
const TEST_DATETIME_2 = "2020-07-24 19:25:00";

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

const TEST_SSH_CONN = {
  host: "127.0.0.1",
  port: 2222,
  user: "user",
  password: "pass",
};

const cliTestSuite = (
  name,
  knexFactory,
  { sshHost = null, sshPort = null } = {}
) => {
  jest.setTimeout(15000);

  const TEST_DUMP_NAME = _.snakeCase(`dump-${name}`).replace(/-/g, "_");
  const TEST_DUMP_PATH = `${process.env.PWD}/.tmp/${TEST_DUMP_NAME}.tgz`;
  const TEST_EXTRACTED_PATH = `${process.env.PWD}/.tmp/${TEST_DUMP_NAME}`;
  const TEST_MIGRATIONS_TABLE = `migrations_${TEST_DUMP_NAME}`;

  describe(`CLI test: ${name}`, () => {
    let knex;
    let connUri;

    beforeAll(async () => {
      knex = hydrateKnex(await knexFactory());
      connUri = knex.getUri();

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
      const output = await runCli(`show ${connUri}/table_2`);
      expect(cleanIndexesName(output)).toMatchSnapshot();
    });

    it("can diff tables", async () => {
      const output = await runCli(`diff ${connUri}/table_1 ${connUri}/table_2`);
      expect(cleanIndexesName(output)).toMatchSnapshot();
    });

    if (sshHost && sshPort) {
      it("can connect through ssh", async () => {
        const { client, connection } = knex.client.config;
        const sshUri = stringifyConn({
          protocol: client,
          host: sshHost,
          port: sshPort,
          user: connection.user,
          password: connection.password,
          database: connection.database,
          sshHost: TEST_SSH_CONN.host,
          sshPort: TEST_SSH_CONN.port,
          sshUser: TEST_SSH_CONN.user,
          sshPassword: TEST_SSH_CONN.password,
        });

        expect(await runCli(`ls ${sshUri}`)).toMatchSnapshot();

        const output = await runCli(`show ${sshUri}/table_1`);
        expect(cleanIndexesName(output)).toMatchSnapshot();
      });
    }

    it("can diff tables data", async () => {
      const output = await runCli(
        `diff ${connUri}/table_1 ${connUri}/table_2 --data`
      );
      expect(output).toMatchSnapshot();
    });

    it("can diff schemas", async () => {
      const output = await runCli(`diff ${connUri} ${connUri}`);
      expect(output).toMatchSnapshot();
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

    it("can run shell", async () => {
      const output = await runCli(`sh ${connUri}`, {
        stdin: "select 1+1 as result;\nselect 2+3 as result2;",
      });
      expect(output.trim().split("\n")).toMatchObject([
        "result",
        "2",
        "result2",
        "5",
      ]);
    });

    it("can create database dump", async () => {
      expect(fs.existsSync(TEST_DUMP_PATH)).toBeFalsy();
      await runCli(`dump create ${connUri} ${TEST_DUMP_PATH}`);
      expect(fs.existsSync(TEST_DUMP_PATH)).toBeTruthy();
    });

    it("database dump contents are valid", async () => {
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

    it("can see dump contents", async () => {
      expect(await runCli(`ls ${TEST_DUMP_PATH}`)).toMatchSnapshot();

      expect(await runCli(`show ${TEST_DUMP_PATH}/table_1`)).toMatchSnapshot();

      expect(
        await runCli(`diff ${connUri} ${TEST_DUMP_PATH}`)
      ).toMatchSnapshot();

      const output = await runCli(
        `diff ${connUri}/table_1 ${TEST_DUMP_PATH}/table_1`
      );
      expect(cleanIndexesName(output)).toMatchSnapshot();
    });

    it("can load database dump", async () => {
      await knex.schema.dropTable("table_1");
      await knex.schema.dropTable("table_2");
      await knex.schema.dropTable("table_3");

      expect(await knex.schema.listTables()).toMatchObject([]);

      await runCli(`dump load ${connUri} ${TEST_DUMP_PATH}`);

      expect(await knex.schema.listTables()).toMatchObject([
        { table: TEST_MIGRATIONS_TABLE },
        { table: `${TEST_MIGRATIONS_TABLE}_lock` },
        { table: "table_1" },
        { table: "table_2" },
        { table: "table_3" },
      ]);

      expect(await knex(TEST_MIGRATIONS_TABLE)).toMatchObject([
        { id: 1, batch: 1, name: "20200722182250-table_1.js" },
        { id: 2, batch: 1, name: "20200722182250-table_2.js" },
        { id: 3, batch: 1, name: "20200722182250-table_3.js" },
      ]);
      expect(await knex(`${TEST_MIGRATIONS_TABLE}_lock`)).toMatchObject([
        { index: 1, is_locked: 0 },
      ]);

      const output1 = await runCli(`show ${connUri}/table_1`);
      const output2 = await runCli(`show ${connUri}/table_2`);
      const output3 = await runCli(`show ${connUri}/table_3`);
      expect(cleanIndexesName(output1)).toMatchSnapshot();
      expect(cleanIndexesName(output2)).toMatchSnapshot();
      expect(cleanIndexesName(output3)).toMatchSnapshot();

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
};

// MSSQL creates primary key indexes with a random name, ex:
// "PK__table_1__84964D886A8CF66A" for a table "table_1".
// Clean up output before saving its snapshot, to be able
// to reproduce results.
const cleanIndexesName = (content) =>
  content.replace(
    /PK__([a-zA-Z0-9_]+)__[a-zA-Z0-9]{16}/g,
    "PK__$1__0000000000000000"
  );

module.exports = { cliTestSuite, migrateTestTables };
