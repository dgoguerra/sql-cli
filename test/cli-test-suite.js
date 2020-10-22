const fs = require("fs");
const path = require("path");
const tar = require("tar");
const _ = require("lodash");
const rimraf = require("rimraf");
const { runCli } = require("./utils");
const { hydrateKnex } = require("../src/knexUtils");
const { stringifyConn } = require("../src/connUtils");

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
      expect(cleanIndexNames(output)).toMatchSnapshot();
    });

    it("can diff tables", async () => {
      const output = await runCli(`diff ${connUri}/table_1 ${connUri}/table_2`);
      expect(cleanIndexNames(output)).toMatchSnapshot();
    });

    it("can diff tables data", async () => {
      const output = await runCli(
        `diff ${connUri}/table_1 ${connUri}/table_2 --data`
      );
      expect(output).toMatchSnapshot();
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
        expect(cleanIndexNames(output)).toMatchSnapshot();
      });
    }

    it("can connect with an alias", async () => {
      await runCli(`alias add test-alias ${connUri}`);
      expect(await runCli("list test-alias")).toMatchSnapshot();
    });

    it("can run shell", async () => {
      const output = await runCli(`sh ${connUri} "select 1+1 as result;"`);
      expect(output.trim().split("\n")).toMatchObject(["result", "2"]);
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

    it("can see dumpfile contents", async () => {
      // Make sure the generated dumpfile can be opened through sqlite
      expect(await runCli(`ls ${TEST_DUMP_PATH}`)).toMatchSnapshot();
      expect(await runCli(`show ${TEST_DUMP_PATH}/table_1`)).toMatchSnapshot();
    });

    it("can load dump to database", async () => {
      await knex.schema.dropTable("table_1");
      await knex.schema.dropTable("table_2");
      await knex.schema.dropTable("table_3");

      await runCli(`dump load ${connUri} ${TEST_DUMP_PATH}`);

      expect(
        cleanIndexNames(await runCli(`show ${connUri}/table_1`))
      ).toMatchSnapshot();
      expect(
        cleanIndexNames(await runCli(`show ${connUri}/table_2`))
      ).toMatchSnapshot();
      expect(
        cleanIndexNames(await runCli(`show ${connUri}/table_3`))
      ).toMatchSnapshot();
    });
  });
};

const migrateTestTables = async (knex) => {
  await knex.schema.createTable("table_1", (t) => {
    t.increments("id");
    t.integer("field_1");
    t.string("field_2", 50);
    t.timestamps();
    t.index(["field_1"]);
  });
  await knex.schema.createTable("table_2", (t) => {
    t.string("id").primary();
    t.decimal("field_1").notNullable();
    t.text("field_2").defaultTo("default text");
    t.timestamps();
    t.unique(["field_1"]);
  });
  await knex.schema.createTable("table_3", (t) => {
    t.bigInteger("field_1");
    t.string("field_2");
    t.timestamps();
    t.primary(["field_1", "field_2"]);
  });

  const date1 = "2020-07-24 18:34:00";
  const date2 = "2020-07-24 19:25:00";

  await knex("table_1").insert([
    { field_1: 12, field_2: "foo", created_at: date1 },
    { field_1: 30, field_2: "bar", created_at: date1, updated_at: date1 },
  ]);
  await knex("table_2").insert([
    {
      id: "1",
      field_1: 12.3,
      field_2: "foo",
      created_at: date1,
      updated_at: date1,
    },
    {
      id: "2",
      field_1: 30.45,
      field_2: "bar",
      created_at: date1,
      updated_at: date2,
    },
  ]);
};

// MSSQL creates primary key indexes with a random name, ex:
// "PK__table_1__84964D886A8CF66A" for a table "table_1".
// Clean up output before saving its snapshot, to be able
// to reproduce results.
const cleanIndexNames = (content) =>
  content.replace(
    /PK__([a-zA-Z0-9_]+)__[a-zA-Z0-9]{16}/g,
    "PK__$1__0000000000000000"
  );

module.exports = { cliTestSuite, migrateTestTables };
