const { getTestKnex } = require("../test/utils");
const { hydrateKnex } = require("./knexUtils");

const EXPECTED_COLUMNS_1_INFO = {
  id: { fullType: "integer", nullable: false },
  field_1: { fullType: "varchar(255)", nullable: true },
  field_2: { fullType: "varchar(255)", nullable: true },
};

const EXPECTED_INDEXES_1_INFO = [
  {
    name: "table_1_field_1_index",
    unique: false,
    columns: ["field_1"],
  },
  {
    name: "table_1_field_1_field_2_unique",
    unique: true,
    columns: ["field_1", "field_2"],
  },
];

describe("hydrateKnex()", () => {
  let knex;

  beforeAll(async () => {
    knex = hydrateKnex(getTestKnex());

    await knex.schema.createTable("table_1", (t) => {
      t.increments("id");
      t.string("field_1");
      t.string("field_2");
      t.index("field_1");
      t.unique(["field_1", "field_2"]);
    });

    await knex.schema.createTable("table_2", (t) => {
      t.bigInteger("field_1");
      t.string("field_2");
      t.primary(["field_1", "field_2"]);
    });

    await knex("table_1").insert([
      { id: 1, field_1: "foo", field_2: "bar" },
      { id: 2, field_1: "bar", field_2: "baz" },
    ]);
  });

  afterAll(async () => {
    await knex.destroy();
  });

  it("can get connection uri", async () => {
    expect(knex.getUri()).toBe(
      `sqlite3://${process.env.PWD}/.tmp/test-${process.env.JEST_WORKER_ID}.db`
    );
  });

  it("can count table rows", async () => {
    expect(await knex("table_1").countRows()).toBe(2);
  });

  it("can count table rows (with filter)", async () => {
    expect(await knex("table_1").where({ field_1: "foo" }).countRows()).toBe(1);
  });

  it("can detect table primary key", async () => {
    expect(await knex("table_1").getPrimaryKey()).toEqual(["id"]);
  });

  it("can detect table primary key (composite)", async () => {
    expect(await knex("table_2").getPrimaryKey()).toEqual([
      "field_1",
      "field_2",
    ]);
  });

  it("can get table columns", async () => {
    expect(await knex("table_1").columnInfo()).toMatchObject(
      EXPECTED_COLUMNS_1_INFO
    );
  });

  it("can list table indexes", async () => {
    expect(await knex.schema.listIndexes("table_1")).toMatchObject(
      EXPECTED_INDEXES_1_INFO
    );
  });

  it("can list tables", async () => {
    expect(await knex.schema.listTables()).toMatchObject([
      { bytes: 4096, rows: 2, table: "table_1", prettyBytes: "4.1 kB" },
      { bytes: 4096, rows: 0, table: "table_2", prettyBytes: "4.1 kB" },
    ]);
  });

  it("can get tables structure", async () => {
    expect(await knex.schema.tablesInfo()).toMatchObject({
      table_1: {
        bytes: 4096,
        rows: 2,
        table: "table_1",
        prettyBytes: "4.1 kB",
        columns: EXPECTED_COLUMNS_1_INFO,
        indexes: EXPECTED_INDEXES_1_INFO,
      },
      table_2: {
        bytes: 4096,
        rows: 0,
        table: "table_2",
        prettyBytes: "4.1 kB",
        columns: {
          field_1: { fullType: "bigint", nullable: true },
          field_2: { fullType: "varchar(255)", nullable: true },
        },
        indexes: [],
      },
    });
  });

  it("can list databases", async () => {
    expect(await knex.schema.listDatabases()).toMatchObject([
      { database: "main" },
    ]);
  });
});
