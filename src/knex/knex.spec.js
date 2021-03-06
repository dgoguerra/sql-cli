const { getTestKnex } = require("../../test/utils");
const { hydrateKnex } = require("./knex");

const EXPECTED_COLUMNS_1_INFO = [
  { name: "id", fullType: "integer", nullable: false },
  { name: "field_1", fullType: "varchar(255)", nullable: true },
  { name: "field_2", fullType: "varchar(255)", nullable: true },
];

const EXPECTED_COLUMNS_2_INFO = [
  { name: "field_1", fullType: "bigint", nullable: true },
  {
    name: "field_2",
    fullType: "varchar(255)",
    nullable: true,
    foreign: "table_1.field_1",
  },
];

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
      t.string("field_2").references("table_1.field_1");
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

  it("can check if table has rows", async () => {
    expect(await knex("table_1").hasRows()).toBeTruthy();
  });

  it("can check if table has rows (with filter)", async () => {
    expect(
      await knex("table_1").where({ field_1: "foo" }).hasRows()
    ).toBeTruthy();
  });

  it("can check if table has rows (when empty)", async () => {
    expect(
      await knex("table_1").where({ field_1: "unknown" }).hasRows()
    ).toBeFalsy();
  });

  it("can count table rows", async () => {
    expect(await knex("table_1").countRows()).toBe(2);
  });

  it("can count table rows (with filter)", async () => {
    expect(await knex("table_1").where({ field_1: "foo" }).countRows()).toBe(1);
  });

  it("can detect table primary key", async () => {
    expect(await knex.schema.getPrimaryKey("table_1")).toEqual(["id"]);
  });

  it("can detect table primary key (composite)", async () => {
    expect(await knex.schema.getPrimaryKey("table_2")).toEqual([
      "field_1",
      "field_2",
    ]);
  });

  it("can get table columns", async () => {
    expect(await knex.schema.listColumns("table_1")).toMatchObject(
      EXPECTED_COLUMNS_1_INFO
    );
  });

  it("can get table columns with foreign keys", async () => {
    expect(await knex.schema.listColumns("table_2")).toMatchObject(
      EXPECTED_COLUMNS_2_INFO
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
    expect(await knex.schema.getSchema()).toMatchObject({
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
        columns: EXPECTED_COLUMNS_2_INFO,
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
