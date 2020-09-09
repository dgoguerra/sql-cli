const { getTestKnex } = require("../test/utils");
const { hydrateKnex } = require("./knexUtils");

const EXPECTED_COLUMNS_INFO = {
  id: { fullType: "integer", nullable: false },
  field_1: { fullType: "varchar(255)", nullable: true },
  field_2: { fullType: "varchar(255)", nullable: true },
};

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

  it("can get table columns", async () => {
    expect(await knex("table_1").columnInfo()).toMatchObject(
      EXPECTED_COLUMNS_INFO
    );
  });

  it("can list table indexes", async () => {
    expect(await knex.schema.listIndexes("table_1")).toMatchObject([
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
    ]);
  });

  it("can list tables", async () => {
    expect(await knex.schema.listTables()).toMatchObject([
      { bytes: 4096, rows: 2, table: "table_1", prettyBytes: "4.1 kB" },
    ]);
  });

  it("can get tables structure", async () => {
    expect(await knex.schema.tablesInfo()).toMatchObject({
      table_1: {
        bytes: 4096,
        rows: 2,
        table: "table_1",
        prettyBytes: "4.1 kB",
        schema: EXPECTED_COLUMNS_INFO,
      },
    });
  });
});
