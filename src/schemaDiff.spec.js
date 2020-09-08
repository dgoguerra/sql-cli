// Disable chalk's coloring of outputs
process.env.FORCE_COLOR = 0;

const _ = require("lodash");
const { diffColumns, diffSchemas } = require("./schemaDiff");

// Output of knex.schema.columnInfo() to extract a table's columns
const KNEX_TABLE_1 = {
  id: { fullType: "bigint", nullable: false },
  name: { fullType: "varchar(255)", nullable: true },
};
const KNEX_TABLE_2 = {
  id: { fullType: "varchar(255)", nullable: true },
  name2: { fullType: "varchar(255)", nullable: true },
};

describe("diffColumns()", () => {
  it("similar tables", () => {
    expect(
      diffColumns(KNEX_TABLE_1, KNEX_TABLE_1, { showSimilar: true })
    ).toMatchObject({
      columns: [
        {
          displayColumn: "id",
          displayType: "bigint",
          status: "similar",
        },
        {
          displayColumn: "name",
          displayType: "varchar(255) nullable",
          status: "similar",
        },
      ],
      summary: "2x similar",
    });
  });

  it("different tables", () => {
    expect(
      diffColumns(KNEX_TABLE_1, KNEX_TABLE_2, { showSimilar: true })
    ).toMatchObject({
      columns: [
        {
          displayColumn: "id",
          displayType: "bigint → varchar(255) nullable",
          status: "changed",
        },
        {
          displayColumn: "name",
          displayType: "varchar(255) nullable",
          status: "deleted",
        },
        {
          displayColumn: "name2",
          displayType: "varchar(255) nullable",
          status: "created",
        },
      ],
      summary: "1x changed, 1x deleted, 1x created",
    });
  });
});

const SCHEMA_1 = {
  table1: {
    table: "table1",
    prettyBytes: "1 kB",
    rows: 5,
    schema: KNEX_TABLE_1,
  },
  table2: {
    table: "table2",
    prettyBytes: "1 kB",
    rows: 5,
    schema: KNEX_TABLE_2,
  },
};
const SCHEMA_2 = {
  table1: {
    table: "table1",
    prettyBytes: "48 kB",
    rows: 100,
    schema: KNEX_TABLE_2,
  },
  table3: {
    table: "table3",
    prettyBytes: "1 kB",
    rows: 5,
    schema: KNEX_TABLE_2,
  },
};

describe("diffSchemas()", () => {
  it("similar schemas", () => {
    expect(
      diffSchemas(SCHEMA_1, SCHEMA_1, { showSimilar: true })
    ).toMatchObject({
      tables: [
        {
          displayBytes: "1 kB",
          displayRows: 5,
          displayTable: "table1",
          summary: "2x similar",
          status: "similar",
        },
        {
          displayBytes: "1 kB",
          displayRows: 5,
          displayTable: "table2",
          summary: "2x similar",
          status: "similar",
        },
      ],
      summary: "2x similar",
    });
  });

  it("different schemas (different table schema)", () => {
    const changedSchema = _.cloneDeep(SCHEMA_1);
    changedSchema.table1.schema.name.fullType = "text";

    expect(
      diffSchemas(SCHEMA_1, changedSchema, { showSimilar: true })
    ).toMatchObject({
      tables: [
        {
          summary: "1x similar, 1x changed",
          displayTable: "table1",
          displayBytes: "1 kB",
          displayRows: 5,
          status: "changed",
        },
        {
          summary: "2x similar",
          displayTable: "table2",
          displayBytes: "1 kB",
          displayRows: 5,
          status: "similar",
        },
      ],
      summary: "1x changed, 1x similar",
    });
  });

  it("different schemas (different size and tables)", () => {
    expect(
      diffSchemas(SCHEMA_1, SCHEMA_2, { showSimilar: true })
    ).toMatchObject({
      tables: [
        {
          displayBytes: "1 kB → 48 kB",
          displayRows: "5 → 100",
          displayTable: "table1",
          summary: "1x changed, 1x deleted, 1x created",
          status: "changed",
        },
        {
          displayBytes: "1 kB",
          displayRows: "5",
          displayTable: "table2",
          summary: "2x deleted",
          status: "deleted",
        },
        {
          displayBytes: "1 kB",
          displayRows: "5",
          displayTable: "table3",
          summary: "2x created",
          status: "created",
        },
      ],
      summary: "1x changed, 1x deleted, 1x created",
    });
  });
});
