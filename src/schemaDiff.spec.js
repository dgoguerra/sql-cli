// Disable chalk's coloring of outputs
process.env.FORCE_COLOR = 0;

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
    expect(diffColumns(KNEX_TABLE_1, KNEX_TABLE_1)).toEqual({
      columns: [
        {
          column: "id",
          descAfter: "bigint",
          descBefore: "bigint",
          displayColumn: "id",
          displayType: "bigint",
          status: "similar",
        },
        {
          column: "name",
          descAfter: "varchar(255) nullable",
          descBefore: "varchar(255) nullable",
          displayColumn: "name",
          displayType: "varchar(255) nullable",
          status: "similar",
        },
      ],
      summary: "2x similar",
    });
  });

  it("different tables", () => {
    expect(diffColumns(KNEX_TABLE_1, KNEX_TABLE_2)).toEqual({
      columns: [
        {
          column: "id",
          descAfter: "varchar(255) nullable",
          descBefore: "bigint",
          displayColumn: "id",
          displayType: "bigint → varchar(255) nullable",
          status: "changed",
        },
        {
          column: "name",
          descAfter: null,
          descBefore: "varchar(255) nullable",
          displayColumn: "name",
          displayType: "varchar(255) nullable",
          status: "deleted",
        },
        {
          column: "name2",
          descAfter: "varchar(255) nullable",
          descBefore: null,
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
    expect(diffSchemas(SCHEMA_1, SCHEMA_1)).toEqual([
      {
        bytesAfter: "1 kB",
        bytesBefore: "1 kB",
        displayBytes: "1 kB",
        displayRows: 5,
        displaySummary: "2x similar",
        displayTable: "table1",
        rowsAfter: 5,
        rowsBefore: 5,
        status: "similar",
        table: "table1",
      },
      {
        bytesAfter: "1 kB",
        bytesBefore: "1 kB",
        displayBytes: "1 kB",
        displayRows: 5,
        displaySummary: "2x similar",
        displayTable: "table2",
        rowsAfter: 5,
        rowsBefore: 5,
        status: "similar",
        table: "table2",
      },
    ]);
  });

  it("different schemas", () => {
    expect(diffSchemas(SCHEMA_1, SCHEMA_2)).toEqual([
      {
        bytesAfter: "48 kB",
        bytesBefore: "1 kB",
        displayBytes: "1 kB → 48 kB",
        displayRows: "5 → 100",
        displaySummary: "1x changed, 1x deleted, 1x created",
        displayTable: "table1",
        rowsAfter: 100,
        rowsBefore: 5,
        status: "changed",
        table: "table1",
      },
      {
        bytesAfter: null,
        bytesBefore: "1 kB",
        displayBytes: "1 kB",
        displayRows: "5",
        displaySummary: "2x deleted",
        displayTable: "table2",
        rowsAfter: null,
        rowsBefore: 5,
        status: "deleted",
        table: "table2",
      },
      {
        bytesAfter: "1 kB",
        bytesBefore: null,
        displayBytes: "1 kB",
        displayRows: "5",
        displaySummary: "2x created",
        displayTable: "table3",
        rowsAfter: 5,
        rowsBefore: null,
        status: "created",
        table: "table3",
      },
    ]);
  });
});
