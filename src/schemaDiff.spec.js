const _ = require("lodash");
const chalk = require("chalk");
const { diffColumns, diffSchemas } = require("./schemaDiff");

// Output of knex.schema.columnInfo() to extract a table's columns
const KNEX_TABLE_1 = {
  id: { type: "bigint", maxLength: null, nullable: false },
  name: { type: "varchar", maxLength: 255, nullable: true },
};
const KNEX_TABLE_2 = {
  id: { type: "varchar", maxLength: 255, nullable: true },
  name2: { type: "varchar", maxLength: 255, nullable: true },
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
          displayType: `${chalk.red("bigint")} → ${chalk.green(
            "varchar(255) nullable"
          )}`,
          status: "changed",
        },
        {
          column: "name",
          descAfter: null,
          descBefore: "varchar(255) nullable",
          displayColumn: chalk.red("name"),
          displayType: chalk.red("varchar(255) nullable"),
          status: "deleted",
        },
        {
          column: "name2",
          descAfter: "varchar(255) nullable",
          descBefore: null,
          displayColumn: chalk.green("name2"),
          displayType: chalk.green("varchar(255) nullable"),
          status: "created",
        },
      ],
      summary: `1x changed, ${chalk.red("1x deleted")}, ${chalk.green(
        "1x created"
      )}`,
    });
  });
});

const SCHEMA_1 = {
  table1: { table: "table1", bytes: 1000, rows: 5, schema: KNEX_TABLE_1 },
  table2: { table: "table2", bytes: 1000, rows: 5, schema: KNEX_TABLE_2 },
};
const SCHEMA_2 = {
  table1: { table: "table1", bytes: 48000, rows: 100, schema: KNEX_TABLE_2 },
  table3: { table: "table3", bytes: 1000, rows: 5, schema: KNEX_TABLE_2 },
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
        displayBytes: `${chalk.red("1 kB")} → ${chalk.green("48 kB")}`,
        displayRows: `${chalk.red("5")} → ${chalk.green("100")}`,
        displaySummary: `1x changed, ${chalk.red("1x deleted")}, ${chalk.green(
          "1x created"
        )}`,
        displayTable: "table1",
        rowsAfter: 100,
        rowsBefore: 5,
        status: "changed",
        table: "table1",
      },
      {
        bytesAfter: null,
        bytesBefore: "1 kB",
        displayBytes: chalk.red("1 kB"),
        displayRows: chalk.red("5"),
        displaySummary: chalk.red("2x deleted"),
        displayTable: chalk.red("table2"),
        rowsAfter: null,
        rowsBefore: 5,
        status: "deleted",
        table: "table2",
      },
      {
        bytesAfter: "1 kB",
        bytesBefore: null,
        displayBytes: chalk.green("1 kB"),
        displayRows: chalk.green("5"),
        displaySummary: chalk.green("2x created"),
        displayTable: chalk.green("table3"),
        rowsAfter: 5,
        rowsBefore: null,
        status: "created",
        table: "table3",
      },
    ]);
  });
});
