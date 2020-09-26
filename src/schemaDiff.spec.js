// Disable chalk's coloring of outputs
process.env.FORCE_COLOR = 0;

const _ = require("lodash");
const { diffColumns, diffSchemas, diffIndexes } = require("./schemaDiff");

// Output of knex.schema.columnInfo() to extract a table's columns
const KNEX_COLUMNS_1 = {
  id: { fullType: "bigint", nullable: false },
  name: { fullType: "varchar(255)", nullable: true },
};
const KNEX_COLUMNS_2 = {
  id: { fullType: "varchar(255)", nullable: true },
  name2: { fullType: "varchar(255)", nullable: true },
};

const KNEX_INDEXES_1 = [
  { name: "id_unique", unique: true, algorithm: "btree", columns: ["id"] },
  { name: "other_index", unique: true, algorithm: "btree", columns: ["name"] },
];

const KNEX_INDEXES_2 = [
  { name: "id_unique", unique: true, algorithm: "btree", columns: ["id"] },
  {
    name: "other_index",
    unique: false,
    algorithm: "btree",
    columns: ["name2"],
  },
  {
    name: "other_index_2",
    unique: true,
    algorithm: "btree",
    columns: ["id", "name2"],
  },
];

const SCHEMA_1 = {
  table1: {
    table: "table1",
    prettyBytes: "1 kB",
    rows: 5,
    columns: KNEX_COLUMNS_1,
    indexes: KNEX_INDEXES_1,
  },
  table2: {
    table: "table2",
    prettyBytes: "1 kB",
    rows: 5,
    columns: KNEX_COLUMNS_2,
    indexes: KNEX_INDEXES_2,
  },
};
const SCHEMA_2 = {
  table1: {
    table: "table1",
    prettyBytes: "48 kB",
    rows: 100,
    columns: KNEX_COLUMNS_2,
    indexes: KNEX_INDEXES_2,
  },
  table3: {
    table: "table3",
    prettyBytes: "1 kB",
    rows: 5,
    columns: KNEX_COLUMNS_2,
    indexes: KNEX_INDEXES_2,
  },
};

describe("diffColumns()", () => {
  it("similar tables", () => {
    expect(
      diffColumns(KNEX_COLUMNS_1, KNEX_COLUMNS_1, { showSimilar: true })
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
      diffColumns(KNEX_COLUMNS_1, KNEX_COLUMNS_2, { showSimilar: true })
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
      summary: "1x deleted, 1x created, 1x changed",
    });
  });
});

describe("diffIndexes()", () => {
  it("similar indexes", () => {
    expect(
      diffIndexes(KNEX_INDEXES_1, KNEX_INDEXES_1, { showSimilar: true })
    ).toMatchObject({
      indexes: [
        {
          status: "similar",
          displayIndex: "id_unique",
          displayUnique: "true",
          displayColumns: "id",
        },
        {
          status: "similar",
          displayIndex: "other_index",
          displayUnique: "true",
          displayColumns: "name",
        },
      ],
      summary: "2x similar",
    });
  });

  it("different indexes", () => {
    expect(
      diffIndexes(KNEX_INDEXES_1, KNEX_INDEXES_2, { showSimilar: true })
    ).toMatchObject({
      indexes: [
        {
          status: "similar",
          displayIndex: "id_unique",
          displayUnique: "true",
          displayColumns: "id",
        },
        {
          status: "changed",
          displayIndex: "other_index",
          displayUnique: "true → false",
          displayColumns: "name → name2",
        },
        {
          status: "created",
          displayIndex: "other_index_2",
          displayUnique: "true",
          displayColumns: "id,name2",
        },
      ],
      summary: "1x created, 1x changed, 1x similar",
    });
  });
});

describe("diffSchemas()", () => {
  it("similar schemas", () => {
    expect(
      diffSchemas(SCHEMA_1, SCHEMA_1, { showSimilar: true })
    ).toMatchObject({
      tables: [
        {
          displayBytes: "1 kB",
          displayRows: "5",
          displayTable: "table1",
          colSummary: "2x similar",
          indSummary: "2x similar",
          status: "similar",
        },
        {
          displayBytes: "1 kB",
          displayRows: "5",
          displayTable: "table2",
          colSummary: "2x similar",
          indSummary: "3x similar",
          status: "similar",
        },
      ],
      summary: "2x similar",
    });
  });

  it("different schemas (different table schema)", () => {
    const changedSchema = _.cloneDeep(SCHEMA_1);
    changedSchema.table1.columns.name.fullType = "text";

    expect(
      diffSchemas(SCHEMA_1, changedSchema, { showSimilar: true })
    ).toMatchObject({
      tables: [
        {
          colSummary: "1x changed, 1x similar",
          indSummary: "2x similar",
          displayTable: "table1",
          displayBytes: "1 kB",
          displayRows: "5",
          status: "changed",
        },
        {
          colSummary: "2x similar",
          indSummary: "3x similar",
          displayTable: "table2",
          displayBytes: "1 kB",
          displayRows: "5",
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
          colSummary: "1x deleted, 1x created, 1x changed",
          indSummary: "1x created, 1x changed, 1x similar",
          status: "changed",
        },
        {
          displayBytes: "1 kB",
          displayRows: "5",
          displayTable: "table2",
          colSummary: "2x deleted",
          indSummary: "3x deleted",
          status: "deleted",
        },
        {
          displayBytes: "1 kB",
          displayRows: "5",
          displayTable: "table3",
          colSummary: "2x created",
          indSummary: "3x created",
          status: "created",
        },
      ],
      summary: "1x deleted, 1x created, 1x changed",
    });
  });
});
