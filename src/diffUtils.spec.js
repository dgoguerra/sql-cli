// Disable chalk's coloring of outputs
process.env.FORCE_COLOR = 0;

const _ = require("lodash");
const { diffArrays, defaultFormatter } = require("./diffUtils");

const COLUMNS_1 = [
  { name: "id", type: "bigint", nullable: false, default: undefined },
  { name: "name", type: "varchar(255)", nullable: true, default: undefined },
];
const COLUMNS_2 = [
  { name: "id", type: "varchar(255)", nullable: true, default: undefined },
  { name: "name2", type: "varchar(255)", nullable: true, default: "john" },
];

const INDEXES_1 = [
  { name: "foo", unique: true, algorithm: "btree", columns: ["id"] },
  { name: "bar", unique: true, algorithm: "btree", columns: ["name"] },
];

const INDEXES_2 = [
  { name: "foo", unique: true, algorithm: "btree", columns: ["id"] },
  { name: "bar", unique: false, algorithm: "btree", columns: ["name2"] },
  { name: "bar2", unique: true, algorithm: "btree", columns: ["id", "name2"] },
];

const SCHEMA_1 = [
  {
    table: "table1",
    bytes: "1 kB",
    rows: 5,
    columns: COLUMNS_1,
    indexes: INDEXES_1,
  },
  {
    table: "table2",
    bytes: "1 kB",
    rows: 5,
    columns: COLUMNS_2,
    indexes: INDEXES_2,
  },
];

const SCHEMA_2 = [
  {
    table: "table1",
    bytes: "48 kB",
    rows: 100,
    columns: COLUMNS_2,
    indexes: INDEXES_2,
  },
  {
    table: "table3",
    bytes: "1 kB",
    rows: 5,
    columns: COLUMNS_2,
    indexes: INDEXES_2,
  },
];

describe("diffArrays()", () => {
  it("similar objects", () => {
    const result = diffArrays(COLUMNS_1, COLUMNS_1, { keyBy: "name" });
    expect(result).toMatchObject({
      items: [
        { name: "id", type: "bigint", nullable: "false", default: undefined },
        {
          name: "name",
          type: "varchar(255)",
          nullable: "true",
          default: undefined,
        },
      ],
      summary: "2x similar",
    });
  });

  it("different objects", () => {
    const result = diffArrays(COLUMNS_1, COLUMNS_2, { keyBy: "name" });
    expect(result).toMatchObject({
      items: [
        {
          name: "id",
          type: "bigint → varchar(255)",
          nullable: "false → true",
          default: undefined,
        },
        {
          name: "name",
          type: "varchar(255)",
          nullable: "true",
          default: undefined,
        },
        {
          name: "name2",
          type: "varchar(255)",
          nullable: "true",
          default: "john",
        },
      ],
      summary: "1x deleted, 1x created, 1x changed",
    });
  });

  it("different objects (array items)", () => {
    const result = diffArrays(INDEXES_1, INDEXES_2, { keyBy: "name" });
    expect(result).toMatchObject({
      items: [
        { name: "foo", unique: "true", columns: "id" },
        { name: "bar", unique: "true → false", columns: "name → name2" },
        { name: "bar2", unique: "true", columns: "id,name2" },
      ],
      summary: "1x created, 1x changed, 1x similar",
    });
  });

  it("different objects with custom formatter", () => {
    const result = diffArrays(SCHEMA_1, SCHEMA_2, {
      keyBy: "table",
      formatter: (status, field, val1, val2) => {
        if (field === "columns" || field === "indexes") {
          return diffArrays(val1, val2, { keyBy: "name" }).summary;
        }
        return defaultFormatter(status, field, val1, val2);
      },
    });
    expect(result).toMatchObject({
      items: [
        {
          bytes: "1 kB → 48 kB",
          rows: "5 → 100",
          table: "table1",
          columns: "1x deleted, 1x created, 1x changed",
          indexes: "1x created, 1x changed, 1x similar",
        },
        {
          bytes: "1 kB",
          rows: "5",
          table: "table2",
          columns: "2x deleted",
          indexes: "3x deleted",
        },
        {
          bytes: "1 kB",
          rows: "5",
          table: "table3",
          columns: "2x created",
          indexes: "3x created",
        },
      ],
      summary: "1x deleted, 1x created, 1x changed",
    });
  });
});
