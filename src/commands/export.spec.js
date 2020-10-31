const fs = require("fs");
const path = require("path");
const xlsx = require("xlsx");
const { getTestKnex, runCli } = require("../../test/utils");

const TMP_DIR = path.resolve(__dirname, "../../.tmp");

"export-schema.xlsx", "export-data.xlsx", "export-query.xlsx";

const TEST_EXPORT_SCHEMA = `${TMP_DIR}/export-schema.xlsx`;
const TEST_EXPORT_DATA = `${TMP_DIR}/export-data.xlsx`;
const TEST_EXPORT_QUERY = `${TMP_DIR}/export-query.xlsx`;

const getXlsxSheetContent = (file, sheet) => {
  const workbook = xlsx.readFile(file);
  return xlsx.utils.sheet_to_json(workbook.Sheets[sheet], {
    header: 1,
  });
};

describe("export", () => {
  let knex;
  let connUri;

  beforeAll(async () => {
    knex = getTestKnex();
    const { client, connection } = knex.client.config;
    connUri = `${client}://${connection.filename}`;

    await knex.schema.createTable("table_1", (t) => {
      t.increments("id");
      t.decimal("field_1");
      t.string("field_2").defaultTo("default text");
    });
    await knex.schema.createTable("table_2", (t) => {
      t.increments("id");
      t.integer("field_3").references("table_1.id");
      t.text("field_4");
    });

    await knex("table_1").insert([
      { id: 1, field_1: 23.45, field_2: "foo" },
      { id: 2, field_1: 43.5, field_2: "bar" },
    ]);

    await knex("table_2").insert([
      { id: 1, field_3: 12, field_4: "foo bar" },
      { id: 4, field_3: 14, field_4: "bar baz" },
    ]);

    if (fs.existsSync(TEST_EXPORT_SCHEMA)) {
      fs.unlinkSync(TEST_EXPORT_SCHEMA);
    }
    if (fs.existsSync(TEST_EXPORT_DATA)) {
      fs.unlinkSync(TEST_EXPORT_DATA);
    }
    if (fs.existsSync(TEST_EXPORT_QUERY)) {
      fs.unlinkSync(TEST_EXPORT_QUERY);
    }
  });

  it("must provide --schema, --data or --query", async () => {
    await expect(runCli(`export ${connUri}`)).rejects.toThrow(
      "Error: Provide either --schema, --data or --query=<sql>"
    );
  });

  it("can export database schema to XLSX", async () => {
    expect(fs.existsSync(TEST_EXPORT_SCHEMA)).toBeFalsy();

    expect(
      await runCli(`export ${connUri} ${TEST_EXPORT_SCHEMA} --schema`)
    ).toBe(TEST_EXPORT_SCHEMA + "\n");

    const content1 = getXlsxSheetContent(TEST_EXPORT_SCHEMA, "table_1");
    expect(content1).toMatchObject([
      ["Column", "Type", "Nullable", "Default", "Foreign Key"],
      ["id", "integer", false],
      ["field_1", "float", true],
      ["field_2", "varchar(255)", true, "default text"],
    ]);

    const content2 = getXlsxSheetContent(TEST_EXPORT_SCHEMA, "table_2");
    expect(content2).toMatchObject([
      ["Column", "Type", "Nullable", "Default", "Foreign Key"],
      ["id", "integer", false],
      ["field_3", "integer", true, undefined, "table_1.id"],
      ["field_4", "text", true],
    ]);

    expect(fs.existsSync(TEST_EXPORT_SCHEMA)).toBeTruthy();
  });

  it("can export database content to XLSX", async () => {
    expect(fs.existsSync(TEST_EXPORT_DATA)).toBeFalsy();

    expect(await runCli(`export ${connUri} ${TEST_EXPORT_DATA} --data`)).toBe(
      TEST_EXPORT_DATA + "\n"
    );

    const content1 = getXlsxSheetContent(TEST_EXPORT_DATA, "table_1");
    expect(content1).toMatchObject([
      ["id", "field_1", "field_2"],
      [1, 23.45, "foo"],
      [2, 43.5, "bar"],
    ]);

    const content2 = getXlsxSheetContent(TEST_EXPORT_DATA, "table_2");
    expect(content2).toMatchObject([
      ["id", "field_3", "field_4"],
      [1, 12, "foo bar"],
      [4, 14, "bar baz"],
    ]);

    expect(fs.existsSync(TEST_EXPORT_DATA)).toBeTruthy();
  });

  it("can export results of a query to XLSX", async () => {
    expect(fs.existsSync(TEST_EXPORT_QUERY)).toBeFalsy();

    expect(
      await runCli(
        `export ${connUri} ${TEST_EXPORT_QUERY} ` +
          `--query="select * from table_1 where field_2 like 'foo'"`
      )
    ).toBe(TEST_EXPORT_QUERY + "\n");

    const content1 = getXlsxSheetContent(TEST_EXPORT_QUERY, "Sheet1");
    expect(content1).toMatchObject([
      ["id", "field_1", "field_2"],
      [1, 23.45, "foo"],
    ]);

    expect(fs.existsSync(TEST_EXPORT_QUERY)).toBeTruthy();
  });
});
