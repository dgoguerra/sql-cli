const { getTestKnex, runCli } = require("../../test/utils");
const { hydrateKnex } = require("../knexUtils");

const TEST_DATETIME_1 = "2020-07-24 18:34:00";
const TEST_DATETIME_2 = "2020-07-24 19:25:00";

const TEST_TABLE1_CONTENT = [
  { field_1: 12, field_2: "foo", created_at: TEST_DATETIME_1 },
  { field_1: 30, field_2: "bar", created_at: TEST_DATETIME_1 },
];

const TEST_TABLE2_CONTENT = [
  { field_1: 12.3, field_2: "foo", created_at: TEST_DATETIME_1 },
  { field_1: 30.45, field_2: "bar", created_at: TEST_DATETIME_2 },
];

describe("diff", () => {
  let knex1;
  let connUri1;

  let knex2;
  let connUri2;

  beforeAll(async () => {
    knex1 = hydrateKnex(getTestKnex("diff-1.db"));
    connUri1 = knex1.getUri();

    knex2 = hydrateKnex(getTestKnex("diff-2.db"));
    connUri2 = knex2.getUri();

    await knex1.schema.createTable("table_1", (t) => {
      t.increments("id");
      t.integer("field_1");
      t.text("field_2");
      t.timestamps();
      t.index(["field_1"]);
    });
    await knex2.schema.createTable("table_2", (t) => {
      t.increments("id");
      t.decimal("field_1").notNullable();
      t.text("field_2").defaultTo("default text");
      t.timestamps();
      t.unique(["field_1"]);
    });
    await knex2.schema.createTable("table_3", (t) => {
      t.bigIncrements("id_field");
      t.bigInteger("field_1");
      t.timestamps();
    });

    await knex1("table_1").insert(TEST_TABLE1_CONTENT);
    await knex2("table_2").insert(TEST_TABLE2_CONTENT);
  });

  it("can diff tables with changes", async () => {
    const output = await runCli(`diff ${connUri1}/table_1 ${connUri2}/table_2`);
    expect(output).toMatchSnapshot();
  });

  it("can diff tables with no changes", async () => {
    const output = await runCli(`diff ${connUri1}/table_1 ${connUri1}/table_1`);
    expect(output).toMatchSnapshot();
  });

  it("can diff tables with no changes, showing all", async () => {
    const output = await runCli(
      `diff ${connUri1}/table_1 ${connUri1}/table_1 --all`
    );
    expect(output).toMatchSnapshot();
  });

  it("can diff tables data with changes", async () => {
    const output = await runCli(
      `diff ${connUri1}/table_1 ${connUri2}/table_2 --data`
    );
    expect(output).toMatchSnapshot();
  });

  it("can diff tables data with no changes", async () => {
    const output = await runCli(
      `diff ${connUri1}/table_1 ${connUri1}/table_1 --data`
    );
    expect(output).toMatchSnapshot();
  });

  it("can diff tables data with no changes, showing all", async () => {
    const output = await runCli(
      `diff ${connUri1}/table_1 ${connUri1}/table_1 --data --all`
    );
    expect(output).toMatchSnapshot();
  });

  it("can diff schemas with changes", async () => {
    const output = await runCli(`diff ${connUri1} ${connUri2}`);
    expect(output).toMatchSnapshot();
  });

  it("can diff schemas with no changes", async () => {
    const output = await runCli(`diff ${connUri1} ${connUri1}`);
    expect(output).toMatchSnapshot();
  });

  it("can diff schemas with no changes, showing all", async () => {
    const output = await runCli(`diff ${connUri1} ${connUri1} --all`);
    expect(output).toMatchSnapshot();
  });
});
