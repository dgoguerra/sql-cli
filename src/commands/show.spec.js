const { getTestKnex, runCli } = require("../../test/utils");
const { hydrateKnex } = require("../knex/knex");

describe("show", () => {
  let knex;
  let connUri;

  beforeAll(async () => {
    knex = hydrateKnex(getTestKnex());
    connUri = knex.getUri();

    await knex.schema.createTable("table_1", (t) => {
      t.increments("id");
      t.integer("field_1");
      t.string("field_2").defaultTo("default text");
      t.string("field_3").references("table_1.field_2");
      t.timestamps();
      t.index(["field_2", "field_1"]);
    });
  });

  it("can show table", async () => {
    const output = await runCli(`show ${connUri}/table_1`);
    expect(output).toMatchSnapshot();
  });
});
