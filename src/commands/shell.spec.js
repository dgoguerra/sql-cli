const { getTestKnex, runCli } = require("../../test/utils");

describe("shell", () => {
  let knex;
  let connUri;

  beforeAll(async () => {
    knex = getTestKnex();
    const { client, connection } = knex.client.config;
    connUri = `${client}://${connection.filename}`;
  });

  it("can pipe queries from stdin to shell", async () => {
    const output = await runCli(`sh ${connUri}`, {
      stdin: "select 1+1 as result;\nselect 2+3 as result2;",
    });
    expect(output.trim().split("\n")).toMatchObject([
      "result",
      "2",
      "result2",
      "5",
    ]);
  });

  it("can pass single query as shell argument", async () => {
    const output = await runCli(`sh ${connUri} "select 1+1 as result;"`);
    expect(output.trim().split("\n")).toMatchObject(["result", "2"]);
  });
});
