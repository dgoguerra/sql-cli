const { stringDate } = require("./stringDate");

describe("stringDate()", () => {
  it("basic usage", () => {
    expect(stringDate(new Date("2020-07-22T15:24:00.068Z"))).toBe(
      "20200722152400"
    );
  });
});
