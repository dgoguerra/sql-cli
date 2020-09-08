const _ = require("lodash");
const chalk = require("chalk");
const { table, getBorderCharacters } = require("table");
const { limitLine } = require("./summarize");

const formatCol = (str) => chalk.underline(str);

const cleanValue = (val) => {
  if (val === null || val === undefined) {
    return "";
  }
  if (typeof val === "string") {
    // Clean control characters from string. Avoids table package error:
    // "Table data must not contain control characters".
    return val.replace(/[\u0001-\u0006\u0008-\u0009\u000B-\u001A]/g, "");
  }
  if (val instanceof Date) {
    return val.toISOString();
  }
  return val;
};

const formatEven = (str) => chalk.gray(str);
const formatOdd = (str) => str;

module.exports = (
  rowObjs,
  {
    headers = Object.keys(rowObjs[0] || []),
    format = (val, ctx) => (ctx.index % 2 ? formatEven : formatOdd)(val),
  } = {}
) => {
  const rows = rowObjs.map((rowObj, index) => {
    // extract row values with the headers order
    const row = _.at(rowObj, headers);
    return row.map((val, valIndex) =>
      format(cleanValue(val), { col: headers[valIndex], row: rowObj, index })
    );
  });

  // Add the headers names to the start of the array
  rows.unshift(headers.map(formatCol));

  const tableStr = table(rows, {
    border: getBorderCharacters("void"),
    columnDefault: {
      paddingLeft: 0,
      paddingRight: 1,
    },
    drawHorizontalLine: () => false,
  });

  return tableStr
    .split("\n")
    .map((line) => limitLine(line))
    .join("\n")
    .replace(/\r?\n$/, "");
};
