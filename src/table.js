const _ = require("lodash");
const chalk = require("chalk");
const { table, getBorderCharacters } = require("table");

const formatCol = str => {
  return chalk.bold.underline(str);
};

const cleanValue = val => {
  if (val === null || val === undefined) {
    return "";
  }
  if (val instanceof Date) {
    return val.toISOString();
  }
  return val;
};

const formatEven = str => chalk.gray(str);
const formatOdd = str => str;

module.exports = (
  rowObjs,
  {
    headers = Object.keys(rowObjs[0]),
    format = (val, { index }) =>
      (index % 2 ? formatEven : formatOdd)(cleanValue(val))
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

  return table(rows, {
    border: getBorderCharacters("void"),
    columnDefault: {
      paddingLeft: 0,
      paddingRight: 1
    },
    drawHorizontalLine: () => false
  });
};
