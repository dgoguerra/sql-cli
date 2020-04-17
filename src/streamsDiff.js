const _ = require("lodash");
const chalk = require("chalk");
const deepEqual = require("deep-equal");
const tupleStream = require("tuple-stream2");

const formatValue = val => {
  if (val instanceof Date) {
    return val.toISOString();
  }
  if (val === null) {
    return "[null]";
  }
  return val;
};

const valueOrDiff = (before, after) => {
  if (before === after) {
    return chalk.reset(before);
  }
  if (before && after) {
    return `${chalk.red(before)} → ${chalk.green(after)}`;
  }
  return before ? chalk.red(before) : chalk.green(after);
};

const streamsDiff = (
  streamA,
  streamB,
  { idKey = "id", allRows = false, allColumns = false, maxRows = 100 } = {}
) => {
  const diffRows = [];

  const comparator = (a, b) => {
    if (!a) return 1;
    if (!b) return -1;
    return a[idKey] - b[idKey];
  };

  const formatResults = rows => {
    const keysWithChanges = {};

    const formatRow = ([a, b]) =>
      _.transform({ ...a, ...b }, (acc, val, key) => {
        const valA = formatValue(a && a[key]);
        const valB = formatValue(b && b[key]);
        if (a && b && !deepEqual(valA, valB)) {
          keysWithChanges[key] = true;
        }
        acc[key] = valueOrDiff(valA, valB);
      });

    const cleanRowKeys = row =>
      _.transform(row, (acc, val, key) => {
        if (allColumns || key === idKey || keysWithChanges[key]) {
          acc[key] = val;
        }
      });

    return rows.map(row => formatRow(row)).map(row => cleanRowKeys(row));
  };

  return new Promise(resolve =>
    tupleStream([streamA, streamB], { comparator })
      .on("data", row => {
        if (allRows || !deepEqual(row[0], row[1])) {
          diffRows.push(row);
        }
      })
      .on("finish", () => resolve(formatResults(diffRows)))
  );
};

module.exports.streamsDiff = streamsDiff;
