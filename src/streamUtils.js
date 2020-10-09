const _ = require("lodash");
const pump = require("pump");
const chalk = require("chalk");
const through = require("through2");
const deepEqual = require("deep-equal");
const tupleStream = require("tuple-stream2");

const isEmpty = (val) => val === undefined || val === null;

const formatValue = (val) => {
  if (val instanceof Date) {
    return val.toISOString();
  }
  return val;
};

const deletedValue = (val) => (isEmpty(val) ? val : chalk.red(val));

const createdValue = (val) => (isEmpty(val) ? val : chalk.green(val));

const valueOrDiff = (before, after) => {
  if (isEmpty(before) && before === after) {
    return before;
  }

  before = isEmpty(before) ? "[null]" : String(before);
  after = isEmpty(after) ? "[null]" : String(after);

  if (before === after) {
    return before;
  }

  return `${chalk.red(before)} → ${chalk.green(after)}`;
};

const streamsDiff = (
  streamA,
  streamB,
  { idKey = "id", allRows = false, allColumns = false } = {}
) => {
  const diffRows = [];

  const comparator = (a, b) => (!a ? 1 : !b ? -1 : a[idKey] - b[idKey]);

  const formatResults = (rows) => {
    const keysWithChanges = {};

    const formatRow = ([a, b]) =>
      _.transform({ ...a, ...b }, (acc, val, key) => {
        const valA = formatValue(a && a[key]);
        const valB = formatValue(b && b[key]);
        if (a && b && !deepEqual(valA, valB)) {
          keysWithChanges[key] = true;
        }
        acc[key] =
          a && b
            ? valueOrDiff(valA, valB)
            : a
            ? deletedValue(valA)
            : createdValue(valB);
      });

    const cleanRowKeys = (row) =>
      _.transform(row, (acc, val, key) => {
        if (allColumns || key === idKey || keysWithChanges[key]) {
          acc[key] = val;
        }
      });

    return rows.map((row) => formatRow(row)).map((row) => cleanRowKeys(row));
  };

  return new Promise((resolve, reject) => {
    streamA.on("error", (err) => reject(err));
    streamB.on("error", (err) => reject(err));

    tupleStream([streamA, streamB], { comparator })
      .on("data", (row) => {
        if (allRows || !deepEqual(row[0], row[1])) {
          diffRows.push(row);
        }
      })
      .on("error", (err) => reject(err))
      .on("finish", () => resolve(formatResults(diffRows)));
  });
};

const runPipeline = (...streams) =>
  new Promise((resolve, reject) =>
    pump(...streams, (err) => (err ? reject(err) : resolve()))
  );

const chunk = (size = 500) => {
  let nextChunk = [];
  return through.obj(
    function (row, enc, next) {
      nextChunk.push(row);
      if (nextChunk.length >= size) {
        this.push(nextChunk);
        nextChunk = [];
      }
      next();
    },
    function (next) {
      if (nextChunk.length) {
        this.push(nextChunk);
      }
      next();
    }
  );
};

module.exports = {
  streamsDiff,
  runPipeline,
  chunk,
};
