const _ = require("lodash");
const chalk = require("chalk");
const deepEqual = require("deep-equal");

const SUMMARY_STATUS_COLORS = {
  created: chalk.green,
  deleted: chalk.red,
  changed: chalk.yellow,
};

const SUMMARY_STATUS_ORDERS = {
  deleted: 1,
  created: 2,
  changed: 3,
};

const defaultFormatter = (status, field, val1, val2) => {
  val1 = formatValue(val1);
  val2 = formatValue(val2);
  if (status === "created") {
    return isEmpty(val2) ? val2 : chalk.green(val2);
  }
  if (status === "deleted") {
    return isEmpty(val1) ? val1 : chalk.red(val1);
  }
  return valueOrDiff(val1, val2);
};

const isEmpty = (val) => val === undefined || val === null;

const formatValue = (val) => {
  if (isEmpty(val)) {
    return val;
  }
  if (val instanceof Date) {
    return val.toISOString();
  }
  return String(val);
};

const valueOrDiff = (before, after) => {
  if (isEmpty(before) && isEmpty(after)) {
    return before;
  }

  before = isEmpty(before) ? "[null]" : before;
  after = isEmpty(after) ? "[null]" : after;

  if (before === after) {
    return before;
  }

  return `${chalk.red(before)} → ${chalk.green(after)}`;
};

const diffArrays = (
  arr1,
  arr2,
  {
    keyBy = (item) => JSON.stringify(item),
    formatter = defaultFormatter,
    allRows = true,
    allColumns = true, // TODO sin implementar
  } = {}
) => {
  //console.log({ arr1, arr2, keyBy });
  const arr1ById = _.keyBy(arr1 || [], keyBy);
  const arr2ById = _.keyBy(arr2 || [], keyBy);
  const allIds = Object.keys({ ...arr1ById, ...arr2ById });

  const keysWithChanges = {};

  const buildItem = (itemId) => {
    const { status, diff, changedKeys } = diffObjects(
      arr1ById[itemId],
      arr2ById[itemId],
      formatter
    );
    changedKeys.forEach((key) => (keysWithChanges[key] = true));
    return { status, diff };
  };

  const filterItem = ({ status }) => allRows || status !== "similar";

  const formatItem = ({ diff }) =>
    _.transform(diff, (acc, val, key) => {
      if (allColumns || key === keyBy || keysWithChanges[key]) {
        acc[key] = val;
      }
    });

  const allItems = allIds.map((itemId) => buildItem(itemId));
  const summary = buildSummary(allItems, { allRows });

  const items = allItems
    .filter((it) => filterItem(it))
    .map((it) => formatItem(it));

  return { items, summary };
};

const diffObjects = (obj1, obj2, formatter = defaultFormatter) => {
  if (!obj2) {
    const status = "deleted";
    const diff = _.transform(obj1, (acc, val, key) => {
      acc[key] = formatter(status, key, val, null);
    });
    return { status, diff, changedKeys: [] };
  }

  if (!obj1) {
    const status = "created";
    const diff = _.transform(obj2, (acc, val, key) => {
      acc[key] = formatter(status, key, null, val);
    });
    return { status, diff, changedKeys: [] };
  }

  const allKeys = Object.keys({ ...obj1, ...obj2 });
  const changedKeys = [];

  allKeys.forEach((key) => {
    if (!deepEqual(obj1[key], obj2[key])) {
      changedKeys.push(key);
    }
  });

  const status = changedKeys.length ? "changed" : "similar";
  const diff = {};

  allKeys.forEach((key) => {
    diff[key] = formatter(status, key, obj1[key], obj2[key]);
  });

  return { status, diff, changedKeys };
};

const buildSummary = (items, { allRows }) => {
  if (!items.length) {
    return "none";
  }

  return _(items)
    .countBy("status")
    .map((num, status) => {
      let text = `${num}x ${status}`;
      if (SUMMARY_STATUS_COLORS[status]) {
        text = SUMMARY_STATUS_COLORS[status](text);
      }
      if (status === "similar" && !allRows) {
        text += " (hidden)";
      }
      return { num, text, status };
    })
    .orderBy((c) => SUMMARY_STATUS_ORDERS[c.status] || 4)
    .map("text")
    .join(", ");
};

module.exports = { diffArrays, diffObjects, defaultFormatter };
