const _ = require("lodash");
const chalk = require("chalk");

const colHash = (col) => `${col.fullType}:${col.nullable}`;

const indHash = (col) => `${col.algorithm}:${col.unique}:${col.columns}`;

const colDesc = (col) => {
  let str = col.fullType;
  if (col.nullable) {
    str += " nullable";
  }
  return str;
};

const valueOrDiff = (before, after) =>
  String(before) === String(after)
    ? String(before)
    : `${chalk.red(before)} → ${chalk.green(after)}`;

const diffColumns = (table1, table2, { showSimilar = false } = {}) => {
  // Column keys in one or both tables
  const allColKeys = _.union(Object.keys(table1), Object.keys(table2));
  const allColumns = allColKeys.map((key) =>
    diffColumnVersions(key, table1[key], table2[key])
  );

  const summary = buildSummary(allColumns, { showSimilar });
  const columns = allColumns.filter(
    (c) => c.status !== "similar" || showSimilar
  );

  return { columns, summary };
};

const diffColumnVersions = (key, col1, col2) => {
  const desc1 = col1 && colDesc(col1);
  const desc2 = col2 && colDesc(col2);

  if (!col2) {
    return {
      status: "deleted",
      displayColumn: chalk.red(key),
      displayType: chalk.red(desc1),
    };
  }

  if (!col1) {
    return {
      status: "created",
      displayColumn: chalk.green(key),
      displayType: chalk.green(desc2),
    };
  }

  const changed = colHash(col1) !== colHash(col2);
  return {
    status: changed ? "changed" : "similar",
    displayColumn: key,
    displayType: valueOrDiff(desc1, desc2),
  };
};

const diffIndexes = (indexes1, indexes2, { showSimilar = false } = {}) => {
  const indByName1 = _.keyBy(indexes1, "name");
  const indByName2 = _.keyBy(indexes2, "name");
  const indByHash1 = _.keyBy(indexes1, (ind) => indHash(ind));
  const indByHash2 = _.keyBy(indexes2, (ind) => indHash(ind));

  // Index keys in one or both tables
  const allIndexNameKeys = _.union(
    Object.keys(indByName1),
    Object.keys(indByName2)
  );
  const allIndexHashKeys = _.union(
    Object.keys(indByHash1),
    Object.keys(indByHash2)
  );

  // Indexes autogenerated by some tools may be created with different
  // randomised names each time, which would appear as totally different
  // indexes if we group them by their name (the "index" field).
  //
  // We want to detect those cases and show the index as "changed". To
  // achieve this, we union all indexes both by name, and by hash, and
  // continue with whichever results in less different indexes (which
  // means more matches found between the indexes of both tables).
  const allIndexKeys =
    allIndexHashKeys.length <= allIndexNameKeys.length
      ? allIndexHashKeys
      : allIndexNameKeys;

  const allIndexes = allIndexKeys.map((key) =>
    diffIndexVersions(
      key,
      indByName1[key] || indByHash1[key],
      indByName2[key] || indByHash2[key]
    )
  );

  const summary = buildSummary(allIndexes, { showSimilar });
  const indexes = allIndexes.filter(
    (c) => c.status !== "similar" || showSimilar
  );

  return { indexes, summary };
};

const diffIndexVersions = (key, ind1, ind2) => {
  if (!ind2) {
    return {
      status: "deleted",
      displayIndex: chalk.red(ind1.name),
      displayAlgorithm: chalk.red(ind1.algorithm),
      displayUnique: chalk.red(ind1.unique),
      displayColumns: chalk.red(ind1.columns),
    };
  }

  if (!ind1) {
    return {
      status: "created",
      displayIndex: chalk.green(ind2.name),
      displayAlgorithm: chalk.green(ind2.algorithm),
      displayUnique: chalk.green(ind2.unique),
      displayColumns: chalk.green(ind2.columns),
    };
  }

  const changed = indHash(ind1) !== indHash(ind2) || ind1.name !== ind2.name;
  return {
    status: changed ? "changed" : "similar",
    displayIndex: valueOrDiff(ind1.name, ind2.name),
    displayAlgorithm: valueOrDiff(ind1.algorithm, ind2.algorithm),
    displayUnique: valueOrDiff(ind1.unique, ind2.unique),
    displayColumns: valueOrDiff(ind1.columns, ind2.columns),
  };
};

const diffSchemas = (
  tablesBefore,
  tablesAfter,
  { showSimilar = false } = {}
) => {
  // Table names in one or both schemas
  const allTableKeys = _.union(
    Object.keys(tablesBefore),
    Object.keys(tablesAfter)
  );
  const allTables = allTableKeys.map((tableKey) =>
    diffTableVersions(tablesBefore[tableKey], tablesAfter[tableKey])
  );

  const summary = buildSummary(allTables, { showSimilar });
  const tables = allTables.filter((t) => t.status !== "similar" || showSimilar);

  return { tables, summary };
};

const diffTableVersions = (table1, table2) => {
  const { columns, summary: colSummary } = diffColumns(
    (table1 && table1.columns) || {},
    (table2 && table2.columns) || {},
    { showSimilar: true }
  );

  const { indexes, summary: indSummary } = diffIndexes(
    (table1 && table1.indexes) || {},
    (table2 && table2.indexes) || {},
    { showSimilar: true }
  );

  if (!table1) {
    return {
      status: "created",
      displayTable: chalk.green(table2.table),
      displayRows: chalk.green(table2.rows),
      displayBytes: chalk.green(table2.prettyBytes),
      colSummary,
      indSummary,
    };
  }

  if (!table2) {
    return {
      status: "deleted",
      displayTable: chalk.red(table1.table),
      displayRows: chalk.red(table1.rows),
      displayBytes: chalk.red(table1.prettyBytes),
      colSummary,
      indSummary,
    };
  }

  const colChanged = columns.filter((c) => c.status !== "similar");
  const indChanged = indexes.filter((c) => c.status !== "similar");

  const areEqual =
    !colChanged.length &&
    !indChanged.length &&
    table1.rows === table2.rows &&
    table1.prettyBytes === table2.prettyBytes;

  return {
    status: areEqual ? "similar" : "changed",
    displayTable: table1.table,
    displayRows: valueOrDiff(table1.rows, table2.rows),
    displayBytes: valueOrDiff(table1.prettyBytes, table2.prettyBytes),
    colSummary,
    indSummary,
  };
};

const buildSummary = (items, { showSimilar }) => {
  const statusColors = {
    created: chalk.green,
    deleted: chalk.red,
    changed: chalk.yellow,
  };
  const statusOrders = {
    deleted: 1,
    created: 2,
    changed: 3,
  };

  const formatText = (status, text) => {
    if (statusColors[status]) {
      return statusColors[status](text);
    }
    return text;
  };

  if (!items.length) {
    return "none";
  }

  return (
    _(items)
      .countBy("status")
      .map((num, status) => {
        let text = formatText(status, `${num}x ${status}`);
        if (status === "similar" && !showSimilar) {
          text += " (hidden)";
        }
        return { num, text, status };
      })
      //.orderBy((c) => -c.num)
      .orderBy((c) => statusOrders[c.status] || 4)
      .map("text")
      .join(", ")
  );
};

module.exports = { diffColumns, diffIndexes, diffSchemas };
