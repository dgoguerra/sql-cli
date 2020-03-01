const _ = require("lodash");

const colHash = col => `${col.type}:${col.maxLength}:${col.nullable}`;

const colDesc = col => {
  let str = col.type;
  if (col.maxLength) {
    str += `(${col.maxLength})`;
  }
  if (col.nullable) {
    str += " nullable";
  }
  return str;
};

module.exports.diffColumns = (schemaBefore, schemaAfter) => {
  const colKeys = _.uniq(
    Object.keys(schemaBefore).concat(Object.keys(schemaAfter))
  );

  return colKeys.map(key => {
    const before = schemaBefore[key] || null;
    const after = schemaAfter[key] || null;

    const colInfo = {
      column: key,
      descBefore: (before && colDesc(before)) || null,
      descAfter: (after && colDesc(after)) || null
    };

    if (!after) {
      return { ...colInfo, status: "deleted" };
    }
    if (!before) {
      return { ...colInfo, status: "created" };
    }

    const changed = colHash(before) !== colHash(after);
    return {
      ...colInfo,
      status: changed ? "changed" : "similar"
    };
  });
};
