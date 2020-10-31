const _ = require("lodash");

const KNEX_TYPES_MAP = {
  text: ["nvarchar(-1)", "longtext"],
  string: ["char", "character", "varchar", "nvarchar", "character varying"],
  boolean: ["tinyint"],
  integer: ["int"],
  bigInteger: ["bigint"],
  datetime: ["datetime2"],
  decimal: ["money", "numeric"],
  timestamp: ["timestamp with time zone"],
};

const toKnexType = (type, opts = {}) => {
  const fullType = toFullType(type, opts);

  const findType = (type) =>
    _.findKey(KNEX_TYPES_MAP, (val, key) => key === type || val.includes(type));

  return findType(fullType) || findType(type) || null;
};

const toFullType = (
  type,
  { unsigned = null, precision = null, scale = null, maxLength = null } = {}
) => {
  let fullType = type;
  if (unsigned) {
    fullType += " unsigned";
  }
  if (precision && scale) {
    fullType += `(${precision},${scale})`;
  }
  if (maxLength) {
    fullType += `(${maxLength})`;
  }
  return fullType;
};

module.exports = { toKnexType, toFullType };
