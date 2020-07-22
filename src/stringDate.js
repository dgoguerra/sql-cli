// Ensure that we have 2 places for each of the date segments.
const padDate = (segment) => {
  segment = segment.toString();
  return segment[1] ? segment : `0${segment}`;
};

// Get a date object in the correct format, without requiring
// a full out library like "moment.js".
const stringDate = (date = null) => {
  // If no date is supplied, default to current time, unless
  // SQL_DUMP_DATE env var is set. This env var will be used
  // to force the date in use during testing.
  if (!date) {
    date = process.env.SQL_DUMP_DATE
      ? new Date(process.env.SQL_DUMP_DATE)
      : new Date();
  }

  return (
    date.getFullYear().toString() +
    padDate(date.getMonth() + 1) +
    padDate(date.getDate()) +
    padDate(date.getHours()) +
    padDate(date.getMinutes()) +
    padDate(date.getSeconds())
  );
};

module.exports = { stringDate };
