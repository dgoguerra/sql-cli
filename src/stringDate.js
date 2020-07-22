// Ensure that we have 2 places for each of the date segments.
function padDate(segment) {
  segment = segment.toString();
  return segment[1] ? segment : `0${segment}`;
}

// Get a date object in the correct format, without requiring
// a full out library like "moment.js".
module.exports.stringDate = (date = new Date()) => {
  return (
    date.getFullYear().toString() +
    padDate(date.getMonth() + 1) +
    padDate(date.getDate()) +
    padDate(date.getHours()) +
    padDate(date.getMinutes()) +
    padDate(date.getSeconds())
  );
};
