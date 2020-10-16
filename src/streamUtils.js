const pump = require("pump");
const through = require("through2");

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

module.exports = { runPipeline, chunk };
