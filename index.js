var idgen = require('idgen')
  , timebucket = require('timebucket')

module.exports = function (options) {
  options || (options = {});
  if (!options.client) throw new Error('missing options.client, needs to be a node_redis client object');
  var client = options.client;
  return {
    record: function (key, val, timestamp, cb) {
      if (typeof timestamp === 'function') {
        cb = timestamp;
        timestamp = null;
      }
      if (!timestamp) timestamp = new Date().getTime();
      client.zadd(key, timestamp, this.serialize(val), cb);
    },
    serialize: function (val) {
      return idgen() + ':' + JSON.stringify(val);
    },
    unserialize: function (val) {
      return JSON.parse(val.split(':')[1]);
    },
    play: function (key, options, cb) {
      var self = this;
      if (typeof options === 'function') {
        cb = options;
        options = {};
      }
      options || (options = {});
      options.start || (options.start = '-inf');
      options.end || (options.end = '+inf');
      if (options.start instanceof Date) options.start = options.start.getTime();
      if (options.end instanceof Date) options.end = options.end.getTime();
      options.limit || (options.limit = 100);
      var offset = 0;
      (function getNext () {
        client.zrangebyscore(key, options.start, options.end, 'WITHSCORES', 'LIMIT', offset, options.limit, function (err, raw) {
          if (err) return cb(err);
          if (!raw.length) return cb(null, [], null);
          var chunk = [], item, idx = 0;
          raw.forEach(function (line, idx) {
            if (idx % 2) {
              item.timestamp = Number(line);
              chunk.push(item);
            }
            else {
              item = {value: self.unserialize(line)};
            }
          });
          if (chunk.length < options.limit) return cb(null, chunk, null);
          offset += options.limit;
          cb(null, chunk, getNext);
        });
      })();
    },
    graph: function (key, interval, options, cb) {
      if (typeof options === 'function') {
        cb = options;
        options = {};
      }
      options || (options = {});
      var currentResult, results = [];

      function initResult (item) {
        var bucket = timebucket(item.timestamp).resize(interval);
        return {
          bucket: bucket,
          time: item.timestamp,
          count: 0,
          sum: 0,
          avg: null,
          max: null,
          min: null,
          open: null,
          close: null
        };
      }

      this.play(key, options, function (err, chunk, getNext) {
        if (err) return cb(err);
        chunk.forEach(function (item) {
          if (!currentResult) {
            currentResult = initResult(item);
          }
          var bucket = timebucket(item.timestamp).resize(interval);
          while (bucket.value > currentResult.bucket.value) {
            results.push(currentResult);
            // make up results between this one and that
            var nextTimestamp = currentResult.bucket.add(1).toMilliseconds();
            delete currentResult.bucket;
            currentResult = initResult({timestamp: nextTimestamp});
          }
          currentResult.count++;
          currentResult.sum += item.value;
          currentResult.avg = currentResult.sum / currentResult.count;
          if (currentResult.max === null) {
            currentResult.max = currentResult.min = currentResult.open = item.value;
          }
          else {
            currentResult.max = Math.max(currentResult.max, item.value);
            currentResult.min = Math.min(currentResult.min, item.value);
          }
          currentResult.close = item.value;
        });
        if (getNext) getNext();
        else {
          if (currentResult) {
            delete currentResult.bucket;
            results.push(currentResult);
          }
          cb(null, results);
        }
      });
    },
    count: function (key, options, cb) {
      if (typeof options === 'function') {
        cb = options;
        options = {};
      }
      options || (options = {});
      options.start || (options.start = '-inf');
      options.end || (options.end = '+inf');
      if (options.start instanceof Date) options.start = options.start.getTime();
      if (options.end instanceof Date) options.end = options.end.getTime();

      client.zcount(key, options.start, options.end, function (err, count) {
        if (err) return cb(err);
        cb(null, Number(count));
      });
    },
    sum: function (key, options, cb) {
      if (typeof options === 'function') {
        cb = options;
        options = {};
      }
      var sum = 0;
      this.play(key, options, function (err, chunk, getNext) {
        if (err) return cb(err);
        chunk.forEach(function (item) {
          if (typeof item.value === 'number') sum += item.value;
        });
        if (getNext) getNext();
        else cb(null, sum);
      });
    }
  };
};
