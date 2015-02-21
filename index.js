var idgen = require('idgen');

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
    }
  };
};
