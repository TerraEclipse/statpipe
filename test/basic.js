describe('basic test', function () {
  var pipe, key = 'statpipe-test-' + idgen();
  before(function () {
    pipe = statpipe({client: require('redis').createClient()});
  });
  it('record some data', function (done) {
    var data = require('./data.json');
    async.forEach(data, function (item, cb) {
      pipe.record(key, item[2], item[1], cb);
    }, function (err) {
      assert.ifError(err);
      done();
    });
  });
  it('play', function (done) {
    pipe.play(key, function (err, chunk, getNext) {
      assert.ifError(err);
      var expected = [ { value: 4.367, timestamp: 31564800000 },
        { value: 5.147, timestamp: 34243200000 },
        { value: 5.418, timestamp: 36662400000 },
        { value: 4.897, timestamp: 39340800000 },
        { value: 5.002, timestamp: 41929200000 },
        { value: 5.329, timestamp: 44607600000 },
        { value: 3.537, timestamp: 47199600000 },
        { value: 3.94, timestamp: 49878000000 },
        { value: 5.226, timestamp: 52556400000 },
        { value: 5.429, timestamp: 55148400000 },
        { value: 5.4, timestamp: 57830400000 },
        { value: 4.446, timestamp: 60422400000 },
        { value: 5.072, timestamp: 63100800000 },
        { value: 5.707, timestamp: 65779200000 },
        { value: 5.76, timestamp: 68284800000 },
        { value: 5.807, timestamp: 70963200000 },
        { value: 5.865, timestamp: 73551600000 },
        { value: 5.909, timestamp: 76230000000 },
        { value: 3.681, timestamp: 78822000000 },
        { value: 3.895, timestamp: 81500400000 },
        { value: 6.247, timestamp: 84178800000 },
        { value: 6.629, timestamp: 86770800000 },
        { value: 6.67, timestamp: 89452800000 },
        { value: 5.52, timestamp: 92044800000 },
        { value: 6.71, timestamp: 94723200000 },
        { value: 7.134, timestamp: 97401600000 },
        { value: 7.097, timestamp: 99820800000 } ];
      assert.deepEqual(chunk, expected);
      done();
    });
  });
  it('graphs', function (done) {
    pipe.graph(key, '1y', function (err, results) {
      assert.ifError(err);
      var expected = [ { time: 31564800000,
        count: 12,
        sum: 58.138,
        avg: 4.844833333333333,
        max: 5.429,
        min: 3.537,
        open: 4.367,
        close: 4.446 },
      { time: 63072000000,
        count: 12,
        sum: 66.762,
        avg: 5.5635,
        max: 6.67,
        min: 3.681,
        open: 5.072,
        close: 5.52 },
      { time: 94608000000,
        count: 3,
        sum: 20.941000000000003,
        avg: 6.980333333333334,
        max: 7.134,
        min: 6.71,
        open: 6.71,
        close: 7.097 } ];
      assert.deepEqual(results, expected);
      done();
    });
  });
  it('counts', function (done) {
    pipe.count(key, {start: 68284800000, end: 97401600000}, function (err, count) {
      assert.ifError(err);
      assert.equal(count, 12);
      done();
    });
  });
  it('sums', function (done) {
    pipe.sum(key, function (err, sum) {
      assert.ifError(err);
      assert.equal(sum, 145.84100000000004);
      done();
    });
  });
});
