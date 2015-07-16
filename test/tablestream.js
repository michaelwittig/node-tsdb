var fs = require("fs");
var assert = require("assert-plus");
var uuid = require("node-uuid");
var mkdirp = require("mkdirp");

var columnstream = require("../lib/columnstream.js");
var tablestream = require("../lib/tablestream.js");
var types = require("../lib/types.js");

function randomfile() {
	"use strict";
	return "./test/tmp/" + uuid.v4() + ".dat";
}

describe("tablestream", function() {
	"use strict";
	before(function() {
		mkdirp.sync("./test/tmp");
	});
	describe("1 col", function() {
		describe("single item", function() {
			var type = types("uint32"), file = randomfile();
			it("writable", function(done) {
				var fd1 = fs.openSync(file + ".1", "w");
				var writable1 = new columnstream.WritableColumnStream(fd1, type);
				var writable = new tablestream.WritableTableStream([writable1]);
				writable.write([1], function() {
					var stats1 = fs.fstatSync(fd1);
					assert.equal(stats1.size, 4, "size");
					done();
				});
			});
			it("readable", function(done) {
				var fd1 = fs.openSync(file + ".1", "r");
				var readable1 = new columnstream.ReadableColumnStream(fd1, type);
				var readable = tablestream.readableTableStream([readable1]);
				var reads = 0;
				readable
					.on("data", function(value) {
						assert.deepEqual(value, [1], "value");
						reads++;
					})
					.on("end", function() {
						assert.equal(reads, 1, "reads");
						done();
					});
			});
		});
		describe("multiple items", function() {
			var type = types("uint32"), file = randomfile();
			it("writable", function(done) {
				var fd1 = fs.openSync(file + ".1", "w");
				var writable1 = new columnstream.WritableColumnStream(fd1, type);
				var writable = new tablestream.WritableTableStream([writable1]);
				writable.write([1], function() {
					writable.write([4], function() {
						var stats1 = fs.fstatSync(fd1);
						assert.equal(stats1.size, 8, "size");
						done();
					});
				});
			});
			it("readable", function(done) {
				var fd1 = fs.openSync(file + ".1", "r");
				var readable1 = new columnstream.ReadableColumnStream(fd1, type);
				var readable = tablestream.readableTableStream([readable1]);
				var reads = 0;
				readable
					.on("data", function(value) {
						if (reads === 0) {
							assert.deepEqual(value, [1], "value");
						} else {
							assert.deepEqual(value, [4], "value");
						}
						reads++;
					})
					.on("end", function() {
						assert.equal(reads, 2, "reads");
						done();
					});
			});
		});
		describe("mass items", function() {
			var type = types("uint32"), file = randomfile(), needed = 10 * 1000 * 1000;
			it("writable", function(done) {
				var fd1 = fs.openSync(file + ".1", "w");
				var writable1 = new columnstream.WritableColumnStream(fd1, type);
				var writable = new tablestream.WritableTableStream([writable1]);
				var i = 0, dones = 0;
				function cb() {
					dones++;
					if (dones === needed) {
						var stats1 = fs.fstatSync(fd1);
						assert.equal(stats1.size, needed * 4, "size");
						done();
					}
				}
				function enterWriteLoop() {
					while (i < needed) {
						var continueWriting = writable.write([i], cb);
						i++;
						if (continueWriting === false) {
							return;
						}
					}
				}
				writable.on("drain", function() {
					enterWriteLoop();
				});
				enterWriteLoop();
			});
			it("readable", function(done) {
				var fd1 = fs.openSync(file + ".1", "r");
				var readable1 = new columnstream.ReadableColumnStream(fd1, type);
				var readable = tablestream.readableTableStream([readable1]);
				var reads = 0;
				readable
					.on("data", function(value) {
						assert.deepEqual(value, [reads], "value");
						reads++;
					})
					.on("end", function() {
						assert.equal(reads, needed, "reads");
						done();
					});
			});
		});
	});
	describe("2 cols", function() {
		describe("single item", function() {
			var type = types("uint32"), file = randomfile();
			it("writable", function(done) {
				var fd1 = fs.openSync(file + ".1", "w");
				var fd2 = fs.openSync(file + ".2", "w");
				var writable1 = new columnstream.WritableColumnStream(fd1, type);
				var writable2 = new columnstream.WritableColumnStream(fd2, type);
				var writable = new tablestream.WritableTableStream([writable1, writable2]);
				writable.write([1, 2], function() {
					var stats1 = fs.fstatSync(fd1);
					var stats2 = fs.fstatSync(fd2);
					assert.equal(stats1.size, 4, "size");
					assert.equal(stats2.size, 4, "size");
					done();
				});
			});
			it("readable", function(done) {
				var fd1 = fs.openSync(file + ".1", "r");
				var fd2 = fs.openSync(file + ".2", "r");
				var readable1 = new columnstream.ReadableColumnStream(fd1, type);
				var readable2 = new columnstream.ReadableColumnStream(fd2, type);
				var readable = tablestream.readableTableStream([readable1, readable2]);
				var reads = 0;
				readable
					.on("data", function(value) {
						assert.deepEqual(value, [1, 2], "value");
						reads++;
					})
					.on("end", function() {
						assert.equal(reads, 1, "reads");
						done();
					});
			});
		});
		describe("multiple items", function() {
			var type = types("uint32"), file = randomfile();
			it("writable", function(done) {
				var fd1 = fs.openSync(file + ".1", "w");
				var fd2 = fs.openSync(file + ".2", "w");
				var writable1 = new columnstream.WritableColumnStream(fd1, type);
				var writable2 = new columnstream.WritableColumnStream(fd2, type);
				var writable = new tablestream.WritableTableStream([writable1, writable2]);
				writable.write([1, 2], function() {
					writable.write([4, 5], function() {
						var stats1 = fs.fstatSync(fd1);
						var stats2 = fs.fstatSync(fd2);
						assert.equal(stats1.size, 8, "size");
						assert.equal(stats2.size, 8, "size");
						done();
					});
				});
			});
			it("readable", function(done) {
				var fd1 = fs.openSync(file + ".1", "r");
				var fd2 = fs.openSync(file + ".2", "r");
				var readable1 = new columnstream.ReadableColumnStream(fd1, type);
				var readable2 = new columnstream.ReadableColumnStream(fd2, type);
				var readable = tablestream.readableTableStream([readable1, readable2]);
				var reads = 0;
				readable
					.on("data", function(value) {
						if (reads === 0) {
							assert.deepEqual(value, [1, 2], "value");
						} else {
							assert.deepEqual(value, [4, 5], "value");
						}
						reads++;
					})
					.on("end", function() {
						assert.equal(reads, 2, "reads");
						done();
					});
			});
		});
		describe("mass items", function() {
			var type = types("uint32"), file = randomfile(), needed = 10 * 1000 * 1000;
			it("writable", function(done) {
				var fd1 = fs.openSync(file + ".1", "w");
				var fd2 = fs.openSync(file + ".2", "w");
				var writable1 = new columnstream.WritableColumnStream(fd1, type);
				var writable2 = new columnstream.WritableColumnStream(fd2, type);
				var writable = new tablestream.WritableTableStream([writable1, writable2]);
				var i = 0, dones = 0;
				function cb() {
					dones++;
					if (dones === needed) {
						var stats1 = fs.fstatSync(fd1);
						var stats2 = fs.fstatSync(fd2);
						assert.equal(stats1.size, needed * 4, "size");
						assert.equal(stats2.size, needed * 4, "size");
						done();
					}
				}
				function enterWriteLoop() {
					while (i < needed) {
						var continueWriting = writable.write([i, i+1], cb);
						i++;
						if (continueWriting === false) {
							return;
						}
					}
				}
				writable.on("drain", function() {
					enterWriteLoop();
				});
				enterWriteLoop();
			});
			it("readable", function(done) {
				var fd1 = fs.openSync(file + ".1", "r");
				var fd2 = fs.openSync(file + ".2", "r");
				var readable1 = new columnstream.ReadableColumnStream(fd1, type);
				var readable2 = new columnstream.ReadableColumnStream(fd2, type);
				var readable = tablestream.readableTableStream([readable1, readable2]);
				var reads = 0;
				readable
					.on("data", function(value) {
						assert.deepEqual(value, [reads, reads+1], "value");
						reads++;
					})
					.on("end", function() {
						assert.equal(reads, needed, "reads");
						done();
					});
			});
		});
	});
	describe("3 cols", function() {
		describe("single item", function() {
			var type = types("uint32"), file = randomfile();
			it("writable", function(done) {
				var fd1 = fs.openSync(file + ".1", "w");
				var fd2 = fs.openSync(file + ".2", "w");
				var fd3 = fs.openSync(file + ".3", "w");
				var writable1 = new columnstream.WritableColumnStream(fd1, type);
				var writable2 = new columnstream.WritableColumnStream(fd2, type);
				var writable3 = new columnstream.WritableColumnStream(fd3, type);
				var writable = new tablestream.WritableTableStream([writable1, writable2, writable3]);
				writable.write([1, 2, 3], function() {
					var stats1 = fs.fstatSync(fd1);
					var stats2 = fs.fstatSync(fd2);
					var stats3 = fs.fstatSync(fd3);
					assert.equal(stats1.size, 4, "size");
					assert.equal(stats2.size, 4, "size");
					assert.equal(stats3.size, 4, "size");
					done();
				});
			});
			it("readable", function(done) {
				var fd1 = fs.openSync(file + ".1", "r");
				var fd2 = fs.openSync(file + ".2", "r");
				var fd3 = fs.openSync(file + ".3", "r");
				var readable1 = new columnstream.ReadableColumnStream(fd1, type);
				var readable2 = new columnstream.ReadableColumnStream(fd2, type);
				var readable3 = new columnstream.ReadableColumnStream(fd3, type);
				var readable = tablestream.readableTableStream([readable1, readable2, readable3]);
				var reads = 0;
				readable
					.on("data", function(value) {
						assert.deepEqual(value, [1, 2, 3], "value");
						reads++;
					})
					.on("end", function() {
						assert.equal(reads, 1, "reads");
						done();
					});
			});
		});
		describe("multiple items", function() {
			var type = types("uint32"), file = randomfile();
			it("writable", function(done) {
				var fd1 = fs.openSync(file + ".1", "w");
				var fd2 = fs.openSync(file + ".2", "w");
				var fd3 = fs.openSync(file + ".3", "w");
				var writable1 = new columnstream.WritableColumnStream(fd1, type);
				var writable2 = new columnstream.WritableColumnStream(fd2, type);
				var writable3 = new columnstream.WritableColumnStream(fd3, type);
				var writable = new tablestream.WritableTableStream([writable1, writable2, writable3]);
				writable.write([1, 2, 3], function() {
					writable.write([4, 5, 6], function() {
						var stats1 = fs.fstatSync(fd1);
						var stats2 = fs.fstatSync(fd2);
						var stats3 = fs.fstatSync(fd3);
						assert.equal(stats1.size, 8, "size");
						assert.equal(stats2.size, 8, "size");
						assert.equal(stats3.size, 8, "size");
						done();
					});
				});
			});
			it("readable", function(done) {
				var fd1 = fs.openSync(file + ".1", "r");
				var fd2 = fs.openSync(file + ".2", "r");
				var fd3 = fs.openSync(file + ".3", "r");
				var readable1 = new columnstream.ReadableColumnStream(fd1, type);
				var readable2 = new columnstream.ReadableColumnStream(fd2, type);
				var readable3 = new columnstream.ReadableColumnStream(fd3, type);
				var readable = tablestream.readableTableStream([readable1, readable2, readable3]);
				var reads = 0;
				readable
					.on("data", function(value) {
						if (reads === 0) {
							assert.deepEqual(value, [1, 2, 3], "value");
						} else {
							assert.deepEqual(value, [4, 5, 6], "value");
						}
						reads++;
					})
					.on("end", function() {
						assert.equal(reads, 2, "reads");
						done();
					});
			});
		});
		describe("mass items", function() {
			var type = types("uint32"), file = randomfile(), needed = 10 * 1000 * 1000;
			it("writable", function(done) {
				var fd1 = fs.openSync(file + ".1", "w");
				var fd2 = fs.openSync(file + ".2", "w");
				var fd3 = fs.openSync(file + ".3", "w");
				var writable1 = new columnstream.WritableColumnStream(fd1, type);
				var writable2 = new columnstream.WritableColumnStream(fd2, type);
				var writable3 = new columnstream.WritableColumnStream(fd3, type);
				var writable = new tablestream.WritableTableStream([writable1, writable2, writable3]);
				var i = 0, dones = 0;
				function cb() {
					dones++;
					if (dones === needed) {
						var stats1 = fs.fstatSync(fd1);
						var stats2 = fs.fstatSync(fd2);
						var stats3 = fs.fstatSync(fd3);
						assert.equal(stats1.size, needed * 4, "size");
						assert.equal(stats2.size, needed * 4, "size");
						assert.equal(stats3.size, needed * 4, "size");
						done();
					}
				}
				function enterWriteLoop() {
					while (i < needed) {
						var continueWriting = writable.write([i, i+1, i+2], cb);
						i++;
						if (continueWriting === false) {
							return;
						}
					}
				}
				writable.on("drain", function() {
					enterWriteLoop();
				});
				enterWriteLoop();
			});
			it("readable", function(done) {
				var fd1 = fs.openSync(file + ".1", "r");
				var fd2 = fs.openSync(file + ".2", "r");
				var fd3 = fs.openSync(file + ".3", "r");
				var readable1 = new columnstream.ReadableColumnStream(fd1, type);
				var readable2 = new columnstream.ReadableColumnStream(fd2, type);
				var readable3 = new columnstream.ReadableColumnStream(fd3, type);
				var readable = tablestream.readableTableStream([readable1, readable2, readable3]);
				var reads = 0;
				readable
					.on("data", function(value) {
						assert.deepEqual(value, [reads, reads+1, reads+2], "value");
						reads++;
					})
					.on("end", function() {
						assert.equal(reads, needed, "reads");
						done();
					});
			});
		});
	});
});
