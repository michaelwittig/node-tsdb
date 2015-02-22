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
	});
});