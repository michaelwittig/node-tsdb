var fs = require("fs");
var assert = require("assert-plus");
var uuid = require("node-uuid");
var mkdirp = require("mkdirp");

var columnstream = require("../lib/columnstream.js");
var types = require("../lib/types.js");

function randomfile() {
	"use strict";
	return "./test/tmp/" + uuid.v4() + ".dat";
}

describe("columnstream", function() {
	"use strict";
	before(function() {
		mkdirp.sync("./test/tmp");
	});
	describe("single item", function() {
		var type = types("uint32"), file = randomfile();
		it("writable", function(done) {
			var fd = fs.openSync(file, "w");
			var writable = new columnstream.WritableColumnStream(fd, type);
			writable.write(1, function() {
				var stats = fs.fstatSync(fd);
				assert.equal(stats.size, 4, "size");
				done();
			});
		});
		it("readable", function(done) {
			var fd = fs.openSync(file, "r");
			var readable = new columnstream.ReadableColumnStream(fd, type);
			var reads = 0;
			readable.on("readable", function() {
				var value;
				while (null !== (value = readable.read())) {
					assert.equal(value, 1, "value");
					reads++;
				}
			});
			readable.on("end", function() {
				assert.equal(reads, 1, "reads");
				done();
			});
		});
	});
	describe("multiple items", function() {
		var type = types("uint32"), file = randomfile();
		it("writable", function(done) {
			var fd = fs.openSync(file, "w");
			var writable = new columnstream.WritableColumnStream(fd, type);
			writable.write(1, function() {
				writable.write(2, function() {
					var stats = fs.fstatSync(fd);
					assert.equal(stats.size, 8, "size");
					done();
				});
			});
		});
		it("readable", function(done) {
			var fd = fs.openSync(file, "r");
			var readable = new columnstream.ReadableColumnStream(fd, type);
			var reads = 0;
			readable.on("readable", function() {
				var value;
				while (null !== (value = readable.read())) {
					if (reads === 0) {
						assert.equal(value, 1, "value");
					} else {
						assert.equal(value, 2, "value");
					}
					reads++;
				}
			});
			readable.on("end", function() {
				assert.equal(reads, 2, "reads");
				done();
			});
		});
	});
	describe("mass items", function() {
		var type = types("uint32"), file = randomfile(), needed = 10 * 1000 * 1000;
		it("writable", function(done) {
			var fd = fs.openSync(file, "w");
			var writable = new columnstream.WritableColumnStream(fd, type);
			var i = 0, dones = 0;
			function cb() {
				dones++;
				if (dones === needed) {
					var stats = fs.fstatSync(fd);
					assert.equal(stats.size, needed * 4, "size");
					done();
				}
			}
			function enterWriteLoop() {
				while (i < needed) {
					var continueWriting = writable.write(i, cb);
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
			var fd = fs.openSync(file, "r");
			var readable = new columnstream.ReadableColumnStream(fd, type);
			var reads = 0;
			readable.on("readable", function() {
				var value;
				while (null !== (value = readable.read())) {
					assert.equal(value, reads, "value");
					reads++;
				}
			});
			readable.on("end", function() {
				assert.equal(reads, needed, "reads");
				done();
			});
		});
	});
});
