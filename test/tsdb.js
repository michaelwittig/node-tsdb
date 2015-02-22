var fs = require("fs");
var assert = require("assert-plus");
var uuid = require("node-uuid");
var mkdirp = require("mkdirp");

var index = require("../index.js");

function randomfile() {
	"use strict";
	return "./test/tmp/" + uuid.v4() + ".dat";
}

describe("index", function() {
	"use strict";
	var tsdb;
	before(function() {
		mkdirp.sync("./test/tmp/tsdb");
		tsdb = new index.TSDB("./test/tmp/tsdb");
		tsdb.defineTable("sensor01", [{name: "timestamp", type: "uint32"}, {name: "val", type: "float"}, {name: "flag", type: "uint8"}]);
	});
	describe("TSDB", function() {
		describe("createStream", function() {
			var needed = 10 * 1000 * 1000;
			it("write", function(done) {
				var i = 0, dones = 0, begin = Date.now();
				function cb() {
					dones++;
					if (dones === needed) {
						done();
					}
				}
				tsdb.createWriteStream("sensor01", 20140222, function(err, writable) {
					if (err) {
						throw err;
					}
					function enterWriteLoop() {
						while (i < needed) {
							var continueWriting = writable.write([i, 1.0, 1], cb);
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
			it("read", function(done) {
				var reads = 0;
				tsdb.createReadStream("sensor01", 20140222, 0, function(err, readable) {
					if (err) {
						throw err;
					}
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
	});
});
