var fs = require("fs");
var underscore = require("underscore");
var async = require("async");
var mkdirp = require("mkdirp");
var assert = require("assert-plus");

var types = require("./lib/types.js");
var columnstream = require("./lib/columnstream.js");
var tablestream = require("./lib/tablestream.js");

function TSDB(dir) {
	"use strict";
	this.schema = {};
	this.dir = dir;
}
TSDB.prototype.defineTable = function(table, columnDefinitions, cb) {
	"use strict";
	assert.string(table, "table");
	assert.arrayOfObject(columnDefinitions, "columnDefinitions");
	assert.optionalFunc(cb, "cb");
	var self = this;
	this.schema[table] = {
		table: table,
		columns: underscore.map(columnDefinitions, function(columnDefinition) {
			return {
				name: columnDefinition.name,
				type: types(columnDefinition.type),
				typeName: columnDefinition.type
			};
		})
	};
	if (cb !== undefined) {
		cb();
	}
};
TSDB.prototype.createWriteStream = function(table, partition, cb) {
	"use strict";
	assert.string(table, "table");
	assert.number(partition, "partition");
	assert.func(cb, "cb");
	var self = this;
	var schema = this.schema[table];
	mkdirp(this.dir + "/" + partition + "/" + table, function(err) {
		if (err) {
			cb(err);
		} else {
			async.map(schema.columns, function(column, cb) {
				var file = self.dir + "/" + partition + "/" + table + "/" + column.name;
				fs.open(file, "a+", function(err, fd) {
					if (err) {
						cb(err);
					} else {
						cb(undefined, new columnstream.WritableColumnStream(fd, column.type));
					}
				});
			}, function(err, writableColumnStreams) {
				if (err) {
					cb(err);
				} else {
					cb(undefined, new tablestream.WritablTableStream(writableColumnStreams));
				}
			});
		}
	});
	
};
TSDB.prototype.createReadStream = function(table, partition, columnIndex, cb) {
	"use strict";
	assert.string(table, "table");
	assert.number(partition, "partition");
	assert.number(columnIndex, "columnIndex");
	assert.func(cb, "cb");
	var schema = this.schema[table];
	var column = schema.columns[columnIndex];
	var type = column.type;
	var file = this.dir + "/" + partition + "/" + table + "/" + column.name;
	var fd = fs.open(file, "a+", function(err, fd) {
		if (err) {
			cb(err);
		} else {
			cb(undefined, new columnstream.ReadableColumnStream(fd, type));
		}
	});
};

var tsdb = new TSDB("./tmp");
tsdb.defineTable("sensor01", [{name: "timestamp", type: "uint32"}, {name: "val", type: "float"}, {name: "flag", type: "uint8"}]);

function createSensor01Data() {
	"use strict";
	var i = 0, dones = 0, needed = 10 * 1000 * 1000, begin = Date.now();
	function done() {
		dones++;
		if (dones === needed) {
			console.log("done1: create sensor01 data", [i, dones, Date.now() - begin]);
		}
	}
	tsdb.createWriteStream("sensor01", 20140222, function(err, writable) {
		if (err) {
			throw err;
		}
		function enterWriteLoop() {
			while (i < needed) {
				var continueWriting = writable.write([i, 1.0, 1], done);
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
}
//createSensor01Data();

function readSensor01Data() {
	"use strict";
	var i = 0, begin = Date.now();
	tsdb.createReadStream("sensor01", 20140222, 0, function(err, readable) {
		if (err) {
			throw err;
		}
		readable.on("readable", function() {
			var value;
			while (null !== (value = readable.read())) {
				//console.log("", value);
				i++;
			}
		});
		readable.on("end", function() {
			console.log("done: read sensor01 data", [i, Date.now() - begin]);
		});
	});
}
readSensor01Data();
