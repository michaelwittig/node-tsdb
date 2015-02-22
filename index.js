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
TSDB.prototype.defineTable = function(table, columnDefinitions, cb) { // first column is assumed to be the "time" column with an natural order
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
					cb(undefined, new tablestream.WritableTableStream(writableColumnStreams));
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

exports.TSDB = TSDB;
