var util = require("util");
var stream = require("stream");
var highland = require("highland");

var READ_HIGH_WATER_MARK = 2048;
var WRITE_HIGH_WATER_MARK = 2048;

function readableTableHighlandStream(columnstreams) {
	"use strict";
	if (columnstreams.legnth === 1) {
		return highland(columnstreams[0]).map(function(val) {return [val];});
	} else {
		return highland(columnstreams[0]).zipAll(columnstreams.slice(1));
	}
}

function WritableTableStream(columnstreams) {
	"use strict";
	stream.Writable.call(this, {objectMode: true, highWaterMark: WRITE_HIGH_WATER_MARK});
	this._columnstreams = columnstreams;
}
util.inherits(WritableTableStream, stream.Writable);
WritableTableStream.prototype._write = function(row, _encoding, cb) {
	"use strict";
	var dones = 0, columns = this._columnstreams.length, fired = false;
	function done() {
		dones++;
		if (dones === columns) {
			if (cb && fired === false) {
				cb();
			}
		}
	}
	for (var i = 0; i < columns; i++) {
		this._columnstreams[i].write(row[i], done);
	}
};
WritableTableStream.prototype._writev = function(rows, cb) {
	"use strict";
	var dones = 0, columns = this._columnstreams.length, neededDones = columns * rows.length, fired = false;
	function done() {
		dones++;
		if (dones === neededDones) {
			if (cb && fired === false) {
				fired = true;
				cb();
			}
		}
	}
	for (var j = 0; j < rows.length; j++) {
		for (var i = 0; i < columns; i++) {
			this._columnstreams[i].write(rows[j].chunk[i], done);
		}
	}
};

exports.readableTableHighlandStream = readableTableHighlandStream;
exports.WritableTableStream = WritableTableStream;
