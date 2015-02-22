var util = require("util");
var stream = require("stream");

var READ_HIGH_WATER_MARK = 2048;
var WRITE_HIGH_WATER_MARK = 2048;

function ReadableTableStream(columnstreams) {
	"use strict";
	stream.Readable.call(this, {objectMode: true, highWaterMark: READ_HIGH_WATER_MARK});
	this._columnstreams = columnstreams;
}
util.inherits(ReadableTableStream, stream.Readable);
ReadableTableStream.prototype._read = function(_size) {
	// TODO this.push(|values]);
};

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

exports.WritableTableStream = WritableTableStream;
