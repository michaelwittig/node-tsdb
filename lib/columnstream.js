var fs = require("fs");
var util = require("util");
var stream = require("stream");

var READ_VALUES_PER_BATCH = 2048;
var READ_HIGH_WATER_MARK = 2048;
var WRITE_HIGH_WATER_MARK = 2048;

function ReadableColumnStream(fd, type) {
	"use strict";
	stream.Readable.call(this, {objectMode: true, highWaterMark: READ_HIGH_WATER_MARK});
	this._fd = fd;
	this._type = type;
	this._position = 0;
	this._buffer = null;
	this._bufferValues = 0;
	this._reading = false;
}
util.inherits(ReadableColumnStream, stream.Readable);
ReadableColumnStream.prototype._read = function(_size) {
	"use strict";
	var self = this;
	function readFromBuffer(availableValues, buffer) {
		for (var i = 0; i < availableValues; i++) {
			var value = self._type.bufferReadFn.call(buffer, i * self._type.length);
			if (!self.push(value)) {
				self._buffer = buffer.slice((i + 1) * self._type.length);
				self._bufferValues = availableValues - (i+1);
				break;
			}
		}
	}
	if (self._bufferValues > 0) {
		var oldBuffer = self._buffer;
		var oldBufferValues = self._bufferValues;
		self._buffer = null;
		self._bufferValues = 0;
		readFromBuffer(oldBufferValues, oldBuffer);
	} else {
		if (self._reading === false) {
			self._reading = true;
			var newBuffer = new Buffer(self._type.length * READ_VALUES_PER_BATCH);
			var bytesPosition = self._position * self._type.length;
			fs.read(self._fd, newBuffer, 0, newBuffer.length, bytesPosition, function(err, bytesRead, buffer) {
				self._reading = false;
				if (err) {
					self.emit("error", err);
				} else {
					if (bytesRead === 0) {
						self.push(null);
					} else {
						var values = bytesRead / self._type.length;
						self._position += values;
						readFromBuffer(values, buffer);
					}
				}
			});
		}
	}
};

function WritableColumnStream(fd, type) {
	"use strict";
	stream.Writable.call(this, {objectMode: true, highWaterMark: WRITE_HIGH_WATER_MARK});
	this._fd = fd;
	this._type = type;
	this._position = 0;
}
util.inherits(WritableColumnStream, stream.Writable);
WritableColumnStream.prototype._write = function(value, _encoding, cb) {
	"use strict";
	var buffer = new Buffer(this._type.length);
	this._type.bufferWriteFn.call(buffer, value, 0, this._type.length);
	var position = this._position;
	this._position += buffer.length;
	fs.write(this._fd, buffer, 0, buffer.length, position, cb);
};
WritableColumnStream.prototype._writev = function(values, cb) {
	"use strict";
	var buffer = new Buffer(this._type.length * values.length);
	for (var i = 0; i < values.length; i++) {
		this._type.bufferWriteFn.call(buffer, values[i].chunk, i * this._type.length);
	}
	var position = this._position;
	this._position += buffer.length;
	fs.write(this._fd, buffer, 0, buffer.length, position, cb);
};

exports.ReadableColumnStream = ReadableColumnStream;
exports.WritableColumnStream = WritableColumnStream;
