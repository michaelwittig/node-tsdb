var TYPES = {
	"uint8": {
		length: 1,
		bufferReadFn: Buffer.prototype.readUInt8,
		bufferWriteFn: Buffer.prototype.writeUInt8
	},
	"int8": {
		length: 1,
		bufferReadFn: Buffer.prototype.readInt8,
		bufferWriteFn: Buffer.prototype.writeInt8
	},
	"uint16": {
		length: 2,
		bufferReadFn: Buffer.prototype.readUInt16LE,
		bufferWriteFn: Buffer.prototype.writeUInt16LE
	},
	"int16": {
		length: 2,
		bufferReadFn: Buffer.prototype.readInt16LE,
		bufferWriteFn: Buffer.prototype.writeInt16LE
	},
	"uint32": {
		length: 4,
		bufferReadFn: Buffer.prototype.readUInt32LE,
		bufferWriteFn: Buffer.prototype.writeUInt32LE
	},
	"int32": {
		length: 4,
		bufferReadFn: Buffer.prototype.readInt32LE,
		bufferWriteFn: Buffer.prototype.writeInt32LE
	},
	"float": {
		length: 4,
		bufferReadFn: Buffer.prototype.readFloatLE,
		bufferWriteFn: Buffer.prototype.writeFloatLE
	},
	"double": {
		length: 8,
		bufferReadFn: Buffer.prototype.readDoubleLE,
		bufferWriteFn: Buffer.prototype.writeDoubleLE
	}
};
module.exports = function(t) {
	"use strict";
	var type = TYPES[t];
	if (type === undefined) {
		throw new Error("unknown type " + t);
	}
	return type;
};
