#!/usr/bin/env node
import { createRequire } from "node:module";
var __create = Object.create;
var __getProtoOf = Object.getPrototypeOf;
var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
function __accessProp(key) {
  return this[key];
}
var __toESMCache_node;
var __toESMCache_esm;
var __toESM = (mod, isNodeMode, target) => {
  var canCache = mod != null && typeof mod === "object";
  if (canCache) {
    var cache = isNodeMode ? __toESMCache_node ??= new WeakMap : __toESMCache_esm ??= new WeakMap;
    var cached = cache.get(mod);
    if (cached)
      return cached;
  }
  target = mod != null ? __create(__getProtoOf(mod)) : {};
  const to = isNodeMode || !mod || !mod.__esModule || !__hasOwnProp.call(mod, "default") ? __defProp(target, "default", { value: mod, enumerable: true }) : target;
  if (mod && typeof mod === "object" || typeof mod === "function") {
    for (let key of __getOwnPropNames(mod))
      if (!__hasOwnProp.call(to, key))
        __defProp(to, key, {
          get: __accessProp.bind(mod, key),
          enumerable: true
        });
  }
  if (canCache)
    cache.set(mod, to);
  return to;
};
var __commonJS = (cb, mod) => () => (mod || cb((mod = { exports: {} }).exports, mod), mod.exports);
var __require = /* @__PURE__ */ createRequire(import.meta.url);

// node_modules/ws/lib/constants.js
var require_constants = __commonJS(function(exports, module) {
  var BINARY_TYPES = ["nodebuffer", "arraybuffer", "fragments"];
  var hasBlob = typeof Blob !== "undefined";
  if (hasBlob)
    BINARY_TYPES.push("blob");
  module.exports = {
    BINARY_TYPES,
    CLOSE_TIMEOUT: 30000,
    EMPTY_BUFFER: Buffer.alloc(0),
    GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
    hasBlob,
    kForOnEventAttribute: Symbol("kIsForOnEventAttribute"),
    kListener: Symbol("kListener"),
    kStatusCode: Symbol("status-code"),
    kWebSocket: Symbol("websocket"),
    NOOP: () => {}
  };
});

// node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS(function(exports, module) {
  var { websocketMask, websocketUnmask } = __require("http");
  var { EMPTY_BUFFER } = require_constants();
  var FastBuffer = Buffer[Symbol.species];
  function concat(list, totalLength) {
    if (list.length === 0)
      return EMPTY_BUFFER;
    if (list.length === 1)
      return list[0];
    const target = Buffer.allocUnsafe(totalLength);
    let offset = 0;
    for (let i = 0;i < list.length; i++) {
      const buf = list[i];
      target.set(buf, offset);
      offset += buf.length;
    }
    if (offset < totalLength) {
      return new FastBuffer(target.buffer, target.byteOffset, offset);
    }
    return target;
  }
  function _mask(source, mask, output, offset, length) {
    for (let i = 0;i < length; i++) {
      output[offset + i] = source[i] ^ mask[i & 3];
    }
  }
  function _unmask(buffer, mask) {
    for (let i = 0;i < buffer.length; i++) {
      buffer[i] ^= mask[i & 3];
    }
  }
  function toArrayBuffer(buf) {
    if (buf.length === buf.buffer.byteLength) {
      return buf.buffer;
    }
    return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
  }
  function toBuffer(data) {
    toBuffer.readOnly = true;
    if (Buffer.isBuffer(data))
      return data;
    let buf;
    if (data instanceof ArrayBuffer) {
      buf = new FastBuffer(data);
    } else if (ArrayBuffer.isView(data)) {
      buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
    } else {
      buf = Buffer.from(data);
      toBuffer.readOnly = false;
    }
    return buf;
  }
  module.exports = {
    concat,
    mask: _mask,
    toArrayBuffer,
    toBuffer,
    unmask: _unmask
  };
  if (websocketMask) {
    module.exports.mask = function(source, mask, output, offset, length) {
      if (length < 24)
        _mask(source, mask, output, offset, length);
      else
        websocketMask(source, mask, output, offset, length);
    };
    module.exports.unmask = function(buffer, mask) {
      if (buffer.length < 24)
        _unmask(buffer, mask);
      else
        websocketUnmask(buffer, mask);
    };
  } else if (!process.env.WS_NO_BUFFER_UTIL) {
    try {
      const bufferUtil = (()=>{throw new Error("Cannot require module "+"bufferutil");})();
      module.exports.mask = function(source, mask2, output, offset, length) {
        if (length < 48)
          _mask(source, mask2, output, offset, length);
        else
          bufferUtil.mask(source, mask2, output, offset, length);
      };
      module.exports.unmask = function(buffer, mask) {
        if (buffer.length < 32)
          _unmask(buffer, mask);
        else
          bufferUtil.unmask(buffer, mask);
      };
    } catch (e) {}
  }
});

// node_modules/ws/lib/limiter.js
var require_limiter = __commonJS(function(exports, module) {
  var kDone = Symbol("kDone");
  var kRun = Symbol("kRun");

  class Limiter {
    constructor(concurrency) {
      this[kDone] = () => {
        this.pending--;
        this[kRun]();
      };
      this.concurrency = concurrency || Infinity;
      this.jobs = [];
      this.pending = 0;
    }
    add(job) {
      this.jobs.push(job);
      this[kRun]();
    }
    [kRun]() {
      if (this.pending === this.concurrency)
        return;
      if (this.jobs.length) {
        const job = this.jobs.shift();
        this.pending++;
        job(this[kDone]);
      }
    }
  }
  module.exports = Limiter;
});

// node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS(function(exports, module) {
  var zlib = __require("zlib");
  var bufferUtil = require_buffer_util();
  var Limiter = require_limiter();
  var { kStatusCode } = require_constants();
  var FastBuffer = Buffer[Symbol.species];
  var TRAILER = Buffer.from([0, 0, 255, 255]);
  var kPerMessageDeflate = Symbol("permessage-deflate");
  var kTotalLength = Symbol("total-length");
  var kCallback = Symbol("callback");
  var kBuffers = Symbol("buffers");
  var kError = Symbol("error");
  var zlibLimiter;

  class PerMessageDeflate {
    constructor(options) {
      this._options = options || {};
      this._threshold = this._options.threshold !== undefined ? this._options.threshold : 1024;
      this._maxPayload = this._options.maxPayload | 0;
      this._isServer = !!this._options.isServer;
      this._deflate = null;
      this._inflate = null;
      this.params = null;
      if (!zlibLimiter) {
        const concurrency = this._options.concurrencyLimit !== undefined ? this._options.concurrencyLimit : 10;
        zlibLimiter = new Limiter(concurrency);
      }
    }
    static get extensionName() {
      return "permessage-deflate";
    }
    offer() {
      const params = {};
      if (this._options.serverNoContextTakeover) {
        params.server_no_context_takeover = true;
      }
      if (this._options.clientNoContextTakeover) {
        params.client_no_context_takeover = true;
      }
      if (this._options.serverMaxWindowBits) {
        params.server_max_window_bits = this._options.serverMaxWindowBits;
      }
      if (this._options.clientMaxWindowBits) {
        params.client_max_window_bits = this._options.clientMaxWindowBits;
      } else if (this._options.clientMaxWindowBits == null) {
        params.client_max_window_bits = true;
      }
      return params;
    }
    accept(configurations) {
      configurations = this.normalizeParams(configurations);
      this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
      return this.params;
    }
    cleanup() {
      if (this._inflate) {
        this._inflate.close();
        this._inflate = null;
      }
      if (this._deflate) {
        const callback = this._deflate[kCallback];
        this._deflate.close();
        this._deflate = null;
        if (callback) {
          callback(new Error("The deflate stream was closed while data was being processed"));
        }
      }
    }
    acceptAsServer(offers) {
      const opts = this._options;
      const accepted = offers.find((params) => {
        if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && (typeof params.client_max_window_bits === "number" ? opts.clientMaxWindowBits > params.client_max_window_bits : !params.client_max_window_bits)) {
          return false;
        }
        return true;
      });
      if (!accepted) {
        throw new Error("None of the extension offers can be accepted");
      }
      if (opts.serverNoContextTakeover) {
        accepted.server_no_context_takeover = true;
      }
      if (opts.clientNoContextTakeover) {
        accepted.client_no_context_takeover = true;
      }
      if (typeof opts.serverMaxWindowBits === "number") {
        accepted.server_max_window_bits = opts.serverMaxWindowBits;
      }
      if (typeof opts.clientMaxWindowBits === "number") {
        accepted.client_max_window_bits = opts.clientMaxWindowBits;
      } else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) {
        delete accepted.client_max_window_bits;
      }
      return accepted;
    }
    acceptAsClient(response) {
      const params = response[0];
      if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) {
        throw new Error('Unexpected parameter "client_no_context_takeover"');
      }
      if (!params.client_max_window_bits) {
        if (typeof this._options.clientMaxWindowBits === "number") {
          params.client_max_window_bits = this._options.clientMaxWindowBits;
        }
      } else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) {
        throw new Error('Unexpected or invalid parameter "client_max_window_bits"');
      }
      return params;
    }
    normalizeParams(configurations) {
      configurations.forEach((params) => {
        Object.keys(params).forEach((key) => {
          let value = params[key];
          if (value.length > 1) {
            throw new Error(`Parameter "${key}" must have only a single value`);
          }
          value = value[0];
          if (key === "client_max_window_bits") {
            if (value !== true) {
              const num = +value;
              if (!Number.isInteger(num) || num < 8 || num > 15) {
                throw new TypeError(`Invalid value for parameter "${key}": ${value}`);
              }
              value = num;
            } else if (!this._isServer) {
              throw new TypeError(`Invalid value for parameter "${key}": ${value}`);
            }
          } else if (key === "server_max_window_bits") {
            const num = +value;
            if (!Number.isInteger(num) || num < 8 || num > 15) {
              throw new TypeError(`Invalid value for parameter "${key}": ${value}`);
            }
            value = num;
          } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
            if (value !== true) {
              throw new TypeError(`Invalid value for parameter "${key}": ${value}`);
            }
          } else {
            throw new Error(`Unknown parameter "${key}"`);
          }
          params[key] = value;
        });
      });
      return configurations;
    }
    decompress(data, fin, callback) {
      zlibLimiter.add((done) => {
        this._decompress(data, fin, (err, result) => {
          done();
          callback(err, result);
        });
      });
    }
    compress(data, fin, callback) {
      zlibLimiter.add((done) => {
        this._compress(data, fin, (err, result) => {
          done();
          callback(err, result);
        });
      });
    }
    _decompress(data, fin, callback) {
      const endpoint = this._isServer ? "client" : "server";
      if (!this._inflate) {
        const key = `${endpoint}_max_window_bits`;
        const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
        this._inflate = zlib.createInflateRaw({
          ...this._options.zlibInflateOptions,
          windowBits
        });
        this._inflate[kPerMessageDeflate] = this;
        this._inflate[kTotalLength] = 0;
        this._inflate[kBuffers] = [];
        this._inflate.on("error", inflateOnError);
        this._inflate.on("data", inflateOnData);
      }
      this._inflate[kCallback] = callback;
      this._inflate.write(data);
      if (fin)
        this._inflate.write(TRAILER);
      this._inflate.flush(() => {
        const err = this._inflate[kError];
        if (err) {
          this._inflate.close();
          this._inflate = null;
          callback(err);
          return;
        }
        const data = bufferUtil.concat(this._inflate[kBuffers], this._inflate[kTotalLength]);
        if (this._inflate._readableState.endEmitted) {
          this._inflate.close();
          this._inflate = null;
        } else {
          this._inflate[kTotalLength] = 0;
          this._inflate[kBuffers] = [];
          if (fin && this.params[`${endpoint}_no_context_takeover`]) {
            this._inflate.reset();
          }
        }
        callback(null, data);
      });
    }
    _compress(data, fin, callback) {
      const endpoint = this._isServer ? "server" : "client";
      if (!this._deflate) {
        const key = `${endpoint}_max_window_bits`;
        const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
        this._deflate = zlib.createDeflateRaw({
          ...this._options.zlibDeflateOptions,
          windowBits
        });
        this._deflate[kTotalLength] = 0;
        this._deflate[kBuffers] = [];
        this._deflate.on("data", deflateOnData);
      }
      this._deflate[kCallback] = callback;
      this._deflate.write(data);
      this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
        if (!this._deflate) {
          return;
        }
        let data = bufferUtil.concat(this._deflate[kBuffers], this._deflate[kTotalLength]);
        if (fin) {
          data = new FastBuffer(data.buffer, data.byteOffset, data.length - 4);
        }
        this._deflate[kCallback] = null;
        this._deflate[kTotalLength] = 0;
        this._deflate[kBuffers] = [];
        if (fin && this.params[`${endpoint}_no_context_takeover`]) {
          this._deflate.reset();
        }
        callback(null, data);
      });
    }
  }
  module.exports = PerMessageDeflate;
  function deflateOnData(chunk) {
    this[kBuffers].push(chunk);
    this[kTotalLength] += chunk.length;
  }
  function inflateOnData(chunk) {
    this[kTotalLength] += chunk.length;
    if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
      this[kBuffers].push(chunk);
      return;
    }
    this[kError] = new RangeError("Max payload size exceeded");
    this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
    this[kError][kStatusCode] = 1009;
    this.removeListener("data", inflateOnData);
    this.reset();
  }
  function inflateOnError(err) {
    this[kPerMessageDeflate]._inflate = null;
    if (this[kError]) {
      this[kCallback](this[kError]);
      return;
    }
    err[kStatusCode] = 1007;
    this[kCallback](err);
  }
});

// node_modules/ws/lib/validation.js
var require_validation = __commonJS(function(exports, module) {
  var { isUtf8 } = __require("buffer");
  var { hasBlob } = require_constants();
  var tokenChars = [
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    1,
    1,
    1,
    1,
    1,
    0,
    0,
    1,
    1,
    0,
    1,
    1,
    0,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    0,
    0,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    1,
    0,
    1,
    0,
    1,
    0
  ];
  function isValidStatusCode(code) {
    return code >= 1000 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3000 && code <= 4999;
  }
  function _isValidUTF8(buf) {
    const len = buf.length;
    let i = 0;
    while (i < len) {
      if ((buf[i] & 128) === 0) {
        i++;
      } else if ((buf[i] & 224) === 192) {
        if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192) {
          return false;
        }
        i += 2;
      } else if ((buf[i] & 240) === 224) {
        if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || buf[i] === 237 && (buf[i + 1] & 224) === 160) {
          return false;
        }
        i += 3;
      } else if ((buf[i] & 248) === 240) {
        if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244) {
          return false;
        }
        i += 4;
      } else {
        return false;
      }
    }
    return true;
  }
  function isBlob(value) {
    return hasBlob && typeof value === "object" && typeof value.arrayBuffer === "function" && typeof value.type === "string" && typeof value.stream === "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
  }
  module.exports = {
    isBlob,
    isValidStatusCode,
    isValidUTF8: _isValidUTF8,
    tokenChars
  };
  if (isUtf8) {
    module.exports.isValidUTF8 = function(buf) {
      return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
    };
  } else if (!process.env.WS_NO_UTF_8_VALIDATE) {
    try {
      const isValidUTF8 = (()=>{throw new Error("Cannot require module "+"utf-8-validate");})();
      module.exports.isValidUTF8 = function(buf) {
        return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
      };
    } catch (e) {}
  }
});

// node_modules/ws/lib/receiver.js
var require_receiver = __commonJS(function(exports, module) {
  var { Writable } = __require("stream");
  var PerMessageDeflate = require_permessage_deflate();
  var {
    BINARY_TYPES,
    EMPTY_BUFFER,
    kStatusCode,
    kWebSocket
  } = require_constants();
  var { concat, toArrayBuffer, unmask } = require_buffer_util();
  var { isValidStatusCode, isValidUTF8 } = require_validation();
  var FastBuffer = Buffer[Symbol.species];
  var GET_INFO = 0;
  var GET_PAYLOAD_LENGTH_16 = 1;
  var GET_PAYLOAD_LENGTH_64 = 2;
  var GET_MASK = 3;
  var GET_DATA = 4;
  var INFLATING = 5;
  var DEFER_EVENT = 6;

  class Receiver extends Writable {
    constructor(options = {}) {
      super();
      this._allowSynchronousEvents = options.allowSynchronousEvents !== undefined ? options.allowSynchronousEvents : true;
      this._binaryType = options.binaryType || BINARY_TYPES[0];
      this._extensions = options.extensions || {};
      this._isServer = !!options.isServer;
      this._maxBufferedChunks = options.maxBufferedChunks | 0;
      this._maxFragments = options.maxFragments | 0;
      this._maxPayload = options.maxPayload | 0;
      this._skipUTF8Validation = !!options.skipUTF8Validation;
      this[kWebSocket] = undefined;
      this._bufferedBytes = 0;
      this._buffers = [];
      this._compressed = false;
      this._payloadLength = 0;
      this._mask = undefined;
      this._fragmented = 0;
      this._masked = false;
      this._fin = false;
      this._opcode = 0;
      this._totalPayloadLength = 0;
      this._messageLength = 0;
      this._numFragments = 0;
      this._fragments = [];
      this._errored = false;
      this._loop = false;
      this._state = GET_INFO;
    }
    _write(chunk, encoding, cb) {
      if (this._opcode === 8 && this._state == GET_INFO)
        return cb();
      if (this._maxBufferedChunks > 0 && this._buffers.length >= this._maxBufferedChunks) {
        cb(this.createError(RangeError, "Too many buffered chunks", false, 1008, "WS_ERR_TOO_MANY_BUFFERED_PARTS"));
        return;
      }
      this._bufferedBytes += chunk.length;
      this._buffers.push(chunk);
      this.startLoop(cb);
    }
    consume(n) {
      this._bufferedBytes -= n;
      if (n === this._buffers[0].length)
        return this._buffers.shift();
      if (n < this._buffers[0].length) {
        const buf = this._buffers[0];
        this._buffers[0] = new FastBuffer(buf.buffer, buf.byteOffset + n, buf.length - n);
        return new FastBuffer(buf.buffer, buf.byteOffset, n);
      }
      const dst = Buffer.allocUnsafe(n);
      do {
        const buf = this._buffers[0];
        const offset = dst.length - n;
        if (n >= buf.length) {
          dst.set(this._buffers.shift(), offset);
        } else {
          dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
          this._buffers[0] = new FastBuffer(buf.buffer, buf.byteOffset + n, buf.length - n);
        }
        n -= buf.length;
      } while (n > 0);
      return dst;
    }
    startLoop(cb) {
      this._loop = true;
      do {
        switch (this._state) {
          case GET_INFO:
            this.getInfo(cb);
            break;
          case GET_PAYLOAD_LENGTH_16:
            this.getPayloadLength16(cb);
            break;
          case GET_PAYLOAD_LENGTH_64:
            this.getPayloadLength64(cb);
            break;
          case GET_MASK:
            this.getMask();
            break;
          case GET_DATA:
            this.getData(cb);
            break;
          case INFLATING:
          case DEFER_EVENT:
            this._loop = false;
            return;
        }
      } while (this._loop);
      if (!this._errored)
        cb();
    }
    getInfo(cb) {
      if (this._bufferedBytes < 2) {
        this._loop = false;
        return;
      }
      const buf = this.consume(2);
      if ((buf[0] & 48) !== 0) {
        const error = this.createError(RangeError, "RSV2 and RSV3 must be clear", true, 1002, "WS_ERR_UNEXPECTED_RSV_2_3");
        cb(error);
        return;
      }
      const compressed = (buf[0] & 64) === 64;
      if (compressed && !this._extensions[PerMessageDeflate.extensionName]) {
        const error = this.createError(RangeError, "RSV1 must be clear", true, 1002, "WS_ERR_UNEXPECTED_RSV_1");
        cb(error);
        return;
      }
      this._fin = (buf[0] & 128) === 128;
      this._opcode = buf[0] & 15;
      this._payloadLength = buf[1] & 127;
      if (this._opcode === 0) {
        if (compressed) {
          const error = this.createError(RangeError, "RSV1 must be clear", true, 1002, "WS_ERR_UNEXPECTED_RSV_1");
          cb(error);
          return;
        }
        if (!this._fragmented) {
          const error = this.createError(RangeError, "invalid opcode 0", true, 1002, "WS_ERR_INVALID_OPCODE");
          cb(error);
          return;
        }
        this._opcode = this._fragmented;
      } else if (this._opcode === 1 || this._opcode === 2) {
        if (this._fragmented) {
          const error = this.createError(RangeError, `invalid opcode ${this._opcode}`, true, 1002, "WS_ERR_INVALID_OPCODE");
          cb(error);
          return;
        }
        this._compressed = compressed;
      } else if (this._opcode > 7 && this._opcode < 11) {
        if (!this._fin) {
          const error = this.createError(RangeError, "FIN must be set", true, 1002, "WS_ERR_EXPECTED_FIN");
          cb(error);
          return;
        }
        if (compressed) {
          const error = this.createError(RangeError, "RSV1 must be clear", true, 1002, "WS_ERR_UNEXPECTED_RSV_1");
          cb(error);
          return;
        }
        if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
          const error = this.createError(RangeError, `invalid payload length ${this._payloadLength}`, true, 1002, "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH");
          cb(error);
          return;
        }
      } else {
        const error = this.createError(RangeError, `invalid opcode ${this._opcode}`, true, 1002, "WS_ERR_INVALID_OPCODE");
        cb(error);
        return;
      }
      if (!this._fin && !this._fragmented)
        this._fragmented = this._opcode;
      this._masked = (buf[1] & 128) === 128;
      if (this._isServer) {
        if (!this._masked) {
          const error = this.createError(RangeError, "MASK must be set", true, 1002, "WS_ERR_EXPECTED_MASK");
          cb(error);
          return;
        }
      } else if (this._masked) {
        const error = this.createError(RangeError, "MASK must be clear", true, 1002, "WS_ERR_UNEXPECTED_MASK");
        cb(error);
        return;
      }
      if (this._payloadLength === 126)
        this._state = GET_PAYLOAD_LENGTH_16;
      else if (this._payloadLength === 127)
        this._state = GET_PAYLOAD_LENGTH_64;
      else
        this.haveLength(cb);
    }
    getPayloadLength16(cb) {
      if (this._bufferedBytes < 2) {
        this._loop = false;
        return;
      }
      this._payloadLength = this.consume(2).readUInt16BE(0);
      this.haveLength(cb);
    }
    getPayloadLength64(cb) {
      if (this._bufferedBytes < 8) {
        this._loop = false;
        return;
      }
      const buf = this.consume(8);
      const num = buf.readUInt32BE(0);
      if (num > Math.pow(2, 53 - 32) - 1) {
        const error = this.createError(RangeError, "Unsupported WebSocket frame: payload length > 2^53 - 1", false, 1009, "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH");
        cb(error);
        return;
      }
      this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4);
      this.haveLength(cb);
    }
    haveLength(cb) {
      if (this._payloadLength && this._opcode < 8) {
        this._totalPayloadLength += this._payloadLength;
        if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
          const error = this.createError(RangeError, "Max payload size exceeded", false, 1009, "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH");
          cb(error);
          return;
        }
      }
      if (this._masked)
        this._state = GET_MASK;
      else
        this._state = GET_DATA;
    }
    getMask() {
      if (this._bufferedBytes < 4) {
        this._loop = false;
        return;
      }
      this._mask = this.consume(4);
      this._state = GET_DATA;
    }
    getData(cb) {
      let data = EMPTY_BUFFER;
      if (this._payloadLength) {
        if (this._bufferedBytes < this._payloadLength) {
          this._loop = false;
          return;
        }
        data = this.consume(this._payloadLength);
        if (this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0) {
          unmask(data, this._mask);
        }
      }
      if (this._opcode > 7) {
        this.controlMessage(data, cb);
        return;
      }
      if (this._maxFragments > 0 && ++this._numFragments > this._maxFragments) {
        const error = this.createError(RangeError, "Too many message fragments", false, 1008, "WS_ERR_TOO_MANY_BUFFERED_PARTS");
        cb(error);
        return;
      }
      if (this._compressed) {
        this._state = INFLATING;
        this.decompress(data, cb);
        return;
      }
      if (data.length) {
        this._messageLength = this._totalPayloadLength;
        this._fragments.push(data);
      }
      this.dataMessage(cb);
    }
    decompress(data, cb) {
      const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
      perMessageDeflate.decompress(data, this._fin, (err, buf) => {
        if (err)
          return cb(err);
        if (buf.length) {
          this._messageLength += buf.length;
          if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
            const error = this.createError(RangeError, "Max payload size exceeded", false, 1009, "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH");
            cb(error);
            return;
          }
          this._fragments.push(buf);
        }
        this.dataMessage(cb);
        if (this._state === GET_INFO)
          this.startLoop(cb);
      });
    }
    dataMessage(cb) {
      if (!this._fin) {
        this._state = GET_INFO;
        return;
      }
      const messageLength = this._messageLength;
      const fragments = this._fragments;
      this._totalPayloadLength = 0;
      this._messageLength = 0;
      this._fragmented = 0;
      this._numFragments = 0;
      this._fragments = [];
      if (this._opcode === 2) {
        let data;
        if (this._binaryType === "nodebuffer") {
          data = concat(fragments, messageLength);
        } else if (this._binaryType === "arraybuffer") {
          data = toArrayBuffer(concat(fragments, messageLength));
        } else if (this._binaryType === "blob") {
          data = new Blob(fragments);
        } else {
          data = fragments;
        }
        if (this._allowSynchronousEvents) {
          this.emit("message", data, true);
          this._state = GET_INFO;
        } else {
          this._state = DEFER_EVENT;
          setImmediate(() => {
            this.emit("message", data, true);
            this._state = GET_INFO;
            this.startLoop(cb);
          });
        }
      } else {
        const buf = concat(fragments, messageLength);
        if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
          const error = this.createError(Error, "invalid UTF-8 sequence", true, 1007, "WS_ERR_INVALID_UTF8");
          cb(error);
          return;
        }
        if (this._state === INFLATING || this._allowSynchronousEvents) {
          this.emit("message", buf, false);
          this._state = GET_INFO;
        } else {
          this._state = DEFER_EVENT;
          setImmediate(() => {
            this.emit("message", buf, false);
            this._state = GET_INFO;
            this.startLoop(cb);
          });
        }
      }
    }
    controlMessage(data, cb) {
      if (this._opcode === 8) {
        if (data.length === 0) {
          this._loop = false;
          this.emit("conclude", 1005, EMPTY_BUFFER);
          this.end();
        } else {
          const code = data.readUInt16BE(0);
          if (!isValidStatusCode(code)) {
            const error = this.createError(RangeError, `invalid status code ${code}`, true, 1002, "WS_ERR_INVALID_CLOSE_CODE");
            cb(error);
            return;
          }
          const buf = new FastBuffer(data.buffer, data.byteOffset + 2, data.length - 2);
          if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
            const error = this.createError(Error, "invalid UTF-8 sequence", true, 1007, "WS_ERR_INVALID_UTF8");
            cb(error);
            return;
          }
          this._loop = false;
          this.emit("conclude", code, buf);
          this.end();
        }
        this._state = GET_INFO;
        return;
      }
      if (this._allowSynchronousEvents) {
        this.emit(this._opcode === 9 ? "ping" : "pong", data);
        this._state = GET_INFO;
      } else {
        this._state = DEFER_EVENT;
        setImmediate(() => {
          this.emit(this._opcode === 9 ? "ping" : "pong", data);
          this._state = GET_INFO;
          this.startLoop(cb);
        });
      }
    }
    createError(ErrorCtor, message, prefix, statusCode, errorCode) {
      this._loop = false;
      this._errored = true;
      const err = new ErrorCtor(prefix ? `Invalid WebSocket frame: ${message}` : message);
      Error.captureStackTrace(err, this.createError);
      err.code = errorCode;
      err[kStatusCode] = statusCode;
      return err;
    }
  }
  module.exports = Receiver;
});

// node_modules/ws/lib/sender.js
var require_sender = __commonJS(function(exports, module) {
  var { Duplex } = __require("stream");
  var { randomFillSync } = __require("crypto");
  var {
    types: { isUint8Array }
  } = __require("util");
  var PerMessageDeflate = require_permessage_deflate();
  var { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants();
  var { isBlob, isValidStatusCode } = require_validation();
  var { mask: applyMask, toBuffer } = require_buffer_util();
  var kByteLength = Symbol("kByteLength");
  var maskBuffer = Buffer.alloc(4);
  var RANDOM_POOL_SIZE = 8 * 1024;
  var randomPool;
  var randomPoolPointer = RANDOM_POOL_SIZE;
  var DEFAULT = 0;
  var DEFLATING = 1;
  var GET_BLOB_DATA = 2;

  class Sender {
    constructor(socket, extensions, generateMask) {
      this._extensions = extensions || {};
      if (generateMask) {
        this._generateMask = generateMask;
        this._maskBuffer = Buffer.alloc(4);
      }
      this._socket = socket;
      this._firstFragment = true;
      this._compress = false;
      this._bufferedBytes = 0;
      this._queue = [];
      this._state = DEFAULT;
      this.onerror = NOOP;
      this[kWebSocket] = undefined;
    }
    static frame(data, options) {
      let mask;
      let merge = false;
      let offset = 2;
      let skipMasking = false;
      if (options.mask) {
        mask = options.maskBuffer || maskBuffer;
        if (options.generateMask) {
          options.generateMask(mask);
        } else {
          if (randomPoolPointer === RANDOM_POOL_SIZE) {
            if (randomPool === undefined) {
              randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
            }
            randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
            randomPoolPointer = 0;
          }
          mask[0] = randomPool[randomPoolPointer++];
          mask[1] = randomPool[randomPoolPointer++];
          mask[2] = randomPool[randomPoolPointer++];
          mask[3] = randomPool[randomPoolPointer++];
        }
        skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
        offset = 6;
      }
      let dataLength;
      if (typeof data === "string") {
        if ((!options.mask || skipMasking) && options[kByteLength] !== undefined) {
          dataLength = options[kByteLength];
        } else {
          data = Buffer.from(data);
          dataLength = data.length;
        }
      } else {
        dataLength = data.length;
        merge = options.mask && options.readOnly && !skipMasking;
      }
      let payloadLength = dataLength;
      if (dataLength >= 65536) {
        offset += 8;
        payloadLength = 127;
      } else if (dataLength > 125) {
        offset += 2;
        payloadLength = 126;
      }
      const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);
      target[0] = options.fin ? options.opcode | 128 : options.opcode;
      if (options.rsv1)
        target[0] |= 64;
      target[1] = payloadLength;
      if (payloadLength === 126) {
        target.writeUInt16BE(dataLength, 2);
      } else if (payloadLength === 127) {
        target[2] = target[3] = 0;
        target.writeUIntBE(dataLength, 4, 6);
      }
      if (!options.mask) {
        return dataLength ? [target, data] : [target];
      }
      target[1] |= 128;
      target[offset - 4] = mask[0];
      target[offset - 3] = mask[1];
      target[offset - 2] = mask[2];
      target[offset - 1] = mask[3];
      if (!dataLength)
        return [target];
      if (skipMasking)
        return [target, data];
      if (merge) {
        applyMask(data, mask, target, offset, dataLength);
        return [target];
      }
      applyMask(data, mask, data, 0, dataLength);
      return [target, data];
    }
    close(code, data, mask, cb) {
      let buf;
      if (code === undefined) {
        buf = EMPTY_BUFFER;
      } else if (typeof code !== "number" || !isValidStatusCode(code)) {
        throw new TypeError("First argument must be a valid error code number");
      } else if (data === undefined || !data.length) {
        buf = Buffer.allocUnsafe(2);
        buf.writeUInt16BE(code, 0);
      } else {
        const length = Buffer.byteLength(data);
        if (length > 123) {
          throw new RangeError("The message must not be greater than 123 bytes");
        }
        buf = Buffer.allocUnsafe(2 + length);
        buf.writeUInt16BE(code, 0);
        if (typeof data === "string") {
          buf.write(data, 2);
        } else if (isUint8Array(data)) {
          buf.set(data, 2);
        } else {
          throw new TypeError("Second argument must be a string or a Uint8Array");
        }
      }
      const options = {
        [kByteLength]: buf.length,
        fin: true,
        generateMask: this._generateMask,
        mask,
        maskBuffer: this._maskBuffer,
        opcode: 8,
        readOnly: false,
        rsv1: false
      };
      if (this._state !== DEFAULT) {
        this.enqueue([this.dispatch, buf, false, options, cb]);
      } else {
        this.sendFrame(Sender.frame(buf, options), cb);
      }
    }
    ping(data, mask, cb) {
      let byteLength;
      let readOnly;
      if (typeof data === "string") {
        byteLength = Buffer.byteLength(data);
        readOnly = false;
      } else if (isBlob(data)) {
        byteLength = data.size;
        readOnly = false;
      } else {
        data = toBuffer(data);
        byteLength = data.length;
        readOnly = toBuffer.readOnly;
      }
      if (byteLength > 125) {
        throw new RangeError("The data size must not be greater than 125 bytes");
      }
      const options = {
        [kByteLength]: byteLength,
        fin: true,
        generateMask: this._generateMask,
        mask,
        maskBuffer: this._maskBuffer,
        opcode: 9,
        readOnly,
        rsv1: false
      };
      if (isBlob(data)) {
        if (this._state !== DEFAULT) {
          this.enqueue([this.getBlobData, data, false, options, cb]);
        } else {
          this.getBlobData(data, false, options, cb);
        }
      } else if (this._state !== DEFAULT) {
        this.enqueue([this.dispatch, data, false, options, cb]);
      } else {
        this.sendFrame(Sender.frame(data, options), cb);
      }
    }
    pong(data, mask, cb) {
      let byteLength;
      let readOnly;
      if (typeof data === "string") {
        byteLength = Buffer.byteLength(data);
        readOnly = false;
      } else if (isBlob(data)) {
        byteLength = data.size;
        readOnly = false;
      } else {
        data = toBuffer(data);
        byteLength = data.length;
        readOnly = toBuffer.readOnly;
      }
      if (byteLength > 125) {
        throw new RangeError("The data size must not be greater than 125 bytes");
      }
      const options = {
        [kByteLength]: byteLength,
        fin: true,
        generateMask: this._generateMask,
        mask,
        maskBuffer: this._maskBuffer,
        opcode: 10,
        readOnly,
        rsv1: false
      };
      if (isBlob(data)) {
        if (this._state !== DEFAULT) {
          this.enqueue([this.getBlobData, data, false, options, cb]);
        } else {
          this.getBlobData(data, false, options, cb);
        }
      } else if (this._state !== DEFAULT) {
        this.enqueue([this.dispatch, data, false, options, cb]);
      } else {
        this.sendFrame(Sender.frame(data, options), cb);
      }
    }
    send(data, options, cb) {
      const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
      let opcode = options.binary ? 2 : 1;
      let rsv1 = options.compress;
      let byteLength;
      let readOnly;
      if (typeof data === "string") {
        byteLength = Buffer.byteLength(data);
        readOnly = false;
      } else if (isBlob(data)) {
        byteLength = data.size;
        readOnly = false;
      } else {
        data = toBuffer(data);
        byteLength = data.length;
        readOnly = toBuffer.readOnly;
      }
      if (this._firstFragment) {
        this._firstFragment = false;
        if (rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"]) {
          rsv1 = byteLength >= perMessageDeflate._threshold;
        }
        this._compress = rsv1;
      } else {
        rsv1 = false;
        opcode = 0;
      }
      if (options.fin)
        this._firstFragment = true;
      const opts = {
        [kByteLength]: byteLength,
        fin: options.fin,
        generateMask: this._generateMask,
        mask: options.mask,
        maskBuffer: this._maskBuffer,
        opcode,
        readOnly,
        rsv1
      };
      if (isBlob(data)) {
        if (this._state !== DEFAULT) {
          this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
        } else {
          this.getBlobData(data, this._compress, opts, cb);
        }
      } else if (this._state !== DEFAULT) {
        this.enqueue([this.dispatch, data, this._compress, opts, cb]);
      } else {
        this.dispatch(data, this._compress, opts, cb);
      }
    }
    getBlobData(blob, compress, options, cb) {
      this._bufferedBytes += options[kByteLength];
      this._state = GET_BLOB_DATA;
      blob.arrayBuffer().then((arrayBuffer) => {
        if (this._socket.destroyed) {
          const err = new Error("The socket was closed while the blob was being read");
          process.nextTick(callCallbacks, this, err, cb);
          return;
        }
        this._bufferedBytes -= options[kByteLength];
        const data = toBuffer(arrayBuffer);
        if (!compress) {
          this._state = DEFAULT;
          this.sendFrame(Sender.frame(data, options), cb);
          this.dequeue();
        } else {
          this.dispatch(data, compress, options, cb);
        }
      }).catch((err) => {
        process.nextTick(onError, this, err, cb);
      });
    }
    dispatch(data, compress, options, cb) {
      if (!compress) {
        this.sendFrame(Sender.frame(data, options), cb);
        return;
      }
      const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
      this._bufferedBytes += options[kByteLength];
      this._state = DEFLATING;
      perMessageDeflate.compress(data, options.fin, (_, buf) => {
        if (this._socket.destroyed) {
          const err = new Error("The socket was closed while data was being compressed");
          callCallbacks(this, err, cb);
          return;
        }
        this._bufferedBytes -= options[kByteLength];
        this._state = DEFAULT;
        options.readOnly = false;
        this.sendFrame(Sender.frame(buf, options), cb);
        this.dequeue();
      });
    }
    dequeue() {
      while (this._state === DEFAULT && this._queue.length) {
        const params = this._queue.shift();
        this._bufferedBytes -= params[3][kByteLength];
        Reflect.apply(params[0], this, params.slice(1));
      }
    }
    enqueue(params) {
      this._bufferedBytes += params[3][kByteLength];
      this._queue.push(params);
    }
    sendFrame(list, cb) {
      if (list.length === 2) {
        this._socket.cork();
        this._socket.write(list[0]);
        this._socket.write(list[1], cb);
        this._socket.uncork();
      } else {
        this._socket.write(list[0], cb);
      }
    }
  }
  module.exports = Sender;
  function callCallbacks(sender, err, cb) {
    if (typeof cb === "function")
      cb(err);
    for (let i = 0;i < sender._queue.length; i++) {
      const params = sender._queue[i];
      const callback = params[params.length - 1];
      if (typeof callback === "function")
        callback(err);
    }
  }
  function onError(sender, err, cb) {
    callCallbacks(sender, err, cb);
    sender.onerror(err);
  }
});

// node_modules/ws/lib/event-target.js
var require_event_target = __commonJS(function(exports, module) {
  var { kForOnEventAttribute, kListener } = require_constants();
  var kCode = Symbol("kCode");
  var kData = Symbol("kData");
  var kError = Symbol("kError");
  var kMessage = Symbol("kMessage");
  var kReason = Symbol("kReason");
  var kTarget = Symbol("kTarget");
  var kType = Symbol("kType");
  var kWasClean = Symbol("kWasClean");

  class Event {
    constructor(type) {
      this[kTarget] = null;
      this[kType] = type;
    }
    get target() {
      return this[kTarget];
    }
    get type() {
      return this[kType];
    }
  }
  Object.defineProperty(Event.prototype, "target", { enumerable: true });
  Object.defineProperty(Event.prototype, "type", { enumerable: true });

  class CloseEvent extends Event {
    constructor(type, options = {}) {
      super(type);
      this[kCode] = options.code === undefined ? 0 : options.code;
      this[kReason] = options.reason === undefined ? "" : options.reason;
      this[kWasClean] = options.wasClean === undefined ? false : options.wasClean;
    }
    get code() {
      return this[kCode];
    }
    get reason() {
      return this[kReason];
    }
    get wasClean() {
      return this[kWasClean];
    }
  }
  Object.defineProperty(CloseEvent.prototype, "code", { enumerable: true });
  Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: true });
  Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: true });

  class ErrorEvent extends Event {
    constructor(type, options = {}) {
      super(type);
      this[kError] = options.error === undefined ? null : options.error;
      this[kMessage] = options.message === undefined ? "" : options.message;
    }
    get error() {
      return this[kError];
    }
    get message() {
      return this[kMessage];
    }
  }
  Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: true });
  Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: true });

  class MessageEvent extends Event {
    constructor(type, options = {}) {
      super(type);
      this[kData] = options.data === undefined ? null : options.data;
    }
    get data() {
      return this[kData];
    }
  }
  Object.defineProperty(MessageEvent.prototype, "data", { enumerable: true });
  var EventTarget = {
    addEventListener(type, handler, options = {}) {
      for (const listener of this.listeners(type)) {
        if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute]) {
          return;
        }
      }
      let wrapper;
      if (type === "message") {
        wrapper = function onMessage(data, isBinary) {
          const event = new MessageEvent("message", {
            data: isBinary ? data : data.toString()
          });
          event[kTarget] = this;
          callListener(handler, this, event);
        };
      } else if (type === "close") {
        wrapper = function onClose(code, message) {
          const event = new CloseEvent("close", {
            code,
            reason: message.toString(),
            wasClean: this._closeFrameReceived && this._closeFrameSent
          });
          event[kTarget] = this;
          callListener(handler, this, event);
        };
      } else if (type === "error") {
        wrapper = function onError(error) {
          const event = new ErrorEvent("error", {
            error,
            message: error.message
          });
          event[kTarget] = this;
          callListener(handler, this, event);
        };
      } else if (type === "open") {
        wrapper = function onOpen() {
          const event = new Event("open");
          event[kTarget] = this;
          callListener(handler, this, event);
        };
      } else {
        return;
      }
      wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
      wrapper[kListener] = handler;
      if (options.once) {
        this.once(type, wrapper);
      } else {
        this.on(type, wrapper);
      }
    },
    removeEventListener(type, handler) {
      for (const listener of this.listeners(type)) {
        if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
          this.removeListener(type, listener);
          break;
        }
      }
    }
  };
  module.exports = {
    CloseEvent,
    ErrorEvent,
    Event,
    EventTarget,
    MessageEvent
  };
  function callListener(listener, thisArg, event) {
    if (typeof listener === "object" && listener.handleEvent) {
      listener.handleEvent.call(listener, event);
    } else {
      listener.call(thisArg, event);
    }
  }
});

// node_modules/ws/lib/extension.js
var require_extension = __commonJS(function(exports, module) {
  var { tokenChars } = require_validation();
  function push(dest, name, elem) {
    if (dest[name] === undefined)
      dest[name] = [elem];
    else
      dest[name].push(elem);
  }
  function parse(header) {
    const offers = Object.create(null);
    let params = Object.create(null);
    let mustUnescape = false;
    let isEscaping = false;
    let inQuotes = false;
    let extensionName;
    let paramName;
    let start = -1;
    let code = -1;
    let end = -1;
    let i = 0;
    for (;i < header.length; i++) {
      code = header.charCodeAt(i);
      if (extensionName === undefined) {
        if (end === -1 && tokenChars[code] === 1) {
          if (start === -1)
            start = i;
        } else if (i !== 0 && (code === 32 || code === 9)) {
          if (end === -1 && start !== -1)
            end = i;
        } else if (code === 59 || code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end === -1)
            end = i;
          const name = header.slice(start, end);
          if (code === 44) {
            push(offers, name, params);
            params = Object.create(null);
          } else {
            extensionName = name;
          }
          start = end = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      } else if (paramName === undefined) {
        if (end === -1 && tokenChars[code] === 1) {
          if (start === -1)
            start = i;
        } else if (code === 32 || code === 9) {
          if (end === -1 && start !== -1)
            end = i;
        } else if (code === 59 || code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end === -1)
            end = i;
          push(params, header.slice(start, end), true);
          if (code === 44) {
            push(offers, extensionName, params);
            params = Object.create(null);
            extensionName = undefined;
          }
          start = end = -1;
        } else if (code === 61 && start !== -1 && end === -1) {
          paramName = header.slice(start, i);
          start = end = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      } else {
        if (isEscaping) {
          if (tokenChars[code] !== 1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (start === -1)
            start = i;
          else if (!mustUnescape)
            mustUnescape = true;
          isEscaping = false;
        } else if (inQuotes) {
          if (tokenChars[code] === 1) {
            if (start === -1)
              start = i;
          } else if (code === 34 && start !== -1) {
            inQuotes = false;
            end = i;
          } else if (code === 92) {
            isEscaping = true;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else if (code === 34 && header.charCodeAt(i - 1) === 61) {
          inQuotes = true;
        } else if (end === -1 && tokenChars[code] === 1) {
          if (start === -1)
            start = i;
        } else if (start !== -1 && (code === 32 || code === 9)) {
          if (end === -1)
            end = i;
        } else if (code === 59 || code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end === -1)
            end = i;
          let value = header.slice(start, end);
          if (mustUnescape) {
            value = value.replace(/\\/g, "");
            mustUnescape = false;
          }
          push(params, paramName, value);
          if (code === 44) {
            push(offers, extensionName, params);
            params = Object.create(null);
            extensionName = undefined;
          }
          paramName = undefined;
          start = end = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      }
    }
    if (start === -1 || inQuotes || code === 32 || code === 9) {
      throw new SyntaxError("Unexpected end of input");
    }
    if (end === -1)
      end = i;
    const token = header.slice(start, end);
    if (extensionName === undefined) {
      push(offers, token, params);
    } else {
      if (paramName === undefined) {
        push(params, token, true);
      } else if (mustUnescape) {
        push(params, paramName, token.replace(/\\/g, ""));
      } else {
        push(params, paramName, token);
      }
      push(offers, extensionName, params);
    }
    return offers;
  }
  function format(extensions) {
    return Object.keys(extensions).map((extension) => {
      let configurations = extensions[extension];
      if (!Array.isArray(configurations))
        configurations = [configurations];
      return configurations.map((params) => {
        return [extension].concat(Object.keys(params).map((k) => {
          let values = params[k];
          if (!Array.isArray(values))
            values = [values];
          return values.map((v) => v === true ? k : `${k}=${v}`).join("; ");
        })).join("; ");
      }).join(", ");
    }).join(", ");
  }
  module.exports = { format, parse };
});

// node_modules/ws/lib/websocket.js
var require_websocket = __commonJS(function(exports, module) {
  var EventEmitter = __require("events");
  var https = __require("https");
  var http = __require("http");
  var net = __require("net");
  var tls = __require("tls");
  var { randomBytes, createHash } = __require("crypto");
  var { Duplex, Readable } = __require("stream");
  var { URL: URL2 } = __require("url");
  var PerMessageDeflate = require_permessage_deflate();
  var Receiver = require_receiver();
  var Sender = require_sender();
  var { isBlob } = require_validation();
  var {
    BINARY_TYPES,
    CLOSE_TIMEOUT,
    EMPTY_BUFFER,
    GUID,
    kForOnEventAttribute,
    kListener,
    kStatusCode,
    kWebSocket,
    NOOP
  } = require_constants();
  var {
    EventTarget: { addEventListener, removeEventListener }
  } = require_event_target();
  var { format, parse } = require_extension();
  var { toBuffer } = require_buffer_util();
  var kAborted = Symbol("kAborted");
  var protocolVersions = [8, 13];
  var readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
  var subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;

  class WebSocket extends EventEmitter {
    constructor(address, protocols, options) {
      super();
      this._binaryType = BINARY_TYPES[0];
      this._closeCode = 1006;
      this._closeFrameReceived = false;
      this._closeFrameSent = false;
      this._closeMessage = EMPTY_BUFFER;
      this._closeTimer = null;
      this._errorEmitted = false;
      this._extensions = {};
      this._paused = false;
      this._protocol = "";
      this._readyState = WebSocket.CONNECTING;
      this._receiver = null;
      this._sender = null;
      this._socket = null;
      if (address !== null) {
        this._bufferedAmount = 0;
        this._isServer = false;
        this._redirects = 0;
        if (protocols === undefined) {
          if (!options || options.protocols === undefined) {
            protocols = [];
          } else if (Array.isArray(options.protocols)) {
            protocols = options.protocols;
          } else {
            protocols = [options.protocols];
          }
        } else if (!Array.isArray(protocols)) {
          if (typeof protocols === "object" && protocols !== null) {
            options = protocols;
            if (options.protocols === undefined) {
              protocols = [];
            } else if (Array.isArray(options.protocols)) {
              protocols = options.protocols;
            } else {
              protocols = [options.protocols];
            }
          } else {
            protocols = [protocols];
          }
        }
        initAsClient(this, address, protocols, options);
      } else {
        this._autoPong = options.autoPong;
        this._closeTimeout = options.closeTimeout;
        this._isServer = true;
      }
    }
    get binaryType() {
      return this._binaryType;
    }
    set binaryType(type) {
      if (!BINARY_TYPES.includes(type))
        return;
      this._binaryType = type;
      if (this._receiver)
        this._receiver._binaryType = type;
    }
    get bufferedAmount() {
      if (!this._socket)
        return this._bufferedAmount;
      return this._socket._writableState.length + this._sender._bufferedBytes;
    }
    get extensions() {
      return Object.keys(this._extensions).join();
    }
    get isPaused() {
      return this._paused;
    }
    get onclose() {
      return null;
    }
    get onerror() {
      return null;
    }
    get onopen() {
      return null;
    }
    get onmessage() {
      return null;
    }
    get protocol() {
      return this._protocol;
    }
    get readyState() {
      return this._readyState;
    }
    get url() {
      return this._url;
    }
    setSocket(socket, head, options) {
      const receiver = new Receiver({
        allowSynchronousEvents: options.allowSynchronousEvents,
        binaryType: this.binaryType,
        extensions: this._extensions,
        isServer: this._isServer,
        maxBufferedChunks: options.maxBufferedChunks,
        maxFragments: options.maxFragments,
        maxPayload: options.maxPayload,
        skipUTF8Validation: options.skipUTF8Validation
      });
      const sender = new Sender(socket, this._extensions, options.generateMask);
      this._receiver = receiver;
      this._sender = sender;
      this._socket = socket;
      receiver[kWebSocket] = this;
      sender[kWebSocket] = this;
      socket[kWebSocket] = this;
      receiver.on("conclude", receiverOnConclude);
      receiver.on("drain", receiverOnDrain);
      receiver.on("error", receiverOnError);
      receiver.on("message", receiverOnMessage);
      receiver.on("ping", receiverOnPing);
      receiver.on("pong", receiverOnPong);
      sender.onerror = senderOnError;
      if (socket.setTimeout)
        socket.setTimeout(0);
      if (socket.setNoDelay)
        socket.setNoDelay();
      if (head.length > 0)
        socket.unshift(head);
      socket.on("close", socketOnClose);
      socket.on("data", socketOnData);
      socket.on("end", socketOnEnd);
      socket.on("error", socketOnError);
      this._readyState = WebSocket.OPEN;
      this.emit("open");
    }
    emitClose() {
      if (!this._socket) {
        this._readyState = WebSocket.CLOSED;
        this.emit("close", this._closeCode, this._closeMessage);
        return;
      }
      if (this._extensions[PerMessageDeflate.extensionName]) {
        this._extensions[PerMessageDeflate.extensionName].cleanup();
      }
      this._receiver.removeAllListeners();
      this._readyState = WebSocket.CLOSED;
      this.emit("close", this._closeCode, this._closeMessage);
    }
    close(code, data) {
      if (this.readyState === WebSocket.CLOSED)
        return;
      if (this.readyState === WebSocket.CONNECTING) {
        const msg = "WebSocket was closed before the connection was established";
        abortHandshake(this, this._req, msg);
        return;
      }
      if (this.readyState === WebSocket.CLOSING) {
        if (this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted)) {
          this._socket.end();
        }
        return;
      }
      this._sender.close(code, data, !this._isServer, (err) => {
        if (err)
          return;
        this._closeFrameSent = true;
        if (this._closeFrameReceived || this._receiver._writableState.errorEmitted) {
          this._socket.end();
        }
      });
      this._readyState = WebSocket.CLOSING;
      setCloseTimer(this);
    }
    pause() {
      if (this.readyState === WebSocket.CONNECTING || this.readyState === WebSocket.CLOSED) {
        return;
      }
      this._paused = true;
      this._socket.pause();
    }
    ping(data, mask, cb) {
      if (this.readyState === WebSocket.CONNECTING) {
        throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
      }
      if (typeof data === "function") {
        cb = data;
        data = mask = undefined;
      } else if (typeof mask === "function") {
        cb = mask;
        mask = undefined;
      }
      if (typeof data === "number")
        data = data.toString();
      if (this.readyState !== WebSocket.OPEN) {
        sendAfterClose(this, data, cb);
        return;
      }
      if (mask === undefined)
        mask = !this._isServer;
      this._sender.ping(data || EMPTY_BUFFER, mask, cb);
    }
    pong(data, mask, cb) {
      if (this.readyState === WebSocket.CONNECTING) {
        throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
      }
      if (typeof data === "function") {
        cb = data;
        data = mask = undefined;
      } else if (typeof mask === "function") {
        cb = mask;
        mask = undefined;
      }
      if (typeof data === "number")
        data = data.toString();
      if (this.readyState !== WebSocket.OPEN) {
        sendAfterClose(this, data, cb);
        return;
      }
      if (mask === undefined)
        mask = !this._isServer;
      this._sender.pong(data || EMPTY_BUFFER, mask, cb);
    }
    resume() {
      if (this.readyState === WebSocket.CONNECTING || this.readyState === WebSocket.CLOSED) {
        return;
      }
      this._paused = false;
      if (!this._receiver._writableState.needDrain)
        this._socket.resume();
    }
    send(data, options, cb) {
      if (this.readyState === WebSocket.CONNECTING) {
        throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
      }
      if (typeof options === "function") {
        cb = options;
        options = {};
      }
      if (typeof data === "number")
        data = data.toString();
      if (this.readyState !== WebSocket.OPEN) {
        sendAfterClose(this, data, cb);
        return;
      }
      const opts = {
        binary: typeof data !== "string",
        mask: !this._isServer,
        compress: true,
        fin: true,
        ...options
      };
      if (!this._extensions[PerMessageDeflate.extensionName]) {
        opts.compress = false;
      }
      this._sender.send(data || EMPTY_BUFFER, opts, cb);
    }
    terminate() {
      if (this.readyState === WebSocket.CLOSED)
        return;
      if (this.readyState === WebSocket.CONNECTING) {
        const msg = "WebSocket was closed before the connection was established";
        abortHandshake(this, this._req, msg);
        return;
      }
      if (this._socket) {
        this._readyState = WebSocket.CLOSING;
        this._socket.destroy();
      }
    }
  }
  Object.defineProperty(WebSocket, "CONNECTING", {
    enumerable: true,
    value: readyStates.indexOf("CONNECTING")
  });
  Object.defineProperty(WebSocket.prototype, "CONNECTING", {
    enumerable: true,
    value: readyStates.indexOf("CONNECTING")
  });
  Object.defineProperty(WebSocket, "OPEN", {
    enumerable: true,
    value: readyStates.indexOf("OPEN")
  });
  Object.defineProperty(WebSocket.prototype, "OPEN", {
    enumerable: true,
    value: readyStates.indexOf("OPEN")
  });
  Object.defineProperty(WebSocket, "CLOSING", {
    enumerable: true,
    value: readyStates.indexOf("CLOSING")
  });
  Object.defineProperty(WebSocket.prototype, "CLOSING", {
    enumerable: true,
    value: readyStates.indexOf("CLOSING")
  });
  Object.defineProperty(WebSocket, "CLOSED", {
    enumerable: true,
    value: readyStates.indexOf("CLOSED")
  });
  Object.defineProperty(WebSocket.prototype, "CLOSED", {
    enumerable: true,
    value: readyStates.indexOf("CLOSED")
  });
  [
    "binaryType",
    "bufferedAmount",
    "extensions",
    "isPaused",
    "protocol",
    "readyState",
    "url"
  ].forEach((property) => {
    Object.defineProperty(WebSocket.prototype, property, { enumerable: true });
  });
  ["open", "error", "close", "message"].forEach((method) => {
    Object.defineProperty(WebSocket.prototype, `on${method}`, {
      enumerable: true,
      get() {
        for (const listener of this.listeners(method)) {
          if (listener[kForOnEventAttribute])
            return listener[kListener];
        }
        return null;
      },
      set(handler) {
        for (const listener of this.listeners(method)) {
          if (listener[kForOnEventAttribute]) {
            this.removeListener(method, listener);
            break;
          }
        }
        if (typeof handler !== "function")
          return;
        this.addEventListener(method, handler, {
          [kForOnEventAttribute]: true
        });
      }
    });
  });
  WebSocket.prototype.addEventListener = addEventListener;
  WebSocket.prototype.removeEventListener = removeEventListener;
  module.exports = WebSocket;
  function initAsClient(websocket, address, protocols, options) {
    const opts = {
      allowSynchronousEvents: true,
      autoPong: true,
      closeTimeout: CLOSE_TIMEOUT,
      protocolVersion: protocolVersions[1],
      maxBufferedChunks: 256 * 1024,
      maxFragments: 16 * 1024,
      maxPayload: 100 * 1024 * 1024,
      skipUTF8Validation: false,
      perMessageDeflate: true,
      followRedirects: false,
      maxRedirects: 10,
      ...options,
      socketPath: undefined,
      hostname: undefined,
      protocol: undefined,
      protocols: undefined,
      timeout: undefined,
      method: "GET",
      host: undefined,
      path: undefined,
      port: undefined
    };
    websocket._autoPong = opts.autoPong;
    websocket._closeTimeout = opts.closeTimeout;
    if (!protocolVersions.includes(opts.protocolVersion)) {
      throw new RangeError(`Unsupported protocol version: ${opts.protocolVersion} ` + `(supported versions: ${protocolVersions.join(", ")})`);
    }
    let parsedUrl;
    if (address instanceof URL2) {
      parsedUrl = address;
    } else {
      try {
        parsedUrl = new URL2(address);
      } catch {
        throw new SyntaxError(`Invalid URL: ${address}`);
      }
    }
    if (parsedUrl.protocol === "http:") {
      parsedUrl.protocol = "ws:";
    } else if (parsedUrl.protocol === "https:") {
      parsedUrl.protocol = "wss:";
    }
    websocket._url = parsedUrl.href;
    const isSecure = parsedUrl.protocol === "wss:";
    const isIpcUrl = parsedUrl.protocol === "ws+unix:";
    let invalidUrlMessage;
    if (parsedUrl.protocol !== "ws:" && !isSecure && !isIpcUrl) {
      invalidUrlMessage = `The URL's protocol must be one of "ws:", "wss:", ` + '"http:", "https:", or "ws+unix:"';
    } else if (isIpcUrl && !parsedUrl.pathname) {
      invalidUrlMessage = "The URL's pathname is empty";
    } else if (parsedUrl.hash) {
      invalidUrlMessage = "The URL contains a fragment identifier";
    }
    if (invalidUrlMessage) {
      const err = new SyntaxError(invalidUrlMessage);
      if (websocket._redirects === 0) {
        throw err;
      } else {
        emitErrorAndClose(websocket, err);
        return;
      }
    }
    const defaultPort = isSecure ? 443 : 80;
    const key = randomBytes(16).toString("base64");
    const request3 = isSecure ? https.request : http.request;
    const protocolSet = new Set;
    let perMessageDeflate;
    opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect);
    opts.defaultPort = opts.defaultPort || defaultPort;
    opts.port = parsedUrl.port || defaultPort;
    opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
    opts.headers = {
      ...opts.headers,
      "Sec-WebSocket-Version": opts.protocolVersion,
      "Sec-WebSocket-Key": key,
      Connection: "Upgrade",
      Upgrade: "websocket"
    };
    opts.path = parsedUrl.pathname + parsedUrl.search;
    opts.timeout = opts.handshakeTimeout;
    if (opts.perMessageDeflate) {
      perMessageDeflate = new PerMessageDeflate({
        ...opts.perMessageDeflate,
        isServer: false,
        maxPayload: opts.maxPayload
      });
      opts.headers["Sec-WebSocket-Extensions"] = format({
        [PerMessageDeflate.extensionName]: perMessageDeflate.offer()
      });
    }
    if (protocols.length) {
      for (const protocol of protocols) {
        if (typeof protocol !== "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol)) {
          throw new SyntaxError("An invalid or duplicated subprotocol was specified");
        }
        protocolSet.add(protocol);
      }
      opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
    }
    if (opts.origin) {
      if (opts.protocolVersion < 13) {
        opts.headers["Sec-WebSocket-Origin"] = opts.origin;
      } else {
        opts.headers.Origin = opts.origin;
      }
    }
    if (parsedUrl.username || parsedUrl.password) {
      opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
    }
    if (isIpcUrl) {
      const i = opts.path.indexOf(":");
      if (i === -1) {
        opts.socketPath = opts.path;
        opts.path = "";
      } else {
        opts.socketPath = opts.path.slice(0, i);
        opts.path = opts.path.slice(i + 1);
      }
    }
    let req;
    if (opts.followRedirects) {
      if (websocket._redirects === 0) {
        websocket._originalIpc = isIpcUrl;
        websocket._originalSecure = isSecure;
        websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
        const headers = options && options.headers;
        options = { ...options, headers: {} };
        if (headers) {
          for (const [key, value] of Object.entries(headers)) {
            options.headers[key.toLowerCase()] = value;
          }
        }
      } else if (websocket.listenerCount("redirect") === 0) {
        const isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : false : websocket._originalIpc ? false : parsedUrl.host === websocket._originalHostOrSocketPath;
        if (!isSameHost || websocket._originalSecure && !isSecure) {
          delete opts.headers.authorization;
          delete opts.headers.cookie;
          if (!isSameHost)
            delete opts.headers.host;
          opts.auth = undefined;
        }
      }
      if (opts.auth && !options.headers.authorization) {
        options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64");
      }
      req = websocket._req = request3(opts);
      if (websocket._redirects) {
        websocket.emit("redirect", websocket.url, req);
      }
    } else {
      req = websocket._req = request3(opts);
    }
    if (opts.timeout) {
      req.on("timeout", () => {
        abortHandshake(websocket, req, "Opening handshake has timed out");
      });
    }
    req.on("error", (err) => {
      if (req === null || req[kAborted])
        return;
      req = websocket._req = null;
      emitErrorAndClose(websocket, err);
    });
    req.on("response", (res) => {
      const location2 = res.headers.location;
      const statusCode = res.statusCode;
      if (location2 && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
        if (++websocket._redirects > opts.maxRedirects) {
          abortHandshake(websocket, req, "Maximum redirects exceeded");
          return;
        }
        req.abort();
        let addr;
        try {
          addr = new URL2(location2, address);
        } catch (e) {
          const err = new SyntaxError(`Invalid URL: ${location2}`);
          emitErrorAndClose(websocket, err);
          return;
        }
        initAsClient(websocket, addr, protocols, options);
      } else if (!websocket.emit("unexpected-response", req, res)) {
        abortHandshake(websocket, req, `Unexpected server response: ${res.statusCode}`);
      }
    });
    req.on("upgrade", (res, socket, head) => {
      websocket.emit("upgrade", res);
      if (websocket.readyState !== WebSocket.CONNECTING)
        return;
      req = websocket._req = null;
      const upgrade = res.headers.upgrade;
      if (upgrade === undefined || upgrade.toLowerCase() !== "websocket") {
        abortHandshake(websocket, socket, "Invalid Upgrade header");
        return;
      }
      const digest = createHash("sha1").update(key + GUID).digest("base64");
      if (res.headers["sec-websocket-accept"] !== digest) {
        abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
        return;
      }
      const serverProt = res.headers["sec-websocket-protocol"];
      let protError;
      if (serverProt !== undefined) {
        if (!protocolSet.size) {
          protError = "Server sent a subprotocol but none was requested";
        } else if (!protocolSet.has(serverProt)) {
          protError = "Server sent an invalid subprotocol";
        }
      } else if (protocolSet.size) {
        protError = "Server sent no subprotocol";
      }
      if (protError) {
        abortHandshake(websocket, socket, protError);
        return;
      }
      if (serverProt)
        websocket._protocol = serverProt;
      const secWebSocketExtensions = res.headers["sec-websocket-extensions"];
      if (secWebSocketExtensions !== undefined) {
        if (!perMessageDeflate) {
          const message = "Server sent a Sec-WebSocket-Extensions header but no extension " + "was requested";
          abortHandshake(websocket, socket, message);
          return;
        }
        let extensions;
        try {
          extensions = parse(secWebSocketExtensions);
        } catch (err) {
          const message = "Invalid Sec-WebSocket-Extensions header";
          abortHandshake(websocket, socket, message);
          return;
        }
        const extensionNames = Object.keys(extensions);
        if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate.extensionName) {
          const message = "Server indicated an extension that was not requested";
          abortHandshake(websocket, socket, message);
          return;
        }
        try {
          perMessageDeflate.accept(extensions[PerMessageDeflate.extensionName]);
        } catch (err) {
          const message = "Invalid Sec-WebSocket-Extensions header";
          abortHandshake(websocket, socket, message);
          return;
        }
        websocket._extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
      }
      websocket.setSocket(socket, head, {
        allowSynchronousEvents: opts.allowSynchronousEvents,
        generateMask: opts.generateMask,
        maxBufferedChunks: opts.maxBufferedChunks,
        maxFragments: opts.maxFragments,
        maxPayload: opts.maxPayload,
        skipUTF8Validation: opts.skipUTF8Validation
      });
    });
    if (opts.finishRequest) {
      opts.finishRequest(req, websocket);
    } else {
      req.end();
    }
  }
  function emitErrorAndClose(websocket, err) {
    websocket._readyState = WebSocket.CLOSING;
    websocket._errorEmitted = true;
    websocket.emit("error", err);
    websocket.emitClose();
  }
  function netConnect(options) {
    options.path = options.socketPath;
    return net.connect(options);
  }
  function tlsConnect(options) {
    options.path = undefined;
    if (!options.servername && options.servername !== "") {
      options.servername = net.isIP(options.host) ? "" : options.host;
    }
    return tls.connect(options);
  }
  function abortHandshake(websocket, stream, message) {
    websocket._readyState = WebSocket.CLOSING;
    const err = new Error(message);
    Error.captureStackTrace(err, abortHandshake);
    if (stream.setHeader) {
      stream[kAborted] = true;
      stream.abort();
      if (stream.socket && !stream.socket.destroyed) {
        stream.socket.destroy();
      }
      process.nextTick(emitErrorAndClose, websocket, err);
    } else {
      stream.destroy(err);
      stream.once("error", websocket.emit.bind(websocket, "error"));
      stream.once("close", websocket.emitClose.bind(websocket));
    }
  }
  function sendAfterClose(websocket, data, cb) {
    if (data) {
      const length = isBlob(data) ? data.size : toBuffer(data).length;
      if (websocket._socket)
        websocket._sender._bufferedBytes += length;
      else
        websocket._bufferedAmount += length;
    }
    if (cb) {
      const err = new Error(`WebSocket is not open: readyState ${websocket.readyState} ` + `(${readyStates[websocket.readyState]})`);
      process.nextTick(cb, err);
    }
  }
  function receiverOnConclude(code, reason) {
    const websocket = this[kWebSocket];
    websocket._closeFrameReceived = true;
    websocket._closeMessage = reason;
    websocket._closeCode = code;
    if (websocket._socket[kWebSocket] === undefined)
      return;
    websocket._socket.removeListener("data", socketOnData);
    process.nextTick(resume, websocket._socket);
    if (code === 1005)
      websocket.close();
    else
      websocket.close(code, reason);
  }
  function receiverOnDrain() {
    const websocket = this[kWebSocket];
    if (!websocket.isPaused)
      websocket._socket.resume();
  }
  function receiverOnError(err) {
    const websocket = this[kWebSocket];
    if (websocket._socket[kWebSocket] !== undefined) {
      websocket._socket.removeListener("data", socketOnData);
      process.nextTick(resume, websocket._socket);
      websocket.close(err[kStatusCode]);
    }
    if (!websocket._errorEmitted) {
      websocket._errorEmitted = true;
      websocket.emit("error", err);
    }
  }
  function receiverOnFinish() {
    this[kWebSocket].emitClose();
  }
  function receiverOnMessage(data, isBinary) {
    this[kWebSocket].emit("message", data, isBinary);
  }
  function receiverOnPing(data) {
    const websocket = this[kWebSocket];
    if (websocket._autoPong)
      websocket.pong(data, !this._isServer, NOOP);
    websocket.emit("ping", data);
  }
  function receiverOnPong(data) {
    this[kWebSocket].emit("pong", data);
  }
  function resume(stream) {
    stream.resume();
  }
  function senderOnError(err) {
    const websocket = this[kWebSocket];
    if (websocket.readyState === WebSocket.CLOSED)
      return;
    if (websocket.readyState === WebSocket.OPEN) {
      websocket._readyState = WebSocket.CLOSING;
      setCloseTimer(websocket);
    }
    this._socket.end();
    if (!websocket._errorEmitted) {
      websocket._errorEmitted = true;
      websocket.emit("error", err);
    }
  }
  function setCloseTimer(websocket) {
    websocket._closeTimer = setTimeout(websocket._socket.destroy.bind(websocket._socket), websocket._closeTimeout);
  }
  function socketOnClose() {
    const websocket = this[kWebSocket];
    this.removeListener("close", socketOnClose);
    this.removeListener("data", socketOnData);
    this.removeListener("end", socketOnEnd);
    websocket._readyState = WebSocket.CLOSING;
    if (!this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
      const chunk = this.read(this._readableState.length);
      websocket._receiver.write(chunk);
    }
    websocket._receiver.end();
    this[kWebSocket] = undefined;
    clearTimeout(websocket._closeTimer);
    if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) {
      websocket.emitClose();
    } else {
      websocket._receiver.on("error", receiverOnFinish);
      websocket._receiver.on("finish", receiverOnFinish);
    }
  }
  function socketOnData(chunk) {
    if (!this[kWebSocket]._receiver.write(chunk)) {
      this.pause();
    }
  }
  function socketOnEnd() {
    const websocket = this[kWebSocket];
    websocket._readyState = WebSocket.CLOSING;
    websocket._receiver.end();
    this.end();
  }
  function socketOnError() {
    const websocket = this[kWebSocket];
    this.removeListener("error", socketOnError);
    this.on("error", NOOP);
    if (websocket) {
      websocket._readyState = WebSocket.CLOSING;
      this.destroy();
    }
  }
});

// node_modules/ws/lib/stream.js
var require_stream = __commonJS(function(exports, module) {
  var WebSocket = require_websocket();
  var { Duplex } = __require("stream");
  function emitClose(stream) {
    stream.emit("close");
  }
  function duplexOnEnd() {
    if (!this.destroyed && this._writableState.finished) {
      this.destroy();
    }
  }
  function duplexOnError(err) {
    this.removeListener("error", duplexOnError);
    this.destroy();
    if (this.listenerCount("error") === 0) {
      this.emit("error", err);
    }
  }
  function createWebSocketStream(ws, options) {
    let terminateOnDestroy = true;
    const duplex = new Duplex({
      ...options,
      autoDestroy: false,
      emitClose: false,
      objectMode: false,
      writableObjectMode: false
    });
    ws.on("message", function message(msg, isBinary) {
      const data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
      if (!duplex.push(data))
        ws.pause();
    });
    ws.once("error", function error(err) {
      if (duplex.destroyed)
        return;
      terminateOnDestroy = false;
      duplex.destroy(err);
    });
    ws.once("close", function close() {
      if (duplex.destroyed)
        return;
      duplex.push(null);
    });
    duplex._destroy = function(err, callback) {
      if (ws.readyState === ws.CLOSED) {
        callback(err);
        process.nextTick(emitClose, duplex);
        return;
      }
      let called = false;
      ws.once("error", function error(err) {
        called = true;
        callback(err);
      });
      ws.once("close", function close() {
        if (!called)
          callback(err);
        process.nextTick(emitClose, duplex);
      });
      if (terminateOnDestroy)
        ws.terminate();
    };
    duplex._final = function(callback) {
      if (ws.readyState === ws.CONNECTING) {
        ws.once("open", function open() {
          duplex._final(callback);
        });
        return;
      }
      if (ws._socket === null)
        return;
      if (ws._socket._writableState.finished) {
        callback();
        if (duplex._readableState.endEmitted)
          duplex.destroy();
      } else {
        ws._socket.once("finish", function finish() {
          callback();
        });
        ws.close();
      }
    };
    duplex._read = function() {
      if (ws.isPaused)
        ws.resume();
    };
    duplex._write = function(chunk, encoding, callback) {
      if (ws.readyState === ws.CONNECTING) {
        ws.once("open", function open() {
          duplex._write(chunk, encoding, callback);
        });
        return;
      }
      ws.send(chunk, callback);
    };
    duplex.on("end", duplexOnEnd);
    duplex.on("error", duplexOnError);
    return duplex;
  }
  module.exports = createWebSocketStream;
});

// node_modules/ws/lib/subprotocol.js
var require_subprotocol = __commonJS(function(exports, module) {
  var { tokenChars } = require_validation();
  function parse(header) {
    const protocols = new Set;
    let start = -1;
    let end = -1;
    let i = 0;
    for (i;i < header.length; i++) {
      const code = header.charCodeAt(i);
      if (end === -1 && tokenChars[code] === 1) {
        if (start === -1)
          start = i;
      } else if (i !== 0 && (code === 32 || code === 9)) {
        if (end === -1 && start !== -1)
          end = i;
      } else if (code === 44) {
        if (start === -1) {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
        if (end === -1)
          end = i;
        const protocol = header.slice(start, end);
        if (protocols.has(protocol)) {
          throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
        }
        protocols.add(protocol);
        start = end = -1;
      } else {
        throw new SyntaxError(`Unexpected character at index ${i}`);
      }
    }
    if (start === -1 || end !== -1) {
      throw new SyntaxError("Unexpected end of input");
    }
    const protocol = header.slice(start, i);
    if (protocols.has(protocol)) {
      throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
    }
    protocols.add(protocol);
    return protocols;
  }
  module.exports = { parse };
});

// node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS(function(exports, module) {
  var EventEmitter = __require("events");
  var http = __require("http");
  var { Duplex } = __require("stream");
  var { createHash } = __require("crypto");
  var extension = require_extension();
  var PerMessageDeflate = require_permessage_deflate();
  var subprotocol = require_subprotocol();
  var WebSocket = require_websocket();
  var { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants();
  var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
  var RUNNING = 0;
  var CLOSING = 1;
  var CLOSED = 2;

  class WebSocketServer extends EventEmitter {
    constructor(options, callback) {
      super();
      options = {
        allowSynchronousEvents: true,
        autoPong: true,
        maxBufferedChunks: 256 * 1024,
        maxFragments: 16 * 1024,
        maxPayload: 100 * 1024 * 1024,
        skipUTF8Validation: false,
        perMessageDeflate: false,
        handleProtocols: null,
        clientTracking: true,
        closeTimeout: CLOSE_TIMEOUT,
        verifyClient: null,
        noServer: false,
        backlog: null,
        server: null,
        host: null,
        path: null,
        port: null,
        WebSocket,
        ...options
      };
      if (options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer) {
        throw new TypeError('One and only one of the "port", "server", or "noServer" options ' + "must be specified");
      }
      if (options.port != null) {
        this._server = http.createServer((req, res) => {
          const body = http.STATUS_CODES[426];
          res.writeHead(426, {
            "Content-Length": body.length,
            "Content-Type": "text/plain"
          });
          res.end(body);
        });
        this._server.listen(options.port, options.host, options.backlog, callback);
      } else if (options.server) {
        this._server = options.server;
      }
      if (this._server) {
        const emitConnection = this.emit.bind(this, "connection");
        this._removeListeners = addListeners(this._server, {
          listening: this.emit.bind(this, "listening"),
          error: this.emit.bind(this, "error"),
          upgrade: (req, socket, head) => {
            this.handleUpgrade(req, socket, head, emitConnection);
          }
        });
      }
      if (options.perMessageDeflate === true)
        options.perMessageDeflate = {};
      if (options.clientTracking) {
        this.clients = new Set;
        this._shouldEmitClose = false;
      }
      this.options = options;
      this._state = RUNNING;
    }
    address() {
      if (this.options.noServer) {
        throw new Error('The server is operating in "noServer" mode');
      }
      if (!this._server)
        return null;
      return this._server.address();
    }
    close(cb) {
      if (this._state === CLOSED) {
        if (cb) {
          this.once("close", () => {
            cb(new Error("The server is not running"));
          });
        }
        process.nextTick(emitClose, this);
        return;
      }
      if (cb)
        this.once("close", cb);
      if (this._state === CLOSING)
        return;
      this._state = CLOSING;
      if (this.options.noServer || this.options.server) {
        if (this._server) {
          this._removeListeners();
          this._removeListeners = this._server = null;
        }
        if (this.clients) {
          if (!this.clients.size) {
            process.nextTick(emitClose, this);
          } else {
            this._shouldEmitClose = true;
          }
        } else {
          process.nextTick(emitClose, this);
        }
      } else {
        const server = this._server;
        this._removeListeners();
        this._removeListeners = this._server = null;
        server.close(() => {
          emitClose(this);
        });
      }
    }
    shouldHandle(req) {
      if (this.options.path) {
        const index = req.url.indexOf("?");
        const pathname = index !== -1 ? req.url.slice(0, index) : req.url;
        if (pathname !== this.options.path)
          return false;
      }
      return true;
    }
    handleUpgrade(req, socket, head, cb) {
      socket.on("error", socketOnError);
      const key = req.headers["sec-websocket-key"];
      const upgrade = req.headers.upgrade;
      const version = +req.headers["sec-websocket-version"];
      if (req.method !== "GET") {
        const message = "Invalid HTTP method";
        abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
        return;
      }
      if (upgrade === undefined || upgrade.toLowerCase() !== "websocket") {
        const message = "Invalid Upgrade header";
        abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
        return;
      }
      if (key === undefined || !keyRegex.test(key)) {
        const message = "Missing or invalid Sec-WebSocket-Key header";
        abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
        return;
      }
      if (version !== 13 && version !== 8) {
        const message = "Missing or invalid Sec-WebSocket-Version header";
        abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
          "Sec-WebSocket-Version": "13, 8"
        });
        return;
      }
      if (!this.shouldHandle(req)) {
        abortHandshake(socket, 400);
        return;
      }
      const secWebSocketProtocol = req.headers["sec-websocket-protocol"];
      let protocols = new Set;
      if (secWebSocketProtocol !== undefined) {
        try {
          protocols = subprotocol.parse(secWebSocketProtocol);
        } catch (err) {
          const message = "Invalid Sec-WebSocket-Protocol header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
      }
      const secWebSocketExtensions = req.headers["sec-websocket-extensions"];
      const extensions = {};
      if (this.options.perMessageDeflate && secWebSocketExtensions !== undefined) {
        const perMessageDeflate = new PerMessageDeflate({
          ...this.options.perMessageDeflate,
          isServer: true,
          maxPayload: this.options.maxPayload
        });
        try {
          const offers = extension.parse(secWebSocketExtensions);
          if (offers[PerMessageDeflate.extensionName]) {
            perMessageDeflate.accept(offers[PerMessageDeflate.extensionName]);
            extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
          }
        } catch (err) {
          const message = "Invalid or unacceptable Sec-WebSocket-Extensions header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
      }
      if (this.options.verifyClient) {
        const info = {
          origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
          secure: !!(req.socket.authorized || req.socket.encrypted),
          req
        };
        if (this.options.verifyClient.length === 2) {
          this.options.verifyClient(info, (verified, code, message, headers) => {
            if (!verified) {
              return abortHandshake(socket, code || 401, message, headers);
            }
            this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
          });
          return;
        }
        if (!this.options.verifyClient(info))
          return abortHandshake(socket, 401);
      }
      this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
    }
    completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
      if (!socket.readable || !socket.writable)
        return socket.destroy();
      if (socket[kWebSocket]) {
        throw new Error("server.handleUpgrade() was called more than once with the same " + "socket, possibly due to a misconfiguration");
      }
      if (this._state > RUNNING)
        return abortHandshake(socket, 503);
      const digest = createHash("sha1").update(key + GUID).digest("base64");
      const headers = [
        "HTTP/1.1 101 Switching Protocols",
        "Upgrade: websocket",
        "Connection: Upgrade",
        `Sec-WebSocket-Accept: ${digest}`
      ];
      const ws = new this.options.WebSocket(null, undefined, this.options);
      if (protocols.size) {
        const protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
        if (protocol) {
          headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
          ws._protocol = protocol;
        }
      }
      if (extensions[PerMessageDeflate.extensionName]) {
        const params = extensions[PerMessageDeflate.extensionName].params;
        const value = extension.format({
          [PerMessageDeflate.extensionName]: [params]
        });
        headers.push(`Sec-WebSocket-Extensions: ${value}`);
        ws._extensions = extensions;
      }
      this.emit("headers", headers, req);
      socket.write(headers.concat(`\r
`).join(`\r
`));
      socket.removeListener("error", socketOnError);
      ws.setSocket(socket, head, {
        allowSynchronousEvents: this.options.allowSynchronousEvents,
        maxBufferedChunks: this.options.maxBufferedChunks,
        maxFragments: this.options.maxFragments,
        maxPayload: this.options.maxPayload,
        skipUTF8Validation: this.options.skipUTF8Validation
      });
      if (this.clients) {
        this.clients.add(ws);
        ws.on("close", () => {
          this.clients.delete(ws);
          if (this._shouldEmitClose && !this.clients.size) {
            process.nextTick(emitClose, this);
          }
        });
      }
      cb(ws, req);
    }
  }
  module.exports = WebSocketServer;
  function addListeners(server, map) {
    for (const event of Object.keys(map))
      server.on(event, map[event]);
    return function removeListeners() {
      for (const event of Object.keys(map)) {
        server.removeListener(event, map[event]);
      }
    };
  }
  function emitClose(server) {
    server._state = CLOSED;
    server.emit("close");
  }
  function socketOnError() {
    this.destroy();
  }
  function abortHandshake(socket, code, message, headers) {
    message = message || http.STATUS_CODES[code];
    headers = {
      Connection: "close",
      "Content-Type": "text/html",
      "Content-Length": Buffer.byteLength(message),
      ...headers
    };
    socket.once("finish", socket.destroy);
    socket.end(`HTTP/1.1 ${code} ${http.STATUS_CODES[code]}\r
` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join(`\r
`) + `\r
\r
` + message);
  }
  function abortHandshakeOrEmitwsClientError(server, req, socket, code, message, headers) {
    if (server.listenerCount("wsClientError")) {
      const err = new Error(message);
      Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);
      server.emit("wsClientError", err, socket, req);
    } else {
      abortHandshake(socket, code, message, headers);
    }
  }
});
// node_modules/effect/dist/Pipeable.js
var pipeArguments = (self, args) => {
  switch (args.length) {
    case 0:
      return self;
    case 1:
      return args[0](self);
    case 2:
      return args[1](args[0](self));
    case 3:
      return args[2](args[1](args[0](self)));
    case 4:
      return args[3](args[2](args[1](args[0](self))));
    case 5:
      return args[4](args[3](args[2](args[1](args[0](self)))));
    case 6:
      return args[5](args[4](args[3](args[2](args[1](args[0](self))))));
    case 7:
      return args[6](args[5](args[4](args[3](args[2](args[1](args[0](self)))))));
    case 8:
      return args[7](args[6](args[5](args[4](args[3](args[2](args[1](args[0](self))))))));
    case 9:
      return args[8](args[7](args[6](args[5](args[4](args[3](args[2](args[1](args[0](self)))))))));
    default: {
      let ret = self;
      for (let i = 0, len = args.length;i < len; i++) {
        ret = args[i](ret);
      }
      return ret;
    }
  }
};
var Prototype = {
  pipe() {
    return pipeArguments(this, arguments);
  }
};
var Class = /* @__PURE__ */ function() {
  function PipeableBase() {}
  PipeableBase.prototype = Prototype;
  return PipeableBase;
}();

// node_modules/effect/dist/Function.js
var dual = function(arity, body) {
  if (typeof arity === "function") {
    return function() {
      return arity(arguments) ? body.apply(this, arguments) : (self) => body(self, ...arguments);
    };
  }
  switch (arity) {
    case 0:
    case 1:
      throw new RangeError(`Invalid arity ${arity}`);
    case 2:
      return function(a, b) {
        if (arguments.length >= 2) {
          return body(a, b);
        }
        return function(self) {
          return body(self, a);
        };
      };
    case 3:
      return function(a, b, c) {
        if (arguments.length >= 3) {
          return body(a, b, c);
        }
        return function(self) {
          return body(self, a, b);
        };
      };
    default:
      return function() {
        if (arguments.length >= arity) {
          return body.apply(this, arguments);
        }
        const args = arguments;
        return function(self) {
          return body(self, ...args);
        };
      };
  }
};
var identity = (a) => a;
var constant = (value) => () => value;
var constTrue = /* @__PURE__ */ constant(true);
var constFalse = /* @__PURE__ */ constant(false);
var constUndefined = /* @__PURE__ */ constant(undefined);
var constVoid = constUndefined;
function pipe(a, ...args) {
  return pipeArguments(a, args);
}
function flow(ab, bc, cd, de, ef, fg, gh, hi, ij) {
  switch (arguments.length) {
    case 1:
      return ab;
    case 2:
      return function() {
        return bc(ab.apply(this, arguments));
      };
    case 3:
      return function() {
        return cd(bc(ab.apply(this, arguments)));
      };
    case 4:
      return function() {
        return de(cd(bc(ab.apply(this, arguments))));
      };
    case 5:
      return function() {
        return ef(de(cd(bc(ab.apply(this, arguments)))));
      };
    case 6:
      return function() {
        return fg(ef(de(cd(bc(ab.apply(this, arguments))))));
      };
    case 7:
      return function() {
        return gh(fg(ef(de(cd(bc(ab.apply(this, arguments)))))));
      };
    case 8:
      return function() {
        return hi(gh(fg(ef(de(cd(bc(ab.apply(this, arguments))))))));
      };
    case 9:
      return function() {
        return ij(hi(gh(fg(ef(de(cd(bc(ab.apply(this, arguments)))))))));
      };
  }
  return;
}
function memoize(f) {
  const cache = new WeakMap;
  return (a) => {
    const cached = cache.get(a);
    if (cached !== undefined)
      return cached;
    const result = f(a);
    cache.set(a, result);
    return result;
  };
}
function memoizeIdempotent(f) {
  const cache = new WeakMap;
  return (a) => {
    const cached = cache.get(a);
    if (cached !== undefined)
      return cached;
    const result = f(a);
    cache.set(a, result);
    if (result !== a) {
      cache.set(result, result);
    }
    return result;
  };
}

// node_modules/effect/dist/internal/equal.js
var getAllObjectKeys = (obj) => {
  const keys = new Set(Reflect.ownKeys(obj));
  if (obj.constructor === Object)
    return keys;
  if (obj instanceof Error) {
    keys.delete("stack");
  }
  const proto = Object.getPrototypeOf(obj);
  let current = proto;
  while (current !== null && current !== Object.prototype) {
    const ownKeys = Reflect.ownKeys(current);
    for (let i = 0;i < ownKeys.length; i++) {
      keys.add(ownKeys[i]);
    }
    current = Object.getPrototypeOf(current);
  }
  if (keys.has("constructor") && typeof obj.constructor === "function" && proto === obj.constructor.prototype) {
    keys.delete("constructor");
  }
  return keys;
};
var byReferenceInstances = /* @__PURE__ */ new WeakSet;
var viewBytes = (view) => new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
var isArrayBufferLike = (u) => u instanceof ArrayBuffer || typeof SharedArrayBuffer !== "undefined" && u instanceof SharedArrayBuffer;
var bufferBytes = (buffer) => {
  try {
    return new Uint8Array(buffer);
  } catch {
    return;
  }
};

// node_modules/effect/dist/internal/hash.js
var backEdges = 0;
var addBackEdge = () => {
  backEdges++;
};

// node_modules/effect/dist/Predicate.js
function isString(input) {
  return typeof input === "string";
}
function isNumber(input) {
  return typeof input === "number";
}
function isBoolean(input) {
  return typeof input === "boolean";
}
function isSymbol(input) {
  return typeof input === "symbol";
}
function isPropertyKey(u) {
  return isString(u) || isNumber(u) || isSymbol(u);
}
function isFunction(input) {
  return typeof input === "function";
}
function isUndefined(input) {
  return input === undefined;
}
function isNotUndefined(input) {
  return input !== undefined;
}
function isNotNull(input) {
  return input !== null;
}
function isNullish(input) {
  return input === null || input === undefined;
}
function isNotNullish(input) {
  return input != null;
}
function isUnknown(_) {
  return true;
}
function isObject(input) {
  return typeof input === "object" && input !== null && !Array.isArray(input);
}
function isObjectKeyword(input) {
  return typeof input === "object" && input !== null || isFunction(input);
}
var hasProperty = /* @__PURE__ */ dual(2, (self, property) => isObjectKeyword(self) && (property in self));
var isTagged = /* @__PURE__ */ dual(2, (self, tag) => hasProperty(self, "_tag") && self["_tag"] === tag);
function isError(input) {
  return input instanceof Error;
}
function isIterable(input) {
  return hasProperty(input, Symbol.iterator) || isString(input);
}

// node_modules/effect/dist/Hash.js
var symbol = "~effect/Hash";
var hash = (self) => {
  switch (typeof self) {
    case "number":
      return number(self);
    case "bigint":
      return string(self.toString(10));
    case "string":
      return string(self);
    case "function":
    case "object": {
      if (self === null) {
        break;
      } else if (self instanceof Date) {
        if (Number.isNaN(self.getTime())) {
          return string("Invalid Date");
        }
        return string(self.toISOString());
      } else if (self instanceof RegExp) {
        return string(self.toString());
      } else {
        if (byReferenceInstances.has(self)) {
          return random(self);
        }
        const cached = hashCache.get(self);
        if (cached !== undefined) {
          return cached;
        }
        if (visitedObjects.has(self)) {
          addBackEdge();
          return string("[Circular]");
        }
        visitedObjects.add(self);
        const seen = backEdges;
        let h;
        try {
          if (isHash(self)) {
            h = self[symbol]();
          } else if (typeof self === "function") {
            h = random(self);
          } else if (isArrayBufferLike(self)) {
            const bytes = bufferBytes(self);
            h = bytes === undefined ? random(self) : array(bytes);
          } else if (self instanceof DataView) {
            h = array(viewBytes(self));
          } else if (Array.isArray(self) || ArrayBuffer.isView(self)) {
            h = array(self);
          } else if (self instanceof Map) {
            h = hashMap(self);
          } else if (self instanceof Set) {
            h = hashSet(self);
          } else {
            h = structure(self);
          }
        } finally {
          visitedObjects.delete(self);
        }
        if (seen === backEdges) {
          hashCache.set(self, h);
        }
        return h;
      }
    }
  }
  return optimize(mix(string(String(self))));
};
var random = (self) => {
  if (!randomHashCache.has(self)) {
    randomHashCache.set(self, optimize(Math.random() * 4294967296 | 0));
  }
  return randomHashCache.get(self);
};
var mix = (h) => {
  h ^= h >>> 16;
  h = Math.imul(h, 2246822507);
  h ^= h >>> 13;
  h = Math.imul(h, 3266489909);
  return h ^ h >>> 16;
};
var combine = /* @__PURE__ */ dual(2, (self, b) => mix(Math.imul(self, 2654435761) + Math.imul(b, 2246822507)));
var optimize = (n) => n & 3221225471 | n >>> 1 & 1073741824;
var isHash = (u) => hasProperty(u, symbol) && typeof u[symbol] === "function";
var float64 = /* @__PURE__ */ new DataView(/* @__PURE__ */ new ArrayBuffer(8));
var number = (n) => {
  const h = n | 0;
  if (h === n) {
    return optimize(h);
  }
  float64.setFloat64(0, n !== n ? NaN : n);
  return optimize(combine(float64.getInt32(0), float64.getInt32(4)));
};
var string = (str) => {
  let h = 5381, i = str.length;
  while (i) {
    h = h * 33 ^ str.charCodeAt(--i);
  }
  return optimize(h);
};
var structureKeys = (o, keys) => {
  let h = 12289;
  for (const key of keys) {
    h ^= combine(hash(key), hash(o[key]));
  }
  return optimize(h);
};
var structure = (o) => structureKeys(o, getAllObjectKeys(o));
var unordered = (seed, f) => (iter) => {
  let h = seed;
  for (const element of iter) {
    h ^= f(element);
  }
  return optimize(h);
};
var array = (arr) => {
  let h = 6151;
  for (const element of arr) {
    h = combine(h, hash(element));
  }
  return optimize(h);
};
var hashMap = /* @__PURE__ */ unordered(/* @__PURE__ */ string("Map"), ([k, v]) => combine(hash(k), hash(v)));
var setSeed = /* @__PURE__ */ string("Set");
var hashSet = /* @__PURE__ */ unordered(setSeed, (element) => combine(setSeed, hash(element)));
var randomHashCache = /* @__PURE__ */ new WeakMap;
var hashCache = /* @__PURE__ */ new WeakMap;
var visitedObjects = /* @__PURE__ */ new WeakSet;

// node_modules/effect/dist/Equal.js
var symbol2 = "~effect/Equal";
function equals() {
  if (arguments.length === 1) {
    return (self) => compareBoth(self, arguments[0]);
  }
  return compareBoth(arguments[0], arguments[1]);
}
function compareBoth(self, that) {
  if (self === that)
    return true;
  if (self == null || that == null)
    return false;
  const selfType = typeof self;
  if (selfType !== typeof that) {
    return false;
  }
  if (selfType === "number" && self !== self && that !== that) {
    return true;
  }
  if (selfType !== "object" && selfType !== "function") {
    return false;
  }
  if (byReferenceInstances.has(self) || byReferenceInstances.has(that)) {
    return false;
  }
  return compareObjects(self, that);
}
function compareObjects(self, that) {
  const depth = pathLeft.length;
  for (let i = depth;i-- > 0; ) {
    if (pathLeft[i] === self && pathRight[i] === that)
      return true;
  }
  if (depth)
    return compareOnPath(self, that);
  let known = results.get(self);
  if (!known)
    results.set(self, known = new WeakMap);
  let result = known.get(that);
  if (result === undefined)
    known.set(that, result = compareOnPath(self, that));
  return result;
}
function compareOnPath(self, that) {
  pathLeft.push(self);
  pathRight.push(that);
  try {
    return compareStructure(self, that);
  } finally {
    pathLeft.pop();
    pathRight.pop();
  }
}
var pathLeft = [];
var pathRight = [];
var results = /* @__PURE__ */ new WeakMap;
function compareStructure(self, that) {
  if (hash(self) !== hash(that)) {
    return false;
  } else if (self instanceof Date) {
    if (!(that instanceof Date))
      return false;
    const selfTime = self.getTime();
    const thatTime = that.getTime();
    return selfTime === thatTime || Number.isNaN(selfTime) && Number.isNaN(thatTime);
  } else if (self instanceof RegExp) {
    return that instanceof RegExp && self.toString() === that.toString();
  }
  const bothEquals = isEqual(self);
  if (bothEquals !== isEqual(that) || typeof self === "function" && !bothEquals) {
    return false;
  } else if (bothEquals) {
    return self[symbol2](that);
  } else if (Array.isArray(self)) {
    if (!Array.isArray(that) || self.length !== that.length) {
      return false;
    }
    return compareArrays(self, that);
  } else if (isArrayBufferLike(self)) {
    if (!isArrayBufferLike(that) || self instanceof ArrayBuffer !== that instanceof ArrayBuffer) {
      return false;
    }
    const selfBytes = bufferBytes(self);
    const thatBytes = bufferBytes(that);
    return selfBytes !== undefined && thatBytes !== undefined && compareTypedArrays(selfBytes, thatBytes);
  } else if (ArrayBuffer.isView(self)) {
    const selfIsDataView = self instanceof DataView;
    if (!ArrayBuffer.isView(that) || self.byteLength !== that.byteLength || selfIsDataView !== that instanceof DataView) {
      return false;
    }
    if (selfIsDataView) {
      return compareTypedArrays(viewBytes(self), viewBytes(that));
    }
    return compareTypedArrays(self, that);
  } else if (self instanceof Map) {
    if (!(that instanceof Map) || self.size !== that.size) {
      return false;
    }
    return compareHashed(self, that, entryHash, equalEntries);
  } else if (self instanceof Set) {
    if (!(that instanceof Set) || self.size !== that.size) {
      return false;
    }
    return compareHashed(self, that, hash, compareBoth);
  }
  return compareRecords(self, that);
}
function compareArrays(self, that) {
  for (let i = 0;i < self.length; i++) {
    if (!compareBoth(self[i], that[i])) {
      return false;
    }
  }
  return true;
}
function compareTypedArrays(self, that) {
  if (self.length !== that.length) {
    return false;
  }
  for (let i = 0;i < self.length; i++) {
    if (self[i] !== that[i]) {
      return false;
    }
  }
  return true;
}
function compareRecords(self, that) {
  const selfKeys = getAllObjectKeys(self);
  const thatKeys = getAllObjectKeys(that);
  if (selfKeys.size !== thatKeys.size) {
    return false;
  }
  for (const key of selfKeys) {
    if (!thatKeys.has(key) || !compareBoth(self[key], that[key])) {
      return false;
    }
  }
  return true;
}
function compareHashed(self, that, hashOf, equivalent) {
  const groups = new Map;
  let remaining = 0;
  for (const item of that) {
    const h = hashOf(item);
    const group = groups.get(h);
    if (group)
      group.push(item);
    else
      groups.set(h, [item]);
    remaining++;
  }
  outer:
    for (const item of self) {
      const group = groups.get(hashOf(item));
      if (group) {
        for (let i = 0;i < group.length; i++) {
          if (equivalent(item, group[i])) {
            group[i] = group[group.length - 1];
            group.pop();
            remaining--;
            continue outer;
          }
        }
      }
      return false;
    }
  return remaining === 0;
}
var entryHash = (entry) => hash(entry[0]);
var equalEntries = (self, that) => compareBoth(self[0], that[0]) && compareBoth(self[1], that[1]);
var isEqual = (u) => hasProperty(u, symbol2) && typeof u[symbol2] === "function";
var asEquivalence = () => equals;

// node_modules/effect/dist/Redactable.js
var symbolRedactable = /* @__PURE__ */ Symbol.for("~effect/Redactable");
var isRedactable = (u) => hasProperty(u, symbolRedactable);
function redact(u) {
  if (isRedactable(u))
    return getRedacted(u);
  return u;
}
function getRedacted(redactable) {
  return redactable[symbolRedactable](globalThis[currentFiberTypeId]?.context ?? emptyContext);
}
var currentFiberTypeId = "~effect/Fiber/currentFiber";
var emptyMap = /* @__PURE__ */ new Map;
var emptyContext = {
  "~effect/Context": {},
  base: emptyMap,
  depth: 0,
  mapUnsafe: emptyMap,
  pipe() {
    return pipeArguments(this, arguments);
  }
};

// node_modules/effect/dist/Formatter.js
function format(input, options) {
  const space = options?.space ?? 0;
  const ancestors = new WeakSet;
  const gap = !space ? "" : typeof space === "number" ? " ".repeat(space) : space;
  const ind = (d) => gap.repeat(d);
  const wrap = (v, body) => {
    const ctor = v?.constructor;
    return ctor && ctor !== Object.prototype.constructor && ctor.name ? `${ctor.name}(${body})` : body;
  };
  const ownKeys = (o) => {
    try {
      return Reflect.ownKeys(o);
    } catch {
      return ["[ownKeys threw]"];
    }
  };
  function recur(v, d = 0) {
    try {
      return recurUnsafe(v, d);
    } catch {
      if (typeof v === "object" && v !== null || typeof v === "function")
        ancestors.delete(v);
      return "[inspection threw]";
    }
  }
  function recurUnsafe(v, d = 0) {
    if (typeof v === "string")
      return JSON.stringify(v);
    if (typeof v === "number" || v == null || typeof v === "boolean" || typeof v === "symbol")
      return String(v);
    if (typeof v === "bigint")
      return String(v) + "n";
    if (typeof v === "object" || typeof v === "function") {
      if (ancestors.has(v))
        return CIRCULAR;
      ancestors.add(v);
      let output;
      if (symbolRedactable in v) {
        output = recur(getRedacted(v), d);
      } else if (Array.isArray(v)) {
        output = !gap || v.length <= 1 ? `[${v.map((x) => recur(x, d)).join(",")}]` : `[
${ind(d + 1)}${v.map((x) => recur(x, d + 1)).join(`,
` + ind(d + 1))}
${ind(d)}]`;
      } else if (v instanceof Date) {
        output = formatDate(v);
      } else if (!options?.ignoreToString && hasProperty(v, "toString") && typeof v["toString"] === "function" && v["toString"] !== Object.prototype.toString && v["toString"] !== Array.prototype.toString) {
        const s = safeToString(v);
        output = v instanceof Error && v.cause !== undefined ? `${s} (cause: ${recur(v.cause, d)})` : s;
      } else if (Symbol.iterator in v) {
        output = `${v.constructor.name}(${recur(Array.from(v), d)})`;
      } else {
        const keys = ownKeys(v);
        if (!gap || keys.length <= 1) {
          const body = `{${keys.map((k) => `${formatPropertyKey(k)}:${recur(safeGet(v, k), d)}`).join(",")}}`;
          output = wrap(v, body);
        } else {
          const body = `{
${keys.map((k) => `${ind(d + 1)}${formatPropertyKey(k)}: ${recur(safeGet(v, k), d + 1)}`).join(`,
`)}
${ind(d)}}`;
          output = wrap(v, body);
        }
      }
      ancestors.delete(v);
      return output;
    }
    return String(v);
  }
  return recur(input, 0);
}
var CIRCULAR = "[Circular]";
function formatPropertyKey(name) {
  return typeof name === "string" ? JSON.stringify(name) : String(name);
}
function formatPath(path) {
  return path.map((key) => `[${formatPropertyKey(key)}]`).join("");
}
function formatDate(date) {
  try {
    return date.toISOString();
  } catch {
    return "Invalid Date";
  }
}
function safeToString(input) {
  try {
    const s = input.toString();
    return typeof s === "string" ? s : String(s);
  } catch {
    return "[toString threw]";
  }
}
function safeGet(input, key) {
  try {
    return input[key];
  } catch {
    return "[property access threw]";
  }
}
function formatJson(input, options) {
  const ancestors = [];
  const sources = new WeakMap;
  return JSON.stringify(input, function(key, value) {
    const original = Object.getOwnPropertyDescriptor(sources.get(this) ?? this, key)?.value;
    const redacted = hasProperty(original, symbolRedactable) ? redact(original) : redact(value);
    if (typeof redacted === "bigint") {
      return format(redacted);
    }
    if (typeof redacted !== "object" || redacted === null) {
      return redacted;
    }
    const current = redacted instanceof Error && !hasProperty(redacted, "toJSON") ? {
      ...redacted,
      name: redacted.name,
      message: redacted.message
    } : redacted;
    while (ancestors.length > 0 && ancestors[ancestors.length - 1] !== this) {
      ancestors.pop();
    }
    if (ancestors.includes(redacted)) {
      return;
    }
    ancestors.push(redacted);
    if (current !== redacted) {
      ancestors.push(current);
    }
    if (!hasGetter(current) || isJsonPrimitiveWrapper(current)) {
      return current;
    }
    const view = redactingView(current);
    sources.set(view, current);
    ancestors.push(view);
    return view;
  }, options?.space) ?? "null";
}
function hasGetter(object) {
  for (const key of Object.getOwnPropertyNames(object)) {
    if (Object.getOwnPropertyDescriptor(object, key)?.get !== undefined) {
      return true;
    }
  }
  return false;
}
function redactingView(object) {
  const view = Array.isArray(object) ? [] : {};
  const keys = Array.isArray(object) ? Array.from({
    length: object.length
  }, (_, i) => String(i)) : Object.keys(object);
  for (const key of keys) {
    Object.defineProperty(view, key, {
      enumerable: true,
      get: () => {
        const value = Reflect.get(object, key);
        return Object.getOwnPropertyDescriptor(object, key)?.get !== undefined ? redact(value) : value;
      }
    });
  }
  return view;
}
function isJsonPrimitiveWrapper(object) {
  for (const valueOf of [Number.prototype.valueOf, Boolean.prototype.valueOf, String.prototype.valueOf]) {
    try {
      Reflect.apply(valueOf, object, []);
      return true;
    } catch {}
  }
  return false;
}

// node_modules/effect/dist/Inspectable.js
var NodeInspectSymbol = /* @__PURE__ */ Symbol.for("nodejs.util.inspect.custom");
var toJson = (input) => {
  try {
    input = redact(input);
    if (hasProperty(input, "toJSON") && isFunction(input["toJSON"]) && input["toJSON"].length === 0) {
      return input.toJSON();
    } else if (Array.isArray(input)) {
      return input.map(toJson);
    }
    return input;
  } catch {
    return "[toJSON threw]";
  }
};
var toStringUnknown = (u, whitespace = 2) => {
  if (typeof u === "string") {
    return u;
  }
  try {
    return typeof u === "object" ? formatJson(u, {
      space: whitespace
    }) : format(u, {
      space: whitespace
    });
  } catch {
    return String(u);
  }
};
var BaseProto = {
  toJSON() {
    return toJson(this);
  },
  [NodeInspectSymbol]() {
    return this.toJSON();
  },
  toString() {
    return format(this.toJSON());
  }
};

class Class2 {
  [NodeInspectSymbol]() {
    return this.toJSON();
  }
  toString() {
    return format(this.toJSON());
  }
}

// node_modules/effect/dist/internal/stackTraceLimit.js
var isStackTraceLimitWritable = () => {
  const desc = Object.getOwnPropertyDescriptor(Error, "stackTraceLimit");
  if (desc === undefined) {
    return Object.isExtensible(Error);
  }
  return Object.hasOwn(desc, "writable") ? desc.writable === true : desc.set !== undefined;
};
var canWriteStackTraceLimit = /* @__PURE__ */ isStackTraceLimitWritable();
var getStackTraceLimit = () => Error.stackTraceLimit;
var setStackTraceLimit = (value) => {
  if (canWriteStackTraceLimit) {
    Error.stackTraceLimit = value;
  }
};

// node_modules/effect/dist/Utils.js
class SingleShotGen {
  constructor(self) {
    this.value = self;
    this.done = false;
  }
  next(a) {
    if (this.done) {
      this.value = a;
      return this;
    }
    this.done = true;
    return {
      value: this.value,
      done: false
    };
  }
}
var pickInternalCall = () => {
  const InternalTypeId = "~effect/Utils/internal";
  const standard = {
    [InternalTypeId]: (body) => {
      return body();
    }
  };
  const forced = {
    [InternalTypeId]: (body) => {
      try {
        return body();
      } finally {}
    }
  };
  const limit = getStackTraceLimit();
  if (limit === 0)
    return forced[InternalTypeId];
  setStackTraceLimit(2);
  const error = standard[InternalTypeId](() => new Error);
  setStackTraceLimit(limit);
  return error.stack?.includes(InternalTypeId) === true ? standard[InternalTypeId] : forced[InternalTypeId];
};
var internalCall = /* @__PURE__ */ pickInternalCall();

// node_modules/effect/dist/internal/record.js
function assignProperty(self, key, value) {
  if (key === "__proto__") {
    Object.defineProperty(self, key, {
      value,
      writable: true,
      enumerable: true,
      configurable: true
    });
  } else {
    self[key] = value;
  }
}
function assignProperties(self, source) {
  if (!Object.hasOwn(source, "__proto__")) {
    Object.assign(self, source);
    return;
  }
  for (const key of Reflect.ownKeys(source)) {
    if (Object.prototype.propertyIsEnumerable.call(source, key)) {
      assignProperty(self, key, source[key]);
    }
  }
}

// node_modules/effect/dist/internal/core.js
var EffectTypeId = `~effect/Effect`;
var ExitTypeId = `~effect/Exit`;
var effectVariance = {
  _A: identity,
  _E: identity,
  _R: identity
};
var identifier = `${EffectTypeId}/identifier`;
var args = `${EffectTypeId}/args`;
var evaluate = `${EffectTypeId}/evaluate`;
var contA = `${EffectTypeId}/successCont`;
var contE = `${EffectTypeId}/failureCont`;
var contAll = `${EffectTypeId}/ensureCont`;
var Yield = /* @__PURE__ */ Symbol.for("effect/Effect/Yield");
var PipeInspectableProto = {
  pipe() {
    return pipeArguments(this, arguments);
  },
  toJSON() {
    return {
      ...this
    };
  },
  toString() {
    return format(this.toJSON(), {
      ignoreToString: true,
      space: 2
    });
  },
  [NodeInspectSymbol]() {
    return this.toJSON();
  }
};
var StructuralProto = {
  [symbol]() {
    return structureKeys(this, Object.keys(this));
  },
  [symbol2](that) {
    const selfKeys = Object.keys(this);
    const thatKeys = Object.keys(that);
    if (selfKeys.length !== thatKeys.length)
      return false;
    for (let i = 0;i < selfKeys.length; i++) {
      if (selfKeys[i] !== thatKeys[i] || !equals(this[selfKeys[i]], that[selfKeys[i]])) {
        return false;
      }
    }
    return true;
  }
};
var EffectProto = {
  [EffectTypeId]: effectVariance,
  ...PipeInspectableProto,
  [Symbol.iterator]() {
    return new SingleShotGen(this);
  },
  toJSON() {
    return {
      _id: "Effect",
      op: this[identifier],
      ...args in this ? {
        args: this[args]
      } : undefined
    };
  }
};
var isEffect = (u) => hasProperty(u, EffectTypeId);
var isExit = (u) => hasProperty(u, ExitTypeId);
var CauseTypeId = "~effect/Cause";
var CauseReasonTypeId = "~effect/Cause/Reason";
var isCause = (self) => hasProperty(self, CauseTypeId);
class CauseImpl {
  constructor(failures) {
    this[CauseTypeId] = CauseTypeId;
    this.reasons = failures;
  }
  pipe() {
    return pipeArguments(this, arguments);
  }
  toJSON() {
    return {
      _id: "Cause",
      reasons: this.reasons.map((f) => f.toJSON())
    };
  }
  toString() {
    return `Cause(${format(this.reasons)})`;
  }
  [NodeInspectSymbol]() {
    return this.toJSON();
  }
  [symbol2](that) {
    return isCause(that) && this.reasons.length === that.reasons.length && this.reasons.every((e, i) => equals(e, that.reasons[i]));
  }
  [symbol]() {
    return array(this.reasons);
  }
}
var annotationsMap = /* @__PURE__ */ new WeakMap;

class ReasonBase {
  [CauseReasonTypeId];
  annotations;
  _tag;
  constructor(_tag, annotations, originalError) {
    this[CauseReasonTypeId] = CauseReasonTypeId;
    this._tag = _tag;
    if (annotations !== constEmptyAnnotations && typeof originalError === "object" && originalError !== null && annotations.size > 0) {
      const prevAnnotations = annotationsMap.get(originalError);
      if (prevAnnotations) {
        annotations = new Map([...prevAnnotations, ...annotations]);
      }
      annotationsMap.set(originalError, annotations);
    }
    this.annotations = annotations;
  }
  annotate(annotations, options) {
    if (annotations.mapUnsafe.size === 0)
      return this;
    const newAnnotations = new Map(this.annotations);
    annotations.mapUnsafe.forEach((value, key) => {
      if (options?.overwrite !== true && newAnnotations.has(key))
        return;
      newAnnotations.set(key, value);
    });
    if (options?.overwrite !== true && newAnnotations.size === this.annotations.size)
      return this;
    const self = Object.assign(Object.create(Object.getPrototypeOf(this)), this);
    self.annotations = newAnnotations;
    return self;
  }
  pipe() {
    return pipeArguments(this, arguments);
  }
  toString() {
    return format(this);
  }
  [NodeInspectSymbol]() {
    return this.toString();
  }
}
var constEmptyAnnotations = /* @__PURE__ */ new Map;

class Fail extends ReasonBase {
  constructor(error, annotations = constEmptyAnnotations) {
    super("Fail", annotations, error);
    this.error = error;
  }
  toString() {
    return `Fail(${format(this.error)})`;
  }
  toJSON() {
    return {
      _tag: "Fail",
      error: this.error
    };
  }
  [symbol2](that) {
    return isFailReason(that) && equals(this.error, that.error) && equals(this.annotations, that.annotations);
  }
  [symbol]() {
    return combine(string(this._tag))(combine(hash(this.error))(hash(this.annotations)));
  }
}
var causeFromReasons = (reasons) => new CauseImpl(reasons);
var dedupeReasons = (self, that) => {
  const buckets = new Map;
  const out = [];
  for (const reason of self.concat(that)) {
    const hash2 = hash(reason);
    const bucket = buckets.get(hash2);
    if (bucket === undefined) {
      buckets.set(hash2, [reason]);
    } else if (bucket.some((previous) => equals(previous, reason))) {
      continue;
    } else {
      bucket.push(reason);
    }
    out.push(reason);
  }
  return out;
};
var causeCombine = /* @__PURE__ */ dual(2, (self, that) => {
  if (self.reasons.length === 0) {
    return that;
  } else if (that.reasons.length === 0) {
    return self;
  }
  const newCause = new CauseImpl(dedupeReasons(self.reasons, that.reasons));
  return equals(self, newCause) ? self : newCause;
});
var causeAddInterrupts = (self, interrupts) => {
  const reasons = self.reasons.slice();
  for (const reason of interrupts.reasons) {
    if (isInterruptReason(reason) && !reasons.some((r) => isInterruptReason(r) && r.fiberId === reason.fiberId && r.annotations === reason.annotations)) {
      reasons.push(reason);
    }
  }
  return reasons.length === self.reasons.length ? self : new CauseImpl(reasons);
};
var causeEmpty = /* @__PURE__ */ new CauseImpl([]);
var causeFail = (error) => new CauseImpl([new Fail(error)]);

class Die extends ReasonBase {
  constructor(defect, annotations = constEmptyAnnotations) {
    super("Die", annotations, defect);
    this.defect = defect;
  }
  toString() {
    return `Die(${format(this.defect)})`;
  }
  toJSON() {
    return {
      _tag: "Die",
      defect: this.defect
    };
  }
  [symbol2](that) {
    return isDieReason(that) && equals(this.defect, that.defect) && equals(this.annotations, that.annotations);
  }
  [symbol]() {
    return combine(string(this._tag))(combine(hash(this.defect))(hash(this.annotations)));
  }
}
var causeDie = (defect) => new CauseImpl([new Die(defect)]);
var causeAnnotate = /* @__PURE__ */ dual((args) => isCause(args[0]), (self, annotations, options) => {
  if (annotations.mapUnsafe.size === 0)
    return self;
  return new CauseImpl(self.reasons.map((f) => f.annotate(annotations, options)));
});
var isFailReason = (self) => self._tag === "Fail";
var isDieReason = (self) => self._tag === "Die";
var isInterruptReason = (self) => self._tag === "Interrupt";
function defaultEvaluate(_fiber) {
  return exitDie(`Effect.evaluate: Not implemented`);
}
var makePrimitiveProto = (options) => ({
  ...EffectProto,
  [identifier]: options.op,
  [evaluate]: options[evaluate] ?? defaultEvaluate,
  [contA]: options[contA],
  [contE]: options[contE],
  [contAll]: options[contAll]
});
var makePrimitive = (options) => {
  const Proto = makePrimitiveProto(options);
  const PrimitiveImpl = function(value) {
    this[args] = value;
  };
  PrimitiveImpl.prototype = Proto;
  return function(value) {
    return new PrimitiveImpl(value);
  };
};
var makeExit = (options) => {
  const Proto = {
    [ExitTypeId]: ExitTypeId,
    _tag: options.op,
    get [options.prop]() {
      return this[args];
    },
    ...makePrimitiveProto(options),
    toString() {
      return `${options.op}(${format(this[args])})`;
    },
    toJSON() {
      return {
        _id: "Exit",
        _tag: options.op,
        [options.prop]: this[args]
      };
    },
    [symbol2](that) {
      return isExit(that) && that._tag === this._tag && equals(this[args], that[args]);
    },
    [symbol]() {
      return combine(string(options.op), hash(this[args]));
    }
  };
  const ExitPrimitive = function(value) {
    this[args] = value;
  };
  ExitPrimitive.prototype = Proto;
  return function(value) {
    return new ExitPrimitive(value);
  };
};
var exitSucceed = /* @__PURE__ */ makeExit({
  op: "Success",
  prop: "value",
  [evaluate](fiber) {
    const cont = fiber.getCont(contA);
    return cont ? cont[contA](this[args], fiber, this) : fiber.yieldWith(this);
  }
});
var StackTraceKey = {
  key: "effect/Cause/StackTrace"
};
var InterruptorStackTrace = {
  key: "effect/Cause/InterruptorStackTrace"
};
var exitFailCause = /* @__PURE__ */ makeExit({
  op: "Failure",
  prop: "cause",
  [evaluate](fiber) {
    let cause = this[args];
    let annotated = false;
    if (fiber.cache.stackFrame) {
      cause = causeAnnotate(cause, {
        mapUnsafe: new Map([[StackTraceKey.key, fiber.cache.stackFrame]])
      });
      annotated = true;
    }
    let cont = fiber.getCont(contE);
    const interruptedCause = fiber._interruptedCause;
    if (interruptedCause && fiber.interruptible) {
      let skippedHandler = false;
      while (cont && fiber.interruptible) {
        skippedHandler ||= identifier in cont;
        cont = fiber.getCont(contE);
      }
      if (skippedHandler) {
        cause = causeFromReasons(cause.reasons.filter((reason) => reason._tag !== "Fail"));
      }
      cause = causeAddInterrupts(cause, interruptedCause);
      annotated = true;
    }
    return cont ? cont[contE](cause, fiber, annotated ? undefined : this) : fiber.yieldWith(annotated ? exitFailCause(cause) : this);
  }
});
var exitFail = (e) => exitFailCause(causeFail(e));
var exitDie = (defect) => exitFailCause(causeDie(defect));
var withFiber = /* @__PURE__ */ makePrimitive({
  op: "WithFiber",
  [evaluate](fiber) {
    return this[args](fiber);
  }
});
var withFiberSucceed = /* @__PURE__ */ makePrimitive({
  op: "WithFiberSucceed",
  [evaluate](fiber) {
    const value = this[args](fiber);
    const cont = fiber.getCont(contA);
    return cont ? cont[contA](value, fiber) : fiber.yieldWith(exitSucceed(value));
  }
});
var YieldableError = /* @__PURE__ */ function() {

  class YieldableError extends globalThis.Error {
  }
  const proto = /* @__PURE__ */ makePrimitiveProto({
    op: "YieldableError",
    [evaluate]() {
      return exitFail(this);
    }
  });
  delete proto.toString;
  Object.assign(YieldableError.prototype, proto);
  return YieldableError;
}();
var Error2 = /* @__PURE__ */ function() {
  const plainArgsSymbol = /* @__PURE__ */ Symbol.for("effect/Data/Error/plainArgs");
  return class Base extends YieldableError {
    constructor(args) {
      super(args?.message, args?.cause ? {
        cause: args.cause
      } : undefined);
      if (args) {
        assignProperties(this, args);
        Object.defineProperty(this, plainArgsSymbol, {
          value: args,
          enumerable: false
        });
      }
    }
    toJSON() {
      return {
        ...this[plainArgsSymbol],
        ...this
      };
    }
  };
}();
var TaggedError = (tag) => {

  class Base extends Error2 {
    _tag = tag;
  }
  Base.prototype.name = tag;
  return Base;
};
var NoSuchElementErrorTypeId = "~effect/Cause/NoSuchElementError";
var isNoSuchElementError = (u) => hasProperty(u, NoSuchElementErrorTypeId);
var DoneTypeId = "~effect/Cause/Done";
var isDone = (u) => hasProperty(u, DoneTypeId);
var DoneVoid = {
  [DoneTypeId]: DoneTypeId,
  _tag: "Done",
  value: undefined
};
var Done = (value) => {
  if (value === undefined)
    return DoneVoid;
  return {
    [DoneTypeId]: DoneTypeId,
    _tag: "Done",
    value
  };
};
var doneVoid = /* @__PURE__ */ exitFail(DoneVoid);
var done = (value) => {
  if (value === undefined)
    return doneVoid;
  return exitFail(Done(value));
};

// node_modules/effect/dist/Effectable.js
var Prototype2 = (options) => makePrimitiveProto({
  op: options.label,
  [evaluate]: options.evaluate
});

// node_modules/effect/dist/Equivalence.js
var make = (isEquivalent) => (self, that) => self === that || isEquivalent(self, that);
var isStrictEquivalent = (x, y) => x === y;
var strictEqual = () => isStrictEquivalent;
function Tuple(elements) {
  return make((self, that) => {
    if (self.length !== that.length) {
      return false;
    }
    for (let i = 0;i < self.length; i++) {
      if (!elements[i](self[i], that[i])) {
        return false;
      }
    }
    return true;
  });
}
function Array_(item) {
  return make((self, that) => {
    if (self.length !== that.length)
      return false;
    for (let i = 0;i < self.length; i++) {
      if (!item(self[i], that[i]))
        return false;
    }
    return true;
  });
}

// node_modules/effect/dist/internal/option.js
var TypeId = "~effect/Option";
var CommonProto = {
  [TypeId]: {
    _A: (_) => _
  },
  ...PipeInspectableProto,
  [Symbol.iterator]() {
    return new SingleShotGen(this);
  }
};
var SomeProto = /* @__PURE__ */ Object.defineProperty(/* @__PURE__ */ Object.assign(/* @__PURE__ */ Object.create(CommonProto), {
  _tag: "Some",
  _op: "Some",
  [symbol2](that) {
    return isOption(that) && isSome(that) && equals(this.value, that.value);
  },
  [symbol]() {
    return combine(hash(this._tag))(hash(this.value));
  },
  toString() {
    return `some(${format(this.value)})`;
  },
  toJSON() {
    return {
      _id: "Option",
      _tag: this._tag,
      value: toJson(this.value)
    };
  }
}), "valueOrUndefined", {
  get() {
    return this.value;
  }
});
var NoneHash = /* @__PURE__ */ hash("None");
var NoneProto = /* @__PURE__ */ Object.assign(/* @__PURE__ */ Object.create(CommonProto), {
  _tag: "None",
  _op: "None",
  valueOrUndefined: undefined,
  [symbol2](that) {
    return isOption(that) && isNone(that);
  },
  [symbol]() {
    return NoneHash;
  },
  toString() {
    return `none()`;
  },
  toJSON() {
    return {
      _id: "Option",
      _tag: this._tag
    };
  }
});
var isOption = (input) => hasProperty(input, TypeId);
var isNone = (fa) => fa._tag === "None";
var isSome = (fa) => fa._tag === "Some";
var none = /* @__PURE__ */ Object.create(NoneProto);
var SomeImpl = function(value) {
  this.value = value;
};
SomeImpl.prototype = SomeProto;
var some = (value) => new SomeImpl(value);

// node_modules/effect/dist/internal/result.js
var TypeId2 = "~effect/Result";
var CommonProto2 = {
  [TypeId2]: {
    _A: (_) => _,
    _E: (_) => _
  },
  ...PipeInspectableProto,
  [Symbol.iterator]() {
    return new SingleShotGen(this);
  }
};
var SuccessProto = /* @__PURE__ */ Object.assign(/* @__PURE__ */ Object.create(CommonProto2), {
  _tag: "Success",
  _op: "Success",
  [symbol2](that) {
    return isResult(that) && isSuccess(that) && equals(this.success, that.success);
  },
  [symbol]() {
    return combine(hash(this._tag))(hash(this.success));
  },
  toString() {
    return `success(${format(this.success)})`;
  },
  toJSON() {
    return {
      _id: "Result",
      _tag: this._tag,
      value: toJson(this.success)
    };
  }
});
var FailureProto = /* @__PURE__ */ Object.assign(/* @__PURE__ */ Object.create(CommonProto2), {
  _tag: "Failure",
  _op: "Failure",
  [symbol2](that) {
    return isResult(that) && isFailure(that) && equals(this.failure, that.failure);
  },
  [symbol]() {
    return combine(hash(this._tag))(hash(this.failure));
  },
  toString() {
    return `failure(${format(this.failure)})`;
  },
  toJSON() {
    return {
      _id: "Result",
      _tag: this._tag,
      failure: toJson(this.failure)
    };
  }
});
var isResult = (input) => hasProperty(input, TypeId2);
var isFailure = (result) => result._tag === "Failure";
var isSuccess = (result) => result._tag === "Success";
var FailureImpl = function(failure) {
  this.failure = failure;
};
FailureImpl.prototype = FailureProto;
var fail = (failure) => new FailureImpl(failure);
var SuccessImpl = function(success) {
  this.success = success;
};
SuccessImpl.prototype = SuccessProto;
var succeed = (success) => new SuccessImpl(success);
var getSuccess = (self) => isFailure(self) ? none : some(self.success);

// node_modules/effect/dist/Order.js
function make2(compare) {
  return (self, that) => self === that ? 0 : compare(self, that);
}
var String2 = /* @__PURE__ */ make2((self, that) => self < that ? -1 : 1);
var Number2 = /* @__PURE__ */ make2((self, that) => {
  if (globalThis.Number.isNaN(self) && globalThis.Number.isNaN(that))
    return 0;
  if (globalThis.Number.isNaN(self))
    return -1;
  if (globalThis.Number.isNaN(that))
    return 1;
  return self < that ? -1 : 1;
});
var mapInput = /* @__PURE__ */ dual(2, (self, f) => make2((b1, b2) => self(f(b1), f(b2))));
var isGreaterThan = (O) => dual(2, (self, that) => O(self, that) === 1);

// node_modules/effect/dist/Option.js
var none2 = () => none;
var some2 = some;
var isNone2 = isNone;
var isSome2 = isSome;
var match = /* @__PURE__ */ dual(2, (self, {
  onNone,
  onSome
}) => isNone2(self) ? onNone() : onSome(self.value));
var getOrElse = /* @__PURE__ */ dual(2, (self, onNone) => isNone2(self) ? onNone() : self.value);
var orElse = /* @__PURE__ */ dual(2, (self, that) => isNone2(self) ? that() : self);
var fromNullishOr = (a) => a == null ? none2() : some2(a);
var fromUndefinedOr = (a) => a === undefined ? none2() : some2(a);
var getOrUndefined = /* @__PURE__ */ getOrElse(constUndefined);
var liftThrowable = (f) => (...a) => {
  try {
    return some2(f(...a));
  } catch {
    return none2();
  }
};
var map = /* @__PURE__ */ dual(2, (self, f) => isNone2(self) ? none2() : some2(f(self.value)));
var flatMap = /* @__PURE__ */ dual(2, (self, f) => isNone2(self) ? none2() : f(self.value));
var flatMapNullishOr = /* @__PURE__ */ dual(2, (self, f) => isNone2(self) ? none2() : fromNullishOr(f(self.value)));
var filter = /* @__PURE__ */ dual(2, (self, predicate) => isNone2(self) ? none2() : predicate(self.value) ? some2(self.value) : none2());
var exists = /* @__PURE__ */ dual(2, (self, refinement) => isNone2(self) ? false : refinement(self.value));

// node_modules/effect/dist/Context.js
var ServiceTypeId = "~effect/Context/Service";
var Service = function() {
  function KeyClass() {}
  const self = KeyClass;
  Object.setPrototypeOf(self, ServiceProto);
  const init = (key, options) => {
    self.key = key;
    if (options?.defaultValue) {
      self[ReferenceTypeId] = ReferenceTypeId;
      self.defaultValue = options.defaultValue;
    }
    if (options?.make) {
      self.make = options.make;
    }
    if (options?.fiberCached) {
      cacheKeys.add(key);
    }
    return self;
  };
  return arguments.length > 0 ? init(arguments[0], arguments[1]) : init;
};
var ServiceProto = {
  [ServiceTypeId]: ServiceTypeId,
  .../* @__PURE__ */ Prototype2({
    label: "Service",
    evaluate(fiber) {
      return exitSucceed(get(fiber.context, this));
    }
  }),
  toJSON() {
    return {
      _id: "Service",
      key: this.key
    };
  },
  of(self) {
    return self;
  },
  context(self) {
    return make3(this, self);
  },
  use(f) {
    return withFiber((fiber) => f(get(fiber.context, this)));
  },
  useSync(f) {
    return withFiber((fiber) => exitSucceed(f(get(fiber.context, this))));
  }
};
var cacheKeys = /* @__PURE__ */ new Set;
var ReferenceTypeId = "~effect/Context/Reference";
var TypeId3 = "~effect/Context";
var FlattenAfterBaseHits = 8;
var makeImpl = (cacheRoot, base, overlay, depth, maxDepth) => {
  const self = Object.create(Proto);
  self.cacheRoot = cacheRoot ?? self;
  self.base = base;
  self.overlay = overlay;
  self.depth = depth;
  self._flat = undefined;
  self.baseHits = 0;
  self.maxDepth = maxDepth;
  return self;
};
var applyOverlays = (map, overlay) => {
  if (!overlay)
    return;
  applyOverlays(map, overlay.parent);
  map.set(overlay.key, overlay.value);
};
var flatten = (self) => {
  if (self._flat)
    return self._flat;
  if (!self.overlay)
    return self._flat = self.base;
  const map = new Map(self.base);
  applyOverlays(map, self.overlay);
  return self._flat = map;
};
var withFlat = (self, f) => {
  const map = new Map(self.mapUnsafe);
  f(map);
  return makeImpl(undefined, map, undefined, 0, self.maxDepth);
};
var notFound = /* @__PURE__ */ Symbol();
var lookup = (self, key) => {
  const impl = self;
  for (let overlay = impl.overlay;overlay; overlay = overlay.parent) {
    if (overlay.key === key)
      return overlay.value;
  }
  const value = impl.base.get(key);
  if (value === undefined && !impl.base.has(key))
    return notFound;
  if (impl.overlay && ++impl.baseHits >= impl.base.size && impl.baseHits >= FlattenAfterBaseHits) {
    impl.base = flatten(impl);
    impl.overlay = undefined;
    impl.depth = 0;
  }
  return value;
};
var makeUnsafe = (mapUnsafe, options) => {
  const maxDepth = options?.maxDepth;
  return makeImpl(undefined, mapUnsafe, undefined, 0, Number.isInteger(maxDepth) && maxDepth >= 0 ? maxDepth : 8);
};
var Proto = {
  get mapUnsafe() {
    return flatten(this);
  },
  ...PipeInspectableProto,
  [TypeId3]: {
    _Services: (_) => _
  },
  toJSON() {
    return {
      _id: "Context",
      services: Array.from(this.mapUnsafe).map(([key, value]) => ({
        key,
        value
      }))
    };
  },
  [symbol2](that) {
    if (!isContext(that))
      return false;
    const self = this.mapUnsafe;
    const other = that.mapUnsafe;
    if (self.size !== other.size)
      return false;
    for (const [key, value] of self) {
      if (!other.has(key) || !equals(value, other.get(key)))
        return false;
    }
    return true;
  },
  [symbol]() {
    return number(this.mapUnsafe.size);
  }
};
var hasSameCache = (self, that) => self.cacheRoot === that.cacheRoot;
var isContext = (u) => hasProperty(u, TypeId3);
var isReference = (u) => !!u[ReferenceTypeId];
var empty = () => emptyContext2;
var emptyContext2 = /* @__PURE__ */ makeUnsafe(/* @__PURE__ */ new Map);
var make3 = (key, service) => makeUnsafe(new Map([[key.key, service]]));
var add = /* @__PURE__ */ dual(3, (self, key, service) => addUnsafe(self, key.key, service));
var addUnsafe = (self, key, service) => {
  const impl = self;
  const cacheRoot = cacheKeys.has(key) ? undefined : impl.cacheRoot;
  if (impl.depth >= impl.maxDepth) {
    const map = new Map(impl._flat ?? impl.base);
    if (!impl._flat)
      applyOverlays(map, impl.overlay);
    map.set(key, service);
    return makeImpl(cacheRoot, map, undefined, 0, impl.maxDepth);
  }
  return makeImpl(cacheRoot, impl.base, {
    key,
    value: service,
    parent: impl.overlay
  }, impl.depth + 1, impl.maxDepth);
};
var getOrUndefined2 = /* @__PURE__ */ dual(2, (self, key) => getOrUndefinedUnsafe(self, key.key));
var getOrUndefinedUnsafe = (self, key) => {
  const value = lookup(self, key);
  return value === notFound ? undefined : value;
};
var getUnsafe = /* @__PURE__ */ dual(2, (self, service) => {
  const value = lookup(self, service.key);
  if (value === notFound) {
    if (isReference(service))
      return getDefaultValue(service);
    throw serviceNotFoundError(service);
  }
  return value;
});
var get = getUnsafe;
var defaultValueCacheKey = "~effect/Context/defaultValue";
var getDefaultValue = (ref) => {
  if (defaultValueCacheKey in ref) {
    return ref[defaultValueCacheKey];
  }
  return ref[defaultValueCacheKey] = ref.defaultValue();
};
var serviceNotFoundError = (service) => {
  const error = new Error(`Service not found${service.key ? `: ${String(service.key)}` : ""}`);
  if (error.stack) {
    const lines = error.stack.split(`
`);
    lines.splice(1, 3);
    error.stack = lines.join(`
`);
  }
  return error;
};
var getOption = /* @__PURE__ */ dual(2, (self, service) => {
  const value = lookup(self, service.key);
  if (value !== notFound)
    return some2(value);
  return isReference(service) ? some2(getDefaultValue(service)) : none2();
});
var merge = /* @__PURE__ */ dual(2, (self, that) => {
  if (self.mapUnsafe.size === 0)
    return that;
  if (that.mapUnsafe.size === 0)
    return self;
  return withFlat(self, (map) => that.mapUnsafe.forEach((value, key) => map.set(key, value)));
});
var mergeAll = (...ctxs) => {
  const map = new Map;
  for (let i = 0;i < ctxs.length; i++) {
    ctxs[i].mapUnsafe.forEach((value, key) => {
      map.set(key, value);
    });
  }
  return makeUnsafe(map);
};
var Reference = Service;

// node_modules/effect/dist/internal/array.js
var isArrayNonEmpty = (self) => self.length > 0;

// node_modules/effect/dist/internal/count.js
var normalize = (n) => n > 0 ? Math.floor(n) : 0;

// node_modules/effect/dist/Result.js
var succeed2 = succeed;
var fail2 = fail;
var failVoid = /* @__PURE__ */ fail2(undefined);
var try_ = (evaluate) => {
  if (isFunction(evaluate)) {
    try {
      return succeed2(evaluate());
    } catch (e) {
      return fail2(e);
    }
  } else {
    try {
      return succeed2(evaluate.try());
    } catch (e) {
      return fail2(evaluate.catch(e));
    }
  }
};
var isResult2 = isResult;
var isFailure2 = isFailure;
var isSuccess2 = isSuccess;
var getSuccess2 = getSuccess;
var mapError = /* @__PURE__ */ dual(2, (self, f) => isFailure2(self) ? fail2(f(self.failure)) : self);
var map2 = /* @__PURE__ */ dual(2, (self, f) => isSuccess2(self) ? succeed2(f(self.success)) : self);
var match2 = /* @__PURE__ */ dual(2, (self, {
  onFailure,
  onSuccess
}) => isFailure2(self) ? onFailure(self.failure) : onSuccess(self.success));
var getOrElse2 = /* @__PURE__ */ dual(2, (self, onFailure) => isFailure2(self) ? onFailure(self.failure) : self.success);
var getOrUndefined3 = /* @__PURE__ */ getOrElse2(constUndefined);
var getOrThrowWith = /* @__PURE__ */ dual(2, (self, onFailure) => {
  if (isSuccess2(self)) {
    return self.success;
  }
  throw onFailure(self.failure);
});
var getOrThrow = /* @__PURE__ */ getOrThrowWith(identity);
var flatMap2 = /* @__PURE__ */ dual(2, (self, f) => isFailure2(self) ? fail2(self.failure) : f(self.success));
var all = (input) => {
  if (Symbol.iterator in input) {
    const out = [];
    for (const e of input) {
      if (isFailure2(e)) {
        return e;
      }
      out.push(e.success);
    }
    return succeed2(out);
  }
  const out = {};
  for (const key of Object.keys(input)) {
    const e = input[key];
    if (isFailure2(e)) {
      return e;
    }
    assignProperty(out, key, e.success);
  }
  return succeed2(out);
};

// node_modules/effect/dist/Tuple.js
var makeEquivalence = Tuple;

// node_modules/effect/dist/Iterable.js
var findFirst = /* @__PURE__ */ dual(2, (self, f) => {
  let i = 0;
  for (const a of self) {
    const o = f(a, i);
    if (isBoolean(o)) {
      if (o) {
        return some2(a);
      }
    } else {
      if (isSome2(o)) {
        return o;
      }
    }
    i++;
  }
  return none2();
});
var constEmpty = {
  [Symbol.iterator]() {
    return constEmptyIterator;
  }
};
var constEmptyIterator = {
  next() {
    return {
      done: true,
      value: undefined
    };
  }
};

// node_modules/effect/dist/Record.js
var isEmptyRecord = (self) => Object.keys(self).length === 0;
var has = /* @__PURE__ */ dual(2, (self, key) => Object.hasOwn(self, key));
var map3 = /* @__PURE__ */ dual(2, (self, f) => {
  const out = {
    ...self
  };
  for (const key of keys(self)) {
    assignProperty(out, key, f(self[key], key));
  }
  return out;
});
var keys = (self) => Object.keys(self);
var isSubrecordBy = (equivalence) => dual(2, (self, that) => {
  for (const key of keys(self)) {
    if (!has(that, key) || !equivalence(self[key], that[key])) {
      return false;
    }
  }
  return true;
});
var makeEquivalence2 = (equivalence) => {
  const is = isSubrecordBy(equivalence);
  return (self, that) => is(self, that) && is(that, self);
};

// node_modules/effect/dist/Array.js
var Array2 = globalThis.Array;
var fromIterable = (collection) => Array2.isArray(collection) ? collection : Array2.from(collection);
var ensure = (self) => Array2.isArray(self) ? self : [self];
var match3 = /* @__PURE__ */ dual(2, (self, {
  onEmpty,
  onNonEmpty
}) => isReadonlyArrayNonEmpty(self) ? onNonEmpty(self) : onEmpty());
var append = /* @__PURE__ */ dual(2, (self, last) => [...self, last]);
var isArray = Array2.isArray;
var isArrayNonEmpty2 = isArrayNonEmpty;
var isReadonlyArrayNonEmpty = isArrayNonEmpty;
function isCanonicalArrayIndex(key) {
  const index = Number(key);
  return String(index) === key && Number.isInteger(index) && index >= 0 && index < 2 ** 32 - 1;
}
var findFirstIndex = /* @__PURE__ */ dual(2, (self, predicate) => {
  let i = 0;
  for (const a of self) {
    if (predicate(a, i)) {
      return some2(i);
    }
    i++;
  }
  return none2();
});
var findFirst2 = findFirst;
var findLast = /* @__PURE__ */ dual(2, (self, f) => {
  const input = fromIterable(self);
  for (let i = input.length - 1;i >= 0; i--) {
    const a = input[i];
    const o = f(a, i);
    if (typeof o === "boolean") {
      if (o) {
        return some2(a);
      }
    } else {
      if (isSome2(o)) {
        return o;
      }
    }
  }
  return none2();
});
var sort = /* @__PURE__ */ dual(2, (self, O) => {
  const out = Array2.from(self);
  out.sort(O);
  return out;
});
var empty2 = () => [];
var of = (a) => [a];
var map4 = /* @__PURE__ */ dual(2, (self, f) => self.map(f));
var filterMap = /* @__PURE__ */ dual(2, (self, f) => {
  const as = fromIterable(self);
  const out = [];
  for (let i = 0;i < as.length; i++) {
    const result = f(as[i], i);
    if (isSuccess2(result)) {
      out.push(result.success);
    }
  }
  return out;
});
var reduce = /* @__PURE__ */ dual(3, (self, b, f) => fromIterable(self).reduce((b, a, i) => f(b, a, i), b));
var makeEquivalence3 = Array_;

// node_modules/effect/dist/Duration.js
var TypeId4 = "~effect/Duration";
var bigint0 = /* @__PURE__ */ BigInt(0);
var bigint1 = /* @__PURE__ */ BigInt(1);
var bigint2 = /* @__PURE__ */ BigInt(2);
var bigint10 = /* @__PURE__ */ BigInt(10);
var bigint1e3 = /* @__PURE__ */ BigInt(1000);
var bigint1e6 = /* @__PURE__ */ BigInt(1e6);
var bigint1e9 = /* @__PURE__ */ BigInt(1e9);
var bigintNanosPerMinute = /* @__PURE__ */ BigInt(60000000000);
var bigintNanosPerHour = /* @__PURE__ */ BigInt(3600000000000);
var bigintNanosPerDay = /* @__PURE__ */ BigInt(86400000000000);
var bigintNanosPerWeek = /* @__PURE__ */ BigInt(604800000000000);
var roundTiesAwayFromZero = (input) => BigInt(Math.sign(input) * Math.round(Math.abs(input)));
var roundMillisToNanos = (millis) => Number.isInteger(millis) ? BigInt(millis) * bigint1e6 : roundTiesAwayFromZero(millis * 1e6);
var fromNanosNumber = (nanos) => make4(Number.isFinite(nanos) ? roundTiesAwayFromZero(nanos) : nanos);
var nanosToUnit = (nanos, divisor) => {
  const value = Number(nanos);
  if (Number.isFinite(value))
    return value / divisor;
  const scale = BigInt(divisor);
  return Number(nanos / scale) + Number(nanos % scale) / divisor;
};
var parseNanos = (input, scale) => {
  const decimalIndex = input.indexOf(".");
  if (decimalIndex === -1)
    return BigInt(input) * scale;
  const isNegative = input[0] === "-";
  const fractional = input.slice(decimalIndex + 1);
  const fractionalScale = bigint10 ** BigInt(fractional.length);
  const scaled = (BigInt(input.slice(isNegative ? 1 : 0, decimalIndex)) * fractionalScale + BigInt(fractional)) * scale;
  return divideTiesAwayFromZero(isNegative ? -scaled : scaled, fractionalScale);
};
var divideTiesAwayFromZero = (numerator, denominator) => {
  const absolute = numerator < bigint0 ? -numerator : numerator;
  const rounded = absolute / denominator + (absolute % denominator * bigint2 >= denominator ? bigint1 : bigint0);
  return numerator < bigint0 ? -rounded : rounded;
};
var fromMillisString = (input, scale) => {
  const millis = Number(input) * scale;
  if (!Number.isFinite(millis))
    return make4(millis);
  const nanos = parseNanos(input, BigInt(scale) * bigint1e6);
  return make4(Number.isInteger(millis) && BigInt(millis) * bigint1e6 === nanos ? millis : nanos);
};
var DURATION_REGEXP = /^(-?\d+(?:\.\d+)?)\s+(nanos?|micros?|millis?|seconds?|minutes?|hours?|days?|weeks?)$/;
var fromInputUnsafe = (input) => {
  switch (typeof input) {
    case "number":
      return millis(input);
    case "bigint":
      return nanos(input);
    case "string": {
      if (input === "Infinity") {
        return infinity;
      }
      if (input === "-Infinity") {
        return negativeInfinity;
      }
      const match = DURATION_REGEXP.exec(input);
      if (!match)
        break;
      const [_, valueStr, unit] = match;
      if (unit === "nano" || unit === "nanos") {
        return nanos(parseNanos(valueStr, bigint1));
      }
      if (unit === "micro" || unit === "micros") {
        return nanos(parseNanos(valueStr, bigint1e3));
      }
      switch (unit) {
        case "milli":
        case "millis":
          return fromMillisString(valueStr, 1);
        case "second":
        case "seconds":
          return fromMillisString(valueStr, 1000);
        case "minute":
        case "minutes":
          return fromMillisString(valueStr, 60000);
        case "hour":
        case "hours":
          return fromMillisString(valueStr, 3600000);
        case "day":
        case "days":
          return fromMillisString(valueStr, 86400000);
        case "week":
        case "weeks":
          return fromMillisString(valueStr, 604800000);
      }
      break;
    }
    case "object": {
      if (input === null)
        break;
      if (TypeId4 in input)
        return input;
      if (Array.isArray(input)) {
        if (input.length !== 2 || !input.every(isNumber)) {
          return invalid(input);
        }
        if (Number.isNaN(input[0]) || Number.isNaN(input[1])) {
          return zero;
        }
        if (input[0] === -Infinity || input[1] === -Infinity) {
          return negativeInfinity;
        }
        if (input[0] === Infinity || input[1] === Infinity) {
          return infinity;
        }
        const nanos = input[0] * 1e9 + input[1];
        return Number.isFinite(nanos) && Number.isInteger(input[0]) && Number.isInteger(input[1]) ? make4(BigInt(input[0]) * bigint1e9 + BigInt(input[1])) : fromNanosNumber(nanos);
      }
      const obj = input;
      let millis = 0;
      if (obj.weeks)
        millis += obj.weeks * 604800000;
      if (obj.days)
        millis += obj.days * 86400000;
      if (obj.hours)
        millis += obj.hours * 3600000;
      if (obj.minutes)
        millis += obj.minutes * 60000;
      if (obj.seconds)
        millis += obj.seconds * 1000;
      if (obj.milliseconds)
        millis += obj.milliseconds;
      if (!obj.microseconds && !obj.nanoseconds) {
        if (!Number.isFinite(millis))
          return make4(millis);
        const exact = sumObjectNanos(obj);
        return exact === undefined || BigInt(millis) * bigint1e6 === exact ? make4(millis) : make4(exact);
      }
      const nanos = millis * 1e6 + (obj.microseconds ?? 0) * 1000 + (obj.nanoseconds ?? 0);
      const exact = Number.isFinite(nanos) ? sumObjectNanos(obj) : undefined;
      return exact === undefined ? fromNanosNumber(nanos) : make4(exact);
    }
  }
  return invalid(input);
};
var isIntegerOrUnset = (n) => !n || Number.isInteger(n);
var sumObjectNanos = (obj) => {
  if (!isIntegerOrUnset(obj.weeks) || !isIntegerOrUnset(obj.days) || !isIntegerOrUnset(obj.hours) || !isIntegerOrUnset(obj.minutes) || !isIntegerOrUnset(obj.seconds) || !isIntegerOrUnset(obj.milliseconds) || !isIntegerOrUnset(obj.microseconds) || !isIntegerOrUnset(obj.nanoseconds))
    return;
  let nanos = bigint0;
  if (obj.weeks)
    nanos += BigInt(obj.weeks) * bigintNanosPerWeek;
  if (obj.days)
    nanos += BigInt(obj.days) * bigintNanosPerDay;
  if (obj.hours)
    nanos += BigInt(obj.hours) * bigintNanosPerHour;
  if (obj.minutes)
    nanos += BigInt(obj.minutes) * bigintNanosPerMinute;
  if (obj.seconds)
    nanos += BigInt(obj.seconds) * bigint1e9;
  if (obj.milliseconds)
    nanos += BigInt(obj.milliseconds) * bigint1e6;
  if (obj.microseconds)
    nanos += BigInt(obj.microseconds) * bigint1e3;
  if (obj.nanoseconds)
    nanos += BigInt(obj.nanoseconds);
  return nanos;
};
var invalid = (input) => {
  throw new Error(`Invalid Input: ${input}`);
};
var zeroDurationValue = {
  _tag: "Millis",
  millis: 0
};
var infinityDurationValue = {
  _tag: "Infinity"
};
var negativeInfinityDurationValue = {
  _tag: "NegativeInfinity"
};
var DurationProto = {
  [TypeId4]: TypeId4,
  [symbol]() {
    switch (this.value._tag) {
      case "Millis":
        return hash(roundMillisToNanos(this.value.millis));
      case "Nanos":
        return hash(this.value.nanos);
      default:
        return structure(this.value);
    }
  },
  [symbol2](that) {
    return isDuration(that) && equals2(this, that);
  },
  toString() {
    switch (this.value._tag) {
      case "Infinity":
        return "Infinity";
      case "NegativeInfinity":
        return "-Infinity";
      case "Nanos":
        return `${this.value.nanos} nanos`;
      case "Millis":
        return `${this.value.millis} millis`;
    }
  },
  toJSON() {
    switch (this.value._tag) {
      case "Millis":
        return {
          _id: "Duration",
          _tag: "Millis",
          millis: this.value.millis
        };
      case "Nanos":
        return {
          _id: "Duration",
          _tag: "Nanos",
          nanos: String(this.value.nanos)
        };
      case "Infinity":
        return {
          _id: "Duration",
          _tag: "Infinity"
        };
      case "NegativeInfinity":
        return {
          _id: "Duration",
          _tag: "NegativeInfinity"
        };
    }
  },
  [NodeInspectSymbol]() {
    return this.toJSON();
  },
  pipe() {
    return pipeArguments(this, arguments);
  }
};
var make4 = (input) => {
  const duration = Object.create(DurationProto);
  if (typeof input === "number") {
    if (isNaN(input) || input === 0 || Object.is(input, -0)) {
      duration.value = zeroDurationValue;
    } else if (!Number.isFinite(input)) {
      duration.value = input > 0 ? infinityDurationValue : negativeInfinityDurationValue;
    } else if (!Number.isInteger(input)) {
      duration.value = {
        _tag: "Nanos",
        nanos: roundMillisToNanos(input)
      };
    } else {
      duration.value = {
        _tag: "Millis",
        millis: input
      };
    }
  } else if (input === bigint0) {
    duration.value = zeroDurationValue;
  } else {
    duration.value = {
      _tag: "Nanos",
      nanos: input
    };
  }
  return duration;
};
var isDuration = (u) => hasProperty(u, TypeId4);
var isFinite = (self) => self.value._tag !== "Infinity" && self.value._tag !== "NegativeInfinity";
var zero = /* @__PURE__ */ make4(0);
var infinity = /* @__PURE__ */ make4(Infinity);
var negativeInfinity = /* @__PURE__ */ make4(-Infinity);
var nanos = (nanos) => make4(nanos);
var millis = (millis) => make4(millis);
var seconds = (seconds) => make4(seconds * 1000);
var toMillis = (self) => match4(fromInputUnsafe(self), {
  onMillis: identity,
  onNanos: (nanos) => nanosToUnit(nanos, 1e6),
  onInfinity: () => Infinity,
  onNegativeInfinity: () => -Infinity
});
var toSeconds = (self) => match4(fromInputUnsafe(self), {
  onMillis: (millis) => millis / 1000,
  onNanos: (nanos) => nanosToUnit(nanos, 1e9),
  onInfinity: () => Infinity,
  onNegativeInfinity: () => -Infinity
});
var toNanosUnsafe = (input) => {
  const self = fromInputUnsafe(input);
  switch (self.value._tag) {
    case "Infinity":
    case "NegativeInfinity":
      throw new Error("Cannot convert infinite duration to nanos");
    case "Nanos":
      return self.value.nanos;
    case "Millis":
      return roundMillisToNanos(self.value.millis);
  }
};
var match4 = /* @__PURE__ */ dual(2, (self, options) => {
  switch (self.value._tag) {
    case "Millis":
      return options.onMillis(self.value.millis);
    case "Nanos":
      return options.onNanos(self.value.nanos);
    case "Infinity":
      return options.onInfinity();
    case "NegativeInfinity":
      return (options.onNegativeInfinity ?? options.onInfinity)();
  }
});
var matchPair = /* @__PURE__ */ dual(3, (self, that, options) => {
  if (self.value._tag === "Infinity" || self.value._tag === "NegativeInfinity" || that.value._tag === "Infinity" || that.value._tag === "NegativeInfinity")
    return options.onInfinity(self, that);
  if (self.value._tag === "Millis") {
    return that.value._tag === "Millis" ? options.onMillis(self.value.millis, that.value.millis) : options.onNanos(toNanosUnsafe(self), that.value.nanos);
  } else {
    return options.onNanos(self.value.nanos, toNanosUnsafe(that));
  }
});
var Equivalence = (self, that) => matchPair(self, that, {
  onMillis: (self, that) => self === that,
  onNanos: (self, that) => self === that,
  onInfinity: (self, that) => self.value._tag === that.value._tag
});
var equals2 = /* @__PURE__ */ dual(2, (self, that) => Equivalence(self, that));

// node_modules/effect/dist/Scheduler.js
var Scheduler = /* @__PURE__ */ Reference("effect/Scheduler", {
  fiberCached: true,
  defaultValue: () => new MixedScheduler
});
var setMicrotask = (f) => {
  let cancelled = false;
  Promise.resolve().then(() => {
    if (!cancelled)
      f();
  });
  return () => {
    cancelled = true;
  };
};
var setTimer = "setImmediate" in globalThis ? (f) => {
  const timer = globalThis.setImmediate(f);
  return () => globalThis.clearImmediate(timer);
} : (f) => {
  const timer = setTimeout(f, 0);
  return () => clearTimeout(timer);
};
var setImmediate2 = (f) => {
  try {
    return setTimer(f);
  } catch {
    return setMicrotask(f);
  }
};

class PriorityBuckets {
  buckets = [];
  scheduleTask(task, priority) {
    const buckets = this.buckets;
    const len = buckets.length;
    let bucket;
    let index = 0;
    for (;index < len; index++) {
      if (buckets[index][0] > priority)
        break;
      bucket = buckets[index];
    }
    if (bucket && bucket[0] === priority) {
      bucket[1].push(task);
    } else if (index === len) {
      buckets.push([priority, [task]]);
    } else {
      buckets.splice(index, 0, [priority, [task]]);
    }
  }
  drain() {
    const buckets = this.buckets;
    this.buckets = [];
    return buckets;
  }
}

class MixedScheduler {
  executionMode;
  setImmediate;
  constructor(executionMode = "async", setImmediateFn) {
    this.executionMode = executionMode;
    this.setImmediate = setImmediateFn ?? (executionMode === "sync" ? setMicrotask : setImmediate2);
  }
  shouldYield(fiber) {
    return fiber.currentOpCount >= fiber.cache.maxOpsBeforeYield;
  }
  makeDispatcher() {
    return new MixedSchedulerDispatcher(this.setImmediate);
  }
}

class MixedSchedulerDispatcher {
  tasks = /* @__PURE__ */ new PriorityBuckets;
  running = undefined;
  setImmediate;
  constructor(setImmediateFn = setImmediate2) {
    this.setImmediate = setImmediateFn;
  }
  scheduleTask(task, priority) {
    this.tasks.scheduleTask(task, priority);
    if (this.running === undefined) {
      this.running = this.setImmediate(this.afterScheduled);
    }
  }
  afterScheduled = () => {
    this.running = undefined;
    this.runTasks();
  };
  runTasks() {
    const buckets = this.tasks.drain();
    for (let i = 0;i < buckets.length; i++) {
      const toRun = buckets[i][1];
      for (let j = 0;j < toRun.length; j++) {
        toRun[j]();
      }
    }
  }
  flush() {
    while (this.tasks.buckets.length > 0) {
      if (this.running !== undefined) {
        this.running();
        this.running = undefined;
      }
      this.runTasks();
    }
  }
}
var MaxOpsBeforeYield = /* @__PURE__ */ Reference("effect/Scheduler/MaxOpsBeforeYield", {
  fiberCached: true,
  defaultValue: () => 2048
});
var PreventSchedulerYield = /* @__PURE__ */ Reference("effect/Scheduler/PreventSchedulerYield", {
  fiberCached: true,
  defaultValue: () => false
});

// node_modules/effect/dist/Data.js
var taggedEnum = () => new Proxy({}, {
  get(_target, tag, _receiver) {
    if (tag === "$is") {
      return isTagged;
    } else if (tag === "$match") {
      return taggedMatch;
    }
    return (props) => ({
      ...props,
      _tag: tag
    });
  }
});
function taggedMatch() {
  if (arguments.length === 1) {
    const cases = arguments[0];
    return function(value) {
      return cases[value._tag](value);
    };
  }
  const value = arguments[0];
  const cases = arguments[1];
  return cases[value._tag](value);
}
var Error3 = Error2;
var TaggedError2 = TaggedError;

// node_modules/effect/dist/encoding/EncodingError.js
var EncodingErrorTypeId = "~effect/encoding/EncodingError";

class EncodingError extends (/* @__PURE__ */ TaggedError2("EncodingError")) {
  [EncodingErrorTypeId] = EncodingErrorTypeId;
}

// node_modules/effect/dist/encoding/Hex.js
var encode = (input) => typeof input === "string" ? hexEncodeUint8Array(encoder.encode(input)) : hexEncodeUint8Array(input);
var random2 = (length) => {
  switch (length) {
    case 16:
      return random16();
    case 32:
      return random32();
    default: {
      let result = "";
      for (let i = length >>> 3;i > 0; i--) {
        result += random8();
      }
      return result;
    }
  }
};
var hexCharCodes = /* @__PURE__ */ Uint8Array.from("0123456789abcdef", (c) => c.charCodeAt(0));
var randomWord = () => Math.random() * 4294967296 >>> 0;
var random8 = () => {
  const a = randomWord();
  return String.fromCharCode(hexCharCodes[a >>> 28], hexCharCodes[a >>> 24 & 15], hexCharCodes[a >>> 20 & 15], hexCharCodes[a >>> 16 & 15], hexCharCodes[a >>> 12 & 15], hexCharCodes[a >>> 8 & 15], hexCharCodes[a >>> 4 & 15], hexCharCodes[a & 15]);
};
var random16 = () => {
  const a = randomWord();
  const b = randomWord();
  return String.fromCharCode(hexCharCodes[a >>> 28], hexCharCodes[a >>> 24 & 15], hexCharCodes[a >>> 20 & 15], hexCharCodes[a >>> 16 & 15], hexCharCodes[a >>> 12 & 15], hexCharCodes[a >>> 8 & 15], hexCharCodes[a >>> 4 & 15], hexCharCodes[a & 15], hexCharCodes[b >>> 28], hexCharCodes[b >>> 24 & 15], hexCharCodes[b >>> 20 & 15], hexCharCodes[b >>> 16 & 15], hexCharCodes[b >>> 12 & 15], hexCharCodes[b >>> 8 & 15], hexCharCodes[b >>> 4 & 15], hexCharCodes[b & 15]);
};
var random32 = () => {
  const a = randomWord();
  const b = randomWord();
  const c = randomWord();
  const d = randomWord();
  return String.fromCharCode(hexCharCodes[a >>> 28], hexCharCodes[a >>> 24 & 15], hexCharCodes[a >>> 20 & 15], hexCharCodes[a >>> 16 & 15], hexCharCodes[a >>> 12 & 15], hexCharCodes[a >>> 8 & 15], hexCharCodes[a >>> 4 & 15], hexCharCodes[a & 15], hexCharCodes[b >>> 28], hexCharCodes[b >>> 24 & 15], hexCharCodes[b >>> 20 & 15], hexCharCodes[b >>> 16 & 15], hexCharCodes[b >>> 12 & 15], hexCharCodes[b >>> 8 & 15], hexCharCodes[b >>> 4 & 15], hexCharCodes[b & 15], hexCharCodes[c >>> 28], hexCharCodes[c >>> 24 & 15], hexCharCodes[c >>> 20 & 15], hexCharCodes[c >>> 16 & 15], hexCharCodes[c >>> 12 & 15], hexCharCodes[c >>> 8 & 15], hexCharCodes[c >>> 4 & 15], hexCharCodes[c & 15], hexCharCodes[d >>> 28], hexCharCodes[d >>> 24 & 15], hexCharCodes[d >>> 20 & 15], hexCharCodes[d >>> 16 & 15], hexCharCodes[d >>> 12 & 15], hexCharCodes[d >>> 8 & 15], hexCharCodes[d >>> 4 & 15], hexCharCodes[d & 15]);
};
var encoder = /* @__PURE__ */ new TextEncoder;
var byteToHex = /* @__PURE__ */ Array.from({
  length: 256
}, (_, i) => i.toString(16).padStart(2, "0"));
var hexEncodeUint8Array = (bytes) => {
  let result = "";
  for (let i = 0;i < bytes.length; i++) {
    result += byteToHex[bytes[i]];
  }
  return result;
};

// node_modules/effect/dist/Tracer.js
var ParentSpanKey = "effect/Tracer/ParentSpan";

class ParentSpan extends (/* @__PURE__ */ Service()(ParentSpanKey, {
  fiberCached: true
})) {
}
var make5 = (options) => options;
var externalSpan = (options) => ({
  _tag: "ExternalSpan",
  spanId: options.spanId,
  traceId: options.traceId,
  sampled: options.sampled ?? true,
  annotations: options.annotations ?? empty()
});
var DisablePropagation = /* @__PURE__ */ Reference("effect/Tracer/DisablePropagation", {
  defaultValue: constFalse
});
var CurrentTraceLevel = /* @__PURE__ */ Reference("effect/Tracer/CurrentTraceLevel", {
  defaultValue: () => "Info"
});
var MinimumTraceLevel = /* @__PURE__ */ Reference("effect/Tracer/MinimumTraceLevel", {
  defaultValue: () => "All"
});
var TracerKey = "effect/Tracer";
var Tracer = /* @__PURE__ */ Reference(TracerKey, {
  fiberCached: true,
  defaultValue: () => nativeTracer
});
var nativeTracer = /* @__PURE__ */ make5({
  span: (options) => new NativeSpan(options)
});

class NativeSpan {
  _tag = "Span";
  sampled;
  name;
  parent;
  annotations;
  links;
  startTime;
  kind;
  status;
  _traceId = undefined;
  _spanId = undefined;
  _attributes = undefined;
  _events = undefined;
  constructor(options) {
    this.name = options.name;
    this.parent = options.parent;
    this.annotations = options.annotations;
    this.links = options.links;
    this.startTime = options.startTime;
    this.kind = options.kind;
    this.sampled = options.sampled;
    this.status = {
      _tag: "Started",
      startTime: options.startTime
    };
  }
  get traceId() {
    return this._traceId ??= getOrUndefined(this.parent)?.traceId ?? random2(32);
  }
  get spanId() {
    return this._spanId ??= random2(16);
  }
  get attributes() {
    return this._attributes ??= new Map;
  }
  get events() {
    return this._events ??= [];
  }
  end(endTime, exit) {
    if (this.status._tag === "Ended")
      return;
    this.status = {
      _tag: "Ended",
      endTime,
      exit,
      startTime: this.status.startTime
    };
  }
  attribute(key, value) {
    if (this.status._tag === "Ended")
      return;
    this.attributes.set(key, value);
  }
  event(name, startTime, attributes) {
    if (this.status._tag === "Ended")
      return;
    this.events.push([name, startTime, attributes ?? {}]);
  }
  addLinks(links) {
    if (this.status._tag === "Ended")
      return;
    this.links.push(...links);
  }
}

// node_modules/effect/dist/internal/metric.js
var FiberRuntimeMetricsKey = "effect/Metric/FiberRuntimeMetrics";

// node_modules/effect/dist/internal/references.js
var CurrentErrorReporters = /* @__PURE__ */ Reference("effect/ErrorReporter/CurrentErrorReporters", {
  defaultValue: () => new Set
});
var CurrentStackFrame = /* @__PURE__ */ Reference("effect/References/CurrentStackFrame", {
  fiberCached: true,
  defaultValue: constUndefined
});
var TracerEnabled = /* @__PURE__ */ Reference("effect/References/TracerEnabled", {
  fiberCached: true,
  defaultValue: constTrue
});
var TracerTimingEnabled = /* @__PURE__ */ Reference("effect/References/TracerTimingEnabled", {
  defaultValue: constTrue
});
var TracerSpanAnnotations = /* @__PURE__ */ Reference("effect/References/TracerSpanAnnotations", {
  defaultValue: () => ({})
});
var TracerSpanLinks = /* @__PURE__ */ Reference("effect/References/TracerSpanLinks", {
  defaultValue: () => []
});
var CurrentLogAnnotations = /* @__PURE__ */ Reference("effect/References/CurrentLogAnnotations", {
  defaultValue: () => ({})
});
var CurrentLogLevel = /* @__PURE__ */ Reference("effect/References/CurrentLogLevel", {
  fiberCached: true,
  defaultValue: () => "Info"
});
var MinimumLogLevel = /* @__PURE__ */ Reference("effect/References/MinimumLogLevel", {
  fiberCached: true,
  defaultValue: () => "Info"
});
var UnhandledLogLevel = /* @__PURE__ */ Reference("effect/References/UnhandledLogLevel", {
  defaultValue: () => "Error"
});
var CurrentLogSpans = /* @__PURE__ */ Reference("effect/References/CurrentLogSpans", {
  defaultValue: () => []
});

// node_modules/effect/dist/internal/tracer.js
var addSpanStackTrace = (options) => {
  if (options?.captureStackTrace === false) {
    return options;
  } else if (options?.captureStackTrace !== undefined && typeof options.captureStackTrace !== "boolean") {
    return options;
  }
  const limit = getStackTraceLimit();
  if (limit === 0 && options?.captureStackTrace !== true) {
    return {
      ...options,
      captureStackTrace: false
    };
  }
  setStackTraceLimit(3);
  const traceError = new Error;
  setStackTraceLimit(limit);
  return {
    ...options,
    captureStackTrace: spanCleaner(() => traceError.stack)
  };
};
var makeStackCleaner = (line) => (stack) => {
  let cache;
  return () => {
    if (cache !== undefined)
      return cache;
    const trace = stack();
    if (!trace)
      return;
    const lines = trace.split(`
`);
    if (lines[line] !== undefined) {
      cache = lines[line].trim();
      return cache;
    }
  };
};
var spanCleaner = /* @__PURE__ */ makeStackCleaner(3);

// node_modules/effect/dist/internal/effect.js
class Interrupt extends ReasonBase {
  constructor(fiberId, annotations = constEmptyAnnotations) {
    super("Interrupt", annotations, "Interrupted");
    this.fiberId = fiberId;
  }
  toString() {
    return `Interrupt(${this.fiberId})`;
  }
  toJSON() {
    return {
      _tag: "Interrupt",
      fiberId: this.fiberId
    };
  }
  [symbol2](that) {
    return isInterruptReason(that) && this.fiberId === that.fiberId && this.annotations === that.annotations;
  }
  [symbol]() {
    return string(`${this._tag}:${this.fiberId}`);
  }
}
var causeInterrupt = (fiberId) => new CauseImpl([new Interrupt(fiberId)]);
var hasFails = (self) => self.reasons.some(isFailReason);
var findError = (self) => {
  for (let i = 0;i < self.reasons.length; i++) {
    const reason = self.reasons[i];
    if (reason._tag === "Fail") {
      return succeed2(reason.error);
    }
  }
  return fail2(self);
};
var hasDies = (self) => self.reasons.some(isDieReason);
var causeElectDefects = (self) => hasDies(self) && hasFails(self) ? causeFromReasons(self.reasons.map((reason) => reason._tag === "Fail" && !isDone(reason.error) ? new Die(reason.error, reason.annotations) : reason)) : self;
var findRecoverableError = (self) => hasDies(self) ? fail2(causeElectDefects(self)) : findError(self);
var hasInterrupts = (self) => self.reasons.some(isInterruptReason);
var causeFilterInterruptors = (self) => {
  let interruptors;
  for (let i = 0;i < self.reasons.length; i++) {
    const f = self.reasons[i];
    if (f._tag !== "Interrupt")
      continue;
    interruptors ??= new Set;
    if (f.fiberId !== undefined) {
      interruptors.add(f.fiberId);
    }
  }
  return interruptors ? succeed2(interruptors) : fail2(self);
};
var hasInterruptsOnly = (self) => self.reasons.length > 0 && self.reasons.every(isInterruptReason);
var reasonAnnotations = (self) => makeUnsafe(self.annotations);
var causeMap = /* @__PURE__ */ dual(2, (self, f) => {
  let hasFail = false;
  const failures = self.reasons.map((failure) => {
    if (isFailReason(failure)) {
      hasFail = true;
      return new Fail(f(failure.error), failure.annotations);
    }
    return failure;
  });
  return hasFail ? causeFromReasons(failures) : self;
});
var causeSquash = (self) => {
  let die;
  for (const reason of self.reasons) {
    if (reason._tag === "Fail")
      return reason.error;
    if (reason._tag === "Die")
      die ??= reason;
  }
  if (die !== undefined)
    return die.defect;
  if (self.reasons.length > 0)
    return new globalThis.Error("All fibers interrupted without error");
  return new globalThis.Error("Empty cause");
};
var causePrettyErrors = (self, options) => {
  const errors = [];
  const interrupts = [];
  if (self.reasons.length === 0)
    return errors;
  const prevStackLimit = getStackTraceLimit();
  if (prevStackLimit !== 0)
    setStackTraceLimit(1);
  try {
    for (const failure of self.reasons) {
      if (failure._tag === "Interrupt") {
        interrupts.push(failure);
        continue;
      }
      errors.push(causePrettyError(failure._tag === "Die" ? failure.defect : failure.error, failure.annotations, options));
    }
    if (errors.length === 0) {
      const cause = new Error("The fiber was interrupted by:");
      cause.name = "InterruptCause";
      cause.stack = interruptCauseStack(cause, interrupts);
      const error = new globalThis.Error("All fibers interrupted without error", {
        cause
      });
      error.name = "InterruptError";
      error.stack = `${error.name}: ${error.message}`;
      errors.push(causePrettyError(error, interrupts[0].annotations, options));
    }
  } finally {
    if (prevStackLimit !== 0)
      setStackTraceLimit(prevStackLimit);
  }
  return errors;
};
var causePrettyError = (original, annotations, options, seen = new Set) => {
  const kind = typeof original;
  let error;
  if (original && kind === "object") {
    seen.add(original);
    const cause = original.cause;
    error = new globalThis.Error(causePrettyMessage(original), {
      cause: cause !== undefined && !seen.has(cause) ? causePrettyError(cause, undefined, undefined, seen) : undefined
    });
    if (typeof original.name === "string") {
      error.name = original.name;
    }
    if (typeof original.stack === "string") {
      error.stack = cleanErrorStack(original.stack, error, annotations);
    } else {
      const stack = `${error.name}: ${error.message}`;
      error.stack = annotations ? addStackAnnotations(stack, annotations) : stack;
    }
    if (options?.includeCauseInStack) {
      error.stack = renderPrettyError(error);
    }
    for (const key of Object.keys(original)) {
      if (!(key in error)) {
        error[key] = original[key];
      }
    }
  } else {
    error = new globalThis.Error(!original ? `Unknown error: ${original}` : kind === "string" ? original : formatJson(original));
    const stack = `${error.name}: ${error.message}`;
    error.stack = annotations ? addStackAnnotations(stack, annotations) : stack;
  }
  return error;
};
var causePrettyMessage = (u) => {
  if (typeof u.message === "string") {
    return u.message;
  } else if (typeof u.toString === "function" && u.toString !== Object.prototype.toString && u.toString !== Array.prototype.toString) {
    try {
      return u.toString();
    } catch {}
  }
  return formatJson(u);
};
var locationRegExp = /\((.*)\)/g;
var cleanErrorStack = (stack, error, annotations) => {
  const message = `${error.name}: ${error.message}`;
  const lines = (stack.startsWith(message) ? stack.slice(message.length) : stack).split(`
`);
  const out = [message];
  for (let i = 1;i < lines.length; i++) {
    if (/Generator\.next|~effect\/(?:Effect|Utils)/.test(lines[i])) {
      break;
    }
    out.push(lines[i]);
  }
  return annotations ? addStackAnnotations(out.join(`
`), annotations) : out.join(`
`);
};
var addStackAnnotations = (stack, annotations) => {
  const frame = annotations?.get(StackTraceKey.key);
  if (frame) {
    stack = `${stack}
${currentStackTrace(frame)}`;
  }
  return stack;
};
var interruptCauseStack = (error, interrupts) => {
  const out = [`${error.name}: ${error.message}`];
  for (const current of interrupts) {
    const fiberId = current.fiberId !== undefined ? `#${current.fiberId}` : "unknown";
    const frame = current.annotations.get(InterruptorStackTrace.key);
    out.push(`    at fiber (${fiberId})`);
    if (frame)
      out.push(currentStackTrace(frame));
  }
  return out.join(`
`);
};
var currentStackTrace = (frame) => {
  const out = [];
  let current = frame;
  let i = 0;
  while (current && i < 10) {
    const stack = current.stack();
    if (stack) {
      const locationMatchAll = stack.matchAll(locationRegExp);
      let match = false;
      for (const [, location2] of locationMatchAll) {
        match = true;
        out.push(`    at ${current.name} (${location2})`);
      }
      if (!match) {
        out.push(`    at ${current.name} (${stack.replace(/^at /, "")})`);
      }
    } else {
      out.push(`    at ${current.name}`);
    }
    current = current.parent;
    i++;
  }
  return out.join(`
`);
};
var causePretty = (cause) => causePrettyErrors(cause).map(renderPrettyError).join(`
`);
var renderPrettyError = (e) => e.cause ? `${e.stack} {
${renderErrorCause(e.cause, "  ")}
}` : e.stack;
var renderErrorCause = (cause, prefix) => {
  const lines = cause.stack.split(`
`);
  let stack = `${prefix}[cause]: ${lines[0]}`;
  for (let i = 1, len = lines.length;i < len; i++) {
    stack += `
${prefix}${lines[i]}`;
  }
  if (cause.cause) {
    stack += ` {
${renderErrorCause(cause.cause, `${prefix}  `)}
${prefix}}`;
  }
  return stack;
};
var FiberTypeId = "~effect/Fiber";
var fiberVariance = {
  _A: identity,
  _E: identity
};
var fiberIdStore = {
  id: 0
};
var AsyncResource = /* @__PURE__ */ (() => {
  try {
    return globalThis.process?.getBuiltinModule?.("node:async_hooks")?.AsyncResource;
  } catch {
    return;
  }
})();
var captureAsyncContext = () => AsyncResource === undefined ? undefined : new AsyncResource("effect/Fiber");
var getCurrentFiber = () => globalThis[currentFiberTypeId];

class FiberImpl {
  constructor(context, interruptible = true) {
    this.setContext(context);
    this.id = ++fiberIdStore.id;
    this.currentOpCount = 0;
    this.interruptible = interruptible;
    this._stack = [];
    this._observers = undefined;
    this._exit = undefined;
    this._children = undefined;
    this._interruptedCause = undefined;
    this._yielded = undefined;
    this._running = false;
    this._deferredInterrupt = false;
    this._parent = undefined;
    this._asyncContext = undefined;
    this.cache.runtimeMetrics?.recordFiberStart(this.context);
  }
  get [FiberTypeId]() {
    return fiberVariance;
  }
  get currentDispatcher() {
    return this._dispatcher ??= this.cache.scheduler.makeDispatcher();
  }
  getRef(ref) {
    return get(this.context, ref);
  }
  addObserver(cb) {
    if (this._exit) {
      cb(this._exit);
      return constVoid;
    }
    if (this._observers === undefined) {
      this._observers = [cb];
    } else {
      this._observers.push(cb);
    }
    return () => this.removeObserver(cb);
  }
  removeObserver(cb) {
    if (this._exit || this._observers === undefined)
      return;
    const index = this._observers.indexOf(cb);
    if (index >= 0) {
      this._observers.splice(index, 1);
    }
  }
  interruptUnsafe(fiberId, annotations) {
    if (this._exit) {
      return;
    }
    let cause = causeInterrupt(fiberId);
    if (this.cache.stackFrame) {
      cause = causeAnnotate(cause, make3(StackTraceKey, this.cache.stackFrame));
    }
    if (annotations) {
      cause = causeAnnotate(cause, annotations);
    }
    this._interruptedCause = this._interruptedCause ? causeAddInterrupts(this._interruptedCause, cause) : cause;
    if (this.interruptible) {
      if (this._running) {
        this._deferredInterrupt = true;
      } else {
        this.evaluate(failCause(this._interruptedCause));
      }
    }
  }
  pollUnsafe() {
    return this._exit;
  }
  evaluate(effect) {
    if (this._exit) {
      return;
    } else if (this._asyncContext !== undefined) {
      const asyncContext = this._asyncContext;
      this._asyncContext = undefined;
      try {
        return asyncContext.runInAsyncScope(this.evaluate, this, effect);
      } finally {
        asyncContext.emitDestroy();
      }
    } else if (this._yielded !== undefined) {
      const yielded = this._yielded;
      this._yielded = undefined;
      yielded();
    }
    const exit = this.runLoop(effect);
    if (exit === Yield) {
      this._asyncContext = captureAsyncContext();
      return;
    }
    const interruptChildren = fiberMiddleware.interruptChildren && fiberMiddleware.interruptChildren(this);
    if (interruptChildren !== undefined) {
      return this.evaluate(flatMap3(interruptChildren, () => exit));
    }
    this._exit = exit;
    this.cache.runtimeMetrics?.recordFiberEnd(this.context, this._exit);
    if (this._parent) {
      this._parent._children?.delete(this);
      this._parent = undefined;
    }
    if (this._observers !== undefined) {
      const observers = this._observers;
      this._observers = undefined;
      for (let i = 0;i < observers.length; i++) {
        observers[i](exit);
      }
    }
    this._stack.length = 0;
    this._children = undefined;
    this.context = empty();
    const cache = this.cache;
    if (cache.span !== undefined || cache.stackFrame !== undefined) {
      this.cache = {
        ...cache,
        span: undefined,
        stackFrame: undefined
      };
    }
  }
  runLoop(effect) {
    const prevFiber = globalThis[currentFiberTypeId];
    globalThis[currentFiberTypeId] = this;
    const prevRunning = this._running;
    this._running = true;
    let yielding = false;
    let current = effect;
    this.currentOpCount = 0;
    try {
      while (true) {
        try {
          if (this._deferredInterrupt) {
            this._deferredInterrupt = false;
            current = failCause(this._interruptedCause);
          }
          this.currentOpCount++;
          const cache = this.cache;
          if (!yielding && !cache.preventYield && cache.scheduler.shouldYield(this)) {
            yielding = true;
            const prev = current;
            current = flatMap3(yieldNow, () => prev);
          }
          current = cache.tracerContext ? cache.tracerContext(current, this) : current[evaluate](this);
          if (current === Yield) {
            const yielded = this._yielded;
            if (ExitTypeId in yielded) {
              this._deferredInterrupt = false;
              this._yielded = undefined;
              return yielded;
            } else if (this._deferredInterrupt) {
              this._yielded = undefined;
              yielded();
              continue;
            }
            return Yield;
          }
        } catch (error) {
          if (!hasProperty(current, evaluate)) {
            return exitDie(`Fiber.runLoop: Not a valid effect: ${String(current)}`);
          }
          current = exitDie(error);
        }
      }
    } finally {
      this._running = prevRunning;
      globalThis[currentFiberTypeId] = prevFiber;
    }
  }
  getCont(symbol) {
    if (this._deferredInterrupt) {
      this._deferredInterrupt = false;
      return deferredInterruptCont;
    }
    while (true) {
      const op = this._stack.pop();
      if (!op)
        return;
      const all = op[contAll];
      if (all !== undefined) {
        const cont = all.call(op, this);
        if (cont) {
          cont[symbol] = cont;
          return cont;
        }
      }
      if (op[symbol])
        return op;
    }
  }
  succeedWith(value) {
    if ((++this.currentOpCount & maxInlineSteps - 1) === 0) {
      return exitSucceed(value);
    }
    const cont = this.getCont(contA);
    return cont ? cont[contA](value, this) : this.yieldWith(exitSucceed(value));
  }
  yieldWith(value) {
    this._yielded = value;
    return Yield;
  }
  children() {
    return this._children ??= new Set;
  }
  pipe() {
    return pipeArguments(this, arguments);
  }
  setContext(context) {
    const previous = this.context;
    this.context = context;
    if (previous !== undefined && hasSameCache(previous, context))
      return;
    const root = context.cacheRoot;
    const cache = root._fiberCache ??= makeFiberContextCache(context);
    if (this.cache?.scheduler !== cache.scheduler) {
      this._dispatcher = undefined;
    }
    this.cache = cache;
  }
  get currentSpanLocal() {
    const span = this.cache.span;
    return span?._tag === "Span" ? span : undefined;
  }
}
var makeFiberContextCache = (context) => {
  const currentTracer = getOrUndefinedUnsafe(context, TracerKey);
  return {
    scheduler: get(context, Scheduler),
    tracer: currentTracer,
    tracerContext: currentTracer ? currentTracer["context"] : undefined,
    tracerEnabled: get(context, TracerEnabled),
    span: getOrUndefinedUnsafe(context, ParentSpanKey),
    logLevel: get(context, CurrentLogLevel),
    minimumLogLevel: get(context, MinimumLogLevel),
    stackFrame: get(context, CurrentStackFrame),
    runtimeMetrics: getOrUndefinedUnsafe(context, FiberRuntimeMetricsKey),
    maxOpsBeforeYield: get(context, MaxOpsBeforeYield),
    preventYield: get(context, PreventSchedulerYield)
  };
};
var deferredInterruptCont = {
  [contA](_value, fiber) {
    return failCause(fiber._interruptedCause);
  },
  [contE](_cause, fiber) {
    return failCause(fiber._interruptedCause);
  }
};
var maxInlineSteps = 32;
var fiberMiddleware = {
  interruptChildren: undefined
};
var fiberStackAnnotations = (fiber) => {
  if (!fiber.cache.stackFrame)
    return;
  const annotations = new Map;
  annotations.set(InterruptorStackTrace.key, fiber.cache.stackFrame);
  return makeUnsafe(annotations);
};
var fiberInterruptChildren = (fiber) => {
  if (fiber._children === undefined || fiber._children.size === 0) {
    return;
  }
  return fiberInterruptAll(fiber._children);
};
var fiberAwait = (self) => {
  const impl = self;
  if (impl._exit)
    return succeed3(impl._exit);
  return callback((resume) => {
    if (impl._exit)
      return resume(succeed3(impl._exit));
    return sync(self.addObserver((exit) => resume(succeed3(exit))));
  });
};
var fiberAwaitAll = (self) => callback((resume) => {
  const iter = self[Symbol.iterator]();
  const exits = [];
  let cancel = undefined;
  function loop() {
    let result = iter.next();
    while (!result.done) {
      if (result.value._exit) {
        exits.push(result.value._exit);
        result = iter.next();
        continue;
      }
      cancel = result.value.addObserver((exit) => {
        exits.push(exit);
        loop();
      });
      return;
    }
    resume(succeed3(exits));
  }
  loop();
  return sync(() => cancel?.());
});
var fiberJoin = (self) => {
  const impl = self;
  if (impl._exit)
    return impl._exit;
  return callback((resume) => {
    if (impl._exit)
      return resume(impl._exit);
    return sync(self.addObserver(resume));
  });
};
var fiberInterrupt = (self) => withFiber((fiber) => fiberInterruptAs(self, fiber.id));
var fiberInterruptAs = /* @__PURE__ */ dual((args) => hasProperty(args[0], FiberTypeId), (self, fiberId, annotations) => withFiber((parent) => {
  let ann = fiberStackAnnotations(parent);
  ann = ann && annotations ? merge(ann, annotations) : ann ?? annotations;
  self.interruptUnsafe(fiberId, ann);
  return asVoid(fiberAwait(self));
}));
var fiberInterruptAll = (fibers) => withFiber((parent) => {
  const annotations = fiberStackAnnotations(parent);
  let fiberArr = empty2();
  for (const fiber of fibers) {
    fiber.interruptUnsafe(parent.id, annotations);
    fiberArr.push(fiber);
  }
  return asVoid(fiberAwaitAll(fiberArr));
});
var succeed3 = exitSucceed;
var failCause = exitFailCause;
var fail3 = exitFail;
var sync = /* @__PURE__ */ makePrimitive({
  op: "Sync",
  [evaluate](fiber) {
    const thunk = this[args];
    const value = thunk();
    const cont = fiber.getCont(contA);
    return cont ? cont[contA](value, fiber) : fiber.yieldWith(exitSucceed(value));
  }
});
var suspend = /* @__PURE__ */ makePrimitive({
  op: "Suspend",
  [evaluate](_fiber) {
    const thunk = this[args];
    return thunk();
  }
});
var fromResult = /* @__PURE__ */ match2({
  onFailure: fail3,
  onSuccess: succeed3
});
var yieldNowWith = /* @__PURE__ */ makePrimitive({
  op: "Yield",
  [evaluate](fiber) {
    let resumed = false;
    fiber.currentDispatcher.scheduleTask(() => {
      if (resumed)
        return;
      fiber.evaluate(exitVoid);
    }, this[args] ?? 0);
    return fiber.yieldWith(() => {
      resumed = true;
    });
  }
});
var yieldNow = /* @__PURE__ */ yieldNowWith(0);
var failCauseSync = (evaluate) => suspend(() => failCause(evaluate()));
var die = (defect) => exitDie(defect);
var failSync = (error) => suspend(() => fail3(error()));
var void_ = /* @__PURE__ */ succeed3(undefined);
var try_2 = (options) => {
  const evaluate = typeof options === "function" ? options : options.try;
  const catcher = typeof options === "function" ? (cause) => new UnknownError(cause, "An error occurred in Effect.try") : options.catch;
  return suspend(() => {
    try {
      return succeed3(evaluate());
    } catch (err) {
      return fail3(catcher(err));
    }
  });
};
var promise = (evaluate) => callbackOptions(function(resume, signal) {
  evaluate(signal).then((a) => resume(succeed3(a)), (e) => resume(die(e)));
}, evaluate.length !== 0);
var tryPromise = (options) => {
  const f = typeof options === "function" ? options : options.try;
  const catcher = typeof options === "function" ? (cause) => new UnknownError(cause, "An error occurred in Effect.tryPromise") : options.catch;
  return callbackOptions(function(resume, signal) {
    const failWithCatch = (cause) => suspend(() => {
      try {
        return fail3(internalCall(() => catcher(cause)));
      } catch (err) {
        return die(err);
      }
    });
    try {
      f(signal).then((a) => resume(succeed3(a)), (e) => resume(failWithCatch(e)));
    } catch (err) {
      resume(failWithCatch(err));
    }
  }, f.length !== 0);
};
var callbackOptions = /* @__PURE__ */ function() {
  const Proto = /* @__PURE__ */ makePrimitiveProto({
    op: "Async",
    [evaluate](fiber) {
      let resumed = false;
      let yielded = false;
      const controller = this.withSignal ? new AbortController : undefined;
      const onCancel = this.register.call(fiber.cache.scheduler, (effect) => {
        if (resumed)
          return;
        resumed = true;
        if (yielded) {
          fiber.evaluate(effect);
        } else {
          yielded = effect;
        }
      }, controller?.signal);
      if (yielded !== false)
        return yielded;
      yielded = true;
      fiber.yieldWith(() => {
        resumed = true;
      });
      if (controller === undefined && onCancel === undefined) {
        return Yield;
      }
      fiber._stack.push(asyncFinalizer(() => {
        resumed = true;
        controller?.abort();
        return onCancel ?? exitVoid;
      }));
      return Yield;
    }
  });
  const AsyncImpl = function(register, withSignal) {
    this.register = register;
    this.withSignal = withSignal;
  };
  AsyncImpl.prototype = Proto;
  return function(register, withSignal) {
    return new AsyncImpl(register, withSignal);
  };
}();
var asyncFinalizer = /* @__PURE__ */ makePrimitive({
  op: "AsyncFinalizer",
  [contAll](fiber) {
    if (fiber.interruptible) {
      fiber.interruptible = false;
      fiber._stack.push(setInterruptibleTrue);
    }
  },
  [contE](cause, _fiber) {
    return hasInterrupts(cause) ? flatMap3(combineFinalizerCause(exitFailCause(cause), this[args]()), () => failCause(cause)) : failCause(cause);
  }
});
var callback = (register) => callbackOptions(register, register.length >= 2);
var never = /* @__PURE__ */ callback(constVoid);
var gen = (...args) => {
  if (args.length === 1) {
    const body = args[0];
    return suspend(() => fromIteratorUnsafe(body()));
  }
  const [options, body] = args;
  return suspend(() => fromIteratorUnsafe(body.call(options.self)));
};
var fnBody = (bodyOrOptions, pipeables) => typeof bodyOrOptions === "function" ? bodyOrOptions : pipeables.shift().bind(bodyOrOptions.self);
var fnUntraced = (bodyOrOptions, ...pipeables) => {
  const body = fnBody(bodyOrOptions, pipeables);
  const fn = pipeables.length === 0 ? function() {
    return suspend(() => fromIteratorUnsafe(body.apply(this, arguments)));
  } : function() {
    let effect = suspend(() => fromIteratorUnsafe(body.apply(this, arguments)));
    for (let i = 0;i < pipeables.length; i++) {
      effect = pipeables[i](effect, ...arguments);
    }
    return effect;
  };
  return defineFunctionLength(body.length, fn);
};
var defineFunctionLength = (length, fn) => Object.defineProperty(fn, "length", {
  value: length,
  configurable: true
});
var fnStackCleaner = /* @__PURE__ */ makeStackCleaner(2);
var fn = function() {
  const nameFirst = typeof arguments[0] === "string";
  const name = nameFirst ? arguments[0] : "Effect.fn";
  const spanOptions = nameFirst ? arguments[1] : undefined;
  const prevLimit = getStackTraceLimit();
  let defError;
  if (prevLimit !== 0) {
    setStackTraceLimit(2);
    defError = new globalThis.Error;
    setStackTraceLimit(prevLimit);
  }
  if (nameFirst) {
    return (body, ...pipeables) => makeFn(name, body, defError, pipeables, nameFirst, spanOptions);
  }
  return makeFn(name, arguments[0], defError, Array.prototype.slice.call(arguments, 1), nameFirst, spanOptions);
};
var makeFn = (name, bodyOrOptions, defError, pipeables, addSpan, spanOptions) => {
  const body = fnBody(bodyOrOptions, pipeables);
  const definitionName = `${name} (definition)`;
  const definitionStack = defError ? fnStackCleaner(() => defError.stack) : constUndefined;
  const makeCallFrame = (callError) => (prev) => ({
    name,
    stack: callError ? fnStackCleaner(() => callError.stack) : constUndefined,
    parent: {
      name: definitionName,
      stack: definitionStack,
      parent: prev
    }
  });
  return defineFunctionLength(body.length, function(...args) {
    let result = suspend(() => {
      const iter = body.apply(this, arguments);
      return isEffect(iter) ? iter : fromIteratorUnsafe(iter);
    });
    for (let i = 0;i < pipeables.length; i++) {
      result = pipeables[i](result, ...args);
    }
    if (!isEffect(result)) {
      return result;
    }
    const prevLimit = getStackTraceLimit();
    let callError;
    if (prevLimit !== 0) {
      setStackTraceLimit(2);
      callError = new globalThis.Error;
      setStackTraceLimit(prevLimit);
    }
    return updateService(addSpan ? useSpan(name, spanOptions, (span) => provideParentSpan(result, span)) : result, CurrentStackFrame, makeCallFrame(callError));
  });
};
var fnUntracedEager = (bodyOrOptions, ...pipeables) => {
  const body = fnBody(bodyOrOptions, pipeables);
  return defineFunctionLength(body.length, pipeables.length === 0 ? function() {
    return fromIteratorEagerUnsafe(() => body.apply(this, arguments));
  } : function() {
    let effect = fromIteratorEagerUnsafe(() => body.apply(this, arguments));
    for (const pipeable of pipeables) {
      effect = pipeable(effect, ...arguments);
    }
    return effect;
  });
};
var fromIteratorEagerUnsafe = (evaluate) => {
  try {
    const iterator = evaluate();
    let value = undefined;
    while (true) {
      const state = iterator.next(value);
      if (state.done) {
        return succeed3(state.value);
      }
      const primitive = state.value;
      if (primitive && primitive._tag === "Success") {
        value = primitive.value;
        continue;
      } else if (primitive && primitive._tag === "Failure") {
        return state.value;
      } else {
        let isFirstExecution = true;
        return suspend(() => {
          if (isFirstExecution) {
            isFirstExecution = false;
            return flatMap3(state.value, (value) => fromIteratorUnsafe(iterator, value));
          } else {
            return suspend(() => fromIteratorUnsafe(evaluate()));
          }
        });
      }
    }
  } catch (error) {
    return die(error);
  }
};
var fromIteratorUnsafe = /* @__PURE__ */ function() {
  const Proto = /* @__PURE__ */ makePrimitiveProto({
    op: "Iterator",
    [contA](value, fiber) {
      const iter = this.iterator;
      while (true) {
        const state = iter.next(value);
        if (state.done)
          return succeed3(state.value);
        if (!effectIsExit(state.value)) {
          fiber._stack.push(this);
          return state.value;
        } else if (state.value._tag === "Failure") {
          return state.value;
        }
        value = state.value.value;
      }
    },
    [evaluate](fiber) {
      return this[contA](this.initial, fiber);
    }
  });
  const IteratorImpl = function(iterator, initial) {
    this.iterator = iterator;
    this.initial = initial;
  };
  IteratorImpl.prototype = Proto;
  return function(iterator, initial) {
    return new IteratorImpl(iterator, initial);
  };
}();
var as = /* @__PURE__ */ dual(2, (self, value) => new ContImpl(self, returnPayload, succeed3(value)));
var evaluateCont = function(fiber) {
  fiber._stack.push(this);
  return this[args];
};
var OnSuccessProto = /* @__PURE__ */ makePrimitiveProto({
  op: "OnSuccess",
  [evaluate]: evaluateCont
});
var ContImpl = function(self, cont, payload) {
  this[args] = self;
  this[contA] = cont;
  this.payload = payload;
};
ContImpl.prototype = OnSuccessProto;
var returnPayload = function() {
  return this.payload;
};
var succeedPayload = function(_value, fiber) {
  return fiber.succeedWith(this.payload);
};
var continuationMarksStack = /* @__PURE__ */ (() => {
  const limit = /* @__PURE__ */ getStackTraceLimit();
  if (limit === 0)
    return false;
  const marker = "~effect/Effect/stackProbe";
  const probe = {
    [marker]: function stackProbe() {
      return new Error;
    }
  };
  setStackTraceLimit(1);
  const error = /* @__PURE__ */ probe[marker]();
  setStackTraceLimit(limit);
  return error.stack?.includes("[as " + marker + "]") === true;
})();
var mapCont = function(value, fiber) {
  const f = this.payload;
  return fiber.succeedWith(continuationMarksStack ? f(value) : internalCall(() => f(value)));
};
var andThenCont = function(value) {
  const f = this.payload;
  return f(value);
};
var tapCont = function(value) {
  const f = this.payload;
  return new ContImpl(f(value), succeedPayload, value);
};
var tapEffectCont = function(value) {
  return new ContImpl(this.payload, succeedPayload, value);
};
var asSome = (self) => map5(self, some2);
var andThen = /* @__PURE__ */ dual(2, (self, f) => new ContImpl(self, isEffect(f) ? returnPayload : andThenCont, f));
var tap = /* @__PURE__ */ dual(2, (self, f) => new ContImpl(self, isEffect(f) ? tapEffectCont : tapCont, f));
var asVoid = (self) => new ContImpl(self, returnPayload, exitVoid);
var raceAllFirst = (all, options) => withFiber((parent) => {
  const fibers = new Set;
  onExitUnsafe(parent, () => fibers.size === 0 ? undefined : fiberInterruptAll(fibers));
  return callback((resume) => {
    let done = false;
    let i = 0;
    for (const effect of all) {
      if (done)
        break;
      const index = i++;
      const fiber = forkUnsafe(parent, effect, true, true, false);
      fibers.add(fiber);
      fiber.addObserver((exit) => {
        fibers.delete(fiber);
        const isWinner = !done;
        done = true;
        resume(exit);
        if (isWinner && options?.onWinner) {
          options.onWinner({
            fiber,
            index,
            parentFiber: parent
          });
        }
      });
    }
  });
});
var raceFirst = /* @__PURE__ */ dual((args) => isEffect(args[1]), (self, that, options) => raceAllFirst([self, that], options));
var flatMap3 = /* @__PURE__ */ dual(2, (self, f) => new ContImpl(self, andThenCont, f));
var matchCauseEffectEager = /* @__PURE__ */ dual(2, (self, options) => {
  if (effectIsExit(self)) {
    return self._tag === "Success" ? options.onSuccess(self.value) : options.onFailure(self.cause);
  }
  return matchCauseEffect(self, options);
});
var effectIsExit = (effect) => effect[ExitTypeId] !== undefined;
var flatMapEager = /* @__PURE__ */ dual(2, (self, f) => {
  if (effectIsExit(self)) {
    return self._tag === "Success" ? f(self.value) : self;
  }
  return flatMap3(self, f);
});
var map5 = /* @__PURE__ */ dual(2, (self, f) => new ContImpl(self, mapCont, f));
var mapEager = /* @__PURE__ */ dual(2, (self, f) => effectIsExit(self) ? exitMap(self, f) : map5(self, f));
var mapErrorEager = /* @__PURE__ */ dual(2, (self, f) => effectIsExit(self) ? exitMapError(self, f) : mapError2(self, f));
var catchEager = /* @__PURE__ */ dual(2, (self, f) => {
  if (effectIsExit(self)) {
    if (self._tag === "Success")
      return self;
    const error = findRecoverableError(self.cause);
    if (isFailure2(error))
      return exitFailCause(error.failure);
    return f(error.success);
  }
  return catch_(self, f);
});
var exitInterrupt = (fiberId) => exitFailCause(causeInterrupt(fiberId));
var exitIsSuccess = (self) => self._tag === "Success";
var exitIsFailure = (self) => self._tag === "Failure";
var exitFilterCause = (self) => self._tag === "Failure" ? succeed2(self.cause) : fail2(self);
var exitHasInterrupts = (self) => self._tag === "Failure" && hasInterrupts(self.cause);
var exitVoid = /* @__PURE__ */ exitSucceed(undefined);
var exitMap = /* @__PURE__ */ dual(2, (self, f) => self._tag === "Success" ? exitSucceed(f(self.value)) : self);
var exitMapError = /* @__PURE__ */ dual(2, (self, f) => {
  if (self._tag === "Success")
    return self;
  return exitFailCause(causeMap(self.cause, f));
});
var exitZipRight = /* @__PURE__ */ dual(2, (self, that) => exitIsSuccess(self) ? that : self);
var exitAsVoidAll = (exits) => {
  const failures = [];
  for (const exit of exits) {
    if (exit._tag === "Failure") {
      failures.push(...exit.cause.reasons);
    }
  }
  return failures.length === 0 ? exitVoid : exitFailCause(causeFromReasons(failures));
};
var serviceOption = (service) => withFiber((fiber) => succeed3(getOption(fiber.context, service)));
var updateContext = /* @__PURE__ */ dual(2, (self, f) => withFiber((fiber) => {
  const prevContext = fiber.context;
  const nextContext = f(prevContext);
  if (prevContext === nextContext)
    return self;
  fiber.setContext(nextContext);
  onExitUnsafe(fiber, () => {
    fiber.setContext(prevContext);
    return;
  });
  return self;
}));
var updateService = /* @__PURE__ */ dual(3, (self, service, f) => updateContext(self, (s) => {
  const prev = getUnsafe(s, service);
  const next = f(prev);
  if (prev === next)
    return s;
  return add(s, service, next);
}));
var context = () => getContext;
var getContext = /* @__PURE__ */ withFiber((fiber) => succeed3(fiber.context));
var contextWith = (f) => withFiber((fiber) => f(fiber.context));
var provideContext = /* @__PURE__ */ dual(2, (self, context) => {
  if (effectIsExit(self))
    return self;
  return updateContext(self, merge(context));
});
var provideService = function() {
  if (arguments.length === 1) {
    return dual(2, (self, impl) => provideServiceImpl(self, arguments[0], impl));
  }
  return dual(3, (self, service, impl) => provideServiceImpl(self, service, impl)).apply(this, arguments);
};
var provideServiceImpl = (self, service, implementation) => updateContext(self, add(service, implementation));
var zipWith = /* @__PURE__ */ dual((args) => isEffect(args[1]), (self, that, f, options) => options?.concurrent ? map5(all2([self, that], {
  concurrency: 2
}), ([a, a2]) => f(a, a2)) : flatMap3(self, (a) => map5(that, (a2) => f(a, a2))));
var forever = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, options) => whileLoop({
  while: constTrue,
  body: constant(options?.disableYield ? self : flatMap3(self, (_) => yieldNow)),
  step: constVoid
}));
var catchCause = /* @__PURE__ */ dual(2, (self, f) => new OnFailureImpl(self, f.length !== 1 ? (cause) => f(cause) : f));
var OnFailureProto = /* @__PURE__ */ makePrimitiveProto({
  op: "OnFailure",
  [evaluate]: evaluateCont
});
var OnFailureImpl = function(self, f) {
  this[args] = self;
  this[contE] = f;
};
OnFailureImpl.prototype = OnFailureProto;
var catchCauseFilter = /* @__PURE__ */ dual(3, (self, filter, f) => catchCause(self, (cause) => {
  const eb = filter(cause);
  return isFailure2(eb) ? failCause(eb.failure) : f(eb.success, cause);
}));
var catch_ = /* @__PURE__ */ dual(2, (self, f) => catchCauseFilter(self, findRecoverableError, (e) => f(e)));
var tapCause = /* @__PURE__ */ dual(2, (self, f) => catchCause(self, (cause) => andThen(f(cause), failCause(cause))));
var tapCauseFilter = /* @__PURE__ */ dual(3, (self, filter, f) => catchCause(self, (cause) => {
  const result = filter(cause);
  if (isFailure2(result)) {
    return failCause(cause);
  }
  return andThen(f(result.success, cause), failCause(cause));
}));
var tapError = /* @__PURE__ */ dual(2, (self, f) => tapCauseFilter(self, findError, (e) => f(e)));
var catchIf = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, predicate, f, orElse) => catchCause(self, (cause) => {
  const error = findRecoverableError(cause);
  if (isFailure2(error))
    return failCause(error.failure);
  if (!predicate(error.success)) {
    return orElse ? orElse(error.success) : failCause(cause);
  }
  return f(error.success);
}));
var catchFilter = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, filter, f, orElse) => catchCause(self, (cause) => {
  const error = findRecoverableError(cause);
  if (isFailure2(error))
    return failCause(error.failure);
  const result = filter(error.success);
  if (isFailure2(result)) {
    return orElse ? orElse(result.failure) : failCause(cause);
  }
  return f(result.success);
}));
var catchTag = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, k, f, orElse) => {
  const pred = Array.isArray(k) ? (e) => hasProperty(e, "_tag") && k.includes(e._tag) : isTagged(k);
  return catchIf(self, pred, f, orElse);
});
var catchTags = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, cases, orElse) => {
  let keys;
  return catchFilter(self, (e) => {
    keys ??= Object.keys(cases);
    return hasProperty(e, "_tag") && isString(e["_tag"]) && keys.includes(e["_tag"]) ? succeed2(e) : fail2(e);
  }, (e) => cases[e["_tag"]](e), orElse);
});
var catchReason = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, errorTag, reasonTag, f, orElse) => catchIf(self, (e) => isTagged(e, errorTag) && hasProperty(e, "reason") && (orElse !== undefined || isTagged(e.reason, reasonTag)), (e) => {
  const reason = e.reason;
  if (isTagged(reason, reasonTag))
    return f(reason, e);
  return orElse ? orElse(reason, e) : fail3(e);
}));
var mapError2 = /* @__PURE__ */ dual(2, (self, f) => catchCause(self, (cause) => failCause(causeMap(cause, f))));
var orDie = (self) => catch_(self, die);
var orElseSucceed = /* @__PURE__ */ dual(2, (self, f) => catch_(self, (error) => sync(() => f(error))));
var ignoreCauseLogger = (log) => log === "Unhandled" ? (...message) => withFiber((fiber) => {
  const level = fiber.getRef(UnhandledLogLevel);
  return level === undefined ? void_ : logWithLevel(level)(...message);
}) : logWithLevel(log === true ? undefined : log);
var ignore = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, options) => {
  if (!options?.log) {
    return matchEffect(self, {
      onFailure: (_) => void_,
      onSuccess: (_) => void_
    });
  }
  const logEffect = ignoreCauseLogger(options.log);
  return matchCauseEffect(self, {
    onFailure(cause) {
      const error = findRecoverableError(cause);
      return isFailure2(error) ? failCause(error.failure) : options.message === undefined ? logEffect(cause) : logEffect(options.message, cause);
    },
    onSuccess: (_) => void_
  });
});
var ignoreCause = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, options) => {
  if (!options?.log) {
    return matchCauseEffect(self, {
      onFailure: (_) => void_,
      onSuccess: (_) => void_
    });
  }
  const logEffect = ignoreCauseLogger(options.log);
  return matchCauseEffect(self, {
    onFailure: (cause) => options.message === undefined ? logEffect(cause) : logEffect(options.message, cause),
    onSuccess: (_) => void_
  });
});
var result = (self) => matchEager(self, {
  onFailure: fail2,
  onSuccess: succeed2
});
var matchCauseEffect = /* @__PURE__ */ dual(2, (self, options) => new OnSuccessAndFailureImpl(self, options.onSuccess.length !== 1 ? (a) => options.onSuccess(a) : options.onSuccess, options.onFailure.length !== 1 ? (cause) => options.onFailure(cause) : options.onFailure));
var OnSuccessAndFailureProto = /* @__PURE__ */ makePrimitiveProto({
  op: "OnSuccessAndFailure",
  [evaluate]: evaluateCont
});
var OnSuccessAndFailureImpl = function(self, onSuccess, onFailure) {
  this[args] = self;
  this[contA] = onSuccess;
  this[contE] = onFailure;
};
OnSuccessAndFailureImpl.prototype = OnSuccessAndFailureProto;
var matchSuccess = function(value, fiber) {
  const handlers = this.payload;
  return fiber.succeedWith(continuationMarksStack ? handlers.onSuccess(value) : internalCall(() => handlers.onSuccess(value)));
};
var makeMatch = (op, onFailure) => {
  const Proto = makePrimitiveProto({
    op,
    [evaluate]: evaluateCont,
    [contA]: matchSuccess,
    [contE]: onFailure
  });
  const MatchImpl = function(self, handlers) {
    this[args] = self;
    this.payload = handlers;
  };
  MatchImpl.prototype = Proto;
  return MatchImpl;
};
var MatchImpl = /* @__PURE__ */ makeMatch("Match", function(cause, fiber) {
  const error = findRecoverableError(cause);
  if (isFailure2(error))
    return failCause(error.failure);
  const handlers = this.payload;
  return fiber.succeedWith(continuationMarksStack ? handlers.onFailure(error.success) : internalCall(() => handlers.onFailure(error.success)));
});
var matchEffect = /* @__PURE__ */ dual(2, (self, options) => matchCauseEffect(self, {
  onFailure: (cause) => {
    const error = findRecoverableError(cause);
    return isFailure2(error) ? failCause(error.failure) : options.onFailure(error.success);
  },
  onSuccess: options.onSuccess
}));
var match5 = /* @__PURE__ */ dual(2, (self, options) => new MatchImpl(self, options));
var matchEager = /* @__PURE__ */ dual(2, (self, options) => {
  if (effectIsExit(self)) {
    if (self._tag === "Success")
      return exitSucceed(options.onSuccess(self.value));
    const error = findRecoverableError(self.cause);
    if (isFailure2(error))
      return exitFailCause(error.failure);
    return exitSucceed(options.onFailure(error.success));
  }
  return match5(self, options);
});
var exit = (self) => effectIsExit(self) ? exitSucceed(self) : exitPrimitive(self);
var exitPrimitive = /* @__PURE__ */ makePrimitive({
  op: "Exit",
  [evaluate](fiber) {
    fiber._stack.push(this);
    return this[args];
  },
  [contA](value, fiber, exit) {
    return fiber.succeedWith(exit ?? exitSucceed(value));
  },
  [contE](cause, fiber, exit) {
    return fiber.succeedWith(exit ?? exitFailCause(cause));
  }
});
var timeoutOrElse = /* @__PURE__ */ dual(2, (self, options) => flatMap3(timeoutOption(self, options.duration), (option) => isNone2(option) ? options.orElse() : succeed3(option.value)));
var timeoutOption = /* @__PURE__ */ dual(2, (self, duration) => raceFirst(asSome(self), as(sleep(duration), none2())));
var ScopeTypeId = "~effect/Scope";
var ScopeCloseableTypeId = "~effect/Scope/Closeable";
var scopeTag = /* @__PURE__ */ Service("effect/Scope");
var scopeClose = (self, exit_) => withFiber((fiber) => {
  fiberEnterUninterruptibleUnsafe(fiber);
  return scopeCloseUnsafe(self, exit_) ?? void_;
});
var scopeCloseUnsafe = (self, exit_) => {
  const state = self.state;
  if (state._tag === "Closed")
    return;
  self.state = {
    _tag: "Closed",
    exit: exit_
  };
  if (self.parent !== undefined) {
    scopeRemoveFinalizerUnsafe(self.parent, self);
  }
  if (state._tag === "Empty")
    return;
  if (state.finalizer !== undefined) {
    return runFinalizer(state.finalizer, exit_);
  }
  const finalizers = state.finalizers;
  if (finalizers.size === 1) {
    return runFinalizer(finalizers.values().next().value, exit_);
  }
  return scopeCloseFinalizers(self, finalizers, exit_);
};
var runFinalizer = (finalizer, exit_) => {
  try {
    return finalizer(exit_);
  } catch (defect) {
    return exitDie(defect);
  }
};
var combineFinalizerCause = (exit_, finalizer) => exitIsSuccess(exit_) ? finalizer : catchCause(finalizer, (cause) => failCause(causeElectDefects(causeCombine(exit_.cause, cause))));
var scopeCloseFinalizers = /* @__PURE__ */ fnUntraced(function* (self, finalizers, exit_) {
  let exits = [];
  const fibers = [];
  const arr = Array.from(finalizers);
  const parent = getCurrentFiber();
  for (let i = arr.length - 1;i >= 0; i--) {
    const [key, finalizer] = arr[i];
    if (self.strategy === "sequential") {
      exits.push(yield* exit(runFinalizer(finalizer, exit_)));
    } else if (key !== parent) {
      fibers.push(forkUnsafe(parent, runFinalizer(finalizer, exit_), true, true, "inherit"));
    }
  }
  if (fibers.length > 0) {
    exits = yield* fiberAwaitAll(fibers);
  }
  return yield* exitAsVoidAll(exits);
});
var scopeForkUnsafe = (scope, finalizerStrategy) => {
  const child = makeScope(finalizerStrategy, scope);
  if (scope.state._tag === "Closed") {
    child.state = scope.state;
    return child;
  }
  scopeAddFinalizerUnsafe(scope, child, (exit) => scopeClose(child, exit));
  return child;
};
var scopeAddFinalizerExit = (scope, finalizer) => {
  return suspend(() => {
    if (scope.state._tag === "Closed") {
      return finalizer(scope.state.exit);
    }
    scopeAddFinalizerUnsafe(scope, {}, finalizer);
    return void_;
  });
};
var scopeAddFinalizer = (scope, finalizer) => scopeAddFinalizerExit(scope, constant(finalizer));
var scopeAddFinalizerUnsafe = (scope, key, finalizer) => {
  if (scope.state._tag === "Empty") {
    scope.state = {
      _tag: "Open",
      finalizerKey: key,
      finalizer,
      finalizers: undefined
    };
  } else if (scope.state._tag === "Open") {
    const state = scope.state;
    if (state.finalizer !== undefined) {
      state.finalizers = new Map([[state.finalizerKey, state.finalizer]]);
      state.finalizerKey = undefined;
      state.finalizer = undefined;
      state.finalizers.set(key, finalizer);
    } else {
      state.finalizers.set(key, finalizer);
    }
  }
};
var scopeRemoveFinalizerUnsafe = (scope, key) => {
  if (scope.state._tag !== "Open")
    return;
  const state = scope.state;
  if (state.finalizerKey === key) {
    scope.state = constScopeEmpty;
  } else if (state.finalizers !== undefined) {
    state.finalizers.delete(key);
    if (state.finalizers.size === 0) {
      scope.state = constScopeEmpty;
    }
  }
};
var scopeMakeUnsafe = (finalizerStrategy) => makeScope(finalizerStrategy, undefined);
var makeScope = (finalizerStrategy = "sequential", parent) => ({
  [ScopeCloseableTypeId]: ScopeCloseableTypeId,
  [ScopeTypeId]: ScopeTypeId,
  strategy: finalizerStrategy,
  parent,
  state: constScopeEmpty
});
var constScopeEmpty = {
  _tag: "Empty"
};
var scope = scopeTag;
var provideScope = /* @__PURE__ */ provideService(scopeTag);
var scoped = (self) => withFiber((fiber) => {
  const prev = fiber.context;
  const scope = scopeMakeUnsafe();
  fiber.setContext(add(fiber.context, scopeTag, scope));
  onExitUnsafe(fiber, (exit) => {
    fiber.setContext(prev);
    return scopeCloseUnsafe(scope, exit);
  });
  return self;
});
var scopedWith = (f) => suspend(() => {
  const scope = scopeMakeUnsafe();
  return onExit(f(scope), (exit) => suspend(() => scopeCloseUnsafe(scope, exit) ?? void_));
});
var acquireRelease = (acquire, release, options) => contextWith((context) => uninterruptibleMask((restore) => flatMap3(scope, (scope) => tap(options?.interruptible ? restore(acquire) : acquire, (a) => scopeAddFinalizerExit(scope, (exit) => provideContext(release(a, exit), context))))));
var addFinalizer = (finalizer) => flatMap3(scope, (scope) => contextWith((context) => scopeAddFinalizerExit(scope, (exit) => provideContext(finalizer(exit), context))));
var OnExitImpl = /* @__PURE__ */ function() {
  const Proto = /* @__PURE__ */ makePrimitiveProto({
    op: "OnExit",
    [evaluate](fiber) {
      fiber._stack.push(this);
      return this.effect;
    },
    [contAll](fiber) {
      if (fiber.interruptible && this.interruptible !== true) {
        fiber._stack.push(setInterruptibleTrue);
        fiber.interruptible = false;
      }
    },
    [contA](value, _, exit) {
      exit ??= exitSucceed(value);
      let eff;
      try {
        eff = this.onExit(exit);
      } catch (defect) {
        eff = exitDie(defect);
      }
      return eff ? flatMap3(eff, (_) => exit) : exit;
    },
    [contE](cause, _, exit) {
      exit ??= exitFailCause(cause);
      let eff;
      try {
        eff = this.onExit(exit);
      } catch (defect) {
        eff = exitDie(defect);
      }
      return eff ? flatMap3(combineFinalizerCause(exit, eff), (_) => exit) : exit;
    }
  });
  const OnExitImpl = function(effect, onExit, interruptible) {
    this.effect = effect;
    this.onExit = onExit;
    this.interruptible = interruptible;
  };
  OnExitImpl.prototype = Proto;
  return OnExitImpl;
}();
var onExitPrimitive = (effect, onExit, interruptible) => new OnExitImpl(effect, onExit, interruptible);
var onExitUnsafe = (fiber, f) => {
  fiber._stack.push(new OnExitImpl(undefined, f, undefined));
};
var onExit = /* @__PURE__ */ dual(2, onExitPrimitive);
var ensuring = /* @__PURE__ */ dual(2, (self, finalizer) => onExit(self, (_) => finalizer));
var onExitFilter = /* @__PURE__ */ dual(3, (self, filter, f) => onExit(self, (exit) => {
  const b = filter(exit);
  return isFailure2(b) ? void_ : f(b.success, exit);
}));
var onError = /* @__PURE__ */ dual(2, (self, f) => onExitFilter(self, exitFilterCause, f));
var onErrorFilter = /* @__PURE__ */ dual(3, (self, filter, f) => onExit(self, (exit) => {
  if (exit._tag !== "Failure") {
    return void_;
  }
  const result = filter(exit.cause);
  return isFailure2(result) ? void_ : f(result.success, exit.cause);
}));
var onInterrupt = /* @__PURE__ */ dual(2, (self, finalizer) => onErrorFilter(causeFilterInterruptors, finalizer)(self));
var infiniteTTL = /* @__PURE__ */ constant(Infinity);
var makeCachedUnsafe = (self, ttlMillis) => {
  let expiresAt = 0;
  let exit;
  let current;
  const join = (fiber, run) => {
    run.awaiters++;
    onExitUnsafe(fiber, () => {
      if (--run.awaiters > 0 || run.done)
        return;
      if (current === run)
        current = undefined;
      return fiberInterrupt(run.fiber);
    });
    return fiberJoin(run.fiber);
  };
  return [withFiber((fiber) => {
    if (current !== undefined) {
      return fiber === current.fiber ? fiberJoin(current.fiber) : join(fiber, current);
    }
    if (exit !== undefined && (expiresAt === Infinity || fiber.getRef(ClockRef).currentTimeMillisUnsafe() < expiresAt)) {
      return exit;
    }
    exit = undefined;
    const clock = fiber.getRef(ClockRef);
    const run = {
      fiber: new FiberImpl(fiber.context),
      awaiters: 0,
      done: false
    };
    current = run;
    const joined = join(fiber, run);
    run.fiber.evaluate(onExitPrimitive(self, (exit_) => {
      run.done = true;
      if (current !== run)
        return;
      current = undefined;
      if (exitHasInterrupts(exit_))
        return;
      const duration = ttlMillis(exit_);
      expiresAt = duration === Infinity ? Infinity : clock.currentTimeMillisUnsafe() + duration;
      exit = exit_;
    }));
    return run.fiber._exit ?? joined;
  }), sync(() => {
    current = undefined;
    exit = undefined;
  })];
};
var cached = (self) => sync(() => makeCachedUnsafe(self, infiniteTTL)[0]);
var interrupt = /* @__PURE__ */ withFiber((fiber) => failCause(causeInterrupt(fiber.id)));
var uninterruptible = (self) => withFiber((fiber) => {
  if (!fiber.interruptible)
    return self;
  fiber.interruptible = false;
  fiber._stack.push(setInterruptibleTrue);
  return self;
});
var setInterruptible = /* @__PURE__ */ makePrimitive({
  op: "SetInterruptible",
  [contAll](fiber) {
    fiber.interruptible = this[args];
    if (fiber._interruptedCause && fiber.interruptible) {
      return () => failCause(fiber._interruptedCause);
    }
  }
});
var setInterruptibleTrue = /* @__PURE__ */ setInterruptible(true);
var setInterruptibleFalse = /* @__PURE__ */ setInterruptible(false);
var setFiberInterruptible = (fiber) => {
  fiber.interruptible = true;
  fiber._stack.push(setInterruptibleFalse);
  if (fiber._interruptedCause)
    return failCause(fiber._interruptedCause);
};
var fiberEnterUninterruptibleUnsafe = (fiber) => {
  const impl = fiber;
  if (!impl.interruptible)
    return;
  impl.interruptible = false;
  impl._stack.push(setInterruptibleTrue);
};
var interruptible = (self) => withFiber((fiber) => {
  if (fiber.interruptible)
    return self;
  return setFiberInterruptible(fiber) ?? self;
});
var uninterruptibleMask = (f) => withFiber((fiber) => {
  if (!fiber.interruptible)
    return f(identity);
  fiber.interruptible = false;
  fiber._stack.push(setInterruptibleTrue);
  return f(interruptible);
});
var all2 = (arg, options) => {
  if (isIterable(arg)) {
    return options?.mode === "result" ? forEach(arg, result, options) : forEach(arg, identity, options);
  } else if (options?.discard) {
    return options.mode === "result" ? forEach(Object.values(arg), result, options) : forEach(Object.values(arg), identity, options);
  }
  return suspend(() => map5(forEach(Object.entries(arg), ([key, effect]) => map5(options?.mode === "result" ? result(effect) : effect, (value) => [key, value]), {
    concurrency: options?.concurrency
  }), Object.fromEntries));
};
var whileLoop = /* @__PURE__ */ makePrimitive({
  op: "While",
  [contA](value, fiber) {
    this[args].step(value);
    if (this[args].while()) {
      fiber._stack.push(this);
      return this[args].body();
    }
    return exitVoid;
  },
  [evaluate](fiber) {
    if (this[args].while()) {
      fiber._stack.push(this);
      return this[args].body();
    }
    return exitVoid;
  }
});
var forEach = /* @__PURE__ */ dual((args) => typeof args[1] === "function", (iterable, f, options) => suspend(() => {
  const concurrency = resolveConcurrency(options?.concurrency);
  if (concurrency === 1) {
    return forEachSequential(iterable, f, options);
  }
  const items = fromIterable(iterable);
  let length = items.length;
  if (length === 0) {
    return options?.discard ? void_ : succeed3([]);
  }
  const out = options?.discard ? undefined : new Array(length);
  const eff = forEachConcurrent({
    f,
    out
  }, items, {
    concurrency
  });
  return eff ? as(eff, out) : succeed3(out);
}));
var forEachSequential = (iterable, f, options) => suspend(() => {
  const out = options?.discard ? undefined : [];
  const iterator = iterable[Symbol.iterator]();
  let state = iterator.next();
  let index = 0;
  return as(whileLoop({
    while: () => !state.done,
    body: () => f(state.value, index++),
    step: (b) => {
      if (out)
        out.push(b);
      state = iterator.next();
    }
  }), out);
});
var resolveConcurrency = (concurrency) => concurrency === "unbounded" ? Number.POSITIVE_INFINITY : Math.max(1, concurrency ?? 1);
var iterateEager = () => (options) => {
  const onItem = options.onItem;
  const step = options.step;
  const resumeSequential = (state, items, index, end, effect) => flatMap3(exit(effect), (itemExit) => step(state, items[index], itemExit, index) ?? runSequential(state, items, index + 1, end) ?? void_);
  const runSequential = (state, items, index = 0, end = items.length) => {
    for (;index < end; index++) {
      const item = items[index];
      const effect = onItem(state, item, index);
      if (!effectIsExit(effect)) {
        return resumeSequential(state, items, index, end, effect);
      }
      const terminal = step(state, item, effect, index);
      if (terminal)
        return terminal._tag === "Failure" ? terminal : undefined;
    }
  };
  return runSequential;
};
var iterateConcurrentImpl = (options) => {
  const onItem = options.onItem;
  const step = options.step;
  return (state, items, opts) => {
    let index = 0;
    const end = opts.end ?? items.length;
    const concurrency = opts.concurrency;
    let done = false;
    let parentFiber;
    let fibers;
    let resume;
    let terminal;
    let effect;
    const failDefect = (error) => {
      const defect = exitDie(error);
      terminal = defect;
      done = true;
      return fibers && fibers.size > 0 ? flatMap3(uninterruptible(fiberInterruptAll(Array.from(fibers))), () => terminal ?? defect) : defect;
    };
    const go = () => {
      let paused = false;
      for (;!terminal && index < end; index++) {
        const item = items[index];
        const eff = effect ?? onItem(state, item, index);
        if (effectIsExit(eff)) {
          terminal = step(state, item, eff, index);
          if (terminal)
            break;
        } else if (!parentFiber) {
          return callback((cb) => {
            parentFiber = getCurrentFiber();
            fibers = new Set;
            effect = eff;
            resume = cb;
            let result;
            try {
              result = go();
            } catch (error) {
              return cb(failDefect(error));
            }
            if (result)
              return cb(result);
            return suspend(() => {
              terminal ??= exitVoid;
              return flatMap3(fibers ? fiberInterruptAll(fibers) : void_, () => terminal?._tag === "Failure" ? terminal : void_);
            });
          });
        } else {
          effect = undefined;
          const fiber = forkUnsafe(parentFiber, eff, true, true, "inherit");
          if (fiber._exit) {
            terminal = step(state, item, fiber._exit, index);
            if (terminal)
              break;
            continue;
          }
          fibers.add(fiber);
          const currentIndex = index;
          fiber.addObserver((exit) => {
            fibers.delete(fiber);
            try {
              if (terminal) {
                if (exit._tag === "Failure") {
                  const reasons = exit.cause.reasons.filter((reason) => reason._tag !== "Interrupt");
                  if (reasons.length > 0) {
                    const cause = causeFromReasons(reasons);
                    terminal = exitFailCause(terminal._tag === "Failure" ? causeElectDefects(causeCombine(terminal.cause, cause)) : cause);
                  }
                }
              } else {
                const result = step(state, item, exit, currentIndex);
                if (result) {
                  terminal = result;
                  go();
                }
              }
              if (paused) {
                const eff = go();
                if (eff)
                  resume(eff);
              } else if (done && fibers.size === 0) {
                resume(terminal ?? void_);
              }
            } catch (error) {
              resume(failDefect(error));
            }
          });
          if (fibers.size < concurrency)
            continue;
          paused = true;
          index++;
          return;
        }
      }
      done = true;
      if (terminal) {
        if (fibers && fibers.size > 0) {
          const annotations = fiberStackAnnotations(parentFiber);
          fibers.forEach((f) => f.interruptUnsafe(parentFiber.id, annotations));
          return;
        }
        if (resume || terminal._tag === "Failure") {
          return terminal;
        }
      } else if (resume) {
        if (!fibers) {
          return exitVoid;
        } else if (fibers.size === 0) {
          resume(void_);
        }
      }
    };
    return go();
  };
};
var iterateConcurrent = () => (options) => iterateConcurrentImpl(options);
var forEachConcurrent = /* @__PURE__ */ iterateConcurrentImpl({
  onItem(state, item, index) {
    return state.f(item, index);
  },
  step(state, _, exit, index) {
    if (exit._tag === "Failure")
      return exit;
    else if (state.out) {
      state.out[index] = exit.value;
    }
  }
});
var forkChild = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, options) => withFiber((fiber) => {
  interruptChildrenPatch();
  return succeed3(forkUnsafe(fiber, self, options?.startImmediately, false, options?.uninterruptible ?? false));
}));
var forkUnsafe = (parent, effect, immediate = false, daemon = false, uninterruptible = false) => {
  const parentRuntime = parent;
  const interruptible = uninterruptible === "inherit" ? parentRuntime.interruptible : !uninterruptible;
  const child = new FiberImpl(parentRuntime.context, interruptible);
  if (!daemon) {
    parentRuntime.children().add(child);
    child._parent = parentRuntime;
  }
  if (immediate) {
    child.evaluate(effect);
  } else {
    child._asyncContext = captureAsyncContext();
    parentRuntime.currentDispatcher.scheduleTask(() => child.evaluate(effect), 0);
  }
  return child;
};
var fiberScopeFinalizer = (fiber) => () => withFiber((current) => current === fiber ? void_ : fiberInterrupt(fiber));
var forkIn = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, scope, options) => withFiber((parent) => {
  const fiber = scope.state._tag === "Closed" ? new FiberImpl(parent.context) : forkUnsafe(parent, self, options?.startImmediately, true, options?.uninterruptible);
  if (!fiber._exit) {
    if (scope.state._tag !== "Closed") {
      scopeAddFinalizerUnsafe(scope, fiber, fiberScopeFinalizer(fiber));
      fiber.addObserver(() => scopeRemoveFinalizerUnsafe(scope, fiber));
    } else {
      fiber.interruptUnsafe(parent.id, fiberStackAnnotations(parent));
    }
  }
  return succeed3(fiber);
}));
var forkScoped = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, options) => flatMap3(scope, (scope) => forkIn(self, scope, options)));
var runForkWith = (context) => (effect, options) => {
  const fiber = new FiberImpl(options?.scheduler ? add(context, Scheduler, options.scheduler) : context, options?.uninterruptible !== true);
  fiber.evaluate(effect);
  if (fiber._exit)
    return fiber;
  if (options?.signal) {
    if (options.signal.aborted) {
      fiber.interruptUnsafe();
    } else {
      const abort = () => fiber.interruptUnsafe();
      options.signal.addEventListener("abort", abort, {
        once: true
      });
      fiber.addObserver(() => options.signal.removeEventListener("abort", abort));
    }
  }
  if (options?.onFiberStart) {
    options.onFiberStart(fiber);
  }
  return fiber;
};
var fiberRunIn = /* @__PURE__ */ dual(2, (self, scope) => {
  if (self._exit) {
    return self;
  } else if (scope.state._tag === "Closed") {
    self.interruptUnsafe(self.id);
    return self;
  }
  scopeRemoveFinalizerUnsafe(scope, self);
  scopeAddFinalizerUnsafe(scope, self, fiberScopeFinalizer(self));
  self.addObserver(() => scopeRemoveFinalizerUnsafe(scope, self));
  return self;
});
var runFork = /* @__PURE__ */ runForkWith(/* @__PURE__ */ empty());
var runPromiseExitWith = (context) => {
  const runFork = runForkWith(context);
  return (effect, options) => {
    const fiber = runFork(effect, options);
    return new Promise((resolve) => {
      fiber.addObserver((exit) => resolve(exit));
    });
  };
};
var runPromiseWith = (context) => {
  const runPromiseExit = runPromiseExitWith(context);
  return (effect, options) => runPromiseExit(effect, options).then((exit) => {
    if (exit._tag === "Failure") {
      throw causeSquash(exit.cause);
    }
    return exit.value;
  });
};
var runPromise = /* @__PURE__ */ runPromiseWith(/* @__PURE__ */ empty());
var runSyncExitWith = (context) => {
  const runFork = runForkWith(context);
  return (effect) => {
    if (effectIsExit(effect))
      return effect;
    const scheduler = new MixedScheduler("sync");
    const fiber = runFork(effect, {
      scheduler
    });
    fiber._dispatcher?.flush();
    return fiber._exit ?? exitDie(new AsyncFiberError(fiber));
  };
};
var runSyncExit = /* @__PURE__ */ runSyncExitWith(/* @__PURE__ */ empty());
var runSyncWith = (context) => {
  const runSyncExit = runSyncExitWith(context);
  return (effect) => {
    const exit = runSyncExit(effect);
    if (exit._tag === "Failure")
      throw causeSquash(exit.cause);
    return exit.value;
  };
};
var runSync = /* @__PURE__ */ runSyncWith(/* @__PURE__ */ empty());
var succeedTrue = /* @__PURE__ */ succeed3(true);
var succeedFalse = /* @__PURE__ */ succeed3(false);

class Latch {
  waiters = [];
  scheduled = undefined;
  _isOpen;
  constructor(isOpen) {
    this._isOpen = isOpen;
  }
  scheduleUnsafe(fiber) {
    if (this.waiters.length === 0) {
      return succeedTrue;
    }
    if (this.scheduled === undefined) {
      this.scheduled = this.waiters;
      fiber.currentDispatcher.scheduleTask(this.flushScheduled, 0);
    } else {
      for (let i = 0;i < this.waiters.length; i++) {
        this.scheduled.push(this.waiters[i]);
      }
    }
    this.waiters = [];
    return succeedTrue;
  }
  flushScheduled = () => {
    if (this.scheduled === undefined)
      return;
    const waiters = this.scheduled;
    this.scheduled = undefined;
    for (let i = 0;i < waiters.length; i++) {
      waiters[i](exitVoid);
    }
  };
  flushWaiters() {
    const waiters = this.waiters;
    this.waiters = [];
    this.flushScheduled();
    for (let i = 0;i < waiters.length; i++) {
      waiters[i](exitVoid);
    }
  }
  open = /* @__PURE__ */ withFiber((fiber) => {
    if (this._isOpen)
      return succeedFalse;
    this._isOpen = true;
    return this.scheduleUnsafe(fiber);
  });
  release = /* @__PURE__ */ withFiber((fiber) => this._isOpen ? succeedFalse : this.scheduleUnsafe(fiber));
  openUnsafe() {
    if (this._isOpen)
      return false;
    this._isOpen = true;
    this.flushWaiters();
    return true;
  }
  await = /* @__PURE__ */ callback((resume) => {
    if (this._isOpen) {
      return resume(void_);
    }
    this.waiters.push(resume);
    return sync(() => {
      let index = this.waiters.indexOf(resume);
      if (index !== -1) {
        this.waiters.splice(index, 1);
      } else if (this.scheduled !== undefined) {
        index = this.scheduled.indexOf(resume);
        if (index !== -1) {
          this.scheduled.splice(index, 1);
        }
      }
    });
  });
  closeUnsafe() {
    if (!this._isOpen)
      return false;
    this._isOpen = false;
    return true;
  }
  close = /* @__PURE__ */ sync(() => this.closeUnsafe());
  whenOpen = (self) => flatMap3(this.await, () => self);
  isOpen() {
    return this._isOpen;
  }
}
var makeLatchUnsafe = (open) => new Latch(open ?? false);
var bigint02 = /* @__PURE__ */ BigInt(0);
var NoopSpanProto = {
  _tag: "Span",
  spanId: "noop",
  traceId: "noop",
  sampled: false,
  status: {
    _tag: "Ended",
    startTime: bigint02,
    endTime: bigint02,
    exit: exitVoid
  },
  attributes: /* @__PURE__ */ new Map,
  links: [],
  kind: "internal",
  attribute() {},
  event() {},
  end() {},
  addLinks() {}
};
var noopSpan = (options) => Object.assign(Object.create(NoopSpanProto), options);
var filterDisablePropagation = (span) => {
  if (!span)
    return none2();
  return get(span.annotations, DisablePropagation) ? span._tag === "Span" ? filterDisablePropagation(getOrUndefined(span.parent)) : none2() : some2(span);
};
var makeSpanUnsafe = (fiber, name, options) => {
  const disablePropagation = !fiber.cache.tracerEnabled || options?.annotations && get(options.annotations, DisablePropagation);
  const parent = options?.parent !== undefined ? some2(options.parent) : options?.root ? none2() : filterDisablePropagation(fiber.cache.span);
  let span;
  if (disablePropagation) {
    span = noopSpan({
      name,
      parent,
      annotations: add(options?.annotations ?? empty(), DisablePropagation, true)
    });
  } else {
    const tracer = fiber.cache.tracer ?? nativeTracer;
    const clock = fiber.getRef(ClockRef);
    const timingEnabled = fiber.getRef(TracerTimingEnabled);
    const annotationsFromEnv = fiber.getRef(TracerSpanAnnotations);
    const linksFromEnv = fiber.getRef(TracerSpanLinks);
    const level = options?.level ?? fiber.getRef(CurrentTraceLevel);
    const links = options?.links !== undefined ? [...linksFromEnv, ...options.links] : linksFromEnv.length === 0 ? [] : linksFromEnv.slice();
    span = tracer.span({
      name,
      parent,
      annotations: options?.annotations ?? empty(),
      links,
      startTime: timingEnabled ? clock.currentTimeNanosUnsafe() : bigint02,
      kind: options?.kind ?? "internal",
      root: options?.root ?? false,
      sampled: options?.sampled ?? (isSome2(parent) && parent.value.sampled === false ? false : !isLogLevelGreaterThan(fiber.getRef(MinimumTraceLevel), level))
    });
    for (const key in annotationsFromEnv) {
      span.attribute(key, annotationsFromEnv[key]);
    }
    if (options?.attributes !== undefined) {
      for (const key in options.attributes) {
        span.attribute(key, options.attributes[key]);
      }
    }
  }
  return span;
};
var provideSpanStackFrame = (name, stack) => {
  stack = typeof stack === "function" ? stack : constUndefined;
  return updateService(CurrentStackFrame, (parent) => ({
    name,
    stack,
    parent
  }));
};
var endSpan = (span, exit, clock, timingEnabled) => sync(() => {
  if (span.status._tag === "Ended")
    return;
  span.end(timingEnabled ? clock.currentTimeNanosUnsafe() : bigint02, exit);
});
var useSpan = (name, ...args) => {
  const options = args.length === 1 ? undefined : args[0];
  const evaluate = args[args.length - 1];
  return withFiber((fiber) => {
    const span = makeSpanUnsafe(fiber, name, options);
    const clock = fiber.getRef(ClockRef);
    const timingEnabled = fiber.getRef(TracerTimingEnabled);
    onExitUnsafe(fiber, (exit) => endSpan(span, exit, clock, timingEnabled));
    return evaluate(span);
  });
};
var provideParentSpan = /* @__PURE__ */ provideService(ParentSpan);
var withParentSpan = function() {
  const dataFirst = isEffect(arguments[0]);
  const span = dataFirst ? arguments[1] : arguments[0];
  let options = dataFirst ? arguments[2] : arguments[1];
  let provideStackFrame = identity;
  if (span._tag === "Span") {
    options = addSpanStackTrace(options);
    provideStackFrame = provideSpanStackFrame(span.name, options?.captureStackTrace);
  }
  if (dataFirst) {
    return provideParentSpan(provideStackFrame(arguments[0]), span);
  }
  return (self) => provideParentSpan(provideStackFrame(self), span);
};
var ClockRef = /* @__PURE__ */ Reference("effect/Clock", {
  defaultValue: () => new ClockImpl
});
var MAX_TIMER_MILLIS = 2 ** 31 - 1;

class ClockImpl {
  currentTimeMillisUnsafe() {
    return Date.now();
  }
  currentTimeMillis = /* @__PURE__ */ sync(() => this.currentTimeMillisUnsafe());
  currentTimeNanosUnsafe() {
    return wallTimeNanos();
  }
  currentTimeNanos = /* @__PURE__ */ sync(() => this.currentTimeNanosUnsafe());
  monotonicTimeNanosUnsafe() {
    return monotonicNowNanos();
  }
  monotonicTimeNanos = /* @__PURE__ */ sync(() => this.monotonicTimeNanosUnsafe());
  sleep(duration) {
    return this.sleepMillis(toMillis(duration));
  }
  sleepMillis(millis) {
    if (millis <= 0)
      return yieldNow;
    else if (!Number.isFinite(millis))
      return never;
    return callback((resume) => {
      const continuation = millis > MAX_TIMER_MILLIS ? this.sleepMillis(millis - MAX_TIMER_MILLIS) : void_;
      const handle = setTimeout(() => resume(continuation), Math.min(millis, MAX_TIMER_MILLIS));
      return sync(() => clearTimeout(handle));
    });
  }
}
var nanosPerMilli = /* @__PURE__ */ BigInt(1e6);
var monotonicNowNanos = /* @__PURE__ */ function() {
  const processHrtime = globalThis.process?.hrtime;
  if (typeof processHrtime?.bigint === "function") {
    return () => processHrtime.bigint();
  }
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return () => BigInt(Math.round(performance.now() * 1e6));
  }
  let previous = /* @__PURE__ */ BigInt(0);
  return () => {
    const current = BigInt(Date.now()) * nanosPerMilli;
    if (current > previous) {
      previous = current;
    }
    return previous;
  };
}();
var wallTimeNanos = /* @__PURE__ */ function() {
  const reanchorThresholdNanos = /* @__PURE__ */ BigInt(1e9);
  let origin;
  return () => {
    const monotonic = monotonicNowNanos();
    const wall = BigInt(Date.now()) * nanosPerMilli;
    if (origin === undefined) {
      origin = wall - monotonic;
    } else {
      const projected = origin + monotonic;
      const skew = wall > projected ? wall - projected : projected - wall;
      if (skew > reanchorThresholdNanos) {
        origin = wall - monotonic;
      }
    }
    return origin + monotonic;
  };
}();
var clockWith = (f) => withFiber((fiber) => f(fiber.getRef(ClockRef)));
var sleep = (duration) => clockWith((clock) => clock.sleep(fromInputUnsafe(duration)));
var currentTimeMillis = /* @__PURE__ */ clockWith((clock) => clock.currentTimeMillis);
var IllegalArgumentErrorTypeId = "~effect/Cause/IllegalArgumentError";
class IllegalArgumentError extends (/* @__PURE__ */ TaggedError("IllegalArgumentError")) {
  [IllegalArgumentErrorTypeId] = IllegalArgumentErrorTypeId;
  constructor(message) {
    super({
      message
    });
  }
}
var AsyncFiberErrorTypeId = "~effect/Cause/AsyncFiberError";
class AsyncFiberError extends (/* @__PURE__ */ TaggedError("AsyncFiberError")) {
  [AsyncFiberErrorTypeId] = AsyncFiberErrorTypeId;
  constructor(fiber) {
    super({
      message: "An asynchronous Effect was executed with Effect.runSync",
      fiber
    });
  }
}
var UnknownErrorTypeId = "~effect/Cause/UnknownError";
class UnknownError extends (/* @__PURE__ */ TaggedError("UnknownError")) {
  [UnknownErrorTypeId] = UnknownErrorTypeId;
  constructor(cause, message) {
    super({
      message,
      cause
    });
  }
}
var ConsoleRef = /* @__PURE__ */ Reference("effect/Console", {
  defaultValue: () => globalThis.console
});
var logLevelToOrder = (level) => {
  switch (level) {
    case "All":
      return Number.MIN_SAFE_INTEGER;
    case "Fatal":
      return 50000;
    case "Error":
      return 40000;
    case "Warn":
      return 30000;
    case "Info":
      return 20000;
    case "Debug":
      return 1e4;
    case "Trace":
      return 0;
    case "None":
      return Number.MAX_SAFE_INTEGER;
  }
};
var LogLevelOrder = /* @__PURE__ */ mapInput(Number2, logLevelToOrder);
var isLogLevelGreaterThan = /* @__PURE__ */ isGreaterThan(LogLevelOrder);
var CurrentLoggers = /* @__PURE__ */ Reference("effect/Logger/CurrentLoggers", {
  defaultValue: () => new Set([defaultLogger, tracerLogger])
});
var LogToStderr = /* @__PURE__ */ Reference("effect/Logger/LogToStderr", {
  defaultValue: constFalse
});
var LoggerTypeId = "~effect/Logger";
var LoggerProto = {
  [LoggerTypeId]: {
    _Message: identity,
    _Output: identity
  },
  pipe() {
    return pipeArguments(this, arguments);
  }
};
var loggerMake = (log) => {
  const self = Object.create(LoggerProto);
  self.log = log;
  return self;
};
var formatLabel = (key) => key.replace(/[\s="]/g, "_");
var formatLogSpan = (self, now) => {
  const label = formatLabel(self[0]);
  return `${label}=${now - self[1]}ms`;
};
var logWithLevel = (level) => (...message) => {
  let cause = undefined;
  for (let i = 0, len = message.length;i < len; i++) {
    const msg = message[i];
    if (isCause(msg)) {
      if (cause) {
        message.splice(i, 1);
      } else {
        message = message.slice(0, i).concat(message.slice(i + 1));
      }
      cause = cause ? causeFromReasons(cause.reasons.concat(msg.reasons)) : msg;
      i--;
    }
  }
  if (cause === undefined) {
    cause = causeEmpty;
  }
  return withFiber((fiber) => {
    const logLevel = level ?? fiber.cache.logLevel;
    if (isLogLevelGreaterThan(fiber.cache.minimumLogLevel, logLevel)) {
      return void_;
    }
    const clock = fiber.getRef(ClockRef);
    const loggers = fiber.getRef(CurrentLoggers);
    if (loggers.size > 0) {
      const date = new Date(clock.currentTimeMillisUnsafe());
      for (const logger of loggers) {
        logger.log({
          cause,
          fiber,
          date,
          logLevel,
          message
        });
      }
    }
    return void_;
  });
};
var colors = {
  bold: "1",
  red: "31",
  green: "32",
  yellow: "33",
  blue: "34",
  cyan: "36",
  white: "37",
  gray: "90",
  black: "30",
  bgBrightRed: "101"
};
var logLevelColors = {
  None: [],
  All: [],
  Trace: [colors.gray],
  Debug: [colors.blue],
  Info: [colors.green],
  Warn: [colors.yellow],
  Error: [colors.red],
  Fatal: [colors.bgBrightRed, colors.black]
};
var defaultDateFormat = (date) => `${date.getHours().toString().padStart(2, "0")}:${date.getMinutes().toString().padStart(2, "0")}:${date.getSeconds().toString().padStart(2, "0")}.${date.getMilliseconds().toString().padStart(3, "0")}`;
var defaultLogger = /* @__PURE__ */ loggerMake(({
  cause,
  date,
  fiber,
  logLevel,
  message
}) => {
  const message_ = Array.isArray(message) ? message.slice() : [message];
  if (cause.reasons.length > 0) {
    message_.push(causePretty(cause));
  }
  const now = date.getTime();
  const spans = fiber.getRef(CurrentLogSpans);
  let spanString = "";
  for (const span of spans) {
    spanString += ` ${formatLogSpan(span, now)}`;
  }
  const annotations = fiber.getRef(CurrentLogAnnotations);
  if (Object.keys(annotations).length > 0) {
    message_.push(annotations);
  }
  const console = fiber.getRef(ConsoleRef);
  const log = fiber.getRef(LogToStderr) ? console.error : console.log;
  log(`[${defaultDateFormat(date)}] ${logLevel.toUpperCase()} (#${fiber.id})${spanString}:`, ...message_);
});
var tracerLogger = /* @__PURE__ */ loggerMake(({
  cause,
  fiber,
  logLevel,
  message
}) => {
  const clock = fiber.getRef(ClockRef);
  const annotations = fiber.getRef(CurrentLogAnnotations);
  const span = fiber.cache.span;
  if (span === undefined || span._tag === "ExternalSpan" || span.status._tag === "Ended")
    return;
  const attributes = {};
  for (const [key, value] of Object.entries(annotations)) {
    assignProperty(attributes, key, value);
  }
  attributes["effect.fiberId"] = fiber.id;
  attributes["effect.logLevel"] = logLevel.toUpperCase();
  if (cause.reasons.length > 0) {
    attributes["effect.cause"] = causePretty(cause);
  }
  span.event(toStringUnknown(Array.isArray(message) && message.length === 1 ? message[0] : message), clock.currentTimeNanosUnsafe(), attributes);
});
function interruptChildrenPatch() {
  fiberMiddleware.interruptChildren ??= fiberInterruptChildren;
}
var reportCauseUnsafe = (fiber, cause, defectsOnly) => {
  const reporters = fiber.getRef(CurrentErrorReporters);
  if (reporters.size === 0)
    return;
  if (defectsOnly && !hasDies(cause))
    return;
  const opts = {
    cause,
    fiber,
    timestamp: fiber.getRef(ClockRef).currentTimeNanosUnsafe()
  };
  reporters.forEach((reporter) => reporter.report(opts));
};

// node_modules/effect/dist/Cause.js
var isCause2 = isCause;
var isFailReason2 = isFailReason;
var isDieReason2 = isDieReason;
var fromReasons = causeFromReasons;
var fail4 = causeFail;
var die2 = causeDie;
var makeFailReason = (error) => new Fail(error);
var makeDieReason = (defect) => new Die(defect);
var hasInterruptsOnly2 = hasInterruptsOnly;
var map6 = causeMap;
var combine2 = causeCombine;
var squash = causeSquash;
var hasInterrupts2 = hasInterrupts;
var isNoSuchElementError2 = isNoSuchElementError;
var isDone2 = isDone;
var Done2 = Done;
var done2 = done;
var IllegalArgumentError2 = IllegalArgumentError;
var UnknownError2 = UnknownError;
var reasonAnnotations2 = reasonAnnotations;
class StackTrace extends (/* @__PURE__ */ Service()("effect/Cause/StackTrace")) {
}

// node_modules/effect/dist/Exit.js
var succeed4 = exitSucceed;
var failCause2 = exitFailCause;
var fail5 = exitFail;
var void_2 = exitVoid;
var isSuccess3 = exitIsSuccess;
var isFailure3 = exitIsFailure;

// node_modules/effect/dist/Deferred.js
var TypeId5 = "~effect/Deferred";
var DeferredProto = {
  [TypeId5]: {
    _A: identity,
    _E: identity
  },
  pipe() {
    return pipeArguments(this, arguments);
  }
};
var DeferredImpl = function() {
  this.resumes = undefined;
  this.effect = undefined;
};
DeferredImpl.prototype = DeferredProto;
var makeUnsafe2 = () => new DeferredImpl;
var make6 = () => sync(() => makeUnsafe2());
var _await = (self) => callback((resume) => {
  if (self.effect)
    return resume(self.effect);
  self.resumes ??= [];
  self.resumes.push(resume);
  return sync(() => {
    const resumes = self.resumes;
    if (resumes === undefined)
      return;
    const index = resumes.indexOf(resume);
    if (index >= 0)
      resumes.splice(index, 1);
  });
});
var completeWith = /* @__PURE__ */ dual(2, (self, effect) => sync(() => doneUnsafe(self, effect)));
var done3 = completeWith;
var fail6 = /* @__PURE__ */ dual(2, (self, error) => done3(self, exitFail(error)));
var isDone3 = (self) => sync(() => isDoneUnsafe(self));
var isDoneUnsafe = (self) => self.effect !== undefined;
var succeed5 = /* @__PURE__ */ dual(2, (self, value) => done3(self, exitSucceed(value)));
var doneUnsafe = (self, effect) => {
  if (self.effect)
    return false;
  self.effect = effect;
  if (self.resumes) {
    const resumes = self.resumes;
    self.resumes = undefined;
    for (let i = 0;i < resumes.length; i++) {
      resumes[i](effect);
    }
  }
  return true;
};

// node_modules/effect/dist/References.js
var MinimumLogLevel2 = MinimumLogLevel;
var TracerTimingEnabled2 = TracerTimingEnabled;

// node_modules/effect/dist/Scope.js
var Scope = scopeTag;
var makeUnsafe3 = scopeMakeUnsafe;
var provide = provideScope;
var addFinalizerExit = scopeAddFinalizerExit;
var addFinalizer2 = scopeAddFinalizer;
var forkUnsafe2 = scopeForkUnsafe;
var close = scopeClose;
var closeUnsafe = scopeCloseUnsafe;

// node_modules/effect/dist/Layer.js
var TypeId6 = "~effect/Layer";
var MemoMapTypeId = "~effect/Layer/MemoMap";
var makeMemoMapEntry = (memoMap, layer) => {
  const entry = {
    observers: 0,
    deferred: makeUnsafe2(),
    scope: makeUnsafe3(),
    finalizer: (exit) => suspend(() => {
      if (--entry.observers > 0)
        return void_;
      memoMap.map.delete(layer);
      return close(entry.scope, exit);
    })
  };
  return entry;
};
var memoMapObserve = (entry, scope) => {
  if (scope.state._tag === "Closed")
    return false;
  entry.observers++;
  scopeAddFinalizerUnsafe(scope, {}, entry.finalizer);
  return true;
};
var LayerProto = {
  [TypeId6]: {
    _ROut: identity,
    _E: identity,
    _RIn: identity
  },
  pipe() {
    return pipeArguments(this, arguments);
  }
};
var fromBuildUnsafe = (build) => {
  const self = Object.create(LayerProto);
  self.build = build;
  return self;
};
var fromBuild = (build) => fromBuildUnsafe((memoMap, scope) => {
  const layerScope = forkUnsafe2(scope);
  return onExit(build(memoMap, layerScope), (exit) => exit._tag === "Failure" ? close(layerScope, exit) : void_);
});
var fromBuildMemo = (build) => {
  const self = fromBuild((memoMap, scope) => memoMap.getOrElseMemoize(self, scope, build));
  return self;
};

class MemoMapImpl {
  get [MemoMapTypeId]() {
    return MemoMapTypeId;
  }
  parent;
  constructor(parent) {
    this.parent = parent;
  }
  map = /* @__PURE__ */ new Map;
  get(layer, scope) {
    const local = this.map.get(layer);
    if (local) {
      memoMapObserve(local, scope);
      return local.deferred.effect ?? _await(local.deferred);
    }
    return this.parent?.get(layer, scope);
  }
  getOrElseMemoize(layer, scope, build) {
    return suspend(() => {
      let deferred;
      return onExitPrimitive(suspend(() => {
        const existing = this.get(layer, scope);
        if (existing)
          return existing;
        const entry = makeMemoMapEntry(this, layer);
        if (!memoMapObserve(entry, scope))
          return build(this, scope);
        deferred = entry.deferred;
        this.map.set(layer, entry);
        return build(this, entry.scope);
      }), (exit) => {
        if (deferred)
          doneUnsafe(deferred, exit);
      });
    });
  }
}
var makeMemoMapUnsafe = () => new MemoMapImpl;
var forkMemoMapUnsafe = (parent) => new MemoMapImpl(parent);
class CurrentMemoMap extends (/* @__PURE__ */ Service()("effect/Layer/CurrentMemoMap")) {
  static forkOrCreate(self) {
    const current = getOrUndefined2(self, CurrentMemoMap);
    return current ? forkMemoMapUnsafe(current) : makeMemoMapUnsafe();
  }
}
var buildWithMemoMap = /* @__PURE__ */ dual(3, (self, memoMap, scope) => provideService(map5(suspend(() => self.build(memoMap, scope)), add(CurrentMemoMap, memoMap)), CurrentMemoMap, memoMap));
var buildWithScope = /* @__PURE__ */ dual(2, (self, scope) => withFiber((fiber) => buildWithMemoMap(self, CurrentMemoMap.forkOrCreate(fiber.context), scope)));
var succeed6 = function() {
  if (arguments.length === 1) {
    return (resource) => succeedContext(make3(arguments[0], resource));
  }
  return succeedContext(make3(arguments[0], arguments[1]));
};
var succeedContext = (context) => fromBuildUnsafe(constant(succeed3(context)));
var effect = function() {
  if (arguments.length === 1) {
    return (effect) => effectImpl(arguments[0], effect);
  }
  return effectImpl(arguments[0], arguments[1]);
};
var effectImpl = (service, effect) => effectContext(map5(effect, (value) => make3(service, value)));
var effectContext = (effect) => fromBuildMemo((_, scope) => provide(effect, scope));
var mergeAllEffect = (layers, memoMap, scope) => {
  const parentScope = forkUnsafe2(scope, "parallel");
  return forEach(layers, (layer) => layer.build(memoMap, forkUnsafe2(parentScope, "sequential")), {
    concurrency: layers.length
  }).pipe(map5((context) => mergeAll(...context)));
};
var mergeAll2 = (...layers) => fromBuildMemo((memoMap, scope) => mergeAllEffect(layers, memoMap, scope));
var provideWith = (self, that, f) => fromBuildMemo((memoMap, scope) => flatMap3(Array.isArray(that) ? mergeAllEffect(that, memoMap, scope) : that.build(memoMap, scope), (context) => self.build(memoMap, scope).pipe(provideContext(context), map5((merged) => f(merged, context)))));
var provide2 = /* @__PURE__ */ dual(2, (self, that) => provideWith(self, that, identity));
var provideMerge = /* @__PURE__ */ dual(2, (self, that) => provideWith(self, that, (self, that) => merge(that, self)));
var fresh = (self) => fromBuildUnsafe((_, scope) => self.build(makeMemoMapUnsafe(), scope));

// node_modules/effect/dist/Clock.js
var Clock = ClockRef;
var currentTimeMillis2 = currentTimeMillis;

// node_modules/effect/dist/internal/dateTime.js
var TypeId7 = "~effect/DateTime";
var TimeZoneTypeId = "~effect/DateTime/TimeZone";
var Proto2 = {
  [TypeId7]: TypeId7,
  pipe() {
    return pipeArguments(this, arguments);
  },
  [NodeInspectSymbol]() {
    return this.toString();
  },
  toJSON() {
    return toDateUtc(this).toJSON();
  }
};
var ProtoUtc = {
  ...Proto2,
  _tag: "Utc",
  [symbol]() {
    return number(this.epochMilliseconds);
  },
  [symbol2](that) {
    return isDateTime(that) && that._tag === "Utc" && this.epochMilliseconds === that.epochMilliseconds;
  },
  toString() {
    return `DateTime.Utc(${toDateUtc(this).toJSON()})`;
  }
};
var ProtoZoned = {
  ...Proto2,
  _tag: "Zoned",
  [symbol]() {
    return combine(number(this.epochMilliseconds))(hash(this.zone));
  },
  [symbol2](that) {
    return isDateTime(that) && that._tag === "Zoned" && this.epochMilliseconds === that.epochMilliseconds && equals(this.zone, that.zone);
  },
  toString() {
    return `DateTime.Zoned(${formatIsoZoned(this)})`;
  }
};
var ProtoTimeZone = {
  [TimeZoneTypeId]: TimeZoneTypeId,
  [NodeInspectSymbol]() {
    return this.toString();
  }
};
var ProtoTimeZoneNamed = {
  ...ProtoTimeZone,
  _tag: "Named",
  [symbol]() {
    return string(`Named:${this.id}`);
  },
  [symbol2](that) {
    return isTimeZone(that) && that._tag === "Named" && this.id === that.id;
  },
  toString() {
    return `TimeZone.Named(${this.id})`;
  },
  toJSON() {
    return {
      _id: "TimeZone",
      _tag: "Named",
      id: this.id
    };
  }
};
var ProtoTimeZoneOffset = {
  ...ProtoTimeZone,
  _tag: "Offset",
  [symbol]() {
    return string(`Offset:${this.offset}`);
  },
  [symbol2](that) {
    return isTimeZone(that) && that._tag === "Offset" && this.offset === that.offset;
  },
  toString() {
    return `TimeZone.Offset(${offsetToString(this.offset)})`;
  },
  toJSON() {
    return {
      _id: "TimeZone",
      _tag: "Offset",
      offset: this.offset
    };
  }
};
var cacheSlot = {
  value: undefined,
  enumerable: false,
  writable: true
};
var sealedCaches = /* @__PURE__ */ new WeakMap;
var cacheAccessor = (key, slots) => ({
  get() {
    return Object.isExtensible(this) ? undefined : sealedCaches.get(this)?.[key];
  },
  set(value) {
    if (Object.isExtensible(this)) {
      for (const slot of slots) {
        Object.defineProperty(this, slot, cacheSlot);
      }
      this[key] = value;
    } else {
      let caches = sealedCaches.get(this);
      if (caches === undefined) {
        caches = {};
        sealedCaches.set(this, caches);
      }
      caches[key] = value;
    }
  }
});
var zonedSlots = ["partsUtc", "adjustedEpochMilliseconds", "partsAdjusted"];
Object.defineProperty(ProtoUtc, "partsUtc", cacheAccessor("partsUtc", ["partsUtc"]));
Object.defineProperties(ProtoZoned, {
  partsUtc: cacheAccessor("partsUtc", zonedSlots),
  adjustedEpochMilliseconds: cacheAccessor("adjustedEpochMilliseconds", zonedSlots),
  partsAdjusted: cacheAccessor("partsAdjusted", zonedSlots)
});
var isDateTime = (u) => hasProperty(u, TypeId7);
var isTimeZone = (u) => hasProperty(u, TimeZoneTypeId);
var makeUtc = (epochMillis) => {
  const self = Object.create(ProtoUtc);
  self.epochMilliseconds = epochMillis;
  return self;
};
var fromDateUnsafe = (date) => {
  const epochMillis = date.getTime();
  if (Number.isNaN(epochMillis)) {
    throw new IllegalArgumentError2("Invalid date");
  }
  return makeUtc(epochMillis);
};
var makeUnsafe4 = (input) => {
  if (isDateTime(input)) {
    return input;
  } else if (input instanceof Date) {
    return fromDateUnsafe(input);
  } else if (typeof input === "object") {
    if ("epochMilliseconds" in input) {
      return fromDateUnsafe(new Date(input.epochMilliseconds));
    }
    const date = new Date(0);
    setPartsDate(date, input);
    return fromDateUnsafe(date);
  } else if (typeof input === "string" && !hasZone(input)) {
    return fromDateUnsafe(new Date(input + "Z"));
  }
  return fromDateUnsafe(new Date(input));
};
var hasZone = (input) => /Z|GMT|[+-]\d{2}$|[+-]\d{2}:?\d{2}$|\]$/.test(input) || /z$|gmt$/i.test(input);
var minEpochMillis = -8640000000000000 + 12 * 60 * 60 * 1000;
var maxEpochMillis = 8640000000000000 - 14 * 60 * 60 * 1000;
var toDateUtc = (self) => new Date(self.epochMilliseconds);
var toDate = (self) => {
  if (self._tag === "Utc") {
    return new Date(self.epochMilliseconds);
  } else if (self.zone._tag === "Offset") {
    return new Date(self.epochMilliseconds + self.zone.offset);
  } else if (self.adjustedEpochMilliseconds !== undefined) {
    return new Date(self.adjustedEpochMilliseconds);
  }
  let bc = false;
  const parts = self.zone.format.formatToParts(self.epochMilliseconds).filter((_) => {
    if (_.type === "era") {
      bc = _.value === "BC";
      return false;
    }
    return _.type !== "literal";
  });
  const year = Number(parts[2].value);
  const date = new Date(0);
  date.setUTCFullYear(bc ? 1 - year : year, Number(parts[0].value) - 1, Number(parts[1].value));
  date.setUTCHours(Number(parts[3].value), Number(parts[4].value), Number(parts[5].value), parts[6]?.type === "fractionalSecond" ? Number(parts[6].value) : (self.epochMilliseconds % 1000 + 1000) % 1000);
  self.adjustedEpochMilliseconds = date.getTime();
  return date;
};
var zonedOffset = (self) => {
  const date = toDate(self);
  return date.getTime() - toEpochMillis(self);
};
var offsetToString = (offset) => {
  const abs = Math.abs(offset);
  let hours = Math.floor(abs / (60 * 60 * 1000));
  let minutes = Math.round(abs % (60 * 60 * 1000) / (60 * 1000));
  if (minutes === 60) {
    hours += 1;
    minutes = 0;
  }
  return `${offset < 0 ? "-" : "+"}${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
};
var zonedOffsetIso = (self) => offsetToString(zonedOffset(self));
var toEpochMillis = (self) => self.epochMilliseconds;
var setPartsDate = (date, parts) => {
  if (parts.year !== undefined || parts.month !== undefined || parts.day !== undefined) {
    date.setUTCFullYear(parts.year ?? date.getUTCFullYear(), parts.month !== undefined ? parts.month - 1 : date.getUTCMonth(), parts.day ?? date.getUTCDate());
  }
  if (parts.weekDay !== undefined) {
    const diff = parts.weekDay - date.getUTCDay();
    date.setUTCDate(date.getUTCDate() + diff);
  }
  if (parts.hour !== undefined) {
    date.setUTCHours(parts.hour);
  }
  if (parts.minute !== undefined) {
    date.setUTCMinutes(parts.minute);
  }
  if (parts.second !== undefined) {
    date.setUTCSeconds(parts.second);
  }
  if (parts.millisecond !== undefined) {
    date.setUTCMilliseconds(parts.millisecond);
  }
};
var constDayMillis = 24 * 60 * 60 * 1000;
var formatIso = (self) => toDateUtc(self).toISOString();
var formatIsoOffset = (self) => {
  const date = toDate(self);
  return self._tag === "Utc" ? date.toISOString() : `${date.toISOString().slice(0, -1)}${zonedOffsetIso(self)}`;
};
var formatIsoZoned = (self) => self.zone._tag === "Offset" ? formatIsoOffset(self) : `${formatIsoOffset(self)}[${self.zone.id}]`;

// node_modules/effect/dist/Number.js
var Number3 = globalThis.Number;
var round = /* @__PURE__ */ dual(2, (self, precision) => {
  const factor = Math.pow(10, precision);
  return Math.round(self * factor) / factor;
});

// node_modules/effect/dist/Pull.js
var catchDone = /* @__PURE__ */ dual(2, (effect, f) => catchCauseFilter(effect, filterDoneLeftover, (l) => f(l)));
var isDoneCause = (cause) => cause.reasons.some(isDoneFailure);
var isDoneFailure = (failure) => failure._tag === "Fail" && isDone2(failure.error);
var filterDone = (cause) => {
  let done;
  let hasFailure = false;
  for (const reason of cause.reasons) {
    if (isDoneFailure(reason)) {
      done ??= reason.error;
    } else if (reason._tag !== "Interrupt") {
      hasFailure = true;
    }
  }
  if (done === undefined)
    return fail2(cause);
  return hasFailure ? fail2(fromReasons(cause.reasons.filter((reason) => !isDoneFailure(reason)))) : succeed2(done);
};
var filterDoneLeftover = (cause) => {
  const done = filterDone(cause);
  return isFailure2(done) ? done : succeed2(done.success.value);
};
var doneExitFromCause = (cause) => {
  const halt = filterDone(cause);
  return !isFailure2(halt) ? succeed4(halt.success.value) : failCause2(halt.failure);
};
var matchEffect2 = /* @__PURE__ */ dual(2, (self, options) => matchCauseEffect(self, {
  onSuccess: options.onSuccess,
  onFailure: (cause) => {
    const halt = filterDone(cause);
    return !isFailure2(halt) ? options.onDone(halt.success.value) : options.onFailure(halt.failure);
  }
}));

// node_modules/effect/dist/internal/layer.js
var provideLayer = (self, layer, options) => scopedWith((scope) => flatMap3(options?.local ? buildWithMemoMap(layer, makeMemoMapUnsafe(), scope) : buildWithScope(layer, scope), (context) => provideContext(self, context)));
var provide3 = /* @__PURE__ */ dual((args) => isEffect(args[0]), (self, source, options) => isContext(source) ? provideContext(self, source) : provideLayer(self, Array.isArray(source) ? mergeAll2(...source) : source, options));

// node_modules/effect/dist/Effect.js
var isEffect2 = isEffect;
var all3 = all2;
var forEach2 = forEach;
var whileLoop2 = whileLoop;
var promise2 = promise;
var tryPromise2 = tryPromise;
var succeed7 = succeed3;
var suspend2 = suspend;
var sync2 = sync;
var void_3 = void_;
var callback2 = callback;
var never2 = never;
var gen2 = gen;
var fail7 = fail3;
var failSync2 = failSync;
var failCause3 = failCause;
var failCauseSync2 = failCauseSync;
var die3 = die;
var try_3 = try_2;
var withFiber2 = withFiber;
var fromResult2 = fromResult;
var flatMap4 = flatMap3;
var andThen2 = andThen;
var tap2 = tap;
var exit2 = exit;
var map7 = map5;
var as2 = as;
var asVoid2 = asVoid;
var zipWith2 = zipWith;
var catch_2 = catch_;
var catchTag2 = catchTag;
var catchTags2 = catchTags;
var catchReason2 = catchReason;
var catchCause2 = catchCause;
var catchIf2 = catchIf;
var catchFilter2 = catchFilter;
var mapError3 = mapError2;
var orDie2 = orDie;
var tapError2 = tapError;
var tapCause2 = tapCause;
var ignore2 = ignore;
var ignoreCause2 = ignoreCause;
var orElseSucceed2 = orElseSucceed;
var timeoutOrElse2 = timeoutOrElse;
var sleep2 = sleep;
var raceFirst2 = raceFirst;
var match6 = match5;
var matchCauseEffectEager2 = matchCauseEffectEager;
var matchCauseEffect2 = matchCauseEffect;
var matchEffect3 = matchEffect;
var context2 = context;
var contextWith2 = contextWith;
var provide4 = provide3;
var provideContext2 = provideContext;
var serviceOption2 = serviceOption;
var updateContext2 = updateContext;
var provideService2 = provideService;
var scope2 = scope;
var scoped2 = scoped;
var scopedWith2 = scopedWith;
var acquireRelease2 = acquireRelease;
var addFinalizer3 = addFinalizer;
var ensuring2 = ensuring;
var onError2 = onError;
var onExitPrimitive2 = onExitPrimitive;
var onExit2 = onExit;
var cached2 = cached;
var interrupt2 = interrupt;
var interruptible2 = interruptible;
var onInterrupt2 = onInterrupt;
var uninterruptible2 = uninterruptible;
var uninterruptibleMask2 = uninterruptibleMask;
var forever2 = forever;
var useSpan2 = useSpan;
var withParentSpan2 = withParentSpan;
var forkChild2 = forkChild;
var forkIn2 = forkIn;
var forkScoped2 = forkScoped;
var runFork2 = runFork;
var runForkWith2 = runForkWith;
var runPromise2 = runPromise;
var runSync2 = runSync;
var runSyncExit2 = runSyncExit;
var fnUntraced2 = fnUntraced;
var fn2 = fn;
var clockWith2 = clockWith;
var logError = /* @__PURE__ */ logWithLevel("Error");
var effectify = (fn, onError, onSyncError) => (...args) => callback2((resume) => {
  let synchronous = true;
  let pending;
  try {
    fn(...args, (err, result) => {
      const effect = err ? onError ? suspend2(() => fail7(onError(err, args))) : fail7(err) : succeed7(result);
      if (synchronous) {
        pending ??= effect;
      } else {
        resume(effect);
      }
    });
  } catch (err) {
    pending = onSyncError ? fail7(onSyncError(err, args)) : die3(err);
  }
  synchronous = false;
  if (pending) {
    resume(pending);
  }
});
var mapEager2 = mapEager;
var mapErrorEager2 = mapErrorEager;
var flatMapEager2 = flatMapEager;
var catchEager2 = catchEager;
var fnUntracedEager2 = fnUntracedEager;

// node_modules/effect/dist/BigInt.js
var BigInt2 = globalThis.BigInt;
var toNumber = (b) => {
  if (b > BigInt2(Number.MAX_SAFE_INTEGER) || b < BigInt2(Number.MIN_SAFE_INTEGER)) {
    return none2();
  }
  return some2(Number(b));
};

// node_modules/effect/dist/ByteSize.js
var bigint03 = /* @__PURE__ */ BigInt(0);
var bigint12 = /* @__PURE__ */ BigInt(1);
var decimalBase = /* @__PURE__ */ BigInt(1000);
var binaryBase = /* @__PURE__ */ BigInt(1024);
var decimalUnits = [{
  symbol: "B",
  factor: bigint12,
  names: ["B", "byte", "bytes"]
}, {
  symbol: "kB",
  factor: decimalBase,
  names: ["kB", "kilobyte", "kilobytes"]
}, {
  symbol: "MB",
  factor: decimalBase ** /* @__PURE__ */ BigInt(2),
  names: ["MB", "megabyte", "megabytes"]
}, {
  symbol: "GB",
  factor: decimalBase ** /* @__PURE__ */ BigInt(3),
  names: ["GB", "gigabyte", "gigabytes"]
}, {
  symbol: "TB",
  factor: decimalBase ** /* @__PURE__ */ BigInt(4),
  names: ["TB", "terabyte", "terabytes"]
}, {
  symbol: "PB",
  factor: decimalBase ** /* @__PURE__ */ BigInt(5),
  names: ["PB", "petabyte", "petabytes"]
}, {
  symbol: "EB",
  factor: decimalBase ** /* @__PURE__ */ BigInt(6),
  names: ["EB", "exabyte", "exabytes"]
}, {
  symbol: "ZB",
  factor: decimalBase ** /* @__PURE__ */ BigInt(7),
  names: ["ZB", "zettabyte", "zettabytes"]
}, {
  symbol: "YB",
  factor: decimalBase ** /* @__PURE__ */ BigInt(8),
  names: ["YB", "yottabyte", "yottabytes"]
}, {
  symbol: "RB",
  factor: decimalBase ** /* @__PURE__ */ BigInt(9),
  names: ["RB", "ronnabyte", "ronnabytes"]
}, {
  symbol: "QB",
  factor: decimalBase ** /* @__PURE__ */ BigInt(10),
  names: ["QB", "quettabyte", "quettabytes"]
}];
var binaryUnits = [decimalUnits[0], {
  symbol: "KiB",
  factor: binaryBase,
  names: ["KiB", "kibibyte", "kibibytes"]
}, {
  symbol: "MiB",
  factor: binaryBase ** /* @__PURE__ */ BigInt(2),
  names: ["MiB", "mebibyte", "mebibytes"]
}, {
  symbol: "GiB",
  factor: binaryBase ** /* @__PURE__ */ BigInt(3),
  names: ["GiB", "gibibyte", "gibibytes"]
}, {
  symbol: "TiB",
  factor: binaryBase ** /* @__PURE__ */ BigInt(4),
  names: ["TiB", "tebibyte", "tebibytes"]
}, {
  symbol: "PiB",
  factor: binaryBase ** /* @__PURE__ */ BigInt(5),
  names: ["PiB", "pebibyte", "pebibytes"]
}, {
  symbol: "EiB",
  factor: binaryBase ** /* @__PURE__ */ BigInt(6),
  names: ["EiB", "exbibyte", "exbibytes"]
}, {
  symbol: "ZiB",
  factor: binaryBase ** /* @__PURE__ */ BigInt(7),
  names: ["ZiB", "zebibyte", "zebibytes"]
}, {
  symbol: "YiB",
  factor: binaryBase ** /* @__PURE__ */ BigInt(8),
  names: ["YiB", "yobibyte", "yobibytes"]
}];
var allUnits = [...decimalUnits, .../* @__PURE__ */ binaryUnits.slice(1)];
var unitsByName = /* @__PURE__ */ new Map(/* @__PURE__ */ allUnits.flatMap((unit) => unit.names.map((name) => [name, unit])));
var unitsBySymbol = /* @__PURE__ */ new Map(/* @__PURE__ */ allUnits.map((unit) => [unit.symbol, unit]));
var make7 = (value) => value;
var zero2 = /* @__PURE__ */ make7(bigint03);
var invalid2 = (message) => {
  throw new Error(`Invalid ByteSize: ${message}`);
};
var fromNumber = (input) => {
  if (!Number.isSafeInteger(input) || input < 0) {
    return invalid2(`expected a non-negative safe integer, received ${input}`);
  }
  return make7(BigInt(input));
};
var maxSafeInteger = /* @__PURE__ */ BigInt(Number.MAX_SAFE_INTEGER);
var scaleDecimal = (quantity, factor) => {
  const match = /^(\d+)(?:\.(\d+))?(?:e([+-]\d+))?$/.exec(quantity);
  if (match === null)
    return;
  const fraction = match[2] ?? "";
  const numerator = BigInt(match[1] + fraction) * factor;
  const shift = Number(match[3] ?? 0) - fraction.length;
  if (shift >= 0)
    return numerator * BigInt(10) ** BigInt(shift);
  const scale = BigInt(10) ** BigInt(-shift);
  return numerator % scale === bigint03 ? numerator / scale : undefined;
};
var fromQuantity = (quantity, unit) => {
  if (typeof quantity === "bigint") {
    if (quantity < bigint03)
      return invalid2(`expected a non-negative quantity, received ${quantity}`);
    return make7(quantity * unit.factor);
  }
  if (binaryUnits.includes(unit)) {
    const value = quantity * Number(unit.factor);
    if (Number.isSafeInteger(value) && value >= 0)
      return make7(BigInt(value));
  } else {
    const value = scaleDecimal(String(quantity), unit.factor);
    if (value !== undefined && value <= maxSafeInteger)
      return make7(value);
  }
  return invalid2(`expected an exact non-negative safe-integer byte result, received ${quantity} ${unit.symbol}`);
};
var fromStringUnsafe = (input) => {
  const match = /^\s*(\d+(?:\.\d+)?)\s*([A-Za-z]+)\s*$/.exec(input);
  if (match === null)
    return invalid2(`unsupported syntax ${JSON.stringify(input)}`);
  const unit = unitsByName.get(match[2]);
  if (unit === undefined)
    return invalid2(`unsupported unit ${JSON.stringify(match[2])}`);
  const value = scaleDecimal(match[1], unit.factor);
  if (value === undefined)
    return invalid2(`${JSON.stringify(input)} does not represent an integral number of bytes`);
  return make7(value);
};
var fromInputUnsafe2 = (input) => {
  switch (typeof input) {
    case "bigint":
      if (input < bigint03)
        return invalid2(`expected a non-negative bigint, received ${input}`);
      return make7(input);
    case "number":
      return fromNumber(input);
    case "string":
      return fromStringUnsafe(input);
  }
  return invalid2(`unsupported input ${input}`);
};
var fromInput = /* @__PURE__ */ liftThrowable(fromInputUnsafe2);
var bytes = (value) => typeof value === "bigint" ? fromInputUnsafe2(value) : fromNumber(value);
var unitConstructor = (symbol) => (value) => fromQuantity(value, unitsBySymbol.get(symbol));
var mebibytes = /* @__PURE__ */ unitConstructor("MiB");

// node_modules/effect/dist/Fiber.js
var join = fiberJoin;
var interrupt3 = fiberInterrupt;
var getCurrent = getCurrentFiber;
var runIn = fiberRunIn;

// node_modules/effect/dist/internal/glob.js
var maxExpansions = 1e4;
var maxExpandedLength = 2 ** 22;
var maxBraceDepth = 64;
var compile = (patterns, dot) => {
  const nodes = [];
  const starts = new Set;
  for (const pattern of patterns) {
    for (const expanded of expandBraces(pattern)) {
      const start = nodes.length;
      if (compilePattern(expanded, nodes)) {
        starts.add(start);
      }
    }
  }
  const initial = closure(nodes, starts);
  return {
    nodes,
    initial,
    dot,
    masks: nodes.length <= 32 ? compileMasks(nodes, initial) : undefined
  };
};
var compileMasks = (nodes, initial) => {
  let start = 0;
  for (const state of initial) {
    start |= 1 << state;
  }
  let globstar = 0;
  let end = 0;
  let endFile = 0;
  for (let i = 0;i < nodes.length; i++) {
    const node = nodes[i];
    const bit = 1 << i;
    if (node._tag === "Globstar") {
      globstar |= bit;
    } else if (node._tag === "End") {
      end |= bit;
      if (!node.directoryOnly) {
        endFile |= bit;
      }
    }
  }
  return {
    initial: start,
    globstar,
    end,
    endFile
  };
};
var isUnc = (path) => path.startsWith("//") && path[2] !== undefined && path[2] !== "/";
var compilePattern = (pattern, nodes) => {
  if (pattern === "" || isUnc(pattern)) {
    return false;
  }
  const raw = splitSegments(pattern);
  const globstar = raw.indexOf("**");
  if (globstar !== -1 && raw.some((segment, i) => i > globstar && (segment === "." || segment === ".."))) {
    return false;
  }
  const {
    directory,
    segments
  } = normalize2(raw);
  if (segments.length === 0) {
    return false;
  }
  for (const segment of segments) {
    if (segment !== "**") {
      nodes.push(compileSegment(Array.from(segment)));
    } else if (nodes[nodes.length - 1] !== globstarNode) {
      nodes.push(globstarNode);
    }
  }
  nodes.push({
    _tag: "End",
    directoryOnly: directory
  });
  return true;
};
var globstarNode = {
  _tag: "Globstar"
};
var splitSegments = (pattern) => {
  const segments = [];
  let start = 0;
  for (let i = 0;i < pattern.length; i++) {
    if (pattern[i] === "\\") {
      i++;
    } else if (pattern[i] === "/") {
      segments.push(pattern.slice(start, i));
      start = i + 1;
    }
  }
  segments.push(pattern.slice(start));
  return segments;
};
var drive = /^[A-Za-z]:/;
var isDrive = (segment) => segment.length === 2 && drive.test(segment);
var isRoot = (name) => name === "" || name.length === 3 && name[2] === "/" && drive.test(name);
var isAbsolute = (path) => path.startsWith("/") || path[2] === "/" && drive.test(path);
var normalize2 = (raw) => {
  const segments = [];
  let i = 0;
  if (raw.length > 1 && (raw[0] === "" || isDrive(raw[0]))) {
    segments.push(raw[0] === "" ? "" : `${raw[0]}/`);
    i = 1;
  }
  let directory = false;
  for (;i < raw.length; i++) {
    const segment = raw[i];
    if (segment === "" || segment === ".") {
      directory = i === raw.length - 1 && segments.length > 0;
    } else {
      segments.push(segment);
      directory = false;
    }
  }
  if (segments.length === 0 && raw.includes(".")) {
    segments.push(".");
  }
  return {
    segments,
    directory
  };
};
var compileSegment = (chars) => {
  const tokens = [];
  let literal = "";
  let magic = false;
  let classes = true;
  for (let i = 0;i < chars.length; i++) {
    const char = chars[i];
    if (char === "\\" && i + 1 < chars.length) {
      literal += chars[++i];
      tokens.push({
        _tag: "Char",
        code: chars[i].codePointAt(0)
      });
    } else if (char === "*") {
      magic = true;
      if (tokens[tokens.length - 1]?._tag !== "Star") {
        tokens.push({
          _tag: "Star"
        });
      }
    } else if (char === "?") {
      magic = true;
      tokens.push({
        _tag: "Any"
      });
    } else if (char === "[" && classes) {
      const parsed = parseClass(chars, i);
      if (parsed === undefined) {
        classes = false;
        literal += char;
        tokens.push({
          _tag: "Char",
          code: char.codePointAt(0)
        });
      } else {
        magic = true;
        tokens.push(parsed.token);
        i = parsed.end;
      }
    } else {
      literal += char;
      tokens.push({
        _tag: "Char",
        code: char.codePointAt(0)
      });
    }
  }
  if (!magic) {
    return {
      _tag: "Literal",
      value: literal
    };
  }
  const first = tokens[0];
  return {
    _tag: "Wildcard",
    tokens,
    leadingDot: first._tag === "Char" && first.code === 46,
    affixes: compileAffixes(tokens)
  };
};
var compileAffixes = (tokens) => {
  let prefix = "";
  let suffix;
  for (const token of tokens) {
    if (token._tag === "Star" && suffix === undefined) {
      suffix = "";
    } else if (token._tag === "Char" && (token.code < 55296 || token.code > 57343)) {
      const char = String.fromCodePoint(token.code);
      if (suffix === undefined) {
        prefix += char;
      } else {
        suffix += char;
      }
    } else {
      return;
    }
  }
  return suffix === undefined ? undefined : {
    prefix,
    suffix
  };
};
var range = (from, to) => [from.codePointAt(0), to.codePointAt(0)];
var posixClasses = {
  alnum: [.../* @__PURE__ */ range("0", "9"), .../* @__PURE__ */ range("A", "Z"), .../* @__PURE__ */ range("a", "z")],
  alpha: [.../* @__PURE__ */ range("A", "Z"), .../* @__PURE__ */ range("a", "z")],
  blank: [9, 9, 32, 32],
  cntrl: [0, 31, 127, 127],
  digit: /* @__PURE__ */ range("0", "9"),
  graph: [33, 126],
  lower: /* @__PURE__ */ range("a", "z"),
  print: [32, 126],
  punct: [33, 47, 58, 64, 91, 96, 123, 126],
  space: [9, 13, 32, 32],
  upper: /* @__PURE__ */ range("A", "Z"),
  word: [.../* @__PURE__ */ range("0", "9"), .../* @__PURE__ */ range("A", "Z"), 95, 95, .../* @__PURE__ */ range("a", "z")],
  xdigit: [.../* @__PURE__ */ range("0", "9"), .../* @__PURE__ */ range("A", "F"), .../* @__PURE__ */ range("a", "f")]
};
var parsePosixClass = (chars, open) => {
  for (let i = open + 2;i < chars.length && i <= open + 8; i++) {
    if (chars[i] === ":" && chars[i + 1] === "]") {
      const ranges = posixClasses[chars.slice(open + 2, i).join("")];
      return ranges === undefined ? undefined : {
        ranges,
        end: i + 1
      };
    }
  }
  return;
};
var parseClass = (chars, open) => {
  let i = open + 1;
  const negated = chars[i] === "!" || chars[i] === "^";
  if (negated) {
    i++;
  }
  const ranges = [];
  for (let first = true;i < chars.length; i++, first = false) {
    let char = chars[i];
    if (char === "/") {
      return;
    }
    if (char === "]" && !first) {
      return {
        token: {
          _tag: "Class",
          ranges,
          negated
        },
        end: i
      };
    }
    if (char === "[" && chars[i + 1] === ":") {
      const posix = parsePosixClass(chars, i);
      if (posix !== undefined) {
        ranges.push(...posix.ranges);
        i = posix.end;
        continue;
      }
    }
    if (char === "\\" && i + 1 < chars.length) {
      char = chars[++i];
    }
    if (chars[i + 1] === "-" && i + 2 < chars.length && chars[i + 2] !== "]") {
      let next = i + 2;
      if (chars[next] === "/") {
        return;
      }
      if (chars[next] === "\\" && next + 1 < chars.length) {
        next++;
      }
      ranges.push(...range(char, chars[next]));
      i = next;
      continue;
    }
    ranges.push(...range(char, char));
  }
  return;
};
var expandBraces = (pattern) => {
  const chars = Array.from(pattern);
  const groups = matchBraceGroups(chars);
  if (groups.size === 0) {
    return [pattern];
  }
  const sequence = parseBraceSequence(chars, 0, chars.length, groups, 0);
  if (sequence === undefined) {
    return [pattern];
  }
  const count = countExpansions(sequence);
  return count > maxExpansions || count * pattern.length > maxExpandedLength ? [pattern] : expandSequence(sequence);
};
var matchBraceGroups = (chars) => {
  const groups = new Map;
  const stack = [];
  let classes = true;
  for (let i = 0;i < chars.length; i++) {
    const char = chars[i];
    if (char === "\\") {
      i++;
    } else if (char === "/") {
      classes = true;
    } else if (char === "[" && classes) {
      const parsed = parseClass(chars, i);
      if (parsed === undefined) {
        classes = false;
      } else {
        i = parsed.end;
      }
    } else if (char === "{") {
      stack.push({
        open: i,
        commas: []
      });
    } else if (char === ",") {
      stack[stack.length - 1]?.commas.push(i);
    } else if (char === "}") {
      const group = stack.pop();
      if (group !== undefined && group.commas.length > 0) {
        groups.set(group.open, {
          commas: group.commas,
          close: i
        });
      }
    }
  }
  return groups;
};
var parseBraceSequence = (chars, start, end, groups, depth) => {
  if (depth > maxBraceDepth) {
    return;
  }
  const parts = [];
  let text = "";
  for (let i = start;i < end; i++) {
    const group = groups.get(i);
    if (group === undefined) {
      if (chars[i] === "\\" && i + 1 < end) {
        text += chars[i++];
      }
      text += chars[i];
      continue;
    }
    const alternatives = [];
    let from = i + 1;
    for (const to of [...group.commas, group.close]) {
      const alternative = parseBraceSequence(chars, from, to, groups, depth + 1);
      if (alternative === undefined) {
        return;
      }
      alternatives.push(alternative);
      from = to + 1;
    }
    if (text !== "") {
      parts.push(text);
      text = "";
    }
    parts.push(alternatives);
    i = group.close;
  }
  if (text !== "") {
    parts.push(text);
  }
  return parts;
};
var countExpansions = (sequence) => {
  let count = 1;
  for (const part of sequence) {
    if (typeof part !== "string") {
      let sum = 0;
      for (const alternative of part) {
        sum += countExpansions(alternative);
      }
      count *= sum;
      if (count > maxExpansions) {
        return Infinity;
      }
    }
  }
  return count;
};
var expandSequence = (sequence) => {
  let out = [""];
  for (const part of sequence) {
    if (typeof part === "string") {
      out = out.map((prefix) => prefix + part);
    } else {
      const alternatives = part.flatMap(expandSequence);
      out = out.flatMap((prefix) => alternatives.map((alternative) => prefix + alternative));
    }
  }
  return out;
};
var matchToken = (token, code) => {
  switch (token._tag) {
    case "Char":
      return token.code === code;
    case "Any":
      return true;
    case "Star":
      return false;
    case "Class": {
      let found = false;
      for (let i = 0;i < token.ranges.length && !found; i += 2) {
        found = code >= token.ranges[i] && code <= token.ranges[i + 1];
      }
      return found !== token.negated;
    }
  }
};
var width = (code) => code > 65535 ? 2 : 1;
var matchWildcard = (node, name, dot) => {
  if (name === "." || name === ".." || name.startsWith(".") && !dot && !node.leadingDot) {
    return false;
  }
  if (node.affixes !== undefined) {
    const {
      prefix,
      suffix
    } = node.affixes;
    return name.length >= prefix.length + suffix.length && name.startsWith(prefix) && name.endsWith(suffix);
  }
  const {
    tokens
  } = node;
  let t = 0;
  let c = 0;
  let star = -1;
  let resume = 0;
  while (c < name.length) {
    if (tokens[t]?._tag === "Star") {
      star = t++;
      resume = c;
      continue;
    }
    const code = name.codePointAt(c);
    if (t < tokens.length && matchToken(tokens[t], code)) {
      t++;
      c += width(code);
    } else if (star !== -1) {
      t = star + 1;
      resume += width(name.codePointAt(resume));
      c = resume;
    } else {
      return false;
    }
  }
  while (tokens[t]?._tag === "Star") {
    t++;
  }
  return t === tokens.length;
};
var closure = (nodes, states) => {
  for (const state of states) {
    if (nodes[state]._tag === "Globstar") {
      states.add(state + 1);
    }
  }
  return states;
};
var step = (compiled, states, name) => {
  const explicit = new Set;
  const viaGlobstar = new Set;
  const root = isRoot(name);
  for (const state of states) {
    const node = compiled.nodes[state];
    switch (node._tag) {
      case "Globstar": {
        if (!root && name !== "." && name !== ".." && (compiled.dot || !name.startsWith("."))) {
          viaGlobstar.add(state);
        }
        break;
      }
      case "Literal": {
        if (node.value === name) {
          explicit.add(state + 1);
        }
        break;
      }
      case "Wildcard": {
        if (!root && matchWildcard(node, name, compiled.dot)) {
          explicit.add(state + 1);
        }
        break;
      }
    }
  }
  return {
    explicit: closure(compiled.nodes, explicit),
    globstar: closure(compiled.nodes, viaGlobstar)
  };
};
var union = (step) => {
  if (step.globstar.size === 0) {
    return step.explicit;
  }
  const states = new Set(step.explicit);
  for (const state of step.globstar) {
    states.add(state);
  }
  return states;
};
var accepts = (compiled, states, isDirectory) => {
  for (const state of states) {
    const node = compiled.nodes[state];
    if (node._tag === "End" && (isDirectory || !node.directoryOnly)) {
      return true;
    }
  }
  return false;
};
var canDescend = (compiled, states) => {
  for (const state of states) {
    if (compiled.nodes[state]._tag !== "End") {
      return true;
    }
  }
  return false;
};
var needsDirectory = (compiled, states) => {
  for (const state of states) {
    const node = compiled.nodes[state];
    if (node._tag === "End" && node.directoryOnly) {
      return true;
    }
  }
  return false;
};
var join2 = (parent, name) => {
  if (parent === "") {
    return name === "" ? "/" : name;
  }
  return parent.endsWith("/") ? parent + name : `${parent}/${name}`;
};
var isMissing = (error) => error.reason._tag === "NotFound" || error.reason._tag === "BadResource";
var isIgnorable = (error) => isMissing(error) || error.reason._tag === "PermissionDenied";
var isNotLink = (error) => isIgnorable(error) || error.reason._tag === "Unknown";
var DIRECTORY_CONCURRENCY = 16;
var byName = /* @__PURE__ */ mapInput(String2, (entry) => entry.name);
var make8 = (fs) => (pattern, options) => walk(fs, compile([pattern], options?.dot ?? false), options);
var walk = /* @__PURE__ */ fnUntraced2(function* (fs, compiled, options) {
  if (compiled.initial.size === 0) {
    return [];
  }
  const root = options?.root ?? "";
  const excludes = compile(options?.exclude ?? [], true);
  const excludeNeedsDirectory = excludes.nodes.some((node) => node._tag === "End" && node.directoryOnly);
  const toFsPath = (relative) => isAbsolute(relative) ? relative : relative === "" ? root === "" ? "." : root : join2(root, relative);
  const isUnlisted = (value) => value === "." || value === ".." || isRoot(value);
  const probe = (parent, name, named) => {
    const path = toFsPath(join2(parent, name));
    return fs.stat(path).pipe(map7((info) => info.type), catchReason2("PlatformError", "NotFound", () => fs.readLink(path).pipe(as2("SymbolicLink"), catchIf2(isNotLink, () => succeed7(undefined)))), catchIf2(named ? isMissing : isIgnorable, () => succeed7(undefined)), map7((type) => type === undefined ? undefined : {
      name,
      type
    }));
  };
  const entries = (relative, states, named) => {
    const literals = new Set;
    let listed = !named;
    for (const state of states) {
      const node = compiled.nodes[state];
      if (node._tag === "Literal") {
        literals.add(node.value);
      } else if (node._tag !== "End") {
        listed = true;
      }
    }
    const probed = listed ? [...literals].filter(isUnlisted) : [...literals];
    return zipWith2(listed ? catchIf2(fs.readDirectoryEntries(toFsPath(relative)), named ? isMissing : isIgnorable, () => succeed7([])) : succeed7([]), forEach2(probed, (name) => probe(relative, name, named), {
      concurrency: 16
    }), (listing, found) => ({
      entries: sort([...listing, ...found.filter(isNotUndefined)], byName),
      named: named && !listed
    }));
  };
  const visit = ({
    name,
    type
  }, parent, states, excluded) => {
    const result = step(compiled, states, name);
    const next = union(result);
    if (next.size === 0) {
      return;
    }
    const relative = join2(parent, name);
    const nextExcluded = union(step(excludes, excluded, name));
    const descend = canDescend(compiled, next);
    const child = (isDirectory, link) => {
      const accepted = accepts(compiled, next, isDirectory);
      if (!accepted && (!isDirectory || !descend) || accepts(excludes, nextExcluded, isDirectory)) {
        return;
      }
      return {
        relative,
        accepted,
        excluded: nextExcluded,
        next: !isDirectory || !descend ? undefined : !link || result.globstar.size === 0 ? next : canDescend(compiled, result.explicit) ? result.explicit : undefined
      };
    };
    if (!descend && !needsDirectory(compiled, next) && !excludeNeedsDirectory) {
      return child(false, false);
    }
    if (type !== "Unknown" && type !== "SymbolicLink") {
      return child(type === "Directory", false);
    }
    const path = toFsPath(relative);
    return fs.stat(path).pipe(map7((info) => info.type === "Directory"), catchIf2(isIgnorable, () => succeed7(false)), flatMap4((isDirectory) => {
      const resolved = child(isDirectory, type === "SymbolicLink");
      if (type === "SymbolicLink" || resolved?.next === undefined || result.globstar.size === 0) {
        return succeed7(resolved);
      }
      return fs.readLink(path).pipe(as2(child(isDirectory, true)), catchIf2(isNotLink, () => succeed7(resolved)));
    }));
  };
  let slots = DIRECTORY_CONCURRENCY;
  let finishing = false;
  const failed = makeUnsafe2();
  const fork = (walk) => walk.pipe(onError2((cause) => sync2(() => {
    if (!finishing) {
      doneUnsafe(failed, failCause3(cause));
    }
  })), ensuring2(sync2(() => {
    slots++;
  })), forkChild2);
  const directory = fnUntraced2(function* (relative, states, excluded, named) {
    const listing = yield* entries(relative, states, named);
    const children = [];
    const pending = [];
    for (const entry of listing.entries) {
      const child = visit(entry, relative, states, excluded);
      if (isEffect2(child)) {
        const index = children.push(undefined) - 1;
        pending.push(map7(child, (child) => {
          children[index] = child;
        }));
      } else {
        children.push(child);
      }
    }
    if (pending.length > 0) {
      yield* all3(pending, {
        concurrency: 16,
        discard: true
      });
    }
    const subtrees = [];
    for (const child of children) {
      if (child?.next !== undefined) {
        const walk = directory(child.relative, child.next, child.excluded, listing.named);
        if (slots > 0) {
          slots--;
          subtrees.push(join(yield* fork(walk)));
        } else {
          subtrees.push(succeed7(yield* walk));
        }
      }
    }
    const out = [];
    let index = 0;
    for (const child of children) {
      if (child === undefined) {
        continue;
      }
      if (child.accepted) {
        out.push(child.relative);
      }
      if (child.next !== undefined) {
        for (const path of yield* subtrees[index++]) {
          out.push(path);
        }
      }
    }
    return out;
  });
  return yield* raceFirst2(ensuring2(directory("", compiled.initial, excludes.initial, true), sync2(() => {
    finishing = true;
  })), _await(failed));
});

// node_modules/effect/dist/PlatformError.js
var TypeId8 = "~effect/PlatformError";

class BadArgument extends (/* @__PURE__ */ TaggedError2("BadArgument")) {
  get message() {
    return `${this.module}.${this.method}${this.description ? `: ${this.description}` : ""}`;
  }
}

class SystemError extends Error3 {
  get message() {
    return `${this._tag}: ${this.module}.${this.method}${this.pathOrDescriptor !== undefined ? ` (${this.pathOrDescriptor})` : ""}${this.description ? `: ${this.description}` : ""}`;
  }
}

class PlatformError extends (/* @__PURE__ */ TaggedError2("PlatformError")) {
  constructor(reason) {
    if ("cause" in reason) {
      super({
        reason,
        cause: reason.cause
      });
    } else {
      super({
        reason
      });
    }
  }
  [TypeId8] = TypeId8;
  get message() {
    return this.reason.message;
  }
}
var systemError = (options) => new PlatformError(new SystemError(options));
var badArgument = (options) => new PlatformError(new BadArgument(options));

// node_modules/effect/dist/Latch.js
var makeUnsafe5 = makeLatchUnsafe;

// node_modules/effect/dist/MutableRef.js
var TypeId9 = "~effect/MutableRef";
var MutableRefProto = {
  [TypeId9]: TypeId9,
  ...PipeInspectableProto,
  toJSON() {
    return {
      _id: "MutableRef",
      current: toJson(this.current)
    };
  }
};
var make9 = (value) => {
  const ref = Object.create(MutableRefProto);
  ref.current = value;
  return ref;
};
var set = /* @__PURE__ */ dual(2, (self, value) => {
  self.current = value;
  return self;
});

// node_modules/effect/dist/MutableList.js
var Empty = /* @__PURE__ */ Symbol.for("effect/MutableList/Empty");
var make10 = () => ({
  head: undefined,
  tail: undefined,
  length: 0
});
var compactHead = (self) => {
  const head = self.head;
  if (head.offset >= 1024 && head === self.tail && head.mutable && (head.array.length - head.offset) * 8 <= head.offset) {
    self.head = self.tail = {
      array: head.array.slice(head.offset),
      mutable: true,
      offset: 0,
      next: undefined
    };
  }
};
var emptyBucket = () => ({
  array: [],
  mutable: true,
  offset: 0,
  next: undefined
});
var append2 = (self, message) => {
  if (!self.tail) {
    self.head = self.tail = emptyBucket();
  } else if (!self.tail.mutable) {
    self.tail.next = emptyBucket();
    self.tail = self.tail.next;
  }
  self.tail.array.push(message);
  self.length++;
};
var clear = (self) => {
  self.head = self.tail = undefined;
  self.length = 0;
};
var takeN = (self, n) => {
  n = normalize(n);
  if (n <= 0 || !self.head)
    return [];
  n = Math.min(n, self.length);
  if (n === self.length && self.head?.offset === 0 && !self.head.next) {
    const array = self.head.array;
    clear(self);
    return array;
  }
  const array = new Array(n);
  let index = 0;
  let chunk = self.head;
  while (chunk) {
    while (chunk.offset < chunk.array.length) {
      array[index++] = chunk.array[chunk.offset];
      if (chunk.mutable)
        chunk.array[chunk.offset] = undefined;
      chunk.offset++;
      if (index === n) {
        self.head = chunk.offset === chunk.array.length && chunk.next ? chunk.next : chunk;
        self.length -= n;
        if (self.length === 0)
          clear(self);
        else
          compactHead(self);
        return array;
      }
    }
    chunk = chunk.next;
  }
  clear(self);
  return array;
};
var take = (self) => {
  if (!self.head)
    return Empty;
  const message = self.head.array[self.head.offset];
  if (self.head.mutable)
    self.head.array[self.head.offset] = undefined;
  self.head.offset++;
  self.length--;
  if (self.head.offset === self.head.array.length) {
    if (self.head.next) {
      self.head = self.head.next;
    } else {
      clear(self);
    }
  } else {
    compactHead(self);
  }
  return message;
};

// node_modules/effect/dist/Queue.js
var TypeId10 = "~effect/Queue";
var EnqueueTypeId = "~effect/Queue/Enqueue";
var DequeueTypeId = "~effect/Queue/Dequeue";
var variance = {
  _A: identity,
  _E: identity
};
var QueueProto = {
  [TypeId10]: variance,
  [EnqueueTypeId]: variance,
  [DequeueTypeId]: variance,
  ...PipeInspectableProto,
  toJSON() {
    return {
      _id: "effect/Queue",
      state: this.state._tag,
      size: sizeUnsafe(this)
    };
  }
};
var make11 = (options) => withFiber((fiber) => {
  const self = Object.create(QueueProto);
  self.dispatcher = fiber.currentDispatcher;
  self.capacity = options?.capacity ?? Number.POSITIVE_INFINITY;
  self.strategy = options?.strategy ?? "suspend";
  self.messages = make10();
  self.scheduleRunning = false;
  self.state = {
    _tag: "Open",
    takers: new Set,
    offers: new Set,
    awaiters: new Set
  };
  return succeed3(self);
});
var bounded = (capacity) => make11({
  capacity
});
var sliding = (capacity) => make11({
  capacity,
  strategy: "sliding"
});
var offer = /* @__PURE__ */ dual(2, (self, message) => suspend(() => offerUnsafe(self, message) ? exitTrue : self.state._tag === "Open" && self.strategy === "suspend" ? offerOrWait(self, message) : exitFalse));
var offerUnsafe = (self, message) => {
  if (self.state._tag !== "Open") {
    return false;
  } else if (self.messages.length >= self.capacity) {
    if (self.strategy === "sliding") {
      take(self.messages);
      append2(self.messages, message);
      return true;
    } else if (self.capacity <= 0 && self.state.takers.size > 0) {
      append2(self.messages, message);
      releaseTakers(self);
      return true;
    }
    return false;
  }
  append2(self.messages, message);
  scheduleReleaseTaker(self);
  return true;
};
var failCause4 = /* @__PURE__ */ dual(2, (self, cause) => sync(() => failCauseUnsafe(self, cause)));
var failCauseUnsafe = (self, cause) => {
  if (self.state._tag !== "Open") {
    return false;
  }
  const exit = exitFailCause(cause);
  const fail = exitZipRight(exit, exitFailDone);
  if (self.state.offers.size === 0 && self.messages.length === 0) {
    finalize(self, fail);
    return true;
  }
  self.state = {
    ...self.state,
    _tag: "Closing",
    exit: fail
  };
  scheduleReleaseTaker(self);
  return true;
};
var endUnsafe = (self) => failCauseUnsafe(self, causeFail(Done()));
var shutdown = (self) => sync(() => shutdownUnsafe(self));
var shutdownUnsafe = (self) => {
  if (self.state._tag === "Done") {
    return false;
  }
  clear(self.messages);
  const offers = self.state.offers;
  finalize(self, self.state._tag === "Open" ? exitInterrupt2 : self.state.exit);
  for (const entry of offers) {
    resumeUnoffered(entry);
  }
  return true;
};
var takeAll2 = (self) => takeBetween(self, 1, Number.POSITIVE_INFINITY);
var takeBetween = /* @__PURE__ */ dual(3, (self, min, max) => {
  min = normalize(min);
  max = normalize(max);
  return suspendTake(() => takeBetweenUnsafe(self, min, max) ?? andThen(awaitTake(self, () => canTake(self, min)), takeBetween(self, min, max)));
});
var take2 = (self) => suspendTake(() => takeUnsafe(self) ?? andThen(awaitTake(self, () => canTake(self, 1)), take2(self)));
var poll = (self) => suspendTake(() => {
  const result = takeUnsafe(self);
  if (result === undefined) {
    return succeed3(none2());
  }
  if (result._tag === "Success") {
    return succeed3(some2(result.value));
  }
  return succeed3(none2());
});
var takeUnsafe = (self) => {
  if (self.state._tag === "Done") {
    return self.state.exit;
  }
  if (self.messages.length > 0) {
    const message = take(self.messages);
    releaseCapacity(self);
    return exitSucceed(message);
  } else if (self.capacity <= 0 && self.state.offers.size > 0) {
    const message = takeOfferUnsafe(self.state.offers);
    releaseCapacity(self);
    return exitSucceed(message);
  }
  return;
};
var sizeUnsafe = (self) => self.state._tag === "Done" ? 0 : self.messages.length;
var exitFalse = /* @__PURE__ */ exitSucceed(false);
var exitTrue = /* @__PURE__ */ exitSucceed(true);
var exitFailDone = /* @__PURE__ */ exitFail(/* @__PURE__ */ Done());
var exitInterrupt2 = /* @__PURE__ */ exitInterrupt();
var releaseTakers = (self) => {
  if (self.state._tag === "Done" || self.state.takers.size === 0) {
    return;
  }
  for (const taker of self.state.takers) {
    if (!taker.ready())
      continue;
    self.state.takers.delete(taker);
    taker.resume(exitVoid);
    if (self.messages.length === 0) {
      break;
    }
  }
};
var scheduleReleaseTaker = (self) => {
  if (self.scheduleRunning || self.state._tag === "Done" || self.state.takers.size === 0) {
    return;
  }
  self.scheduleRunning = true;
  self.dispatcher.scheduleTask(() => {
    self.scheduleRunning = false;
    releaseTakers(self);
  }, 0);
};
var takeBetweenUnsafe = (self, min, max) => {
  if (self.state._tag === "Done") {
    return self.state.exit;
  } else if (max <= 0 || min <= 0) {
    return exitSucceed([]);
  } else if (!canTake(self, min)) {
    return;
  }
  const messages = self.messages.length > 0 ? takeN(self.messages, max) : [takeOfferUnsafe(self.state.offers)];
  releaseCapacity(self);
  return exitSucceed(messages);
};
var suspendTake = /* @__PURE__ */ makePrimitive({
  op: "QueueTake",
  [evaluate](fiber) {
    const effect = this[args]();
    return isExit(effect) ? effect[evaluate](fiber) : effect;
  }
});
var canTake = (self, min) => self.messages.length >= (self.state._tag === "Closing" ? 1 : Math.min(min, self.capacity || 1)) || self.capacity <= 0 && self.state._tag !== "Done" && self.state.offers.size > 0;
var awaitTake = (self, ready) => callback((resume) => {
  if (self.state._tag === "Done")
    return resume(self.state.exit);
  if (ready())
    return resume(exitVoid);
  const taker = {
    ready,
    resume
  };
  self.state.takers.add(taker);
  return sync(() => {
    if (self.state._tag !== "Done")
      self.state.takers.delete(taker);
  });
});
var offerOrWait = (self, message) => callback((resume) => offerUnsafe(self, message) ? resume(exitTrue) : waitToOffer(self, {
  _tag: "Single",
  message,
  resume
}));
var waitToOffer = (self, entry) => {
  if (self.state._tag !== "Open")
    return resumeUnoffered(entry);
  const offers = self.state.offers;
  offers.add(entry);
  return sync(() => {
    if (self.state._tag === "Done")
      return;
    offers.delete(entry);
    if (self.state._tag === "Closing" && offers.size === 0 && self.messages.length === 0) {
      finalize(self, self.state.exit);
    }
  });
};
var resumeUnoffered = (entry) => entry._tag === "Single" ? entry.resume(exitFalse) : entry.resume(exitSucceed(entry.remaining.slice(entry.offset)));
var takeOfferUnsafe = (offers) => {
  const entry = offers.values().next().value;
  if (entry._tag === "Single") {
    offers.delete(entry);
    entry.resume(exitTrue);
    return entry.message;
  }
  const message = entry.remaining[entry.offset++];
  if (entry.offset === entry.remaining.length) {
    offers.delete(entry);
    entry.resume(exitSucceed([]));
  }
  return message;
};
var releaseCapacity = (self) => {
  if (self.state._tag === "Done") {
    return isDoneCause(self.state.exit.cause);
  } else if (self.state.offers.size === 0) {
    if (self.state._tag === "Closing" && self.messages.length === 0) {
      finalize(self, self.state.exit);
      return isDoneCause(self.state.exit.cause);
    }
    return false;
  }
  for (const entry of self.state.offers) {
    let n = self.capacity - self.messages.length;
    if (n <= 0)
      break;
    else if (entry._tag === "Single") {
      append2(self.messages, entry.message);
      self.state.offers.delete(entry);
      entry.resume(exitTrue);
    } else {
      for (;entry.offset < entry.remaining.length; entry.offset++) {
        if (n === 0)
          return false;
        append2(self.messages, entry.remaining[entry.offset]);
        n--;
      }
      self.state.offers.delete(entry);
      entry.resume(exitSucceed([]));
    }
    const state = self.state;
    if (state._tag === "Done") {
      return isDoneCause(state.exit.cause);
    }
  }
  return false;
};
var finalize = (self, exit) => {
  if (self.state._tag === "Done") {
    return;
  }
  const openState = self.state;
  self.state = {
    _tag: "Done",
    exit
  };
  for (const taker of openState.takers) {
    taker.resume(exit);
  }
  openState.takers.clear();
  for (const awaiter of openState.awaiters) {
    awaiter(exit);
  }
  openState.awaiters.clear();
};

// node_modules/effect/dist/Channel.js
var TypeId11 = "~effect/Channel";
var isChannel = (u) => hasProperty(u, TypeId11);
var ChannelProto = {
  [TypeId11]: {
    _Env: identity,
    _InErr: identity,
    _InElem: identity,
    _OutErr: identity,
    _OutElem: identity
  },
  pipe() {
    return pipeArguments(this, arguments);
  }
};
var fromTransform = (transform) => {
  const self = Object.create(ChannelProto);
  self.transform = (upstream, scope) => catchCause2(transform(upstream, scope), (cause) => succeed7(failCause3(cause)));
  return self;
};
var transformPull = (self, f) => fromTransform((upstream, scope) => flatMap4(toTransform(self)(upstream, scope), (pull) => f(pull, scope)));
var fromPull = (effect) => fromTransform((_, __) => effect);
var fromTransformBracket = (f) => fromTransform(fnUntraced2(function* (upstream, scope) {
  const closableScope = forkUnsafe2(scope);
  const onCause = (cause) => close(closableScope, doneExitFromCause(cause));
  const pull = yield* onError2(f(upstream, scope, closableScope), onCause);
  return onError2(pull, onCause);
}));
var toTransform = (channel) => channel.transform;
var asyncQueue = (scope, f, options) => make11({
  capacity: options?.bufferSize,
  strategy: options?.strategy
}).pipe(tap2((queue) => addFinalizer2(scope, shutdown(queue))), tap2((queue) => provide(f(queue), scope).pipe(catchCause2((cause) => failCause4(queue, cause)), forkIn2(scope, {
  startImmediately: true
}))));
var callbackArray = (f, options) => fromTransform((_, scope) => map7(asyncQueue(scope, f, options), takeAll2));
var suspend3 = (evaluate) => fromTransform((upstream, scope) => suspend2(() => toTransform(evaluate())(upstream, scope)));
var empty3 = /* @__PURE__ */ fromPull(/* @__PURE__ */ succeed7(/* @__PURE__ */ done2()));
var fail8 = (error) => fromPull(succeed7(fail7(error)));
var failCause5 = (cause) => fromPull(failCause3(cause));
var die4 = (defect) => failCause5(die2(defect));
var fromEffectDrain = (effect) => fromPull(flatMap4(effect, () => done2()));
var fromReadableStream = (options) => fromTransform((_, scope) => readableStreamToPullUnsafe({
  scope,
  readable: options.evaluate(),
  onError: options.onError,
  releaseLockOnEnd: options.releaseLockOnEnd
}));
var readableStreamToPullUnsafe = (options) => {
  const reader = options.readable.getReader();
  const exit = options.exit ?? make9(undefined);
  const pull = suspend2(() => {
    if (exit.current)
      return exit.current;
    return matchCauseEffect2(tryPromise2({
      try: () => reader.read(),
      catch: options.onError
    }), {
      onFailure: (cause) => exit.current ?? failCause3(cause),
      onSuccess: ({
        done,
        value
      }) => {
        if (exit.current)
          return exit.current;
        return done ? done2() : succeed7(of(value));
      }
    });
  });
  return as2(addFinalizer2(options.scope, options.releaseLockOnEnd ? sync2(() => reader.releaseLock()) : promise2(() => reader.cancel().catch((error) => reader.closed.then(() => Promise.reject(error), constVoid))).pipe(ensuring2(sync2(() => reader.releaseLock())))), pull);
};
var map8 = /* @__PURE__ */ dual(2, (self, f) => transformPull(self, (pull) => sync2(() => {
  let i = 0;
  return map7(pull, (o) => f(o, i++));
})));
var mapDone = /* @__PURE__ */ dual(2, (self, f) => mapDoneEffect(self, (o) => succeed7(f(o))));
var mapDoneEffect = /* @__PURE__ */ dual(2, (self, f) => transformPull(self, (pull) => succeed7(catchDone(pull, (done) => flatMap4(f(done), done2)))));
var catchCause3 = /* @__PURE__ */ dual(2, (self, f) => fromTransform((upstream, scope) => {
  let forkedScope = forkUnsafe2(scope);
  return map7(toTransform(self)(upstream, forkedScope), (pull) => {
    let currentPull = pull.pipe(catchCause2((cause) => {
      if (isDoneCause(cause)) {
        return failCause3(cause);
      }
      const toClose = forkedScope;
      forkedScope = forkUnsafe2(scope);
      return close(toClose, failCause2(cause)).pipe(andThen2(toTransform(f(cause))(upstream, forkedScope)), flatMap4((childPull) => {
        currentPull = childPull;
        return childPull;
      }));
    }));
    return suspend2(() => currentPull);
  });
}));
var catchCauseFilter2 = /* @__PURE__ */ dual(3, (self, filter, f) => catchCause3(self, (cause) => {
  const result = filter(cause);
  return isFailure2(result) ? failCause5(result.failure) : f(result.success, cause);
}));
var catch_3 = /* @__PURE__ */ dual(2, (self, f) => catchCauseFilter2(self, findRecoverableError, (e) => f(e)));
var tapError3 = /* @__PURE__ */ dual(2, (self, f) => transformPull(self, (pull) => succeed7(tapError2(pull, (err) => isDone2(err) ? void_3 : asVoid2(f(err))))));
var orDie3 = (self) => catch_3(self, die4);
var merge2 = /* @__PURE__ */ dual((args) => isChannel(args[0]) && isChannel(args[1]), (left, right, options) => fromTransformBracket(fnUntraced2(function* (upstream, _scope, forkedScope) {
  const strategy = options?.haltStrategy ?? "both";
  const queue = yield* bounded(0);
  yield* addFinalizer2(forkedScope, shutdown(queue));
  let done = 0;
  function onExit(side, cause) {
    done++;
    if (!isDoneCause(cause)) {
      return failCause4(queue, cause);
    }
    switch (strategy) {
      case "both": {
        return done === 2 ? failCause4(queue, cause) : void_3;
      }
      case "left":
      case "right": {
        return side === strategy ? failCause4(queue, cause) : void_3;
      }
      case "either": {
        return failCause4(queue, cause);
      }
    }
  }
  const runSide = (side, channel, scope) => toTransform(channel)(upstream, scope).pipe(flatMap4((pull) => pull.pipe(flatMap4((value) => offer(queue, value)), forever2)), onError2((cause) => onExitPrimitive2(doneExitFromCause(cause), (exit) => close(scope, exit)).pipe(matchCauseEffect2({
    onFailure: (cause) => onExit(side, cause),
    onSuccess: () => onExit(side, cause)
  }))), forkIn2(forkedScope));
  yield* runSide("left", left, forkUnsafe2(forkedScope));
  yield* runSide("right", right, forkUnsafe2(forkedScope));
  return take2(queue);
})));
var mergeEffect = /* @__PURE__ */ dual(2, (self, effect) => merge2(self, fromEffectDrain(effect), {
  haltStrategy: "left"
}));
var splitLines = () => fromTransform((upstream, _scope) => sync2(() => {
  let stringBuilder = "";
  let midCRLF = false;
  let done = none2();
  function splitLinesArray(chunk) {
    const chunkBuilder = [];
    function pushLine(segment) {
      if (stringBuilder.length === 0) {
        chunkBuilder.push(segment);
      } else {
        chunkBuilder.push(stringBuilder + segment);
        stringBuilder = "";
      }
    }
    for (let i = 0;i < chunk.length; i++) {
      const str = chunk[i];
      if (str.length !== 0) {
        let from = 0;
        let indexOfCR = str.indexOf("\r");
        let indexOfLF = str.indexOf(`
`);
        if (midCRLF) {
          if (indexOfLF === 0) {
            from = 1;
            indexOfLF = str.indexOf(`
`, from);
          }
          midCRLF = false;
        }
        while (indexOfCR !== -1 || indexOfLF !== -1) {
          if (indexOfCR === -1 || indexOfLF !== -1 && indexOfLF < indexOfCR) {
            pushLine(str.substring(from, indexOfLF));
            from = indexOfLF + 1;
            indexOfLF = str.indexOf(`
`, from);
          } else {
            pushLine(str.substring(from, indexOfCR));
            if (str.length === indexOfCR + 1) {
              midCRLF = true;
              from = str.length;
              indexOfCR = -1;
            } else {
              from = indexOfCR + (indexOfLF === indexOfCR + 1 ? 2 : 1);
              indexOfCR = str.indexOf("\r", from);
              indexOfLF = str.indexOf(`
`, from);
            }
          }
        }
        stringBuilder = stringBuilder + str.substring(from);
      }
    }
    return isReadonlyArrayNonEmpty(chunkBuilder) ? chunkBuilder : null;
  }
  const pullOrFlush = suspend2(() => {
    if (done._tag === "Some") {
      return done2(done.value);
    }
    return matchEffect2(upstream, {
      onSuccess: loop,
      onFailure: failCause3,
      onDone: (leftover) => {
        done = some2(leftover);
        if (stringBuilder.length > 0) {
          const last = stringBuilder;
          stringBuilder = "";
          midCRLF = false;
          return succeed7([last]);
        }
        return done2(leftover);
      }
    });
  });
  function loop(chunk) {
    const lines = splitLinesArray(chunk);
    return lines !== null ? succeed7(lines) : pullOrFlush;
  }
  return pullOrFlush;
}));
var decodeText = (encoding, options) => fromTransform((upstream, _scope) => sync2(() => {
  const decoder = new TextDecoder(encoding, options);
  const streamOptions = {
    stream: true
  };
  let pull = upstream.pipe(map7(map4((bytes) => decoder.decode(bytes, streamOptions))), catchDone((leftover) => {
    pull = done2(leftover);
    const last = decoder.decode();
    return last.length > 0 ? succeed7(of(last)) : pull;
  }));
  return suspend2(() => pull);
}));
var pipeTo = /* @__PURE__ */ dual(2, (self, that) => fromTransform((upstream, scope) => flatMap4(toTransform(self)(upstream, scope), (upstream) => toTransform(that)(upstream, scope))));
var pipeToOrFail = /* @__PURE__ */ dual(2, (self, that) => fromTransform((upstream, scope) => flatMap4(toTransform(self)(upstream, scope), (upstream) => map7(toTransform(that)(upstreamFailureToDefect(upstream), scope), restoreUpstreamFailure))));
var isUpstreamFailure = (reason) => reason._tag === "Die" && isDone2(reason.defect) && isCause2(reason.defect.value);
var hasUpstreamFailure = (cause) => cause.reasons.some(isUpstreamFailure);
var upstreamFailureToDefect = (pull) => catchCause2(pull, (cause) => isDoneCause(cause) ? failCause3(cause) : die3(Done2(cause)));
var unwrapUpstreamFailure = (cause) => hasUpstreamFailure(cause) ? fromReasons(cause.reasons.flatMap((reason) => isUpstreamFailure(reason) ? reason.defect.value.reasons : [reason])) : cause;
var restoreUpstreamFailure = (effect) => catchCause2(effect, (cause) => failCause3(unwrapUpstreamFailure(cause)));
var unwrap = (channel) => fromTransform((upstream, scope) => {
  let pull;
  return succeed7(suspend2(() => {
    if (pull)
      return pull;
    return channel.pipe(provide(scope), flatMap4((channel) => toTransform(channel)(upstream, scope)), flatMap4((pull_) => pull = pull_));
  }));
});
var interruptWhen = /* @__PURE__ */ dual(2, (self, effect) => merge2(self, fromPull(succeed7(flatMap4(effect, done2))), {
  haltStrategy: "either"
}));
var onExit3 = /* @__PURE__ */ dual(2, (self, finalizer) => fromTransformBracket((upstream, scope, forkedScope) => addFinalizerExit(forkedScope, finalizer).pipe(andThen2(toTransform(self)(upstream, scope)))));
var ensuring3 = /* @__PURE__ */ dual(2, (self, finalizer) => onExit3(self, (_) => finalizer));
var runWith = (self, f, onHalt) => suspend2(() => {
  const scope = makeUnsafe3();
  const makePull = toTransform(self)(done2(), scope);
  return catchDone(flatMap4(makePull, f), onHalt ? onHalt : succeed7).pipe(onExit2((exit) => close(scope, exit)));
});
var runDrain = (self) => runWith(self, (pull) => forever2(pull, {
  disableYield: true
}));
var runForEach = /* @__PURE__ */ dual(2, (self, f) => runWith(self, (pull) => forever2(flatMap4(pull, f), {
  disableYield: true
})));
var mkUint8Array = (self) => map7(runFold(self, () => ({
  bytes: 0,
  arrays: []
}), (acc, chunk) => {
  for (let i = 0;i < chunk.length; i++) {
    acc.bytes += chunk[i].length;
    acc.arrays.push(chunk[i]);
  }
  return acc;
}), ({
  arrays,
  bytes
}) => {
  const result = new Uint8Array(bytes);
  let offset = 0;
  for (let i = 0;i < arrays.length; i++) {
    const array = arrays[i];
    result.set(array, offset);
    offset += array.length;
  }
  return result;
});
var runFold = /* @__PURE__ */ dual(3, (self, initial, f) => suspend2(() => {
  let state = initial();
  return runWith(self, (pull) => whileLoop2({
    while: constTrue,
    body: () => pull,
    step: (value) => {
      state = f(state, value);
    }
  }), () => succeed7(state));
}));
var toPullScoped = (self, scope) => toTransform(self)(done2(), scope);

// node_modules/effect/dist/internal/stream.js
var TypeId12 = "~effect/Stream";
var streamVariance = {
  _R: identity,
  _E: identity,
  _A: identity
};
var Stream = function(channel) {
  this.channel = channel;
};
Stream.prototype = {
  [TypeId12]: streamVariance,
  pipe() {
    return pipeArguments(this, arguments);
  }
};
var fromChannel = (channel) => new Stream(channel);

// node_modules/effect/dist/Sink.js
var TypeId13 = "~effect/Sink";
var endVoid = /* @__PURE__ */ succeed7([undefined]);
var sinkVariance = {
  _A: identity,
  _In: identity,
  _L: identity,
  _E: identity,
  _R: identity
};
var SinkProto = {
  [TypeId13]: sinkVariance,
  pipe() {
    return pipeArguments(this, arguments);
  }
};
var isSink = (u) => hasProperty(u, TypeId13);
var fromChannel2 = (channel) => fromTransform2((upstream, scope) => toTransform(channel)(upstream, scope).pipe(flatMap4(forever2({
  disableYield: true
})), catchDone(succeed7)));
var fromTransform2 = (transform) => {
  const self = Object.create(SinkProto);
  self.transform = transform;
  return self;
};
var toChannel = (self) => fromTransform((upstream, scope) => succeed7(flatMap4(self.transform(upstream, scope), done2)));
var drain = /* @__PURE__ */ fromTransform2((upstream) => catchDone(forever2(upstream, {
  disableYield: true
}), () => endVoid));
var forEach3 = (f) => forEachArray(forEach2((_) => f(_), {
  discard: true
}));
var forEachArray = (f) => fromTransform2((upstream) => upstream.pipe(flatMap4(f), forever2({
  disableYield: true
}), catchDone(() => endVoid)));
var unwrap2 = (effect) => fromChannel2(unwrap(map7(effect, toChannel)));

// node_modules/effect/dist/internal/rcRef.js
var TypeId14 = "~effect/RcRef";
var stateEmpty = {
  _tag: "Empty"
};
var stateClosed = {
  _tag: "Closed"
};
var variance2 = {
  _A: identity,
  _E: identity
};

class RcRefImpl {
  [TypeId14] = variance2;
  pipe() {
    return pipeArguments(this, arguments);
  }
  state = stateEmpty;
  acquire;
  context;
  scope;
  idleTimeToLive;
  constructor(acquire, context, scope, idleTimeToLive) {
    this.acquire = acquire;
    this.context = context;
    this.scope = scope;
    this.idleTimeToLive = idleTimeToLive;
  }
}
var make12 = (options) => withFiber2((fiber) => {
  const context = fiber.context;
  const scope = get(context, Scope);
  const ref = new RcRefImpl(options.acquire, context, scope, options.idleTimeToLive !== undefined ? fromInputUnsafe(options.idleTimeToLive) : undefined);
  return as2(addFinalizerExit(scope, () => {
    const state = ref.state;
    ref.state = stateClosed;
    if (state._tag === "Acquired")
      return close(state.scope, void_2);
    if (state._tag === "Acquiring")
      return interrupt3(state.fiber);
    return void_3;
  }), ref);
});
var getState = (self, restore) => {
  switch (self.state._tag) {
    case "Closed": {
      return interrupt2;
    }
    case "Acquired": {
      const state = self.state;
      state.refCount++;
      return state.fiber ? as2(interrupt3(state.fiber), state) : succeed7(state);
    }
    case "Acquiring": {
      const state = self.state;
      state.awaiters++;
      return awaitAcquiring(self, state, restore);
    }
    case "Empty": {
      const acquiring = {
        _tag: "Acquiring",
        fiber: undefined,
        awaiters: 1,
        acquired: undefined
      };
      self.state = acquiring;
      const scope = makeUnsafe3();
      const acquire = provideContext2(self.acquire, add(self.context, Scope, scope)).pipe(flatMap4((value) => {
        if (self.state !== acquiring) {
          return interrupt2;
        }
        const state = {
          _tag: "Acquired",
          value,
          scope,
          fiber: undefined,
          refCount: acquiring.awaiters,
          invalidated: false
        };
        self.state = state;
        acquiring.acquired = state;
        return succeed7(state);
      }), onExit2((exit) => {
        if (isSuccess3(exit))
          return void_3;
        if (self.state === acquiring)
          self.state = stateEmpty;
        return close(scope, exit);
      }));
      acquiring.fiber = runForkWith2(getCurrent().context)(acquire);
      return awaitAcquiring(self, acquiring, restore);
    }
  }
};
var awaitAcquiring = (self, acquiring, restore) => onExit2(restore(join(acquiring.fiber)), (exit) => {
  const acquired = acquiring.acquired;
  if (acquired !== undefined) {
    return isFailure3(exit) ? release(self, acquired) : void_3;
  }
  if (--acquiring.awaiters > 0 || self.state !== acquiring)
    return void_3;
  self.state = stateEmpty;
  return interrupt3(acquiring.fiber);
});
var get2 = (self_) => {
  const self = self_;
  return uninterruptibleMask2((restore) => flatMap4(scope2, (scope) => flatMap4(getState(self, restore), (state) => as2(addFinalizerExit(scope, () => release(self, state)), state.value))));
};
var release = (self, state) => {
  state.refCount--;
  if (state.refCount > 0) {
    return void_3;
  }
  const idleTimeToLive = self.idleTimeToLive;
  if (idleTimeToLive === undefined || state.invalidated) {
    if (self.state === state) {
      self.state = stateEmpty;
    }
    return close(state.scope, void_2);
  } else if (!isFinite(idleTimeToLive)) {
    return void_3;
  }
  state.fiber = uninterruptibleMask2((restore) => flatMap4(restore(sleep2(idleTimeToLive)), () => {
    if (self.state === state && state.refCount === 0) {
      self.state = stateEmpty;
      return ignoreCause2(close(state.scope, void_2), {
        log: "Unhandled",
        message: "Unhandled error in RcRef finalizer"
      });
    }
    return void_3;
  })).pipe(ensuring2(sync2(() => {
    state.fiber = undefined;
  })), runForkWith2(self.context), runIn(self.scope));
  return void_3;
};

// node_modules/effect/dist/RcRef.js
var make13 = make12;
var get3 = get2;

// node_modules/effect/dist/Stream.js
var TypeId15 = "~effect/Stream";
var isStream = (u) => hasProperty(u, TypeId15);
var fromChannel3 = fromChannel;
var fromPull2 = (pull) => fromChannel3(fromPull(pull));
var transformPull2 = (self, f) => fromChannel3(fromTransform((_, scope) => flatMap4(toPullScoped(self.channel, scope), (pull) => f(pull, scope))));
var toChannel2 = (stream) => stream.channel;
var callback3 = (f, options) => fromChannel3(callbackArray(f, options));
var empty4 = /* @__PURE__ */ fromChannel3(empty3);
var suspend4 = (stream) => {
  const suspended = fromChannel3(suspend3(() => stream().channel));
  suspendedStreams.set(suspended, stream);
  return suspended;
};
var suspendedStreams = /* @__PURE__ */ new WeakMap;
var fail9 = (error) => fromChannel3(fail8(error));
var fromReadableStream2 = (options) => fromChannel3(fromReadableStream(options));
var unwrap3 = (effect) => fromChannel3(unwrap(map7(effect, toChannel2)));
var map9 = /* @__PURE__ */ dual(2, (self, f) => suspend4(() => {
  let i = 0;
  return fromChannel3(map8(self.channel, map4((o) => f(o, i++))));
}));
var mapArray = /* @__PURE__ */ dual(2, (self, f) => fromChannel3(map8(self.channel, f)));
var drainFork = /* @__PURE__ */ dual(2, (self, that) => mergeEffect2(self, runDrain2(that)));
var merge3 = /* @__PURE__ */ dual((args) => isStream(args[0]) && isStream(args[1]), (self, that, options) => fromChannel3(merge2(toChannel2(self), toChannel2(that), options)));
var mergeEffect2 = /* @__PURE__ */ dual(2, (self, effect) => self.channel.pipe(mergeEffect(effect), fromChannel3));
var tapError4 = /* @__PURE__ */ dual(2, (self, f) => self.channel.pipe(tapError3(f), fromChannel3));
var orDie4 = (self) => fromChannel3(orDie3(self.channel));
var transduce = /* @__PURE__ */ dual(2, (self, sink) => transformPull2(self, (upstream, scope) => sync2(() => {
  let done;
  let leftover;
  const upstreamWithLeftover = suspend2(() => {
    if (leftover !== undefined) {
      const chunk = leftover;
      leftover = undefined;
      return succeed7(chunk);
    }
    return upstream;
  }).pipe(catch_2((error) => {
    done = fail5(error);
    return done2();
  }));
  const pull = map7(suspend2(() => sink.transform(upstreamWithLeftover, scope)), ([value, leftover_]) => {
    leftover = leftover_;
    return of(value);
  });
  return suspend2(() => done ? done : pull);
})));
var pipeThroughChannelOrFail = /* @__PURE__ */ dual(2, (self, channel) => fromChannel3(pipeToOrFail(self.channel, channel)));
var decodeText2 = /* @__PURE__ */ dual((args) => isStream(args[0]), (self, options) => self.channel.pipe(pipeTo(decodeText(options?.encoding)), fromChannel3));
var splitLines2 = (self) => self.channel.pipe(pipeTo(splitLines()), fromChannel3);
var interruptWhen2 = /* @__PURE__ */ dual(2, (self, effect) => fromChannel3(interruptWhen(self.channel, effect)));
var ensuring4 = /* @__PURE__ */ dual(2, (self, finalizer) => fromChannel3(ensuring3(self.channel, finalizer)));
var run = /* @__PURE__ */ dual(2, (self, sink) => scopedWith2((scope) => toPullScoped(self.channel, scope).pipe(flatMap4((upstream) => runSink(sink, upstream, scope)), map7(([a]) => a))));
var runSink = (sink, upstream, scope) => restoreUpstreamFailure(sink.transform(upstreamFailureToDefect(upstream), scope));
var runCollect = (self) => runFold(self.channel, () => [], (acc, chunk) => {
  for (let i = 0;i < chunk.length; i++) {
    acc.push(chunk[i]);
  }
  return acc;
});
var runForEach2 = /* @__PURE__ */ dual(2, (self, f) => runForEach(self.channel, (arr) => {
  let i = 0;
  return whileLoop2({
    while: () => i < arr.length,
    body: () => f(arr[i++]),
    step: constVoid
  });
}));
var runForEachArray = /* @__PURE__ */ dual(2, (self, f) => runForEach(self.channel, f));
var runDrain2 = (self) => runDrain(self.channel);
var mkString = (self) => runFold(self.channel, () => "", (acc, chunk) => acc + chunk.join(""));
var mkArrayBuffer = (self) => map7(mkUint8Array(self.channel), (bytes) => bytes.buffer);
var toReadableStreamWith = /* @__PURE__ */ dual((args) => isStream(args[0]), (self, context, options) => {
  let currentResolve = undefined;
  let fiber = undefined;
  let cancelled = false;
  const latch = makeUnsafe5(false);
  const signal = options?.signal;
  const onAbort = () => fiber.interruptUnsafe();
  return new ReadableStream({
    start(controller) {
      fiber = runFork2(provideContext2(runForEach(transformPull(self.channel, (pull) => succeed7(latch.whenOpen(pull))), (chunk) => sync2(() => {
        latch.closeUnsafe();
        for (let i = 0;i < chunk.length; i++) {
          controller.enqueue(chunk[i]);
        }
        currentResolve();
        currentResolve = undefined;
      })), context));
      fiber.addObserver((exit) => {
        signal?.removeEventListener("abort", onAbort);
        if (cancelled)
          return;
        if (exit._tag === "Failure" && signal?.aborted !== true) {
          controller.error(squash(exit.cause));
        } else {
          controller.close();
        }
      });
      if (signal !== undefined && fiber.pollUnsafe() === undefined) {
        if (signal.aborted)
          onAbort();
        else
          signal.addEventListener("abort", onAbort, {
            once: true
          });
      }
    },
    pull() {
      return new Promise((resolve) => {
        currentResolve = resolve;
        latch.openUnsafe();
      });
    },
    cancel() {
      if (!fiber)
        return;
      cancelled = true;
      return runPromise2(asVoid2(interrupt3(fiber)));
    }
  }, options?.strategy);
});
var toReadableStreamEffect = /* @__PURE__ */ dual((args) => isStream(args[0]), (self, options) => map7(context2(), (context) => toReadableStreamWith(self, context, options)));

// node_modules/effect/dist/FileSystem.js
var TypeId16 = "~effect/FileSystem";
var FileSystem = /* @__PURE__ */ Service("effect/FileSystem");
var make14 = (impl) => FileSystem.of({
  ...impl,
  [TypeId16]: TypeId16,
  glob: make8(impl),
  exists: (path) => pipe(impl.access(path), as2(true), catchTag2("PlatformError", (e) => e.reason._tag === "NotFound" ? succeed7(false) : fail7(e))),
  readFileString: (path, encoding) => flatMap4(impl.readFile(path), (_) => try_3({
    try: () => new TextDecoder(encoding).decode(_),
    catch: (cause) => badArgument({
      module: "FileSystem",
      method: "readFileString",
      description: "invalid encoding",
      cause
    })
  })),
  stream: fnUntraced2(function* (path, options) {
    const file = yield* impl.open(path, {
      flag: "r"
    });
    const offset = options?.offset === undefined ? undefined : fromInputUnsafe2(options.offset);
    if (offset) {
      yield* file.seek(offset, "start");
    }
    const bytesToRead = options?.bytesToRead === undefined ? undefined : fromInputUnsafe2(options.bytesToRead);
    let totalBytesRead = BigInt(0);
    const chunkSize = Number(BigInt(options?.chunkSize ?? 64 * 1024));
    const readChunk = file.readAlloc(chunkSize);
    return fromPull2(succeed7(flatMap4(suspend2(() => {
      if (bytesToRead !== undefined && bytesToRead <= totalBytesRead) {
        return done2();
      }
      return bytesToRead !== undefined && bytesToRead - totalBytesRead < chunkSize ? file.readAlloc(Number(bytesToRead - totalBytesRead)) : readChunk;
    }), match({
      onNone: () => done2(),
      onSome: (buf) => {
        totalBytesRead += BigInt(buf.length);
        return succeed7(of(buf));
      }
    }))));
  }, unwrap3),
  sink: (path, options) => pipe(impl.open(path, {
    ...options,
    flag: options?.flag ?? "w"
  }), map7((file) => forEach3((_) => file.writeAll(_))), unwrap2),
  writeFileString: (path, data, options) => flatMap4(try_3({
    try: () => new TextEncoder().encode(data),
    catch: (cause) => badArgument({
      module: "FileSystem",
      method: "writeFileString",
      description: "could not encode string",
      cause
    })
  }), (_) => impl.writeFile(path, _, options))
});
var FileTypeId = "~effect/FileSystem/File";
class WatchBackend extends (/* @__PURE__ */ Service()("effect/FileSystem/WatchBackend")) {
}

// node_modules/effect/dist/Path.js
var TypeId17 = "~effect/Path";
var Path = /* @__PURE__ */ Service("effect/Path");
var make15 = (impl) => Path.of({
  ...impl,
  [TypeId17]: TypeId17,
  resolveWithin(root, input) {
    if (impl.parse(input).root !== "")
      return none2();
    const base = impl.resolve(root);
    const candidate = impl.resolve(base, input);
    const relative = impl.relative(base, candidate);
    return relative === ".." || relative.startsWith(`..${impl.sep}`) || impl.isAbsolute(relative) ? none2() : some2(candidate);
  }
});

// node_modules/effect/dist/ConfigProvider.js
function makeValue(value) {
  return {
    _tag: "Value",
    value
  };
}
function makeRecord(keys, value) {
  return {
    _tag: "Record",
    keys,
    value
  };
}
function makeArray(length, value) {
  return {
    _tag: "Array",
    length,
    value
  };
}
var ConfigProvider = /* @__PURE__ */ Reference("effect/ConfigProvider", {
  defaultValue: () => fromEnv()
});
var Proto3 = {
  ...PipeInspectableProto,
  toJSON() {
    return {
      _id: "ConfigProvider"
    };
  }
};
var identityPath = (path) => path;
function makeProvider(load, mapInput) {
  const self = Object.create(Proto3);
  self.load = load;
  self.mapInput = mapInput;
  return self;
}
function makeSource(get, transform) {
  return makeProvider((path) => get(transform(path)), (f) => makeSource(get, (path) => f(transform(path))));
}
function make16(get) {
  return makeSource(get, identityPath);
}
function emptyStringAsMissing(value, preserveEmptyStrings) {
  return value === "" && !preserveEmptyStrings ? undefined : value;
}
function fromEnvRecord(env, options) {
  const preserveEmptyStrings = options?.preserveEmptyStrings === true;
  const trie = buildEnvTrie(env);
  return make16((path) => succeed7(nodeAtEnv(trie, env, path, preserveEmptyStrings)));
}
function fromEnv(options) {
  const env = options?.env ?? {
    ...globalThis.process?.env,
    ...import.meta?.env
  };
  return fromEnvRecord(env, {
    preserveEmptyStrings: options?.preserveEmptyStrings
  });
}
function buildEnvTrie(env) {
  const trie = {};
  for (const [name, value] of Object.entries(env)) {
    if (value === undefined)
      continue;
    const segments = name.split("_");
    let node = trie;
    for (const seg of segments) {
      const children = node.children ??= Object.create(null);
      node = children[seg] ??= {};
    }
  }
  return trie;
}
function nodeAtEnv(trie, env, path, preserveEmptyStrings) {
  const key = path.map(String).join("_");
  const leafValue = emptyStringAsMissing(Object.hasOwn(env, key) ? env[key] : undefined, preserveEmptyStrings);
  const trieNode = trieNodeAt(trie, path);
  const children = trieNode?.children ? Object.keys(trieNode.children) : [];
  if (children.length === 0) {
    return leafValue === undefined ? undefined : makeValue(leafValue);
  }
  const allNumeric = children.every(isCanonicalArrayIndex);
  if (allNumeric) {
    const length = Math.max(...children.map((k) => parseInt(k, 10))) + 1;
    return makeArray(length, leafValue);
  }
  return makeRecord(new Set(children), leafValue);
}
function trieNodeAt(root, path) {
  if (path.length === 0)
    return root;
  let node = root;
  for (const seg of path) {
    node = node?.children?.[String(seg)];
    if (!node)
      return;
  }
  return node;
}

// node_modules/effect/dist/crypto/WebCrypto.js
var WebCrypto = /* @__PURE__ */ Reference("effect/crypto/WebCrypto", {
  defaultValue: () => globalThis.crypto
});

// node_modules/effect/dist/crypto/internal/webCrypto.js
var subtleUnavailable = () => new Error("crypto.subtle is unavailable; browsers expose it only in secure contexts");

// node_modules/effect/dist/internal/ulid.js
var maxTimestamp = 2 ** 48 - 1;
var base32Chars = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
var ulidString = (timestampMillis, bytes) => {
  if (bytes.length !== 10) {
    throw new Error(`ULID randomness must be exactly 10 bytes, received ${bytes.length}`);
  }
  const timestamp = Math.min(Math.max(0, Math.trunc(timestampMillis)), maxTimestamp);
  let out = "";
  for (let shift = 45;shift >= 0; shift -= 5) {
    out += base32Chars[Math.floor(timestamp / 2 ** shift) & 31];
  }
  let accumulator = 0;
  let bits = 0;
  for (const byte of bytes) {
    accumulator = accumulator << 8 | byte;
    bits += 8;
    while (bits >= 5) {
      bits -= 5;
      out += base32Chars[accumulator >>> bits & 31];
    }
    accumulator &= (1 << bits) - 1;
  }
  return out;
};
var makeMonotonic = () => {
  let lastTimestamp = -1;
  const random = new Uint8Array(10);
  return (timestampMillis, randomBytes) => {
    const timestamp = timestampMillis >= 0 ? Math.min(Math.trunc(timestampMillis), maxTimestamp) : 0;
    if (timestamp > lastTimestamp) {
      lastTimestamp = timestamp;
      random.set(randomBytes());
    } else if (!increment(random)) {
      lastTimestamp = Math.min(lastTimestamp + 1, maxTimestamp);
      random.set(randomBytes());
    }
    return ulidString(lastTimestamp, random);
  };
};
var increment = (bytes) => {
  for (let i = bytes.length - 1;i >= 0; i--) {
    if (bytes[i] !== 255) {
      bytes[i]++;
      return true;
    }
    bytes[i] = 0;
  }
  return false;
};

// node_modules/effect/dist/internal/uuid.js
var hex = /* @__PURE__ */ Array.from({
  length: 256
}, (_, byte) => byte.toString(16).padStart(2, "0"));
var stringify = (bytes) => {
  if (bytes.length < 16) {
    return [bytes.subarray(0, 4), bytes.subarray(4, 6), bytes.subarray(6, 8), bytes.subarray(8, 10), bytes.subarray(10, 16)].map((segment) => Array.from(segment, (byte) => hex[byte]).join("")).join("-");
  }
  return hex[bytes[0]] + hex[bytes[1]] + hex[bytes[2]] + hex[bytes[3]] + "-" + hex[bytes[4]] + hex[bytes[5]] + "-" + hex[bytes[6]] + hex[bytes[7]] + "-" + hex[bytes[8]] + hex[bytes[9]] + "-" + hex[bytes[10]] + hex[bytes[11]] + hex[bytes[12]] + hex[bytes[13]] + hex[bytes[14]] + hex[bytes[15]];
};
var v4Bytes = (bytes) => {
  bytes[6] = bytes[6] & 15 | 64;
  bytes[8] = bytes[8] & 63 | 128;
  return bytes;
};
var maxV7Timestamp = 2 ** 48 - 1;
var v7Bytes = (timestampMillis, bytes) => {
  const timestamp = Math.min(Math.max(0, Math.trunc(timestampMillis)), maxV7Timestamp);
  bytes[0] = Math.floor(timestamp / 2 ** 40);
  bytes[1] = Math.floor(timestamp / 2 ** 32) & 255;
  bytes[2] = Math.floor(timestamp / 2 ** 24) & 255;
  bytes[3] = Math.floor(timestamp / 2 ** 16) & 255;
  bytes[4] = Math.floor(timestamp / 2 ** 8) & 255;
  bytes[5] = timestamp & 255;
  bytes[6] = bytes[6] & 15 | 112;
  bytes[8] = bytes[8] & 63 | 128;
  return bytes;
};
var clampV7Timestamp = (timestampMillis) => timestampMillis >= 0 ? Math.min(Math.trunc(timestampMillis), maxV7Timestamp) : 0;
var maxV7Counter = 2 ** 42 - 1;
var makeV7Monotonic = () => {
  let lastTimestamp = -1;
  let counter = 0;
  return (timestampMillis, bytes) => {
    const timestamp = clampV7Timestamp(timestampMillis);
    if (timestamp > lastTimestamp) {
      lastTimestamp = timestamp;
      counter = seedV7Counter(bytes);
    } else if (counter < maxV7Counter) {
      counter++;
    } else {
      lastTimestamp = Math.min(lastTimestamp + 1, maxV7Timestamp);
      counter = seedV7Counter(bytes);
    }
    v7Bytes(lastTimestamp, bytes);
    const high = Math.floor(counter / 2 ** 30);
    const low = counter - high * 2 ** 30;
    bytes[6] = 112 | high >>> 8;
    bytes[7] = high & 255;
    bytes[8] = 128 | low >>> 24;
    bytes[9] = low >>> 16 & 255;
    bytes[10] = low >>> 8 & 255;
    bytes[11] = low & 255;
    return bytes;
  };
};
var seedV7Counter = (bytes) => ((bytes[6] & 7) << 8 | bytes[7]) * 2 ** 30 + ((bytes[8] & 63) << 24 | bytes[9] << 16 | bytes[10] << 8 | bytes[11]);

// node_modules/effect/dist/crypto/SecureRandom.js
class SecureRandom extends (/* @__PURE__ */ Service()("effect/crypto/SecureRandom")) {
}
var checkSize = (size) => {
  if (!Number.isSafeInteger(size) || size < 0) {
    throw new RangeError("size must be a non-negative safe integer");
  }
};
var readUint53 = (bytes) => (bytes[0] & 31) * 2 ** 48 + bytes[1] * 2 ** 40 + bytes[2] * 2 ** 32 + bytes[3] * 2 ** 24 + bytes[4] * 2 ** 16 + bytes[5] * 2 ** 8 + bytes[6];
var intBetweenUnsafe = (min, max, halfOpen, draw) => {
  const minInt = Math.ceil(min);
  const maxInt = Math.floor(max);
  if (!Number.isSafeInteger(minInt) || !Number.isSafeInteger(maxInt)) {
    throw new RangeError("bounds must round to safe integers");
  }
  if (maxInt - minInt < Number.MAX_SAFE_INTEGER) {
    const count = maxInt - minInt + (halfOpen ? 0 : 1);
    if (count <= 0) {
      throw new RangeError("range must contain at least one integer");
    }
    if (count === 1)
      return minInt + 0;
    const bucketSize = (2 ** 53 - 2 ** 53 % count) / count;
    const limit = bucketSize * count;
    while (true) {
      const value = readUint53(draw());
      if (value < limit)
        return minInt + (value - value % bucketSize) / bucketSize;
    }
  }
  const lower = BigInt(minInt);
  const count = BigInt(maxInt) - lower + (halfOpen ? BigInt(0) : BigInt(1));
  const wide = count > BigInt(1) << BigInt(53);
  const bucketSize = (wide ? BigInt(1) << BigInt(54) : BigInt(1) << BigInt(53)) / count;
  const limit = bucketSize * count;
  while (true) {
    const bytes = draw();
    const value = BigInt(readUint53(bytes)) + (wide && (bytes[0] & 32) !== 0 ? BigInt(1) << BigInt(53) : BigInt(0));
    if (value < limit)
      return Number(lower + value / bucketSize);
  }
};
var makeUuid = (bytes, randomBytes, format) => {
  const uuid = bytes(randomBytes(16));
  return format === "bytes" ? uuid : stringify(uuid);
};
var make17 = (randomBytes) => {
  const randomBytesChecked = (size) => {
    checkSize(size);
    return randomBytes(size);
  };
  const draw = () => randomBytes(7);
  const uuidV7 = makeV7Monotonic();
  const ulid = makeMonotonic();
  const ulidRandomBytes = () => randomBytes(10);
  return {
    randomBytes: (size) => sync2(() => randomBytesChecked(size)),
    intBetween: (min, max, options) => sync2(() => intBetweenUnsafe(min, max, options?.halfOpen ?? false, draw)),
    shuffle: (elements) => sync2(() => {
      const buffer = Array.from(elements);
      for (let i = buffer.length - 1;i >= 1; i--) {
        const index = intBetweenUnsafe(0, i, false, draw);
        const value = buffer[i];
        buffer[i] = buffer[index];
        buffer[index] = value;
      }
      return buffer;
    }),
    uuidV4: (options) => sync2(() => makeUuid(v4Bytes, randomBytes, options?.format)),
    uuidV7: (options) => clockWith2((clock) => sync2(() => makeUuid((bytes) => uuidV7(clock.currentTimeMillisUnsafe(), bytes), randomBytes, options?.format))),
    ulid: clockWith2((clock) => sync2(() => ulid(clock.currentTimeMillisUnsafe(), ulidRandomBytes)))
  };
};
var fillRandom = (crypto, size) => {
  const bytes = new Uint8Array(size);
  for (let offset = 0;offset < size; offset += 65536) {
    crypto.getRandomValues(bytes.subarray(offset, offset + 65536));
  }
  return bytes;
};
var layerWebCrypto = /* @__PURE__ */ effect(SecureRandom, /* @__PURE__ */ WebCrypto.use((crypto) => succeed7(make17((size) => fillRandom(crypto, size)))));

// node_modules/effect/dist/encoding/Base64.js
var encode2 = (input) => encodeBytes(typeof input === "string" ? encoder2.encode(input) : input);
var decode = (str) => {
  const stripped = stripCrlf(str);
  const length = stripped.length;
  if (length % 4 !== 0) {
    return fail2(new EncodingError({
      kind: "Decode",
      module: "Base64",
      input: stripped,
      message: `Length must be a multiple of 4, but is ${length}`
    }));
  }
  const index = stripped.indexOf("=");
  if (index !== -1 && (index < length - 2 || index === length - 2 && stripped[length - 1] !== "=")) {
    return fail2(new EncodingError({
      kind: "Decode",
      module: "Base64",
      input: stripped,
      message: "Found a '=' character, but it is not at the end"
    }));
  }
  try {
    const missingOctets = stripped.endsWith("==") ? 2 : stripped.endsWith("=") ? 1 : 0;
    const result = new Uint8Array(3 * (length / 4) - missingOctets);
    for (let i = 0, j = 0;i < length; i += 4, j += 3) {
      const buffer = getCode(stripped.charCodeAt(i)) << 18 | getCode(stripped.charCodeAt(i + 1)) << 12 | getCode(stripped.charCodeAt(i + 2)) << 6 | getCode(stripped.charCodeAt(i + 3));
      result[j] = buffer >> 16;
      result[j + 1] = buffer >> 8 & 255;
      result[j + 2] = buffer & 255;
    }
    return succeed2(result);
  } catch (cause) {
    return fail2(new EncodingError({
      kind: "Decode",
      module: "Base64",
      input: stripped,
      message: cause instanceof Error ? cause.message : "Invalid input"
    }));
  }
};
var encodeBytes = (bytes) => {
  const length = bytes.length;
  const toBase64 = Uint8Array.prototype.toBase64;
  if (length >= 32 && typeof toBase64 === "function")
    return toBase64.call(bytes);
  return length < 128 ? encodeShort(bytes) : encodeLong(bytes);
};
var encodeShort = (bytes) => {
  const length = bytes.length;
  let result = "";
  let i;
  for (i = 2;i < length; i += 3) {
    result += alphabet[bytes[i - 2] >> 2];
    result += alphabet[(bytes[i - 2] & 3) << 4 | bytes[i - 1] >> 4];
    result += alphabet[(bytes[i - 1] & 15) << 2 | bytes[i] >> 6];
    result += alphabet[bytes[i] & 63];
  }
  if (i === length + 1) {
    result += alphabet[bytes[i - 2] >> 2];
    result += alphabet[(bytes[i - 2] & 3) << 4];
    result += "==";
  }
  if (i === length) {
    result += alphabet[bytes[i - 2] >> 2];
    result += alphabet[(bytes[i - 2] & 3) << 4 | bytes[i - 1] >> 4];
    result += alphabet[(bytes[i - 1] & 15) << 2];
    result += "=";
  }
  return result;
};
var encodeLong = (bytes) => {
  const length = bytes.length;
  const result = new Uint8Array(Math.ceil(length / 3) * 4);
  let i;
  let j = 0;
  for (i = 2;i < length; i += 3, j += 4) {
    result[j] = alphabetCodes[bytes[i - 2] >> 2];
    result[j + 1] = alphabetCodes[(bytes[i - 2] & 3) << 4 | bytes[i - 1] >> 4];
    result[j + 2] = alphabetCodes[(bytes[i - 1] & 15) << 2 | bytes[i] >> 6];
    result[j + 3] = alphabetCodes[bytes[i] & 63];
  }
  if (i === length + 1) {
    result[j] = alphabetCodes[bytes[i - 2] >> 2];
    result[j + 1] = alphabetCodes[(bytes[i - 2] & 3) << 4];
    result[j + 2] = paddingCode;
    result[j + 3] = paddingCode;
  }
  if (i === length) {
    result[j] = alphabetCodes[bytes[i - 2] >> 2];
    result[j + 1] = alphabetCodes[(bytes[i - 2] & 3) << 4 | bytes[i - 1] >> 4];
    result[j + 2] = alphabetCodes[(bytes[i - 1] & 15) << 2];
    result[j + 3] = paddingCode;
  }
  return decoder.decode(result);
};
var encoder2 = /* @__PURE__ */ new TextEncoder;
var decoder = /* @__PURE__ */ new TextDecoder;
var stripCrlf = (str) => str.replace(/[\n\r]/g, "");
var alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
var alphabetCodes = /* @__PURE__ */ encoder2.encode(alphabet);
var paddingCode = 61;
var codes = /* @__PURE__ */ new Uint8Array(123).fill(255);
for (let i = 0;i < alphabet.length; i++)
  codes[/* @__PURE__ */ alphabet.charCodeAt(i)] = i;
codes[61] = 0;
var getCode = (charCode) => {
  if (charCode >= codes.length || codes[charCode] === 255) {
    throw new TypeError(`Invalid character ${String.fromCharCode(charCode)}`);
  }
  return codes[charCode];
};

// node_modules/effect/dist/encoding/Base64Url.js
var encode3 = (input) => encode2(input).replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
var decode2 = (str) => {
  const stripped = str.replace(/[\n\r]/g, "");
  const length = stripped.length;
  const padded = stripped.endsWith("=");
  if (padded ? length % 4 !== 0 : length % 4 === 1) {
    return fail2(new EncodingError({
      module: "Base64Url",
      kind: "Decode",
      input: stripped,
      message: padded ? `Padded length should be a multiple of 4, but is ${length}` : `Unpadded length should not be 1 more than a multiple of 4, but is ${length}`
    }));
  }
  if (!/^[-_A-Z0-9]*?={0,2}$/i.test(stripped)) {
    return fail2(new EncodingError({
      module: "Base64Url",
      kind: "Decode",
      input: stripped,
      message: "Invalid input"
    }));
  }
  const end = !padded ? length : stripped.charCodeAt(length - 2) === 61 ? length - 2 : length - 1;
  const fullEnd = end & ~3;
  const result = new Uint8Array(Math.floor(end * 3 / 4));
  let j = 0;
  for (let i = 0;i < fullEnd; i += 4) {
    const a = codes2[stripped.charCodeAt(i)];
    const b = codes2[stripped.charCodeAt(i + 1)];
    const c = codes2[stripped.charCodeAt(i + 2)];
    const d = codes2[stripped.charCodeAt(i + 3)];
    result[j++] = a << 2 | b >> 4;
    result[j++] = b << 4 | c >> 2;
    result[j++] = c << 6 | d;
  }
  if (end > fullEnd) {
    const a = codes2[stripped.charCodeAt(fullEnd)];
    const b = codes2[stripped.charCodeAt(fullEnd + 1)];
    result[j++] = a << 2 | b >> 4;
    if (end - fullEnd === 3)
      result[j] = b << 4 | codes2[stripped.charCodeAt(fullEnd + 2)] >> 2;
  }
  return succeed2(result);
};
var alphabet2 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
var codes2 = /* @__PURE__ */ new Uint8Array(128);
for (let i = 0;i < alphabet2.length; i++)
  codes2[/* @__PURE__ */ alphabet2.charCodeAt(i)] = i;
var decodeString = (str) => map2(decode2(str), (_) => decoder2.decode(_));
var decoder2 = /* @__PURE__ */ new TextDecoder;

// node_modules/effect/dist/crypto/internal/digest.js
var webCryptoDigest = (crypto, algorithm, data) => {
  const implementation = algorithm.implementation;
  if (implementation !== undefined) {
    return sync2(() => implementation(data));
  }
  return suspend2(() => {
    const subtle = crypto?.subtle;
    if (subtle === undefined)
      return die3(subtleUnavailable());
    return promise2(async () => new Uint8Array(await subtle.digest(algorithm.name, data)));
  });
};

// node_modules/effect/dist/crypto/Digest.js
var sha256 = {
  name: "SHA-256",
  outputLength: 32,
  implementation: undefined
};
class Digest extends (/* @__PURE__ */ Service()("effect/crypto/Digest")) {
}
var layerWebCrypto2 = /* @__PURE__ */ effect(Digest, /* @__PURE__ */ WebCrypto.use((crypto) => succeed7({
  digest: (algorithm, data) => webCryptoDigest(crypto, algorithm, data),
  digestHex: (algorithm, data) => map7(webCryptoDigest(crypto, algorithm, data), encode)
})));

// node_modules/effect/dist/DateTime.js
var makeUnsafe6 = makeUnsafe4;
var formatIso2 = formatIso;

// node_modules/effect/dist/http/Cookies.js
var TypeId18 = "~effect/http/Cookies";
var CookieTypeId = "~effect/http/Cookies/Cookie";
var CookieErrorTypeId = "~effect/http/Cookies/CookieError";

class CookiesErrorReason extends Error3 {
}

class CookiesError extends (/* @__PURE__ */ TaggedError2("CookiesError")) {
  static fromReason(reason, cause) {
    return new CookiesError({
      reason: new CookiesErrorReason({
        _tag: reason,
        cause
      })
    });
  }
  [CookieErrorTypeId] = CookieErrorTypeId;
  get message() {
    return this.reason._tag;
  }
}
var Proto4 = {
  [TypeId18]: TypeId18,
  ...BaseProto,
  toJSON() {
    return {
      _id: "effect/Cookies",
      cookies: map3(this.cookies, (cookie) => cookie.toJSON())
    };
  },
  pipe() {
    return pipeArguments(this, arguments);
  }
};
var fromReadonlyRecord = (cookies) => {
  const self = Object.create(Proto4);
  self.cookies = cookies;
  return self;
};
var fromIterable2 = (cookies) => {
  const record = {};
  for (const cookie of cookies) {
    assignProperty(record, cookie.name, cookie);
  }
  return fromReadonlyRecord(record);
};
var fromSetCookie = (headers) => {
  const arrayHeaders = typeof headers === "string" ? [headers] : headers;
  const cookies = [];
  for (const header of arrayHeaders) {
    const cookie = parseSetCookie(header.trim());
    if (cookie) {
      cookies.push(cookie);
    }
  }
  return fromIterable2(cookies);
};
function parseSetCookie(header) {
  const parts = header.split(";").map((_) => _.trim()).filter((_) => _ !== "");
  if (parts.length === 0) {
    return;
  }
  const firstEqual = parts[0].indexOf("=");
  if (firstEqual === -1) {
    return;
  }
  const name = parts[0].slice(0, firstEqual).replace(cookieWhitespaceRegExp, "");
  if (!cookieNameRegExp.test(name)) {
    return;
  }
  const valueEncoded = parts[0].slice(firstEqual + 1).replace(cookieWhitespaceRegExp, "");
  const value = tryDecodeURIComponent(valueEncoded);
  if (parts.length === 1) {
    return Object.assign(Object.create(CookieProto), {
      name,
      value,
      valueEncoded
    });
  }
  const options = {};
  for (let i = 1;i < parts.length; i++) {
    const part = parts[i];
    const equalIndex = part.indexOf("=");
    const key = equalIndex === -1 ? part : part.slice(0, equalIndex).trim();
    const value = equalIndex === -1 ? undefined : part.slice(equalIndex + 1).trim();
    switch (key.toLowerCase()) {
      case "domain": {
        if (value === undefined) {
          break;
        }
        const domain = value.trim().replace(/^\./, "");
        if (domain) {
          options.domain = domain;
        }
        break;
      }
      case "expires": {
        if (value === undefined) {
          break;
        }
        const date = new Date(value);
        if (!isNaN(date.getTime())) {
          options.expires = date;
        }
        break;
      }
      case "max-age": {
        if (value === undefined || !maxAgeRegExp.test(value)) {
          break;
        }
        options.maxAge = seconds(parseInt(value, 10));
        break;
      }
      case "path": {
        if (value === undefined) {
          break;
        }
        if (value[0] === "/") {
          options.path = value;
        }
        break;
      }
      case "priority": {
        if (value === undefined) {
          break;
        }
        switch (value.toLowerCase()) {
          case "low":
            options.priority = "low";
            break;
          case "medium":
            options.priority = "medium";
            break;
          case "high":
            options.priority = "high";
            break;
        }
        break;
      }
      case "httponly": {
        options.httpOnly = true;
        break;
      }
      case "secure": {
        options.secure = true;
        break;
      }
      case "partitioned": {
        options.partitioned = true;
        break;
      }
      case "samesite": {
        if (value === undefined) {
          break;
        }
        switch (value.toLowerCase()) {
          case "lax":
            options.sameSite = "lax";
            break;
          case "strict":
            options.sameSite = "strict";
            break;
          case "none":
            options.sameSite = "none";
            break;
        }
        break;
      }
    }
  }
  return Object.assign(Object.create(CookieProto), {
    name,
    value,
    valueEncoded,
    options: Object.keys(options).length > 0 ? options : undefined
  });
}
var empty5 = /* @__PURE__ */ fromIterable2([]);
var isEmpty = (self) => isEmptyRecord(self.cookies);
var fieldContentRegExp = /^[\u0009\u0020-\u007e\u0080-\u00ff]+$/;
var cookieNameRegExp = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
var cookieWhitespaceRegExp = /^[\t ]+|[\t ]+$/g;
var maxAgeRegExp = /^-?[0-9]+$/;
var cookieDomainRegExp = /^[\u0009\u0020-\u003a\u003c-\u007e\u0080-\u00ff]+$/;
var cookiePathRegExp = /^[\u0020-\u003a\u003c-\u007e]+$/;
var CookieProto = {
  [CookieTypeId]: CookieTypeId,
  ...BaseProto,
  toJSON() {
    return {
      _id: "effect/Cookies/Cookie",
      name: this.name,
      value: this.value,
      options: this.options
    };
  }
};
function validateCookie(name, encodedValue, options) {
  if (!cookieNameRegExp.test(name)) {
    return CookiesError.fromReason("InvalidCookieName");
  }
  if (encodedValue && !fieldContentRegExp.test(encodedValue)) {
    return CookiesError.fromReason("InvalidCookieValue");
  }
  if (options?.domain !== undefined && !cookieDomainRegExp.test(options.domain)) {
    return CookiesError.fromReason("InvalidCookieDomain");
  }
  if (options?.path !== undefined && !cookiePathRegExp.test(options.path)) {
    return CookiesError.fromReason("InvalidCookiePath");
  }
  if (options?.maxAge !== undefined && !isFinite(fromInputUnsafe(options.maxAge))) {
    return CookiesError.fromReason("CookieInfinityMaxAge");
  }
}
function serializeCookie(self) {
  const error = validateCookie(self.name, self.valueEncoded, self.options);
  if (error !== undefined) {
    throw error;
  }
  let str = self.name + "=" + self.valueEncoded;
  if (self.options === undefined) {
    return str;
  }
  const options = self.options;
  if (options.maxAge !== undefined) {
    const maxAge = toSeconds(fromInputUnsafe(options.maxAge));
    str += "; Max-Age=" + Math.trunc(maxAge);
  }
  if (options.domain !== undefined && !options.hostOnly) {
    str += "; Domain=" + options.domain;
  }
  if (options.path !== undefined) {
    str += "; Path=" + options.path;
  }
  if (options.priority !== undefined) {
    switch (options.priority) {
      case "low":
        str += "; Priority=Low";
        break;
      case "medium":
        str += "; Priority=Medium";
        break;
      case "high":
        str += "; Priority=High";
        break;
    }
  }
  if (options.expires !== undefined) {
    str += "; Expires=" + options.expires.toUTCString();
  }
  if (options.httpOnly) {
    str += "; HttpOnly";
  }
  if (options.secure) {
    str += "; Secure";
  }
  if (options.partitioned) {
    str += "; Partitioned";
  }
  if (options.sameSite !== undefined) {
    switch (options.sameSite) {
      case "lax":
        str += "; SameSite=Lax";
        break;
      case "strict":
        str += "; SameSite=Strict";
        break;
      case "none":
        str += "; SameSite=None";
        break;
    }
  }
  return str;
}
var toSetCookieHeaders = (self) => Object.values(self.cookies).map(serializeCookie);
function parseHeader(header) {
  const result = {};
  const strLen = header.length;
  let pos = 0;
  let terminatorPos = 0;
  while (true) {
    if (terminatorPos === strLen)
      break;
    terminatorPos = header.indexOf(";", pos);
    if (terminatorPos === -1)
      terminatorPos = strLen;
    let eqIdx = header.indexOf("=", pos);
    if (eqIdx === -1)
      break;
    if (eqIdx > terminatorPos) {
      pos = terminatorPos + 1;
      continue;
    }
    const key = header.substring(pos, eqIdx++).trim();
    if (!Object.hasOwn(result, key)) {
      let value = header.substring(eqIdx, terminatorPos).trim();
      if (value.charCodeAt(0) === 34)
        value = value.slice(1, -1).trim();
      assignProperty(result, key, !(value.indexOf("%") === -1) ? tryDecodeURIComponent(value) : value);
    }
    pos = terminatorPos + 1;
  }
  return result;
}
var tryDecodeURIComponent = (str) => {
  try {
    return decodeURIComponent(str);
  } catch (_) {
    return str;
  }
};

// node_modules/effect/dist/internal/redacted.js
var redactedRegistry = /* @__PURE__ */ new WeakMap;
var value = (self) => {
  if (redactedRegistry.has(self)) {
    return redactedRegistry.get(self);
  } else {
    throw new Error("Unable to get redacted value" + (self.label ? ` with label: "${self.label}"` : ""));
  }
};
var stringOrRedacted = (val) => typeof val === "string" ? val : value(val);

// node_modules/effect/dist/Redacted.js
var TypeId19 = "~effect/Redacted";
var isRedacted = (u) => hasProperty(u, TypeId19);
var make19 = (value, options) => {
  const self = Object.create(Proto5);
  if (options?.label) {
    self.label = options.label;
  }
  redactedRegistry.set(self, value);
  return self;
};
var Proto5 = {
  [TypeId19]: {
    _A: (_) => _
  },
  label: undefined,
  ...PipeInspectableProto,
  toJSON() {
    return this.toString();
  },
  toString() {
    return `<redacted${isString(this.label) ? ":" + this.label : ""}>`;
  },
  [symbol]() {
    return hash(redactedRegistry.get(this));
  },
  [symbol2](that) {
    return isRedacted(that) && equals(redactedRegistry.get(this), redactedRegistry.get(that));
  }
};
var value2 = value;

// node_modules/effect/dist/http/Headers.js
var TypeId20 = /* @__PURE__ */ Symbol.for("~effect/http/Headers");
var Proto6 = /* @__PURE__ */ Object.defineProperties(/* @__PURE__ */ Object.create(null), {
  [TypeId20]: {
    value: TypeId20
  },
  [symbolRedactable]: {
    value(context) {
      return redact2(this, get(context, CurrentRedactedNames));
    }
  },
  toJSON: {
    value() {
      return redact(this);
    }
  },
  [symbol2]: {
    value(that) {
      return Equivalence3(this, that);
    }
  },
  [symbol]: {
    value() {
      return structure(this);
    }
  },
  toString: {
    value: BaseProto.toString
  },
  [NodeInspectSymbol]: {
    value: BaseProto[NodeInspectSymbol]
  }
});
var make20 = (input) => Object.assign(Object.create(Proto6), input);
var Equivalence3 = /* @__PURE__ */ makeEquivalence2(/* @__PURE__ */ strictEqual());
var empty6 = /* @__PURE__ */ Object.create(Proto6);
var fromInput2 = (input) => {
  if (input === undefined) {
    return empty6;
  } else if (Symbol.iterator in input) {
    const out = Object.create(Proto6);
    for (const [k, v] of input) {
      out[k.toLowerCase()] = v;
    }
    return out;
  }
  const out = Object.create(Proto6);
  for (const [k, v] of Object.entries(input)) {
    if (Array.isArray(v)) {
      out[k.toLowerCase()] = v.join(", ");
    } else if (v !== undefined) {
      out[k.toLowerCase()] = v;
    }
  }
  return out;
};
var fromRecordUnsafe = (input) => Object.setPrototypeOf(input, Proto6);
var set3 = /* @__PURE__ */ dual(3, (self, key, value) => {
  const out = make20(self);
  out[key.toLowerCase()] = value;
  return out;
});
var setAll = /* @__PURE__ */ dual(2, (self, headers) => make20({
  ...self,
  ...fromInput2(headers)
}));
var merge4 = /* @__PURE__ */ dual(2, (self, headers) => {
  const out = make20(self);
  Object.assign(out, headers);
  return out;
});
var remove2 = /* @__PURE__ */ dual(2, (self, key) => {
  const out = make20(self);
  delete out[key.toLowerCase()];
  return out;
});
var redact2 = /* @__PURE__ */ dual(2, (self, key) => {
  const out = {
    ...self
  };
  const modify = (key) => {
    if (typeof key === "string") {
      const k = key.toLowerCase();
      if (k in self) {
        out[k] = make19(self[k]);
      }
    } else {
      for (const name in self) {
        if (name.search(key) !== -1) {
          out[name] = make19(self[name]);
        }
      }
    }
  };
  if (Array.isArray(key)) {
    for (let i = 0;i < key.length; i++) {
      modify(key[i]);
    }
  } else {
    modify(key);
  }
  return out;
});
var isRedactedName = (name, patterns) => {
  const lowerName = name.toLowerCase();
  for (let i = 0;i < patterns.length; i++) {
    const pattern = patterns[i];
    if (typeof pattern === "string") {
      if (pattern.toLowerCase() === lowerName) {
        return true;
      }
    } else if (name.search(pattern) !== -1) {
      return true;
    }
  }
  return false;
};
var CurrentRedactedNames = /* @__PURE__ */ Reference("effect/Headers/CurrentRedactedNames", {
  defaultValue: () => ["authorization", "cookie", "set-cookie", "x-api-key"]
});

// node_modules/effect/dist/http/internal/urlParams.js
var unwrap4 = (value) => typeof value === "string" ? value : value2(value);
var stripSearchAndHash = (url) => {
  const queryIndex = url.indexOf("?");
  const hashIndex = url.indexOf("#");
  if (queryIndex === -1) {
    return hashIndex === -1 ? url : url.slice(0, hashIndex);
  }
  if (hashIndex === -1) {
    return url.slice(0, queryIndex);
  }
  return url.slice(0, Math.min(queryIndex, hashIndex));
};
var bodyDecoder = /* @__PURE__ */ new TextDecoder("utf-8", {
  ignoreBOM: true
});
var fromBody = (bytes) => new URLSearchParams(bodyDecoder.decode(bytes).replace(/^\?/, "%3F"));
var originForm = /^\/(?![/\\])/;
var toPath = (url) => {
  if (url === "*" || originForm.test(url)) {
    return stripSearchAndHash(url);
  }
  try {
    return new URL(url, "http://localhost").pathname;
  } catch {
    return "<invalid URL>";
  }
};

// node_modules/effect/dist/http/UrlParams.js
var TypeId21 = "~effect/http/UrlParams";
var isUrlParams = (u) => hasProperty(u, TypeId21);
var Proto7 = {
  ...PipeInspectableProto,
  [TypeId21]: TypeId21,
  [Symbol.iterator]() {
    return this.params[Symbol.iterator]();
  },
  [symbolRedactable](context) {
    return {
      _id: "UrlParams",
      params: Object.fromEntries(redact3(this, get(context, CurrentRedactedNames2)))
    };
  },
  toJSON() {
    return redact(this);
  },
  [symbol2](that) {
    return Equivalence4(this, that);
  },
  [symbol]() {
    return array(this.params.flat());
  }
};
var make21 = (params) => {
  const self = Object.create(Proto7);
  self.params = params;
  return self;
};
var fromInput3 = (input) => {
  if (isUrlParams(input)) {
    return input;
  }
  const parsed = fromInputNested(input);
  const out = [];
  for (let i = 0;i < parsed.length; i++) {
    if (Array.isArray(parsed[i][0])) {
      const [keys, value] = parsed[i];
      out.push([`${keys[0]}[${keys.slice(1).join("][")}]`, value]);
    } else {
      out.push(parsed[i]);
    }
  }
  return make21(out);
};
var fromInputNested = (input) => {
  const entries = typeof input[Symbol.iterator] === "function" ? fromIterable(input) : Object.entries(input);
  const out = [];
  for (const [key, value] of entries) {
    if (Array.isArray(value)) {
      for (let i = 0;i < value.length; i++) {
        if (value[i] !== undefined) {
          out.push([key, coerce(value[i])]);
        }
      }
    } else if (value !== null && typeof value === "object" && !isRedacted(value)) {
      const nested = fromInputNested(value);
      for (const [k, v] of nested) {
        out.push([[key, ...typeof k === "string" ? [k] : k], v]);
      }
    } else if (value !== undefined) {
      out.push([key, coerce(value)]);
    }
  }
  return out;
};
var coerce = (value) => isRedacted(value) ? value : String(value);
var Equivalence4 = /* @__PURE__ */ make((a, b) => arrayEquivalence(a.params, b.params));
var arrayEquivalence = /* @__PURE__ */ makeEquivalence3(/* @__PURE__ */ makeEquivalence([/* @__PURE__ */ strictEqual(), /* @__PURE__ */ asEquivalence()]));
var empty7 = /* @__PURE__ */ make21([]);
var setAll2 = /* @__PURE__ */ dual(2, (self, input) => {
  const params = fromInput3(input).params.slice();
  const keys = new Set;
  for (let i = 0;i < params.length; i++) {
    keys.add(params[i][0]);
  }
  for (let i = 0;i < self.params.length; i++) {
    if (keys.has(self.params[i][0]))
      continue;
    params.push(self.params[i]);
  }
  return make21(params);
});
var toString = (input) => new URLSearchParams(fromInput3(input).params.map(([key, value]) => [key, unwrap4(value)])).toString();
var redact3 = /* @__PURE__ */ dual(2, (self, key) => {
  const patterns = Array.isArray(key) ? key : [key];
  return self.params.map(([name, value]) => [name, typeof value === "string" && isRedactedName2(name, patterns) ? make19(value) : value]);
});
var isRedactedName2 = isRedactedName;
var redactUrl = (url, redactedNames = currentRedactedNames()) => {
  if (url.includes("@"))
    url = redactUserinfo(url);
  const queryStart = url.indexOf("?");
  if (queryStart === -1 || redactedNames.length === 0)
    return url;
  const hashStart = url.indexOf("#");
  if (hashStart !== -1 && hashStart < queryStart)
    return url;
  const queryEnd = hashStart === -1 ? url.length : hashStart;
  return url.slice(0, queryStart + 1) + redactQuery(url.slice(queryStart + 1, queryEnd), redactedNames) + url.slice(queryEnd);
};
var authorityStart = /^(?:(https?|ftp|wss?):[/\\]*|[a-z][a-z\d+.-]*:\/\/|[/\\]{2,})/i;
var specialAuthorityEnd = /[/\\?#]/;
var authorityEnd = /[/?#]/;
var redactUserinfo = (url) => {
  let normalized = "";
  const offsets = [];
  for (let i = 0;i < url.length; i++) {
    const code = url.charCodeAt(i);
    if (code === 9 || code === 10 || code === 13 || normalized === "" && code <= 32)
      continue;
    normalized += url[i];
    offsets.push(i);
  }
  const authority = authorityStart.exec(normalized);
  if (authority === null)
    return url;
  const start = authority[0].length;
  const special = authority[1] !== undefined || !authority[0].includes(":");
  const rest = normalized.slice(start);
  const end = rest.search(special ? specialAuthorityEnd : authorityEnd);
  const userinfoEnd = (end === -1 ? rest : rest.slice(0, end)).lastIndexOf("@");
  return userinfoEnd === -1 ? url : url.slice(0, offsets[start]) + "REDACTED:REDACTED" + url.slice(offsets[start + userinfoEnd]);
};
var redactQuery = (query, redactedNames) => query.split("&").map((part) => {
  const index = part.indexOf("=");
  return index !== -1 && isRedactedName2(decodeQueryKey(part.slice(0, index)), redactedNames) ? part.slice(0, index + 1) + "REDACTED" : part;
}).join("&");
var decodeQueryKey = (key) => {
  if (!key.includes("%") && !key.includes("+"))
    return key;
  try {
    return decodeURIComponent(key.replace(/\+/g, " "));
  } catch {
    return key;
  }
};
var currentRedactedNames = () => get(globalThis[currentFiberTypeId]?.context ?? empty(), CurrentRedactedNames2);
var CurrentRedactedNames2 = /* @__PURE__ */ Reference("effect/http/UrlParams/CurrentRedactedNames", {
  defaultValue: () => ["X-Amz-Signature", "X-Amz-Credential", "X-Amz-Security-Token", "AWSAccessKeyId", "Signature", "sig", "X-Goog-Signature"]
});

// node_modules/effect/dist/internal/schema/annotations.js
function resolve(ast) {
  return ast.checks ? ast.checks[ast.checks.length - 1].annotations : ast.annotations;
}
var STRUCTURAL_ANNOTATION_KEY = "~structural";
var SENTINELS_ANNOTATION_KEY = "~sentinels";
var CONSTRUCTOR_ANNOTATION_KEY = "~constructor";
var getExpected = /* @__PURE__ */ memoize((ast) => {
  const identifier = resolve(ast)?.identifier;
  if (typeof identifier === "string")
    return identifier;
  return ast.getExpected(getExpected);
});

// node_modules/effect/dist/internal/schema/parser.js
var missing = /* @__PURE__ */ Symbol();
var succeed9 = succeed4;
var missingExit = /* @__PURE__ */ succeed9(missing);
var sameExit = /* @__PURE__ */ succeed9(missing);
var toOption = (value) => value === missing ? none2() : some2(value);
var fromOptionExit = (option) => option._tag === "None" ? missingExit : succeed9(option.value);

// node_modules/effect/dist/SchemaIssue.js
var TypeId22 = "~effect/SchemaIssue/Issue";
function isIssue(u) {
  return hasProperty(u, TypeId22) && u[TypeId22] === TypeId22;
}
function hasInput(issue) {
  return Object.hasOwn(issue, "input");
}

class IssueNodeImpl {
  [TypeId22] = TypeId22;
  constructor(input, options) {
    if (options?.reportInput === true && input !== missing) {
      this.input = input;
    }
  }
}
var Filter = class extends IssueNodeImpl {
  _tag = "Filter";
  filter;
  issue;
  constructor(filter, issue, input, options) {
    super(input, options);
    this.filter = filter;
    this.issue = issue;
  }
};
var Encoding = class extends IssueNodeImpl {
  _tag = "Encoding";
  ast;
  issue;
  constructor(ast, issue, input, options) {
    super(input, options);
    this.ast = ast;
    this.issue = issue;
  }
};
var Pointer = class extends IssueNodeImpl {
  _tag = "Pointer";
  path;
  issue;
  constructor(path, issue) {
    super();
    this.path = path;
    this.issue = issue;
  }
};
var MissingKey = class extends IssueNodeImpl {
  _tag = "MissingKey";
  annotations;
  constructor(annotations) {
    super();
    this.annotations = annotations;
  }
};
var UnexpectedKey = class extends IssueNodeImpl {
  _tag = "UnexpectedKey";
  ast;
  constructor(ast, input, options) {
    super(input, options);
    this.ast = ast;
  }
};
var Composite = class extends IssueNodeImpl {
  _tag = "Composite";
  ast;
  issues;
  constructor(ast, issues, input, options) {
    super(input, options);
    this.ast = ast;
    this.issues = issues;
  }
};
var InvalidType = class extends IssueNodeImpl {
  _tag = "InvalidType";
  ast;
  constructor(ast, input, options) {
    super(input, options);
    this.ast = ast;
  }
};
var InvalidValue = class extends IssueNodeImpl {
  _tag = "InvalidValue";
  annotations;
  constructor(annotations, input, options) {
    super(input, options);
    this.annotations = annotations;
  }
};
var AnyOf = class extends IssueNodeImpl {
  _tag = "AnyOf";
  ast;
  issues;
  constructor(ast, issues, input, options) {
    super(input, options);
    this.ast = ast;
    this.issues = issues;
  }
};
var OneOf = class extends IssueNodeImpl {
  _tag = "OneOf";
  ast;
  successes;
  constructor(ast, successes, input, options) {
    super(input, options);
    this.ast = ast;
    this.successes = successes;
  }
};
function makeFilterIssue(entry, input, options) {
  if (isIssue(entry)) {
    return entry;
  }
  if (typeof entry === "string") {
    return new InvalidValue({
      message: entry
    }, input, options);
  }
  const inner = typeof entry.issue === "string" ? new InvalidValue({
    message: entry.issue
  }, input, options) : entry.issue;
  return new Pointer(entry.path, inner);
}
function makeSingle(out, input, options) {
  if (out === undefined) {
    return;
  }
  if (typeof out === "boolean") {
    return out ? undefined : new InvalidValue(undefined, input, options);
  }
  return makeFilterIssue(out, input, options);
}
function normalizeFilterOutput(ast, out, input, options) {
  if (Array.isArray(out)) {
    if (!isReadonlyArrayNonEmpty(out)) {
      return;
    }
    return out.length === 1 ? makeFilterIssue(out[0], input, options) : new Composite(ast, map4(out, (entry) => makeFilterIssue(entry, input, options)), input, options);
  }
  return makeSingle(out, input, options);
}
var defaultLeafHook = (issue) => {
  const message = findMessage(issue);
  if (message !== undefined)
    return message;
  switch (issue._tag) {
    case "InvalidType":
      return getExpectedMessage(getExpected(issue.ast), issue);
    case "InvalidValue": {
      const expected = findExpected(issue);
      if (expected !== undefined)
        return getExpectedMessage(expected, issue);
      const input = formatInput(issue);
      return input === undefined ? "Expected a valid value" : `Invalid data ${input}`;
    }
    case "MissingKey":
      return "Missing key";
    case "UnexpectedKey": {
      const input = formatInput(issue);
      return input === undefined ? "Expected no excess property" : `Unexpected key with value ${input}`;
    }
    case "Forbidden":
      return "Forbidden operation";
    case "OneOf": {
      const input = formatInput(issue);
      return input === undefined ? "Expected exactly one member to match" : `Expected exactly one member to match the input ${input}`;
    }
  }
};
var defaultCheckHook = (issue) => findMessage(issue) ?? findMessage(issue.issue);
function formatInput(issue) {
  return hasInput(issue) ? format(issue.input) : undefined;
}
function findExpected(issue) {
  const expected = issue.annotations?.expected;
  return typeof expected === "string" ? expected : undefined;
}
function getExpectedMessage(expected, issue) {
  const input = formatInput(issue);
  return input === undefined ? `Expected ${expected}` : `Expected ${expected}, got ${input}`;
}
function getFilterMessage(issue, checkHook) {
  const message = checkHook(issue);
  if (message !== undefined || issue.issue._tag !== "InvalidValue")
    return message;
  const expected = findExpected(issue.issue);
  return expected === undefined ? getExpectedMessage(formatCheck(issue.filter), issue) : getExpectedMessage(expected, issue.issue);
}
function formatCheck(check) {
  const expected = check.annotations?.expected;
  if (typeof expected === "string")
    return expected;
  switch (check._tag) {
    case "Filter":
      return "<filter>";
    case "FilterGroup":
      return check.checks.map((check) => formatCheck(check)).join(" & ");
  }
}
function makeFormatterDefault() {
  return (issue) => formatIssue(issue, "");
}
var defaultFormatter = /* @__PURE__ */ makeFormatterDefault();
function formatIssue(issue, path, messageOverride) {
  let message;
  switch (issue._tag) {
    case "Filter":
      return formatIssue(issue.issue, path, messageOverride ?? getFilterMessage(issue, defaultCheckHook));
    case "Encoding":
      return formatIssue(issue.issue, path, messageOverride);
    case "Pointer":
      return formatIssue(issue.issue, path + formatPath(issue.path), messageOverride);
    case "Composite":
    case "AnyOf": {
      if (issue._tag === "Composite" || issue.issues.length > 0) {
        return issue.issues.map((issue) => formatIssue(issue, path, messageOverride)).join(`
`);
      }
      message = messageOverride ?? findMessage(issue) ?? getExpectedMessage(getExpected(issue.ast), issue);
      break;
    }
    default:
      message = messageOverride ?? defaultLeafHook(issue);
      break;
  }
  return path ? `${message}
  at ${path}` : message;
}
function findMessage(issue) {
  if (issue._tag === "Pointer" || issue._tag === "Composite")
    return;
  if (issue._tag === "Encoding")
    return findMessage(issue.issue);
  const annotations = issue._tag === "Filter" ? issue.filter.annotations : ("annotations" in issue) ? issue.annotations : issue.ast.annotations;
  const message = annotations?.[issue._tag === "MissingKey" ? "messageMissingKey" : issue._tag === "UnexpectedKey" ? "messageUnexpectedKey" : "message"];
  if (typeof message === "string")
    return message;
}

// node_modules/effect/dist/internal/schema/cause.js
function getSchemaIssue(cause) {
  let issue;
  for (const reason of cause.reasons) {
    if (!isFailReason2(reason) || !isIssue(reason.error)) {
      return;
    }
    issue ??= reason.error;
  }
  return issue;
}
function getSchemaIssueOrThrow(cause, message) {
  const issue = getSchemaIssue(cause);
  if (issue === undefined) {
    throw new Error(message, {
      cause
    });
  }
  return issue;
}

// node_modules/effect/dist/SchemaGetter.js
var makeGetter = (fields) => Object.assign(Object.create(Prototype), fields);
var passthrough_ = /* @__PURE__ */ makeGetter({
  _tag: "Passthrough"
});
function passthrough() {
  return passthrough_;
}
function transform(f) {
  return makeGetter({
    _tag: "Transform",
    transform: f
  });
}
function transformEffect(f) {
  return makeGetter({
    _tag: "TransformEffect",
    transform: f
  });
}
function String3() {
  return transform(globalThis.String);
}
function Number4() {
  return transform(globalThis.Number);
}
function parseJson(options) {
  return transformEffect((input, parseOptions) => try_3({
    try: () => JSON.parse(input, options?.reviver),
    catch: () => new InvalidValue({
      expected: "a valid JSON string"
    }, input, parseOptions)
  }));
}
function stringifyJson(options) {
  return transformEffect((input, parseOptions) => try_3({
    try: () => {
      const output = JSON.stringify(input, options?.replacer, options?.space);
      if (output === undefined) {
        throw new TypeError("Value cannot be represented as JSON");
      }
      return output;
    },
    catch: () => new InvalidValue({
      expected: "a JSON-serializable value"
    }, input, parseOptions)
  }));
}
function encodeBase64() {
  return transform(encode2);
}
function decodeBase64() {
  return transformEffect((input, options) => mapErrorEager2(fromResult2(decode(input)), () => new InvalidValue({
    expected: "a valid Base64 string"
  }, input, options)));
}

// node_modules/effect/dist/internal/schema/content.js
var ContentTypeId = /* @__PURE__ */ Symbol.for("~effect/Schema/content");
function setContent(operation, content) {
  Object.defineProperty(operation, ContentTypeId, {
    value: content
  });
  return operation;
}

// node_modules/effect/dist/SchemaTransformation.js
var TypeId23 = "~effect/SchemaTransformation/Transformation";
var Transformation = class extends Class {
  [TypeId23] = TypeId23;
  _tag = "Transformation";
  decode;
  encode;
  constructor(decode, encode) {
    super();
    this.decode = decode;
    this.encode = encode;
  }
  flip() {
    return new Transformation(this.encode, this.decode);
  }
};
function isTransformation(u) {
  return hasProperty(u, TypeId23) && u[TypeId23] === TypeId23;
}
var makeTransformation = (options) => {
  if (isTransformation(options)) {
    return options;
  }
  return new Transformation(options.decode, options.encode);
};
function transformEffect2(options) {
  return new Transformation(transformEffect(options.decode), transformEffect(options.encode));
}
function transform2(options) {
  return new Transformation(transform(options.decode), transform(options.encode));
}
var passthrough_2 = /* @__PURE__ */ new Transformation(/* @__PURE__ */ passthrough(), /* @__PURE__ */ passthrough());
function passthrough2() {
  return passthrough_2;
}
var numberFromString = /* @__PURE__ */ new Transformation(/* @__PURE__ */ Number4(), /* @__PURE__ */ String3());
var isJsonError = (input) => isObject(input) && typeof input["message"] === "string";
var decodeJsonError = (input) => {
  const hasCause = Object.hasOwn(input, "cause");
  const err = hasCause ? new Error(input.message, {
    cause: decodeDefect(input.cause)
  }) : new Error(input.message);
  if (typeof input.name === "string" && input.name !== "Error")
    err.name = input.name;
  if (typeof input.stack === "string")
    err.stack = input.stack;
  return err;
};
var encodeUnknownAsJson = (input) => {
  try {
    const json = formatJson(input);
    return json === undefined ? format(input) : JSON.parse(json);
  } catch {
    return format(input);
  }
};
var encodeJsonError = (input, options, encodeDefect) => {
  const encoded = {
    name: input.name,
    message: typeof input.message === "string" ? input.message : ""
  };
  if (options?.includeStack && typeof input.stack === "string") {
    encoded.stack = input.stack;
  }
  if (!options?.excludeCause && input.cause !== undefined) {
    encoded.cause = encodeDefect(input.cause);
  }
  return encoded;
};
var makeEncodeDefect = (options) => {
  const seen = new WeakSet;
  const encode = (input) => {
    if (isError(input)) {
      if (seen.has(input)) {
        return "[Circular]";
      }
      seen.add(input);
      const encoded = encodeJsonError(input, options, encode);
      seen.delete(input);
      return encoded;
    }
    return encodeUnknownAsJson(input);
  };
  return encode;
};
var decodeDefect = (input) => isJsonError(input) ? decodeJsonError(input) : input;
var defectFromJson = (options) => transform2({
  decode: decodeDefect,
  encode: makeEncodeDefect(options)
});
var urlFromString = /* @__PURE__ */ transformEffect2({
  decode: (s, options) => URL.canParse(s) ? succeed7(new URL(s)) : fail7(new InvalidValue({
    expected: "a valid URL string"
  }, s, options)),
  encode: (url) => succeed7(url.href)
});
var uint8ArrayFromBase64String = /* @__PURE__ */ new Transformation(/* @__PURE__ */ decodeBase64(), /* @__PURE__ */ encodeBase64());
function fromJsonString(options) {
  return new Transformation(parseJson(options ?? {}), setContent(stringifyJson(options), {
    mediaType: "application/json",
    preserveSchema: options?.reviver === undefined && options?.replacer === undefined
  }));
}

// node_modules/effect/dist/SchemaAST.js
function makeGuard(tag) {
  return (ast) => ast._tag === tag;
}
var isDeclaration = /* @__PURE__ */ makeGuard("Declaration");
var isNever2 = /* @__PURE__ */ makeGuard("Never");
var isLiteral = /* @__PURE__ */ makeGuard("Literal");
var isUniqueSymbol = /* @__PURE__ */ makeGuard("UniqueSymbol");
var isArrays = /* @__PURE__ */ makeGuard("Arrays");
var isObjects = /* @__PURE__ */ makeGuard("Objects");
var isSuspend = /* @__PURE__ */ makeGuard("Suspend");
var Link = class {
  to;
  transformation;
  constructor(to, transformation) {
    this.to = to;
    this.transformation = transformation;
  }
};
var defaultParseOptions = {};
var Context = class {
  isOptional;
  isMutable;
  constructorDefault;
  annotations;
  constructor(isOptional, isMutable, constructorDefault = undefined, annotations = undefined) {
    this.isOptional = isOptional;
    this.isMutable = isMutable;
    this.constructorDefault = constructorDefault;
    this.annotations = annotations;
  }
};
var TypeId24 = "~effect/Schema";

class ASTNodeImpl {
  [TypeId24] = TypeId24;
  annotations;
  checks;
  encoding;
  context;
  constructor(annotations = undefined, checks = undefined, encoding = undefined, context = undefined) {
    this.annotations = annotations;
    this.checks = checks;
    this.encoding = encoding;
    this.context = context;
  }
  toString() {
    return `<${this._tag}>`;
  }
}
var Declaration = class extends ASTNodeImpl {
  _tag = "Declaration";
  typeParameters;
  run;
  encodingChecks;
  encodingRun;
  constructor(typeParameters, run, annotations, checks, encoding, context, encodingChecks, encodingRun) {
    super(annotations, checks, encoding, context);
    this.typeParameters = typeParameters;
    this.run = run;
    this.encodingChecks = encodingChecks;
    this.encodingRun = encodingRun;
  }
  getParser() {
    let run;
    return (input, options) => {
      if (input === missing)
        return missingExit;
      run ??= this.run(this.typeParameters);
      try {
        return run(input, this, options);
      } catch (error) {
        return die3(error);
      }
    };
  }
  _rebuild(recur, checks, encodingChecks, run, encodingRun) {
    const tps = mapOrSame(this.typeParameters, recur);
    return tps === this.typeParameters && checks === this.checks && encodingChecks === this.encodingChecks && run === this.run && encodingRun === this.encodingRun ? this : new Declaration(tps, run, this.annotations, checks, undefined, this.context, encodingChecks, encodingRun);
  }
  recur(recur) {
    return this._rebuild(recur, this.checks, this.encodingChecks, this.run, this.encodingRun);
  }
  flip(recur) {
    return this._rebuild(recur, this.encodingChecks, this.checks, this.encodingRun ?? this.run, this.run);
  }
  getExpected() {
    const expected = this.annotations?.expected;
    if (typeof expected === "string")
      return expected;
    return "<Declaration>";
  }
};
var Null = class extends ASTNodeImpl {
  _tag = "Null";
  getParser() {
    return fromConst(this, null);
  }
  getExpected() {
    return "null";
  }
};
var null_ = /* @__PURE__ */ new Null;
var Undefined = class extends ASTNodeImpl {
  _tag = "Undefined";
  getParser() {
    return fromConst(this, undefined);
  }
  toCodecJson() {
    return replaceEncoding(this, [undefinedToNull]);
  }
  getExpected() {
    return "undefined";
  }
};
var undefinedToNull = /* @__PURE__ */ new Link(null_, /* @__PURE__ */ new Transformation(/* @__PURE__ */ transform(() => {
  return;
}), /* @__PURE__ */ transform(() => null)));
var undefined_2 = /* @__PURE__ */ new Undefined;
var Unknown = class extends ASTNodeImpl {
  _tag = "Unknown";
  getParser() {
    return fromRefinement(this, isUnknown);
  }
  getExpected() {
    return "unknown";
  }
};
var unknown = /* @__PURE__ */ new Unknown;
var Literal = class extends ASTNodeImpl {
  _tag = "Literal";
  literal;
  constructor(literal, annotations, checks, encoding, context) {
    super(annotations, checks, encoding, context);
    if (typeof literal === "number" && !globalThis.Number.isFinite(literal)) {
      throw new Error(`A numeric literal must be finite, got ${format(literal)}`);
    }
    this.literal = literal;
  }
  getParser() {
    return fromConst(this, this.literal);
  }
  matchPart(s, _options) {
    return s === globalThis.String(this.literal) ? this.literal : undefined;
  }
  toCodecJson() {
    return typeof this.literal === "bigint" ? literalToString(this) : this;
  }
  toCodecStringTree() {
    return typeof this.literal === "string" ? this : literalToString(this);
  }
  getExpected() {
    return typeof this.literal === "string" ? JSON.stringify(this.literal) : globalThis.String(this.literal);
  }
};
function literalToString(ast) {
  const literalAsString = globalThis.String(ast.literal);
  return replaceEncoding(ast, [new Link(new Literal(literalAsString), new Transformation(transform(() => ast.literal), transform(() => literalAsString)))]);
}
var String4 = class extends ASTNodeImpl {
  _tag = "String";
  getParser() {
    return fromRefinement(this, isString);
  }
  matchPart(s, options) {
    const checks = this.checks;
    return checks && !options.disableChecks && collectIssues(checks, s, undefined, this, options) ? undefined : s;
  }
  getExpected() {
    return "string";
  }
};
var string2 = /* @__PURE__ */ new String4;
var Number5 = class extends ASTNodeImpl {
  _tag = "Number";
  getParser() {
    return fromRefinement(this, isNumber);
  }
  matchKey(s, options) {
    return this._match(isStringNumberRegExp, s, options);
  }
  matchPart(s, options) {
    return this._match(isStringFiniteRegExp, s, options);
  }
  _match(regexp, s, options) {
    if (!regexp.test(s))
      return;
    const value = globalThis.Number(s);
    if (options.disableChecks || !this.checks)
      return value;
    return collectIssues(this.checks, value, undefined, this, options) ? undefined : value;
  }
  toCodecJson() {
    if (this.checks && (hasCheck(this.checks, "effect/schema/isFinite") || hasCheck(this.checks, "effect/schema/isInt"))) {
      return this;
    }
    return replaceEncoding(this, [numberToJson]);
  }
  toCodecStringTree() {
    if (this.toCodecJson() === this) {
      return replaceEncoding(this, [finiteToString]);
    }
    return replaceEncoding(this, [numberToString]);
  }
  getExpected() {
    return "number";
  }
};
function hasCheck(checks, id) {
  return checks.some((check) => check.annotations?.representation?.id === id || check._tag === "FilterGroup" && hasCheck(check.checks, id));
}
var number2 = /* @__PURE__ */ new Number5;
var Boolean2 = class extends ASTNodeImpl {
  _tag = "Boolean";
  getParser() {
    return fromRefinement(this, isBoolean);
  }
  getExpected() {
    return "boolean";
  }
};
var boolean = /* @__PURE__ */ new Boolean2;
var Arrays = class extends ASTNodeImpl {
  _tag = "Arrays";
  isMutable;
  elements;
  rest;
  encodingChecks;
  constructor(isMutable, elements, rest, annotations, checks, encoding, context, encodingChecks) {
    super(annotations, checks, encoding, context);
    this.isMutable = isMutable;
    this.elements = elements;
    this.rest = rest;
    this.encodingChecks = encodingChecks;
    let hasOptional = false;
    for (let i = 0;i < elements.length; i++) {
      if (isOptional(elements[i])) {
        hasOptional = true;
      } else if (hasOptional) {
        throw new Error("A required element cannot follow an optional element. ts(1257)");
      }
    }
    if (hasOptional && rest.length > 1) {
      throw new Error("A required element cannot follow an optional element. ts(1257)");
    }
    for (let i = 1;i < rest.length; i++) {
      if (isOptional(rest[i])) {
        throw new Error("An optional element cannot follow a rest element. ts(1266)");
      }
    }
  }
  getParser(compile, compileField = compile) {
    const ast = this;
    let elements;
    let rest;
    const elementLen = ast.elements.length;
    const tailLen = Math.max(0, ast.rest.length - 1);
    function getParser(tailThreshold, index) {
      if (index < elementLen) {
        return elements[index];
      } else if (index >= tailThreshold) {
        return rest[index - tailThreshold + 1];
      }
      return rest[0];
    }
    const finish = (state) => {
      const {
        input,
        len,
        options
      } = state;
      if (ast.rest.length === 0 && len > elementLen) {
        for (let i = elementLen;i < len; i++) {
          const unexpected = new UnexpectedKey(ast, input[i], options);
          const issue = new Pointer([i], unexpected);
          if (options.errors === "all") {
            if (state.issues)
              state.issues.push(issue);
            else
              state.issues = [issue];
          } else {
            return fail7(new Composite(ast, [issue], input, options));
          }
        }
      }
      if (state.issues) {
        return fail7(new Composite(ast, state.issues, input, options));
      }
      return succeed9(state.output);
    };
    const parse = (input, options) => {
      const len = input.length;
      const state = {
        ast,
        getParser,
        input,
        len,
        tailThreshold: Math.max(elementLen, len - tailLen),
        output: [],
        issues: undefined,
        options
      };
      const end = ast.rest.length === 0 ? elementLen : Math.max(len, elementLen + tailLen);
      const concurrency = options.concurrency === undefined ? 1 : resolveConcurrency(options.concurrency);
      const eff = concurrency === 1 ? parseArray(state, input, 0, end) : parseArrayConcurrent(state, input, {
        concurrency,
        end
      });
      if (!eff)
        return finish(state);
      if (effectIsExit(eff))
        return flatMapEager2(eff, () => finish(state));
      let first = true;
      return suspend2(() => {
        if (!first)
          return parse(input, options);
        first = false;
        return flatMap4(eff, () => finish(state));
      });
    };
    return (input, options) => {
      if (input === missing)
        return missingExit;
      try {
        if (!Array.isArray(input)) {
          return fail7(new InvalidType(ast, input, options));
        }
        if (!elements) {
          elements = ast.elements.map((ast) => ({
            ast,
            parser: compileField(ast)
          }));
          rest = ast.rest.map((ast) => ({
            ast,
            parser: compileField(ast)
          }));
        }
        return parse(input, options);
      } catch (error) {
        return die3(error);
      }
    };
  }
  _rebuild(recur, checks, encodingChecks) {
    const elements = mapOrSame(this.elements, recur);
    const rest = mapOrSame(this.rest, recur);
    return elements === this.elements && rest === this.rest && checks === this.checks && encodingChecks === this.encodingChecks ? this : new Arrays(this.isMutable, elements, rest, this.annotations, checks, undefined, this.context, encodingChecks);
  }
  recur(recur) {
    return this._rebuild(recur, this.checks, this.encodingChecks);
  }
  flip(recur) {
    return this._rebuild(recur, this.encodingChecks, this.checks);
  }
  getExpected() {
    return "array";
  }
};
function stepArray(s, item, exit, i) {
  if (exit._tag === "Failure") {
    return wrapPropertyKeyIssue(s, s.ast, i, exit);
  }
  const value = exit === sameExit ? item : exit[args];
  if (value !== missing) {
    s.output[i] = value;
  } else {
    const p = s.getParser(s.tailThreshold, i);
    if (isOptional(p.ast))
      return;
    const issue = new Pointer([i], new MissingKey(p.ast.context?.annotations));
    if (s.options.errors === "all") {
      if (s.issues)
        s.issues.push(issue);
      else
        s.issues = [issue];
    } else {
      return fail5(new Composite(s.ast, [issue], s.input, s.options));
    }
  }
}
var parseArrayOptions = {
  onItem(s, item, i) {
    const value = i < s.len ? item : missing;
    return s.getParser(s.tailThreshold, i).parser(value, s.options);
  },
  step: stepArray
};
var parseArray = /* @__PURE__ */ iterateEager()(parseArrayOptions);
var parseArrayConcurrent = /* @__PURE__ */ iterateConcurrent()(parseArrayOptions);
var wrapPropertyKeyIssue = (s, ast, key, exit) => {
  if (exit.cause.reasons.length === 0) {
    return exit;
  }
  const issue = getSchemaIssue(exit.cause);
  if (issue === undefined) {
    return failCause2(map6(exit.cause, (issue) => new Composite(ast, [new Pointer([key], issue)], s.input, s.options)));
  }
  const pointer = new Pointer([key], issue);
  if (s.options.errors === "all") {
    if (s.issues)
      s.issues.push(pointer);
    else
      s.issues = [pointer];
  } else {
    return fail5(new Composite(ast, [pointer], s.input, s.options));
  }
};
var FINITE_PATTERN = "[+-]?(?:\\d+(?:\\.\\d+)?|\\.\\d+)(?:[Ee][+-]?\\d+)?";
function getIndexSignatureKeys(input, parameter, options = defaultParseOptions) {
  let stringKeys;
  let symbolKeys;
  function go(parameter) {
    switch (parameter._tag) {
      case "String":
      case "TemplateLiteral":
        return (stringKeys ??= Object.keys(input)).filter((k) => parameter.matchPart(k, options) !== undefined);
      case "Number":
        return (stringKeys ??= Object.keys(input)).filter((k) => parameter.matchKey(k, options) !== undefined);
      case "Symbol":
        return (symbolKeys ??= Object.getOwnPropertySymbols(input)).filter((k) => Object.prototype.propertyIsEnumerable.call(input, k) && parameter.matchKey(k, options) !== undefined);
      case "Union":
        return [...new Set(parameter.types.flatMap(go))];
      default:
        return [];
    }
  }
  return go(parameterFromPropertyKey(toEncoded(parameter)));
}
var PropertySignature = class {
  name;
  type;
  constructor(name, type) {
    this.name = name;
    this.type = type;
  }
};
function isIndexSignatureParameterSide(ast) {
  switch (ast._tag) {
    case "String":
    case "Number":
    case "Symbol":
    case "TemplateLiteral":
      return true;
    case "Union":
      return ast.types.every(isIndexSignatureParameterSide);
    default:
      return false;
  }
}
function isIndexSignatureParameterEncodedSide(ast) {
  const encoded = getLastEncoding(ast);
  switch (encoded._tag) {
    case "String":
    case "Number":
    case "Symbol":
    case "TemplateLiteral":
      return true;
    case "Union":
      return encoded.types.every(isIndexSignatureParameterEncodedSide);
    default:
      return false;
  }
}
function isIndexSignatureParameter(ast) {
  return isIndexSignatureParameterSide(ast) && isIndexSignatureParameterEncodedSide(ast);
}
var IndexSignature = class {
  parameter;
  type;
  constructor(parameter, type) {
    if (!isIndexSignatureParameter(parameter)) {
      throw new Error(`Invalid index signature parameter ${parameter._tag}`);
    }
    this.parameter = parameter;
    this.type = type;
    if (isOptional(type) || type.encoding !== undefined && isOptional(toEncoded(type))) {
      throw new Error("Cannot use `Schema.optionalKey` or `Schema.optional` with index signatures. Index signatures already allow keys to be absent; use `Schema.UndefinedOr` to allow `undefined` values.");
    }
  }
};
var Objects = class extends ASTNodeImpl {
  _tag = "Objects";
  propertySignatures;
  indexSignatures;
  encodingChecks;
  constructor(propertySignatures, indexSignatures, annotations, checks, encoding, context, encodingChecks) {
    super(annotations, checks, encoding, context);
    this.propertySignatures = propertySignatures;
    this.indexSignatures = indexSignatures;
    this.encodingChecks = encodingChecks;
  }
  getParser(compile, compileField = compile) {
    const ast = this;
    const hasProperties = ast.propertySignatures.length;
    const indexCount = ast.indexSignatures.length;
    if (!hasProperties && !indexCount) {
      return fromRefinement(ast, isNotNullish);
    }
    let properties;
    let indexes;
    const compileMembers = () => {
      if (!properties) {
        properties = ast.propertySignatures.map((ps) => ({
          parser: compileField(ps.type),
          name: ps.name,
          type: ps.type
        }));
        indexes = indexCount ? ast.indexSignatures.map((is) => ({
          is,
          parserKey: compile(parameterFromPropertyKey(is.parameter)),
          parserValue: compileField(is.type)
        })) : undefined;
      }
      return properties;
    };
    const makeFallback = () => {
      const expectedKeys = new Set(ast.propertySignatures.map((ps) => typeof ps.name === "number" ? globalThis.String(ps.name) : ps.name));
      const finishIndex = (s, key, k2, inputValue, exitValue) => {
        if (exitValue._tag === "Failure") {
          return wrapPropertyKeyIssue(s, ast, key, exitValue) ?? void_2;
        }
        const value = exitValue === sameExit ? inputValue : exitValue[args];
        if (k2 !== missing && value !== missing) {
          if (hasProperties && (expectedKeys.has(key) || expectedKeys.has(typeof k2 === "number" ? globalThis.String(k2) : k2)))
            return void_2;
          assignProperty(s.out, k2, value);
        }
        return void_2;
      };
      const parseIndex = (s, key, index, exitKey) => {
        if (!exitKey) {
          const eff = index.parserKey(key, s.options);
          if (!effectIsExit(eff)) {
            return flatMap4(exit2(eff), (exit) => parseIndex(s, key, index, exit));
          }
          exitKey = eff;
        }
        if (exitKey._tag === "Failure") {
          return wrapPropertyKeyIssue(s, ast, key, exitKey) ?? void_2;
        }
        const k2 = exitKey === sameExit ? key : exitKey[args];
        const inputValue = s.input[key];
        const result = index.parserValue(inputValue, s.options);
        return effectIsExit(result) ? finishIndex(s, key, k2, inputValue, result) : flatMap4(exit2(result), (exit) => finishIndex(s, key, k2, inputValue, exit));
      };
      const parseStringIndex = (s, key, index) => {
        const inputValue = s.input[key];
        const result = index.parserValue(inputValue, s.options);
        return effectIsExit(result) ? finishIndex(s, key, key, inputValue, result) : flatMap4(exit2(result), (exit) => finishIndex(s, key, key, inputValue, exit));
      };
      const parseIndexes = indexCount ? iterateConcurrent()({
        onItem: (s, [key, index]) => index.is.parameter === string2 ? parseStringIndex(s, key, index) : parseIndex(s, key, index),
        step: (_s, _item, exit) => exit._tag === "Failure" ? exit : undefined
      }) : undefined;
      return fnUntracedEager2(function* (input, options) {
        if (input === missing) {
          return missing;
        }
        if (!(typeof input === "object" && input !== null && !Array.isArray(input))) {
          return yield* fail7(new InvalidType(ast, input, options));
        }
        compileMembers();
        const record = input;
        const out = {};
        const state = {
          ast,
          input: record,
          out,
          issues: undefined,
          options
        };
        const errorsAllOption = options.errors === "all";
        const onExcessPropertyError = options.onExcessProperty === "error";
        const concurrency = options.concurrency === undefined ? 1 : resolveConcurrency(options.concurrency);
        const indexKeys = indexCount && onExcessPropertyError ? ast.indexSignatures.map((index) => getIndexSignatureKeys(record, index.parameter, options)) : undefined;
        if (onExcessPropertyError) {
          const coveredKeys = indexKeys ? new Set(expectedKeys) : expectedKeys;
          if (indexKeys) {
            for (const keys of indexKeys) {
              for (const key of keys)
                coveredKeys.add(key);
            }
          }
          const inputKeys = Reflect.ownKeys(record);
          for (let i = 0;i < inputKeys.length; i++) {
            const key = inputKeys[i];
            if (!coveredKeys.has(key) && Object.prototype.propertyIsEnumerable.call(record, key)) {
              const unexpected = new UnexpectedKey(ast, record[key], options);
              const issue = new Pointer([key], unexpected);
              if (errorsAllOption) {
                if (state.issues) {
                  state.issues.push(issue);
                } else {
                  state.issues = [issue];
                }
                continue;
              } else {
                return yield* fail7(new Composite(ast, [issue], input, options));
              }
            }
          }
        }
        if (hasProperties) {
          const eff = concurrency === 1 ? parseProperties(state, properties) : parsePropertiesConcurrent(state, properties, {
            concurrency
          });
          if (eff)
            yield* eff;
        }
        if (indexCount && concurrency === 1) {
          for (let i = 0;i < indexCount; i++) {
            const index = indexes[i];
            const parse = index.is.parameter === string2 ? parseStringIndex : parseIndex;
            const keys = indexKeys?.[i] ?? (index.is.parameter === string2 ? Object.keys(record) : getIndexSignatureKeys(record, index.is.parameter, options));
            for (let j = 0;j < keys.length; j++) {
              const eff = parse(state, keys[j], index);
              if (!effectIsExit(eff))
                yield* eff;
              else if (eff._tag === "Failure")
                return yield* eff;
            }
          }
        } else if (parseIndexes) {
          const keyPairs = empty2();
          for (let i = 0;i < indexCount; i++) {
            const index = indexes[i];
            const keys = indexKeys?.[i] ?? (index.is.parameter === string2 ? Object.keys(record) : getIndexSignatureKeys(record, index.is.parameter, options));
            for (let j = 0;j < keys.length; j++) {
              keyPairs.push([keys[j], index]);
            }
          }
          const eff = parseIndexes(state, keyPairs, {
            concurrency
          });
          if (eff)
            yield* eff;
        }
        if (state.issues) {
          return yield* fail7(new Composite(ast, state.issues, input, options));
        }
        return out;
      });
    };
    if (indexCount)
      return makeFallback();
    let fallback;
    const resume = (snapshot, index, pending) => {
      const property = properties[index];
      return flatMap4(exit2(pending), (exit) => {
        const state = {
          ...snapshot,
          out: {
            ...snapshot.out
          }
        };
        const terminal = stepProperty(state, property, exit);
        if (terminal)
          return terminal;
        const done = () => succeed9(state.out);
        const eff = parseProperties(state, properties.slice(index + 1));
        return eff ? flatMapEager2(eff, done) : done();
      });
    };
    return (input, options) => {
      if (input === missing)
        return missingExit;
      if (options.errors === "all" || options.onExcessProperty !== undefined || options.concurrency !== undefined && resolveConcurrency(options.concurrency) !== 1) {
        return (fallback ??= makeFallback())(input, options);
      }
      if (!(typeof input === "object" && input !== null && !Array.isArray(input))) {
        return fail7(new InvalidType(ast, input, options));
      }
      const props = compileMembers();
      const record = input;
      const out = {};
      const state = {
        ast,
        input: record,
        out,
        issues: undefined,
        options
      };
      try {
        for (let index = 0;index < props.length; index++) {
          const property = props[index];
          const name = property.name;
          const hasKey = hasPropertySignature(record, name);
          const value = hasKey ? record[name] : missing;
          const exit = property.parser(value, options);
          if (!effectIsExit(exit)) {
            return resume(state, index, exit);
          }
          if (exit === sameExit) {
            if (hasKey)
              assignProperty(out, name, value);
            continue;
          }
          const terminal = stepProperty(state, property, exit);
          if (terminal)
            return terminal;
        }
      } catch (error) {
        return die3(error);
      }
      return succeed9(out);
    };
  }
  _rebuild(recur, recurParameter, checks, encodingChecks) {
    const props = mapOrSame(this.propertySignatures, (ps) => {
      const t = recur(ps.type);
      return t === ps.type ? ps : new PropertySignature(ps.name, t);
    });
    const indexes = mapOrSame(this.indexSignatures, (is) => {
      const p = recurParameter(is.parameter);
      const t = recur(is.type);
      return p === is.parameter && t === is.type ? is : new IndexSignature(p, t);
    });
    return props === this.propertySignatures && indexes === this.indexSignatures && checks === this.checks && encodingChecks === this.encodingChecks ? this : new Objects(props, indexes, this.annotations, checks, undefined, this.context, encodingChecks);
  }
  flip(recur) {
    return this._rebuild(recur, recur, this.encodingChecks, this.checks);
  }
  recur(recur, recurParameter = recur) {
    return this._rebuild(recur, recurParameter, this.checks, this.encodingChecks);
  }
  getExpected() {
    if (this.propertySignatures.length === 0 && this.indexSignatures.length === 0)
      return "object | array";
    return "object";
  }
};
function stepProperty(s, p, exit) {
  if (exit._tag === "Failure") {
    return wrapPropertyKeyIssue(s, s.ast, p.name, exit);
  }
  if (exit === sameExit)
    return;
  const value = exit[args];
  if (value !== missing) {
    assignProperty(s.out, p.name, value);
    return;
  }
  delete s.out[p.name];
  if (!isOptional(p.type)) {
    const issue = new Pointer([p.name], new MissingKey(p.type.context?.annotations));
    if (s.options.errors === "all") {
      if (s.issues)
        s.issues.push(issue);
      else
        s.issues = [issue];
      return;
    } else {
      return fail5(new Composite(s.ast, [issue], s.input, s.options));
    }
  }
}
var parsePropertiesOptions = {
  onItem(s, p) {
    if (!hasPropertySignature(s.input, p.name)) {
      return p.parser(missing, s.options);
    }
    const value = s.input[p.name];
    assignProperty(s.out, p.name, value);
    return p.parser(value, s.options);
  },
  step: stepProperty
};
var parseProperties = /* @__PURE__ */ iterateEager()(parsePropertiesOptions);
var parsePropertiesConcurrent = /* @__PURE__ */ iterateConcurrent()(parsePropertiesOptions);
function combineChecks(a, b) {
  if (!a)
    return b;
  if (!b)
    return a;
  return [...a, ...b];
}
function struct(fields, checks, annotations) {
  return new Objects(Reflect.ownKeys(fields).map((key) => {
    return new PropertySignature(key, fields[key].ast);
  }), [], annotations, checks);
}
function getAST(self) {
  return self.ast;
}
function union2(members, options, checks) {
  return new Union(members.map(getAST), options, undefined, checks);
}
var toCandidate = /* @__PURE__ */ memoizeIdempotent((ast) => {
  while (true) {
    if (isSuspend(ast))
      return unknown;
    const encoding = ast.encoding;
    if (!encoding) {
      return ast.recur?.(toCandidate, identity) ?? ast;
    }
    if (encoding.some((link) => link.transformation._tag === "Middleware" && link.transformation.decode !== identity))
      return unknown;
    ast = encoding[encoding.length - 1].to;
  }
});
function getCandidateTypes(ast) {
  switch (ast._tag) {
    case "Null":
      return ["null"];
    case "Undefined":
      return ["undefined"];
    case "String":
    case "TemplateLiteral":
      return ["string"];
    case "Number":
      return ["number"];
    case "Boolean":
      return ["boolean"];
    case "Symbol":
    case "UniqueSymbol":
      return ["symbol"];
    case "BigInt":
      return ["bigint"];
    case "Arrays":
      return ["array"];
    case "ObjectKeyword":
      return ["object", "array", "function"];
    case "Objects":
      return ast.propertySignatures.length || ast.indexSignatures.length ? ["object"] : ["string", "number", "boolean", "symbol", "bigint", "object", "array", "function"];
    case "Enum":
      return Array.from(new Set(ast.enums.map(([, v]) => typeof v)));
    case "Literal":
      return [typeof ast.literal];
    case "Union":
      return Array.from(new Set(ast.types.flatMap(getCandidateTypes)));
    default:
      return ["null", "undefined", "string", "number", "boolean", "symbol", "bigint", "object", "array", "function"];
  }
}
function collectSentinels(ast) {
  switch (ast._tag) {
    default:
      return [];
    case "Declaration": {
      const s = ast.annotations?.[SENTINELS_ANNOTATION_KEY];
      return Array.isArray(s) ? s : [];
    }
    case "Objects":
      return ast.propertySignatures.flatMap((ps) => {
        const type = ps.type;
        if (!isOptional(type)) {
          if (isLiteral(type)) {
            return [{
              key: ps.name,
              literal: type.literal
            }];
          }
          if (isUniqueSymbol(type)) {
            return [{
              key: ps.name,
              literal: type.symbol
            }];
          }
        }
        return [];
      });
    case "Arrays":
      return ast.elements.flatMap((e, i) => {
        if (!isOptional(e)) {
          if (isLiteral(e)) {
            return [{
              key: i,
              literal: e.literal
            }];
          }
          if (isUniqueSymbol(e)) {
            return [{
              key: i,
              literal: e.symbol
            }];
          }
        }
        return [];
      });
    case "Union": {
      if (ast.types.length === 0)
        return [];
      const members = ast.types.map((type) => collectSentinels(toCandidate(type)));
      return members[0].filter((s) => members.every((sentinels) => sentinels.some((o) => o.key === s.key && o.literal === s.literal)));
    }
    case "Suspend":
      return collectSentinels(ast.thunk());
  }
}
var candidateIndexCache = /* @__PURE__ */ new WeakMap;
var emptyCandidates = /* @__PURE__ */ Object.freeze([]);
var getRuntimeType = (input) => input === null ? "null" : Array.isArray(input) ? "array" : typeof input;
var hasPropertySignature = (input, key) => key === "__proto__" ? Object.hasOwn(input, key) : (key in input);
function getCandidateIndex(types) {
  let index = candidateIndexCache.get(types);
  if (index)
    return index;
  let bySentinel;
  let sentinelCandidateCount = 0;
  let otherwise;
  let literalCandidates;
  let onlyLiterals = true;
  const literalOf = [];
  for (let i = 0;i < types.length; i++) {
    const a = types[i];
    const encoded = toCandidate(a);
    if (isNever2(encoded))
      continue;
    if (isLiteral(encoded) || isUniqueSymbol(encoded)) {
      literalCandidates ??= new Map;
      const literal = isLiteral(encoded) ? encoded.literal : encoded.symbol;
      literalOf[i] = literal;
      let arr = literalCandidates.get(literal);
      if (!arr)
        literalCandidates.set(literal, arr = []);
      arr.push(i);
    } else {
      onlyLiterals = false;
    }
    const sentinels = collectSentinels(encoded);
    if (sentinels.length) {
      bySentinel ??= new Map;
      sentinelCandidateCount++;
      for (const {
        key,
        literal
      } of sentinels) {
        let entry = bySentinel.get(key);
        if (!entry)
          bySentinel.set(key, entry = [new Map, new Set]);
        entry[1].add(i);
        let indexes = entry[0].get(literal);
        if (!indexes)
          entry[0].set(literal, indexes = new Set);
        indexes.add(i);
      }
    } else {
      otherwise ??= {};
      const candidateTypes = getCandidateTypes(encoded);
      for (const t of candidateTypes)
        (otherwise[t] ??= []).push(i);
    }
  }
  const fallbacks = {};
  const getFallback = (type) => fallbacks[type] ??= Object.freeze(otherwise?.[type] ?? emptyCandidates);
  if (onlyLiterals && literalCandidates) {
    literalCandidates.forEach(Object.freeze);
    index = (input) => literalCandidates.get(input) ?? emptyCandidates;
  } else if (bySentinel?.size === 1 && !otherwise) {
    const [key, [byValue]] = bySentinel.entries().next().value;
    const candidates = new Map;
    for (const [literal, indexes] of byValue) {
      candidates.set(literal, Object.freeze(Array.from(indexes)));
    }
    const all = Object.freeze(types.map((_, i) => i));
    index = (input, isConstructor) => {
      if (isObjectKeyword(input)) {
        const value = hasPropertySignature(input, key) ? input[key] : undefined;
        if (value !== undefined)
          return candidates.get(value) ?? emptyCandidates;
        if (isConstructor)
          return all;
      }
      return emptyCandidates;
    };
  } else if (bySentinel) {
    let commonSentinel;
    for (const entry of bySentinel) {
      if ((!commonSentinel || entry[1][0].size > commonSentinel[1][0].size) && entry[1][1].size === sentinelCandidateCount) {
        commonSentinel = entry;
      }
    }
    index = (input, isConstructor) => {
      const runtimeType = getRuntimeType(input);
      if (!isObjectKeyword(input))
        return getFallback(runtimeType);
      const selected = new Set(otherwise?.[runtimeType]);
      let directKey;
      if (commonSentinel) {
        const [key, [byValue]] = commonSentinel;
        const hasKey = hasPropertySignature(input, key);
        const value = hasKey ? input[key] : undefined;
        if (hasKey && (!isConstructor || value !== undefined)) {
          const match = byValue.get(value);
          if (!match)
            return getFallback(runtimeType);
          for (const i of match)
            selected.add(i);
          directKey = key;
        }
      }
      if (directKey === undefined) {
        for (const [key, [byValue, all]] of bySentinel) {
          const hasKey = hasPropertySignature(input, key);
          const value = hasKey ? input[key] : undefined;
          if (hasKey && (!isConstructor || value !== undefined)) {
            const match = byValue.get(value);
            if (match) {
              for (const i of match)
                selected.add(i);
            }
          } else if (isConstructor) {
            for (const i of all)
              selected.add(i);
          }
        }
      }
      for (const [key, [byValue, all]] of bySentinel) {
        if (key === directKey)
          continue;
        const hasKey = hasPropertySignature(input, key);
        const value = hasKey ? input[key] : undefined;
        if (hasKey && (!isConstructor || value !== undefined)) {
          const match = byValue.get(value);
          for (const i of selected) {
            if (all.has(i) && !match?.has(i))
              selected.delete(i);
          }
        }
      }
      return Array.from(selected).sort((a, b) => a - b);
    };
  } else {
    index = (input) => {
      const fallback = getFallback(getRuntimeType(input));
      return literalCandidates ? fallback.filter((i) => literalOf[i] === undefined || literalOf[i] === input) : fallback;
    };
  }
  candidateIndexCache.set(types, index);
  return index;
}
var Union = class extends ASTNodeImpl {
  _tag = "Union";
  types;
  options;
  encodingChecks;
  constructor(types, options, annotations, checks, encoding, context, encodingChecks) {
    super(annotations, checks, encoding, context);
    this.types = types;
    this.options = options;
    this.encodingChecks = encodingChecks;
  }
  getParser(compile, compileField) {
    const ast = this;
    const isConstructor = compileField !== undefined;
    const parsers = [];
    const parser = (i) => parsers[i] ??= compile(ast.types[i]);
    let index;
    return (input, options) => {
      if (input === missing) {
        return missingExit;
      }
      const candidates = (index ??= getCandidateIndex(ast.types))(input, isConstructor);
      if (candidates.length === 0) {
        return fail7(new AnyOf(ast, [], input, options));
      }
      if (candidates.length === 1) {
        const result = parser(candidates[0])(input, options);
        if (result._tag === "Success")
          return result;
        return effectIsExit(result) ? failSingleUnionCandidate(ast, result.cause, input, options) : catchSingleUnionCandidate(ast, result, input, options);
      }
      return parseUnionCandidates(ast, parser, candidates, input, options);
    };
  }
  _rebuild(recur, checks, encodingChecks) {
    const types = mapOrSame(this.types, recur);
    return types === this.types && checks === this.checks && encodingChecks === this.encodingChecks ? this : new Union(types, this.options, this.annotations, checks, undefined, this.context, encodingChecks);
  }
  recur(recur) {
    return this._rebuild(recur, this.checks, this.encodingChecks);
  }
  flip(recur) {
    return this._rebuild(recur, this.encodingChecks, this.checks);
  }
  matchPart(s, options) {
    for (const type of this.types) {
      const out = type.matchPart(s, options);
      if (out !== undefined)
        return out;
    }
    return;
  }
  getExpected(getExpected) {
    const expected = this.annotations?.expected;
    if (typeof expected === "string")
      return expected;
    if (this.types.length === 0)
      return "never";
    const types = this.types.map((type) => {
      const encoded = toEncoded(type);
      switch (encoded._tag) {
        case "Arrays": {
          const literals = encoded.elements.filter(isLiteral);
          if (literals.length > 0) {
            return `${formatIsMutable(encoded.isMutable)}[ ${literals.map((e) => getExpected(e) + formatIsOptional(e.context?.isOptional)).join(", ")}, ... ]`;
          }
          break;
        }
        case "Objects": {
          const literals = encoded.propertySignatures.filter((ps) => isLiteral(ps.type));
          if (literals.length > 0) {
            return `{ ${literals.map((ps) => `${formatIsMutable(ps.type.context?.isMutable)}${formatPropertyKey(ps.name)}${formatIsOptional(ps.type.context?.isOptional)}: ${getExpected(ps.type)}`).join(", ")}, ... }`;
          }
          break;
        }
      }
      return getExpected(encoded);
    });
    return Array.from(new Set(types)).join(" | ");
  }
};
function failSingleUnionCandidate(ast, cause, input, options) {
  const issue = getSchemaIssue(cause);
  if (!issue)
    return failCause2(cause);
  return fail5(new AnyOf(ast, [issue], input, options));
}
function catchSingleUnionCandidate(ast, result, input, options) {
  return catchCause2(result, (cause) => failSingleUnionCandidate(ast, cause, input, options));
}
function parseUnionCandidates(ast, parser, candidates, input, options) {
  const state = {
    ast,
    parser,
    input,
    out: undefined,
    successes: ast.options?.mode === "oneOf" ? [] : undefined,
    issues: undefined,
    options
  };
  const eff = parseUnion(state, candidates);
  if (!eff) {
    if (state.out)
      return state.out;
    return fail7(new AnyOf(ast, state.issues ?? [], input, options));
  }
  if (effectIsExit(eff))
    return resumeUnion(eff, state);
  let first = true;
  return suspend2(() => {
    if (!first)
      return parseUnionCandidates(ast, parser, candidates, input, options);
    first = false;
    return resumeUnion(eff, state);
  });
}
function resumeUnion(eff, state) {
  return flatMapEager2(eff, (_) => {
    if (state.out === sameExit)
      return succeed7(state.input);
    if (state.out)
      return state.out;
    return fail7(new AnyOf(state.ast, state.issues ?? [], state.input, state.options));
  });
}
var parseUnion = /* @__PURE__ */ iterateEager()({
  onItem(s, i) {
    return s.parser(i)(s.input, s.options);
  },
  step(s, i, exit) {
    if (exit._tag === "Failure") {
      const issue = getSchemaIssue(exit.cause);
      if (issue === undefined) {
        return exit;
      }
      if (s.issues)
        s.issues.push(issue);
      else
        s.issues = [issue];
    } else {
      if (s.out && s.successes) {
        s.successes.push(s.ast.types[i]);
        return fail5(new OneOf(s.ast, s.successes, s.input, s.options));
      }
      s.out = exit;
      if (s.successes) {
        s.successes.push(s.ast.types[i]);
      } else {
        return void_2;
      }
    }
  }
});
var nonFiniteLiterals = /* @__PURE__ */ new Union([/* @__PURE__ */ new Literal("Infinity"), /* @__PURE__ */ new Literal("-Infinity"), /* @__PURE__ */ new Literal("NaN")]);
function formatIsMutable(isMutable) {
  return isMutable ? "" : "readonly ";
}
function formatIsOptional(isOptional) {
  return isOptional ? "?" : "";
}
var Filter2 = class extends Class {
  _tag = "Filter";
  run;
  annotations;
  aborted;
  constructor(run, annotations = undefined, aborted = false) {
    super();
    this.run = run;
    this.annotations = annotations;
    this.aborted = aborted;
  }
  annotate(annotations) {
    return new Filter2(this.run, {
      ...this.annotations,
      ...annotations
    }, this.aborted);
  }
  abort() {
    return new Filter2(this.run, this.annotations, true);
  }
  and(other, annotations) {
    return new FilterGroup([this, other], annotations);
  }
};
var FilterGroup = class extends Class {
  _tag = "FilterGroup";
  checks;
  annotations;
  constructor(checks, annotations = undefined) {
    super();
    this.checks = checks;
    this.annotations = annotations;
  }
  annotate(annotations) {
    return new FilterGroup(this.checks, {
      ...this.annotations,
      ...annotations
    });
  }
  and(other, annotations) {
    return new FilterGroup([this, other], annotations);
  }
};
function makeFilter(filter, annotations, aborted = false) {
  return new Filter2((input, ast, options) => normalizeFilterOutput(ast, filter(input, ast, options), input, options), annotations, aborted);
}
function isFinite2(annotations) {
  return makeFilter((n) => globalThis.Number.isFinite(n), {
    expected: "a finite number",
    representation: {
      id: "effect/schema/isFinite",
      payload: null
    },
    toJsonSchema: () => ({
      type: "number"
    }),
    toCode: () => ({
      runtime: "Schema.isFinite()"
    }),
    arbitraryConstraint: {
      number: "finite"
    },
    ...annotations
  });
}
var finite = /* @__PURE__ */ appendChecks(number2, [/* @__PURE__ */ isFinite2()]);
var numberToJson = /* @__PURE__ */ new Link(/* @__PURE__ */ new Union([finite, nonFiniteLiterals]), /* @__PURE__ */ new Transformation(/* @__PURE__ */ Number4(), /* @__PURE__ */ transform((n) => globalThis.Number.isFinite(n) ? n : globalThis.String(n))));
function isPattern(regExp, annotations) {
  const copy = new globalThis.RegExp(regExp);
  const payload = {
    source: copy.source,
    flags: copy.flags
  };
  return makeFilter((s) => {
    copy.lastIndex = 0;
    return copy.test(s);
  }, {
    expected: `a string matching the RegExp ${payload.source}`,
    representation: {
      id: "effect/schema/isPattern",
      payload
    },
    toJsonSchema: () => [{}, true],
    arbitraryConstraint: {
      patterns: [payload]
    },
    ...annotations
  });
}
var bodyOwners = /* @__PURE__ */ new WeakMap;
function copy(ast, changes) {
  const out = Object.assign(Object.create(Object.getPrototypeOf(ast)), ast, changes);
  if (Reflect.ownKeys(changes).every((key) => key === "context" || key === "encoding")) {
    bodyOwners.set(out, getContextOwner(ast));
  }
  return out;
}
function getContextOwner(ast) {
  const existing = bodyOwners.get(ast);
  if (existing !== undefined)
    return existing;
  if (ast.encoding === undefined)
    return ast;
  const owner = Object.assign(Object.create(Object.getPrototypeOf(ast)), ast, {
    encoding: undefined
  });
  bodyOwners.set(ast, owner);
  return owner;
}
function replaceEncoding(ast, encoding) {
  return ast.encoding === encoding ? ast : copy(ast, {
    encoding
  });
}
function replaceContext(ast, context) {
  if (ast.context === context) {
    return ast;
  }
  const owner = getContextOwner(ast);
  if (owner.context === context && owner.encoding === ast.encoding) {
    return owner;
  }
  return copy(ast, {
    context
  });
}
function getLastEncoding(ast) {
  return ast.encoding ? getLastEncoding(ast.encoding[ast.encoding.length - 1].to) : ast;
}
function annotate(ast, annotations) {
  if (ast.checks) {
    const last = ast.checks[ast.checks.length - 1];
    return replaceChecks(ast, append(ast.checks.slice(0, -1), last.annotate(annotations)));
  }
  return copy(ast, {
    annotations: {
      ...ast.annotations,
      ...annotations
    }
  });
}
function replaceChecks(ast, checks) {
  if (ast._tag === "Suspend" && checks) {
    throw new Error("Cannot add checks to Suspend");
  }
  if (ast.checks === checks) {
    return ast;
  }
  return copy(ast, {
    checks
  });
}
function appendChecks(ast, checks) {
  return replaceChecks(ast, combineChecks(ast.checks, checks));
}
function mapLink(link, f) {
  const to = f(link.to);
  return to === link.to ? link : new Link(to, link.transformation);
}
function updateLastLink(encoding, f) {
  const links = encoding;
  const last = links[links.length - 1];
  const out = mapLink(last, f);
  return out === last ? encoding : append(encoding.slice(0, encoding.length - 1), out);
}
function applyToLastLink(f) {
  return (ast) => ast.encoding ? replaceEncoding(ast, updateLastLink(ast.encoding, f)) : ast;
}
function replaceContextLastLink(ast, context) {
  return applyToLastLink((ast) => replaceContext(ast, context))(ast);
}
function applyToSelfOrLastLinkEncoding(f) {
  function out(ast) {
    return ast.encoding ? replaceEncoding(ast, updateLastLink(ast.encoding, out)) : f(ast);
  }
  return memoize(out);
}
function applyToSelfOrLastLinkEncodingIdempotent(f, options) {
  function out(ast) {
    if (ast.encoding) {
      const last = ast.encoding[ast.encoding.length - 1];
      return options?.stopAt?.(last) ? ast : replaceEncoding(ast, updateLastLink(ast.encoding, out));
    }
    return f(ast);
  }
  return memoizeIdempotent(out);
}
function appendTransformation(from, transformation, to) {
  const link = new Link(from, transformation);
  return replaceEncoding(to, to.encoding ? [...to.encoding, link] : [link]);
}
function mapOrSame(as, f) {
  let out;
  for (let i = 0;i < as.length; i++) {
    const a = as[i];
    const fa = f(a);
    if (out) {
      out[i] = fa;
    } else if (fa !== a) {
      out = new Array(as.length);
      for (let j = 0;j < i; j++) {
        out[j] = as[j];
      }
      out[i] = fa;
    }
  }
  return out ?? as;
}
function annotateKey(ast, annotations) {
  const context = ast.context ? new Context(ast.context.isOptional, ast.context.isMutable, ast.context.constructorDefault, {
    ...ast.context.annotations,
    ...annotations
  }) : new Context(false, false, undefined, annotations);
  return replaceContext(ast, context);
}
var optionalKey = /* @__PURE__ */ memoizeIdempotent((ast) => {
  const context = ast.context ? ast.context.isOptional === false ? new Context(true, ast.context.isMutable, ast.context.constructorDefault, ast.context.annotations) : ast.context : new Context(true, false);
  return optionalKeyLastLink(replaceContext(ast, context));
});
var optionalKeyLastLink = /* @__PURE__ */ applyToLastLink(optionalKey);
var optional = /* @__PURE__ */ memoize((ast) => optionalKey(new Union([ast, undefined_2])));
function withConstructorDefault(ast, defaultValue) {
  const context = ast.context ? new Context(ast.context.isOptional, ast.context.isMutable, defaultValue, ast.context.annotations) : new Context(false, false, defaultValue);
  return replaceContext(ast, context);
}
function decodeTo(from, to, transformation) {
  return appendTransformation(from, transformation, to);
}
function parseParameter(ast) {
  const literals = [];
  const parameters = [];
  function addLiteral(literal) {
    if (!literals.includes(literal)) {
      literals.push(literal);
    }
  }
  function go(ast) {
    switch (ast._tag) {
      case "Literal":
        if (isPropertyKey(ast.literal)) {
          addLiteral(ast.literal);
        }
        return;
      case "UniqueSymbol":
        addLiteral(ast.symbol);
        return;
      case "Enum":
        for (const [, value] of ast.enums) {
          addLiteral(value);
        }
        return;
      case "Never":
        return;
      case "Union":
        for (let i = 0;i < ast.types.length; i++) {
          go(ast.types[i]);
        }
        return;
      default:
        parameters.push(ast);
    }
  }
  go(ast);
  return {
    literals,
    parameters
  };
}
function record(key, value) {
  const {
    literals,
    parameters: indexSignatures
  } = parseParameter(key);
  return new Objects(literals.map((literal) => new PropertySignature(literal, value)), indexSignatures.map((parameter) => new IndexSignature(parameter, value)));
}
function isOptional(ast) {
  return ast.context?.isOptional ?? false;
}
function isStructuralCheck(check) {
  return check.annotations?.[STRUCTURAL_ANNOTATION_KEY] === true || check._tag === "FilterGroup" && check.checks.every(isStructuralCheck);
}
function extractStructuralChecks(checks) {
  function extract(check) {
    if (isStructuralCheck(check))
      return [check];
    return check._tag === "FilterGroup" ? check.checks.flatMap(extract) : [];
  }
  const out = checks.flatMap(extract);
  return isArrayNonEmpty2(out) ? out : undefined;
}
function canPreserveEncodingChecks(ast) {
  let preserve = true;
  function visit(child) {
    preserve = preserve && !child.encoding && !isSuspend(child);
    if (preserve && "recur" in child)
      child.recur(visit);
    return child;
  }
  if ("recur" in ast)
    ast.recur(visit);
  return preserve;
}
var toType = /* @__PURE__ */ memoizeIdempotent((ast) => {
  const owner = getContextOwner(ast);
  if (owner !== ast) {
    const type = toType(owner);
    return type === owner && ast.encoding === undefined ? ast : replaceContext(type, ast.context);
  }
  const type = "recur" in ast ? ast.recur(toType) : ast;
  if ("encodingChecks" in type && type.encodingChecks) {
    const checks = canPreserveEncodingChecks(ast) ? type.encodingChecks : isArrays(type) || isObjects(type) || isDeclaration(type) && type.typeParameters.length > 0 ? extractStructuralChecks(type.encodingChecks) : undefined;
    return copy(type, {
      encodingChecks: undefined,
      checks: combineChecks(type.checks, checks)
    });
  }
  return type;
});
var toEncoded = /* @__PURE__ */ memoizeIdempotent((ast) => {
  return toType(flip2(ast));
});
function flipEncoding(ast, encoding) {
  const links = encoding;
  const len = links.length;
  const last = links[len - 1];
  const ls = [new Link(flip2(replaceEncoding(ast, undefined)), links[0].transformation.flip())];
  for (let i = 1;i < len; i++) {
    ls.unshift(new Link(flip2(links[i - 1].to), links[i].transformation.flip()));
  }
  const to = flip2(last.to);
  if (to.encoding) {
    return replaceEncoding(to, [...to.encoding, ...ls]);
  } else {
    return replaceEncoding(to, ls);
  }
}
var flip2 = /* @__PURE__ */ memoize((ast) => {
  if (ast.encoding) {
    return flipEncoding(ast, ast.encoding);
  }
  const owner = getContextOwner(ast);
  if (owner !== ast) {
    const flipped = flip2(owner);
    return flipped === owner ? ast : replaceContext(flipped, ast.context);
  }
  return "flip" in ast ? ast.flip(flip2) : ("recur" in ast) ? ast.recur(flip2) : ast;
});
function fromConst(ast, value) {
  const succeed = value === 0 ? sameExit : succeed9(value);
  return (input, options) => {
    if (input === missing)
      return missingExit;
    if (input === value)
      return succeed;
    return fail7(new InvalidType(ast, input, options));
  };
}
function fromRefinement(ast, refinement) {
  return (input, options) => {
    if (input === missing)
      return missingExit;
    if (refinement(input))
      return sameExit;
    return fail7(new InvalidType(ast, input, options));
  };
}
var parameterFromPropertyKey = /* @__PURE__ */ applyToSelfOrLastLinkEncodingIdempotent((ast) => {
  switch (ast._tag) {
    default:
      return ast;
    case "Number":
      return ast.toCodecStringTree();
    case "Union":
      return ast.recur(parameterFromPropertyKey);
  }
});
var parameterFromString = /* @__PURE__ */ applyToSelfOrLastLinkEncodingIdempotent((ast) => {
  switch (ast._tag) {
    default:
      return ast;
    case "Symbol":
    case "UniqueSymbol":
      return ast.toCodecStringTree();
    case "Union":
      return ast.recur(parameterFromString);
  }
});
var isStringFiniteRegExp = /* @__PURE__ */ new globalThis.RegExp(`^${FINITE_PATTERN}$`);
var isStringNumberRegExp = /* @__PURE__ */ new globalThis.RegExp(`^(?:${FINITE_PATTERN}|Infinity|-Infinity|NaN)$`);
function isStringFinite(annotations) {
  return isPattern(isStringFiniteRegExp, {
    expected: "a string representing a finite number",
    representation: {
      id: "effect/schema/isStringFinite",
      payload: null
    },
    toJsonSchema: () => ({
      pattern: isStringFiniteRegExp.source
    }),
    ...annotations
  });
}
var finiteString = /* @__PURE__ */ appendChecks(string2, [/* @__PURE__ */ isStringFinite()]);
var finiteToString = /* @__PURE__ */ new Link(finiteString, numberFromString);
var numberToString = /* @__PURE__ */ new Link(/* @__PURE__ */ new Union([finiteString, nonFiniteLiterals]), numberFromString);
var BIGINT_PATTERN = "-?\\d+";
var isStringBigIntRegExp = /* @__PURE__ */ new globalThis.RegExp(`^${BIGINT_PATTERN}$`);
var REGEXP_PATTERN = "Symbol\\(([\\s\\S]*)\\)";
var isStringSymbolRegExp = /* @__PURE__ */ new globalThis.RegExp(`^${REGEXP_PATTERN}$`);
function collectIssues(checks, value, issues, ast, options) {
  for (let i = 0;i < checks.length; i++) {
    const check = checks[i];
    if (check._tag === "FilterGroup") {
      issues = collectIssues(check.checks, value, issues, ast, options);
      if (issues && (options.errors !== "all" || issues[issues.length - 1].filter.aborted)) {
        return issues;
      }
    } else {
      const issue = check.run(value, ast, options);
      if (issue) {
        const filter = new Filter(check, issue, value, options);
        if (issues)
          issues.push(filter);
        else
          issues = [filter];
        if (options.errors !== "all" || check.aborted) {
          return issues;
        }
      }
    }
  }
  return issues;
}
function getConstructorDescriptor(ast) {
  if (!isDeclaration(ast))
    return;
  const getDescriptor = ast.annotations?.[CONSTRUCTOR_ANNOTATION_KEY];
  return isFunction(getDescriptor) ? getDescriptor(ast.typeParameters) : undefined;
}
function isJsonLeaf(u) {
  return u === null || typeof u === "string" || typeof u === "boolean" || typeof u === "number" && globalThis.Number.isFinite(u);
}
function isStringTreeLeaf(u) {
  return u === undefined || typeof u === "string";
}
function isTree(u, isLeaf) {
  const cache = new WeakMap;
  const stack = [];
  outer:
    while (true) {
      if (typeof u !== "object" || u === null) {
        if (!isLeaf(u)) {
          return false;
        }
      } else {
        const value = u;
        const cached = cache.get(value);
        if (cached === false) {
          return false;
        }
        if (cached === undefined) {
          const isArray = Array.isArray(value);
          if (!isArray) {
            const prototype = Object.getPrototypeOf(value);
            if (prototype !== null && prototype !== Object.prototype && Object.getPrototypeOf(prototype) !== null) {
              return false;
            }
          }
          cache.set(value, false);
          stack.push({
            value,
            keys: isArray ? value.length : Object.keys(value),
            index: 0
          });
        }
      }
      while (stack.length > 0) {
        const frame = stack[stack.length - 1];
        const keys = frame.keys;
        if (typeof keys === "number") {
          if (frame.index < keys) {
            u = frame.value[frame.index++];
            continue outer;
          }
        } else if (frame.index < keys.length) {
          u = frame.value[keys[frame.index++]];
          continue outer;
        }
        cache.set(frame.value, true);
        stack.pop();
      }
      return true;
    }
}
function isJson(u) {
  return isTree(u, isJsonLeaf);
}
var Json = /* @__PURE__ */ new Declaration([], () => (input, ast, options) => isJson(input) ? sameExit : fail7(new InvalidType(ast, input, options)), {
  representation: {
    id: "effect/schema/Json",
    payload: null
  },
  expected: "JSON value",
  toCodecJson: () => {
    return;
  },
  toCodecStringTree: () => unknownToStringTree
});
function isStringTree(u) {
  return isTree(u, isStringTreeLeaf);
}
var StringTree = /* @__PURE__ */ new Declaration([], () => (input, ast, options) => isStringTree(input) ? sameExit : fail7(new InvalidType(ast, input, options)), {
  expected: "StringTree",
  toCodecStringTree: () => {
    return;
  }
});
var unknownToStringTree = /* @__PURE__ */ new Link(StringTree, /* @__PURE__ */ passthrough2());

// node_modules/effect/dist/internal/schema/interpreter.js
var flatMapTransformation = (result, current, f) => result === sameExit ? f(current) : flatMapEager2(result, f);
function compileTransformation(transformation) {
  const apply = compileTransformationUnsafe(transformation);
  return (result, current, options) => {
    try {
      return apply(result, current, options);
    } catch (error) {
      return die3(error);
    }
  };
}
function compileTransformationUnsafe(transformation) {
  if (transformation._tag === "Middleware") {
    return (result, current, options) => {
      const transformed = result === sameExit ? transformation.decode(succeed9(toOption(current)), options) : transformation.decode(mapEager2(result, toOption), options);
      return fromOptionalEffect(transformed);
    };
  }
  const getter = transformation.decode;
  switch (getter._tag) {
    case "Passthrough":
      return (result, current) => result === sameExit ? succeed9(current) : result;
    case "Transform": {
      const transform = (value) => value === missing ? missingExit : succeed9(getter.transform(value));
      return (result, current) => flatMapTransformation(result, current, transform);
    }
    case "TransformOptional": {
      const transform = (value) => fromOptionExit(getter.transform(toOption(value)));
      return (result, current) => flatMapTransformation(result, current, transform);
    }
    case "TransformEffect":
      return (result, current, options) => flatMapTransformation(result, current, (value) => value === missing ? missingExit : getter.transform(value, options));
    case "TransformOptionalEffect":
      return (result, current, options) => flatMapTransformation(result, current, (value) => fromOptionalEffect(getter.transform(toOption(value), options)));
  }
}
var fromOptionalEffect = (effect) => flatMapEager2(effect, fromOptionExit);
var wrapEncoding = (ast, input, options, effect) => catchCause2(effect, (cause) => failCauseSync2(() => map6(cause, (issue) => new Encoding(ast, issue, input, options))));
function makeConstructorParser(descriptor, compile) {
  const transform = compileTransformation(descriptor.link.transformation);
  let sourceParser;
  return (input, options) => {
    if (input === missing)
      return missingExit;
    if (descriptor.isConstructed(input))
      return sameExit;
    const result = (sourceParser ??= compile(descriptor.link.to))(input, options);
    return transform(result, input, options);
  };
}
function withDefault(ast, parser) {
  const defaultValue = ast.context.constructorDefault;
  return (input, options) => {
    if (input !== missing && input !== undefined)
      return parser(input, options);
    const result = defaultValue;
    if (effectIsExit(result) && result._tag === "Success") {
      const local = parser(result[args], options);
      return local === sameExit ? result : local;
    }
    return flatMapEager2(wrapEncoding(ast, input, options, result), (value) => {
      const local = parser(value, options);
      return local === sameExit ? succeed9(value) : local;
    });
  };
}
function compileField(ast, compile) {
  const parser = compile(ast);
  return ast.context?.constructorDefault === undefined ? parser : withDefault(ast, parser);
}
function compile2(ast, compile, compileField, base, specialize) {
  if (ast._tag === "Declaration") {
    for (const parameter of ast.typeParameters)
      compile(parameter);
  }
  const descriptor = compileField ? getConstructorDescriptor(ast) : undefined;
  const parser = descriptor ? makeConstructorParser(descriptor, compile) : base ?? ast.getParser(compile, compileField);
  const checks = ast.checks;
  const links = ast.encoding;
  const transformations = links?.map((link) => compileTransformation(link.transformation));
  const encodingChecks = ast.encodingChecks;
  if (!links && !checks && !encodingChecks) {
    return parser;
  }
  let encodingParsers;
  const parseChecks = (input, options) => {
    let result = parser(input, options);
    try {
      if (encodingChecks && !options.disableChecks) {
        if (effectIsExit(result)) {
          if (result._tag === "Success") {
            const output = result === sameExit ? input : result[args];
            if (input !== missing && output !== missing) {
              const issues = collectIssues(encodingChecks, input, undefined, ast, options);
              if (issues) {
                result = fail7(new Composite(ast, issues, input, options));
              }
            }
          }
        } else {
          result = flatMap4(result, (value) => {
            if (input !== missing && value !== missing) {
              const issues = collectIssues(encodingChecks, input, undefined, ast, options);
              if (issues) {
                return fail7(new Composite(ast, issues, input, options));
              }
            }
            return succeed7(value);
          });
        }
      }
      if (checks && !options.disableChecks) {
        if (effectIsExit(result)) {
          if (result._tag === "Success") {
            const value = result === sameExit ? input : result[args];
            if (value === missing)
              return result;
            const issues = collectIssues(checks, value, undefined, ast, options);
            if (issues) {
              result = fail7(new Composite(ast, issues, value, options));
            }
          }
        } else {
          result = flatMap4(result, (value) => {
            if (value !== missing) {
              const issues = collectIssues(checks, value, undefined, ast, options);
              if (issues) {
                return fail7(new Composite(ast, issues, value, options));
              }
            }
            return succeed7(value);
          });
        }
      }
      return result;
    } catch (error) {
      return die3(error);
    }
  };
  const parseLocal = specialize === undefined ? parseChecks : specialize(parseChecks);
  if (!links) {
    return parseLocal;
  }
  return (input, options) => {
    const parsers = encodingParsers ??= links.map((link) => compile(link.to));
    let current = input;
    let result = parsers[parsers.length - 1](input, options);
    for (let i = links.length - 1;i >= 0; i--) {
      result = transformations[i](result, current, options);
      if (i !== 0) {
        const next = parsers[i - 1];
        if (result._tag === "Success") {
          current = result[args];
          result = next(current, options);
        } else {
          result = flatMapEager2(result, (value) => {
            const nextResult = next(value, options);
            return nextResult === sameExit ? succeed9(value) : nextResult;
          });
        }
      }
    }
    if (result._tag === "Success") {
      const value = result[args];
      const local = parseLocal(value, options);
      return local === sameExit ? result : local;
    }
    result = wrapEncoding(ast, input, options, result);
    return flatMapEager2(result, (value) => {
      const local = parseLocal(value, options);
      return local === sameExit ? succeed9(value) : local;
    });
  };
}

// node_modules/effect/dist/internal/schema/compilerRegistry.js
var invalid3 = /* @__PURE__ */ Symbol();
var cache = /* @__PURE__ */ new WeakMap;
var compiler;
var compilerAdaptersEnabled = false;
var decodeChild = (ast) => compilerAdaptersEnabled ? lazyParser(resolve2, ast, "parser") : resolve2(ast).parser;
var makeChild = (ast) => compilerAdaptersEnabled ? lazyParser(resolve2, ast, "makeEffect") : resolve2(ast).makeEffect;
var makeField = (ast) => compileField(ast, makeChild);

class InterpretedEntry {
  ast;
  constructor(ast) {
    this.ast = ast;
  }
  get decodeEffect() {
    return this.cachedDecodeEffect ??= compile2(this.ast, decodeChild);
  }
  get parser() {
    return this.decodeEffect;
  }
  get makeEffect() {
    return this.cachedMakeEffect ??= compile2(this.ast, makeChild, makeField);
  }
}

class CompilerEntry extends InterpretedEntry {
  compiled;
  resolve;
  constructor(ast, compiled, resolve) {
    super(ast);
    this.compiled = compiled;
    this.resolve = resolve;
  }
  save(key, value) {
    Object.defineProperty(this, key, {
      value
    });
    return value;
  }
  operation(key) {
    const compiled = this.compiled;
    return typeof compiled === "function" ? compiled(this.ast, this.resolve, key) : compiled?.[key];
  }
  get is() {
    return this.save("is", this.operation("is"));
  }
  get decode() {
    return this.save("decode", this.operation("decode"));
  }
  get make() {
    return this.save("make", this.operation("make"));
  }
  get compiledMakeEffect() {
    return this.save("compiledMakeEffect", this.operation("makeEffect"));
  }
  get decodeEffect() {
    return this.save("decodeEffect", this.operation("decodeEffect") ?? compile2(this.ast, (ast) => lazyParser(this.resolve, ast, "parser")));
  }
  get parser() {
    const decode = this.decode;
    return decode === undefined ? this.decodeEffect : this.save("parser", withDecode(decode, () => this.decodeEffect));
  }
  get makeEffect() {
    const makeEffect = this.compiledMakeEffect;
    if (makeEffect !== undefined)
      return this.save("makeEffect", makeEffect);
    const child = (ast) => lazyParser(this.resolve, ast, "makeEffect");
    return this.save("makeEffect", compile2(this.ast, child, (ast) => compileField(ast, child)));
  }
}
function withDecode(fastDecode, decodeEffect) {
  let detailed;
  return (input, options) => {
    if (input !== missing) {
      try {
        const value = fastDecode(input, options);
        if (value !== invalid3)
          return value === input ? sameExit : succeed9(value);
      } catch (error) {
        return die3(error);
      }
    }
    return (detailed ??= decodeEffect())(input, options);
  };
}
function lazyParser(resolve, ast, operation) {
  const entry = resolve(ast);
  if (entry.compiled === undefined || Object.hasOwn(entry, operation)) {
    return entry[operation];
  }
  let parser;
  return (input, options) => (parser ??= entry[operation])(input, options);
}
function resolve2(ast) {
  const cached = cache.get(ast);
  if (cached !== undefined)
    return cached;
  const entry = compiler === undefined ? new InterpretedEntry(ast) : new CompilerEntry(ast, compiler(ast, resolve2), resolve2);
  cache.set(ast, entry);
  return entry;
}

// node_modules/effect/dist/SchemaParser.js
function makeEffect(schema) {
  const ast = schema.ast;
  let parser;
  return (input, options) => {
    return (parser ??= runWithCompiler(constructorCompiler, toType(ast)))(input, options?.disableChecks ? options?.parseOptions ? {
      ...options.parseOptions,
      disableChecks: true
    } : {
      disableChecks: true
    } : options?.parseOptions);
  };
}
function makeOption(schema) {
  const parser = makeEffect(schema);
  return (input, options) => {
    const exit = runSyncExit2(parser(input, options));
    if (isSuccess3(exit)) {
      return some2(exit.value);
    }
    getSchemaIssueOrThrow(exit.cause, "Option adapter can only return none for schema issues");
    return none2();
  };
}
function make22(schema) {
  return makeConstructorSync(toType(schema.ast));
}
function is(schema) {
  return _is(schema.ast);
}
function makeIs(ast) {
  if (!compilerAdaptersEnabled) {
    const parser = asExit(run2(ast));
    return (input) => {
      const exit = parser(input, defaultParseOptions);
      if (isSuccess3(exit))
        return true;
      getSchemaIssueOrThrow(exit.cause, "Type guard adapter can only return false for schema issues");
      return false;
    };
  }
  const entry = resolve2(ast);
  const guard = entry.is;
  if (guard !== undefined) {
    return (input) => {
      try {
        return guard(input, defaultParseOptions);
      } catch (error) {
        getSchemaIssueOrThrow(die2(error), "Type guard adapter can only return false for schema issues");
        return false;
      }
    };
  }
  const parser = entry.parser;
  return (input) => {
    const exit = runSyncExit2(parserResult(parser(input, defaultParseOptions), input));
    if (isSuccess3(exit))
      return true;
    getSchemaIssueOrThrow(exit.cause, "Type guard adapter can only return false for schema issues");
    return false;
  };
}
function _is(ast) {
  const typeAST = toType(ast);
  let guard = (input) => {
    guard = makeIs(typeAST);
    return guard(input);
  };
  return (input) => {
    return guard(input);
  };
}
function decodeUnknownEffect(schema, options) {
  const parser = run2(schema.ast);
  return options === undefined ? parser : (input, overrideOptions) => parser(input, mergeParseOptions(options, overrideOptions));
}
function decodeUnknownOption(schema, options) {
  return asOption(decodeUnknownEffect(schema, options));
}
function decodeUnknownResult(schema, options) {
  return asResult(decodeUnknownEffect(schema, options));
}
function encodeUnknownEffect(schema, options) {
  const parser = run2(flip2(schema.ast));
  return options === undefined ? parser : (input, overrideOptions) => parser(input, mergeParseOptions(options, overrideOptions));
}
var mergeParseOptions = (options, overrideOptions) => overrideOptions ? {
  ...options,
  ...overrideOptions
} : options;
var getValue = (value) => {
  if (value === missing) {
    return fail7(new InvalidValue);
  }
  return succeed7(value);
};
function run2(ast) {
  return runWithCompiler(normalCompiler, ast);
}
function parserResult(result, input) {
  if (result === sameExit) {
    return succeed7(input);
  }
  if (!effectIsExit(result)) {
    return flatMapEager2(result, getValue);
  }
  return result[args] === missing ? getValue(missing) : result;
}
function runWithCompiler(compiler, ast) {
  let parser;
  return (input, options) => {
    const result = (parser ??= compiler(ast))(input, options ?? defaultParseOptions);
    if (result === sameExit) {
      return succeed7(input);
    }
    if (!effectIsExit(result)) {
      return flatMapEager2(result, getValue);
    }
    return result[args] === missing ? getValue(missing) : result;
  };
}
function asExit(parser) {
  return (input, options) => runSyncExit2(parser(input, options));
}
function asOption(parser) {
  const parserExit = asExit(parser);
  return (input, options) => {
    const exit = parserExit(input, options);
    if (isSuccess3(exit)) {
      return some2(exit.value);
    }
    getSchemaIssueOrThrow(exit.cause, "Option adapter can only return none for schema issues");
    return none2();
  };
}
function asResult(parser) {
  const parserExit = asExit(parser);
  return (input, options) => {
    const exit = parserExit(input, options);
    if (isSuccess3(exit)) {
      return succeed2(exit.value);
    }
    return fail2(getSchemaIssueOrThrow(exit.cause, "Result adapter can only return schema issues"));
  };
}
function runSync3(effect, message) {
  const exit = runSyncExit2(effect);
  if (isSuccess3(exit)) {
    return exit.value;
  }
  const issue = getSchemaIssueOrThrow(exit.cause, message);
  throw new Error("Schema validation failed", {
    cause: issue
  });
}
var constructorDefectMessage = "Constructor adapter can only throw schema issues";
function throwConstructorDefect(error) {
  throw new Error(constructorDefectMessage, {
    cause: die2(error)
  });
}
function makeConstructorSync(ast) {
  let entry;
  return (input, options) => constructSync(entry ??= resolve2(ast), input, options);
}
function constructSync(entry, input, options) {
  const parseOptions = options?.disableChecks ? options.parseOptions ? {
    ...options.parseOptions,
    disableChecks: true
  } : {
    disableChecks: true
  } : options?.parseOptions ?? defaultParseOptions;
  const make = entry.make;
  if (make !== undefined && input !== missing) {
    let output;
    try {
      output = make(input, parseOptions);
    } catch (error) {
      throwConstructorDefect(error);
    }
    if (output !== invalid3 && output !== missing)
      return output;
  }
  return runSync3(parserResult(entry.makeEffect(input, parseOptions), input), constructorDefectMessage);
}
function makeClassSync(ast, input, options, construct) {
  const entry = resolve2(ast);
  const descriptor = getConstructorDescriptor(ast);
  if (descriptor === undefined || entry.make !== undefined || entry.compiledMakeEffect !== undefined) {
    return constructSync(entry, input, options);
  }
  if (descriptor.isConstructed(input))
    return input;
  const fields = constructSync((entry.resolve ?? resolve2)(descriptor.link.to), input, options);
  try {
    return construct(fields);
  } catch (error) {
    throwConstructorDefect(error);
  }
}
var normalCompiler = (ast) => resolve2(ast).parser;
var constructorCompiler = (ast) => resolve2(ast).makeEffect;

// node_modules/effect/dist/internal/schema/make.js
var TypeId25 = "~effect/Schema/Schema";
var RebuildOptions = /* @__PURE__ */ Symbol();
var SchemaProto = {
  [TypeId25]: TypeId25,
  get make() {
    const value = make22(this);
    Object.defineProperty(this, "make", {
      value,
      enumerable: true
    });
    return value;
  },
  get makeEffect() {
    const value = makeEffect(this);
    Object.defineProperty(this, "makeEffect", {
      value,
      enumerable: true
    });
    return value;
  },
  get makeOption() {
    const value = makeOption(this);
    Object.defineProperty(this, "makeOption", {
      value,
      enumerable: true
    });
    return value;
  },
  pipe() {
    return pipeArguments(this, arguments);
  },
  annotate(annotations) {
    return this.rebuild(annotate(this.ast, annotations));
  },
  annotateKey(annotations) {
    return this.rebuild(annotateKey(this.ast, annotations));
  },
  check(...checks) {
    return this.rebuild(appendChecks(this.ast, checks));
  },
  rebuild(ast) {
    return make23(ast, this[RebuildOptions]);
  }
};
function make23(ast, options) {
  function Schema() {}
  const self = Object.setPrototypeOf(Schema, SchemaProto);
  if (options && (Object.hasOwn(options, "name") || Object.hasOwn(options, "length") || Object.hasOwn(options, "__proto__"))) {
    Object.defineProperties(self, Object.getOwnPropertyDescriptors({
      ...options
    }));
  } else {
    Object.assign(self, options);
  }
  self[RebuildOptions] = options;
  self.ast = ast;
  return self;
}

// node_modules/effect/dist/internal/schema/toCodec.js
function makeCodecDerivation(step, options) {
  const deriveBody = memoize((owner) => {
    const body = replaceContext(owner, undefined);
    const out = step(body, recur);
    return out === body ? null : out;
  });
  const recur = applyToSelfOrLastLinkEncodingIdempotent((ast) => {
    const derived = deriveBody(getContextOwner(ast));
    if (derived === null)
      return ast;
    const out = replaceContext(derived, ast.context);
    return ast.context === undefined ? out : replaceContextLastLink(out, withoutConstructorDefault(ast.context));
  }, options);
  return recur;
}
function withoutConstructorDefault(context) {
  return context.constructorDefault === undefined ? context : new Context(context.isOptional, context.isMutable, undefined, context.annotations);
}
function validateCanonicalObjectPropertyNames(ast) {
  if (ast.propertySignatures.some((ps) => typeof ps.name !== "string")) {
    throw new globalThis.Error("Objects property names must be strings", {
      cause: ast
    });
  }
}
function makeReorder(getPriority) {
  return (types) => {
    const sortedTypes = [...types].sort((a, b) => getPriority(toEncoded(a)) - getPriority(toEncoded(b)));
    const orderChanged = sortedTypes.some((ast, index) => ast !== types[index]);
    if (!orderChanged)
      return types;
    return sortedTypes;
  };
}
function toCodecStringTree(schema) {
  return make23(toCodecStringTreeAST(schema.ast), {
    schema
  });
}
var toStringTreeReorder = /* @__PURE__ */ makeReorder((ast) => {
  switch (ast._tag) {
    case "Null":
    case "Boolean":
    case "Number":
    case "BigInt":
    case "Symbol":
    case "UniqueSymbol":
      return 0;
    default:
      return 1;
  }
});
function toCodecStringTreeASTStep(ast, recur, onMissingAnnotation) {
  switch (ast._tag) {
    case "Declaration": {
      const typeParameters = ast.typeParameters.map((tp) => make23(recur(toEncoded(tp))));
      const getStringTreeLink = ast.annotations?.toCodecStringTree;
      if (isFunction(getStringTreeLink)) {
        const link = getStringTreeLink(typeParameters);
        if (link === undefined)
          return ast;
        return replaceEncoding(ast, [mapLink(link, recur)]);
      }
      const getJsonLink = ast.annotations?.toCodecJson;
      const jsonLink = isFunction(getJsonLink) ? getJsonLink(typeParameters) : undefined;
      const getLink = jsonLink === undefined ? ast.annotations?.toCodec : undefined;
      const link = jsonLink ?? (isFunction(getLink) ? getLink(typeParameters) : undefined);
      return link === undefined ? onMissingAnnotation(ast) : replaceEncoding(ast, [mapLink(link, recur)]);
    }
    case "Null":
      return replaceEncoding(ast, [nullToString]);
    case "Boolean":
      return replaceEncoding(ast, [booleanToString]);
    case "Unknown":
    case "ObjectKeyword":
      return replaceEncoding(ast, [unknownToStringTree]);
    case "Enum":
    case "Number":
    case "Literal":
    case "UniqueSymbol":
    case "Symbol":
    case "BigInt":
      return ast.toCodecStringTree();
    case "Objects": {
      validateCanonicalObjectPropertyNames(ast);
      return ast.recur(recur, parameterFromString);
    }
    case "Union": {
      const sortedTypes = toStringTreeReorder(ast.types);
      if (sortedTypes !== ast.types) {
        return new Union(sortedTypes, ast.options, ast.annotations, ast.checks, ast.encoding, ast.context, ast.encodingChecks).recur(recur);
      }
      return ast.recur(recur);
    }
    case "Arrays":
    case "Suspend":
      return ast.recur(recur);
  }
  return ast;
}
var nullToString = /* @__PURE__ */ new Link(/* @__PURE__ */ new Literal("null"), /* @__PURE__ */ new Transformation(/* @__PURE__ */ transform(() => null), /* @__PURE__ */ transform(() => "null")));
var booleanToString = /* @__PURE__ */ new Link(/* @__PURE__ */ new Union([/* @__PURE__ */ new Literal("true"), /* @__PURE__ */ new Literal("false")]), /* @__PURE__ */ new Transformation(/* @__PURE__ */ transform((s) => s === "true"), /* @__PURE__ */ String3()));
var arrayFromSingleTransformation = /* @__PURE__ */ new Transformation(/* @__PURE__ */ transform((input) => typeof input === "string" ? [input] : input), /* @__PURE__ */ passthrough());
var isCodecArrayFromSingleLink = (link) => link.transformation === arrayFromSingleTransformation;
var toCodecStringTreeAST = /* @__PURE__ */ makeCodecDerivation((ast, recur) => toCodecStringTreeASTStep(ast, recur, (ast) => {
  throw new globalThis.Error("Missing structural codec for StringTree", {
    cause: ast
  });
}), {
  stopAt: isCodecArrayFromSingleLink
});

// node_modules/effect/dist/Struct.js
var lambda = (f) => f;

// node_modules/effect/dist/internal/schemaError.js
var SchemaErrorTypeId = "~effect/Schema/SchemaError";
function isSchemaError(u) {
  return hasProperty(u, SchemaErrorTypeId) && u[SchemaErrorTypeId] === SchemaErrorTypeId;
}

// node_modules/effect/dist/Bytes.js
var concat = (chunks) => {
  let length = 0;
  for (let i = 0;i < chunks.length; i++) {
    length += chunks[i].length;
  }
  const out = new Uint8Array(length);
  let offset = 0;
  for (let i = 0;i < chunks.length; i++) {
    out.set(chunks[i], offset);
    offset += chunks[i].length;
  }
  return out;
};
var Equivalence5 = /* @__PURE__ */ make((self, that) => {
  if (self.length !== that.length) {
    return false;
  }
  for (let i = 0;i < self.length; i++) {
    if (self[i] !== that[i]) {
      return false;
    }
  }
  return true;
});

// node_modules/effect/dist/net/NetAddress.js
var TypeId26 = "~effect/net/NetAddress";
var ipv4Value = (self) => self.value;
var ipv6Words = (self) => self;
var getMacBytes = (self) => self.bytes;

class NetAddressError extends (/* @__PURE__ */ TaggedError2("NetAddressError")) {
}
var isAddress = (u) => hasProperty(u, TypeId26);
var isIpv4Address = (u) => isAddress(u) && u._tag === "Ipv4Address";
var isIpv6Address = (u) => isAddress(u) && u._tag === "Ipv6Address";
var isMacAddress = (u) => isAddress(u) && u._tag === "MacAddress";
var isInetAddressV4 = (u) => isAddress(u) && u._tag === "InetAddressV4";
var isInetAddressV6 = (u) => isAddress(u) && u._tag === "InetAddressV6";
var isInetAddress = (u) => isInetAddressV4(u) || isInetAddressV6(u);
var isUnixPathAddress = (u) => isAddress(u) && u._tag === "UnixPathAddress";
var Ipv4Proto = {
  ...BaseProto,
  _tag: "Ipv4Address",
  [TypeId26]: TypeId26,
  [symbol2](that) {
    return isIpv4Address(that) && ipv4Value(this) === ipv4Value(that);
  },
  [symbol]() {
    return optimize(combine(ipv4HashSeed, ipv4Value(this) | 0));
  },
  toString() {
    return formatIp(this);
  },
  toJSON() {
    return this.toString();
  }
};
var Ipv6Proto = {
  ...BaseProto,
  _tag: "Ipv6Address",
  [TypeId26]: TypeId26,
  [symbol2](that) {
    if (!isIpv6Address(that))
      return false;
    const self = ipv6Words(this);
    const other = ipv6Words(that);
    return self.w0 === other.w0 && self.w1 === other.w1 && self.w2 === other.w2 && self.w3 === other.w3;
  },
  [symbol]() {
    const self = ipv6Words(this);
    let h = combine(ipv6HashSeed, self.w0 | 0);
    h = combine(h, self.w1 | 0);
    h = combine(h, self.w2 | 0);
    return optimize(combine(h, self.w3 | 0));
  },
  toString() {
    return formatIp(this);
  },
  toJSON() {
    return this.toString();
  }
};
var MacProto = {
  ...BaseProto,
  _tag: "MacAddress",
  [TypeId26]: TypeId26,
  [symbol2](that) {
    return isMacAddress(that) && Equivalence5(getMacBytes(this), getMacBytes(that));
  },
  [symbol]() {
    return combine(macHashSeed, array(getMacBytes(this)));
  },
  toString() {
    return formatMacAddress(this);
  },
  toJSON() {
    return this.toString();
  }
};
var ipv4HashSeed = /* @__PURE__ */ string("Ipv4Address");
var ipv6HashSeed = /* @__PURE__ */ string("Ipv6Address");
var macHashSeed = /* @__PURE__ */ string("MacAddress");
var makeIpv4 = (value) => {
  const self = Object.create(Ipv4Proto);
  self.value = value >>> 0;
  return Object.freeze(self);
};
var makeIpv6 = (w0, w1, w2, w3) => {
  const self = Object.create(Ipv6Proto);
  self.w0 = w0 >>> 0;
  self.w1 = w1 >>> 0;
  self.w2 = w2 >>> 0;
  self.w3 = w3 >>> 0;
  return Object.freeze(self);
};
var packOctets = (a, b, c, d) => (a << 24 | b << 16 | c << 8 | d) >>> 0;
var wordToOctets = (word) => [word >>> 24, word >>> 16 & 255, word >>> 8 & 255, word & 255];
var ipv4Loopback = /* @__PURE__ */ makeIpv4(2130706433);
var addressError = (input, message) => fail2(new NetAddressError({
  input,
  message
}));
var allIntegersInRange = (values, max) => {
  for (let i = 0;i < values.length; i++) {
    const n = values[i];
    if (!Number.isInteger(n) || n < 0 || n > max)
      return false;
  }
  return true;
};
var ipv4FromOctets = (octets) => {
  if (!allIntegersInRange(octets, 255)) {
    return addressError(octets, "octets must be integers from 0 through 255");
  }
  return succeed2(makeIpv4(packOctets(octets[0], octets[1], octets[2], octets[3])));
};
var ipv6FromSegments = (segments) => {
  if (!allIntegersInRange(segments, 65535)) {
    return addressError(segments, "segments must be integers from 0 through 65535");
  }
  return succeed2(makeIpv6(segments[0] * 65536 + segments[1], segments[2] * 65536 + segments[3], segments[4] * 65536 + segments[5], segments[6] * 65536 + segments[7]));
};
var ipv4FromString = (input) => {
  const parts = input.split(".");
  if (parts.length !== 4 || parts.some((part) => !/^\d{1,3}$/.test(part))) {
    return addressError(input, "expected exactly four decimal octets");
  }
  if (parts.some((part) => part.length > 1 && part[0] === "0")) {
    return addressError(input, "leading zeroes are not allowed");
  }
  const octets = parts.map(Number);
  if (octets.some((part) => part > 255)) {
    return addressError(input, "octets must be at most 255");
  }
  return ipv4FromOctets([octets[0], octets[1], octets[2], octets[3]]);
};
var parseIpv6Segments = (input) => {
  if (input.includes("[") || input.includes("]") || input.includes("%")) {
    return addressError(input, "brackets and zone identifiers are not valid in a bare IPv6 address");
  }
  const halves = input.split("::");
  if (halves.length > 2)
    return addressError(input, "only one compression marker is allowed");
  const head = halves[0] === "" ? [] : halves[0].split(":");
  const tail = halves.length === 2 && halves[1] !== "" ? halves[1].split(":") : [];
  if (head.some((part) => part === "") || tail.some((part) => part === "")) {
    return addressError(input, "empty segments are only valid in the compression marker");
  }
  const trailing = tail.length > 0 ? tail[tail.length - 1] : head.length > 0 ? head[head.length - 1] : "";
  let embedded;
  if (trailing.includes(".")) {
    if (halves.length === 2 && tail.length === 0) {
      return addressError(input, "embedded IPv4 syntax must be trailing");
    }
    const parsed = map2(ipv4FromString(trailing), (address) => {
      const value = ipv4Value(address);
      return [value >>> 16, value & 65535];
    });
    if (isFailure2(parsed))
      return parsed;
    embedded = parsed.success;
    if (tail.length > 0)
      tail.pop();
    else
      head.pop();
  }
  const explicit = head.length + tail.length + (embedded ? 2 : 0);
  if (halves.length === 1 ? explicit !== 8 : explicit >= 8) {
    return addressError(input, halves.length === 1 ? "expected eight segments" : "compression must replace at least one segment");
  }
  const parse = (part) => /^[0-9a-fA-F]{1,4}$/.test(part) ? Number.parseInt(part, 16) : undefined;
  const parsedHead = head.map(parse);
  const parsedTail = tail.map(parse);
  if (parsedHead.some((part) => part === undefined) || parsedTail.some((part) => part === undefined)) {
    return addressError(input, "segments must contain one through four hexadecimal digits");
  }
  return succeed2([...parsedHead, ...Array(8 - explicit).fill(0), ...parsedTail, ...embedded ?? []]);
};
var ipv6FromString = (input) => {
  return flatMap2(parseIpv6Segments(input), (segments) => ipv6FromSegments(segments));
};
var ipFromString = (input) => {
  const result = input.includes(":") ? ipv6FromString(input) : ipv4FromString(input);
  return result;
};
var ipv4ToOctets = (self) => wordToOctets(ipv4Value(self));
var ipv6ToSegments = (self) => {
  const {
    w0,
    w1,
    w2,
    w3
  } = ipv6Words(self);
  return [w0 >>> 16, w0 & 65535, w1 >>> 16, w1 & 65535, w2 >>> 16, w2 & 65535, w3 >>> 16, w3 & 65535];
};
var formatMacAddress = (self) => Array.from(getMacBytes(self), (byte) => byte.toString(16).padStart(2, "0")).join(":");
var formatIp = (self) => {
  if (isIpv4Address(self))
    return ipv4ToOctets(self).join(".");
  const segments = ipv6ToSegments(self);
  if (isIpv4Mapped(self)) {
    return `::ffff:${segments[6] >> 8}.${segments[6] & 255}.${segments[7] >> 8}.${segments[7] & 255}`;
  }
  let bestStart = -1;
  let bestLength = 0;
  let start = -1;
  for (let index = 0;index <= 8; index++) {
    if (index < 8 && segments[index] === 0) {
      if (start === -1)
        start = index;
    } else if (start !== -1) {
      if (index - start > bestLength) {
        bestStart = start;
        bestLength = index - start;
      }
      start = -1;
    }
  }
  if (bestLength < 2)
    return segments.map((segment) => segment.toString(16)).join(":");
  const head = segments.slice(0, bestStart).map((segment) => segment.toString(16)).join(":");
  const tail = segments.slice(bestStart + bestLength).map((segment) => segment.toString(16)).join(":");
  return `${head}::${tail}`;
};
var isUnspecified = (self) => {
  if (isIpv4Address(self))
    return ipv4Value(self) === 0;
  const {
    w0,
    w1,
    w2,
    w3
  } = ipv6Words(self);
  return w0 === 0 && w1 === 0 && w2 === 0 && w3 === 0;
};
var isIpv4Mapped = (self) => {
  const {
    w0,
    w1,
    w2
  } = ipv6Words(self);
  return w0 === 0 && w1 === 0 && w2 === 65535;
};
var InetV4Proto = {
  ...BaseProto,
  _tag: "InetAddressV4",
  [TypeId26]: TypeId26,
  [symbol2](that) {
    return isInetAddressV4(that) && this.port === that.port && equals(this.address, that.address);
  },
  [symbol]() {
    return combine(hash(this.address), number(this.port));
  },
  toString() {
    return formatInet(this);
  },
  toJSON() {
    return this.toString();
  }
};
var InetV6Proto = {
  ...BaseProto,
  _tag: "InetAddressV6",
  [TypeId26]: TypeId26,
  [symbol2](that) {
    return isInetAddressV6(that) && this.port === that.port && this.scopeId === that.scopeId && equals(this.address, that.address);
  },
  [symbol]() {
    return combine(combine(hash(this.address), number(this.port)), number(this.scopeId));
  },
  toString() {
    return formatInet(this);
  },
  toJSON() {
    return this.toString();
  }
};
var checkPort = (port) => Number.isInteger(port) && port >= 0 && port <= 65535;
var inetAddressV4 = (address, port) => {
  if (!checkPort(port)) {
    return addressError(address, "port must be an integer from 0 through 65535");
  }
  const self = Object.create(InetV4Proto);
  self.address = address;
  self.port = port;
  return succeed2(Object.freeze(self));
};
var inetAddressV6 = (address, port, options) => {
  if (!checkPort(port)) {
    return addressError(address, "port must be an integer from 0 through 65535");
  }
  const scopeId = options?.scopeId ?? 0;
  if (!Number.isInteger(scopeId) || scopeId < 0 || scopeId > 4294967295) {
    return addressError(address, "scopeId must be an unsigned 32-bit integer");
  }
  const self = Object.create(InetV6Proto);
  self.address = address;
  self.port = port;
  self.scopeId = scopeId;
  return succeed2(Object.freeze(self));
};
var inetAddress = (address, port) => isIpv4Address(address) ? inetAddressV4(address, port) : inetAddressV6(address, port);
var inetAddressFromIpString = (address, port) => {
  return flatMap2(ipFromString(address), (address) => inetAddress(address, port));
};
var formatHost = (self) => formatIp(self.address) + (isInetAddressV6(self) && self.scopeId !== 0 ? `%${self.scopeId}` : "");
var formatInet = (self) => {
  const host = formatHost(self);
  return isInetAddressV4(self) ? `${host}:${self.port}` : `[${host}]:${self.port}`;
};
var formatUrlHost = (self) => isIpv4Address(self) ? formatIp(self) : `[${formatIp(self)}]`;
var toUrl = (self, scheme = "http") => {
  if (self._tag === "InetAddressV6" && self.scopeId !== 0) {
    return addressError(self, "scoped IPv6 addresses are not supported by WHATWG URLs");
  }
  return try_({
    try: () => new URL(`${scheme}://${isInetAddress(self) ? formatInet(self) : formatUrlHost(self)}`),
    catch: (cause) => new NetAddressError({
      input: self,
      message: "failed to construct URL",
      cause
    })
  });
};
var UnixPathProto = {
  ...BaseProto,
  _tag: "UnixPathAddress",
  [TypeId26]: TypeId26,
  [symbol2](that) {
    return isUnixPathAddress(that) && this.path === that.path;
  },
  [symbol]() {
    return combine(string("UnixPathAddress"), string(this.path));
  },
  toString() {
    return this.path;
  },
  toJSON() {
    return this.toString();
  }
};
var unixPathAddress = (path) => {
  const self = Object.create(UnixPathProto);
  self.path = path;
  return Object.freeze(self);
};

// node_modules/effect/dist/Schema.js
var TypeId27 = TypeId25;
function declareConstructor() {
  return (typeParameters, run, annotations) => {
    return make24(new Declaration(typeParameters.map(getAST), (typeParameters) => run(typeParameters.map((ast) => make24(ast))), annotations));
  };
}
function declare(is, annotations) {
  return declareConstructor()([], () => (input, ast, options) => is(input) ? succeed7(input) : fail7(new InvalidType(ast, input, options)), annotations);
}
class SchemaError extends (/* @__PURE__ */ TaggedError2("SchemaError")) {
  [SchemaErrorTypeId] = SchemaErrorTypeId;
  constructor(issue) {
    const stackTraceLimit = getStackTraceLimit();
    setStackTraceLimit(0);
    try {
      super({
        issue
      });
    } finally {
      setStackTraceLimit(stackTraceLimit);
    }
  }
  get message() {
    return defaultFormatter(this.issue);
  }
  toJSON() {
    return {
      _tag: this._tag,
      message: this.message
    };
  }
  [NodeInspectSymbol]() {
    return this.toJSON();
  }
  toString() {
    return `SchemaError(${this.message})`;
  }
}
function fromIssueEffect(self) {
  if (effectIsExit(self)) {
    return fromIssueExit(self);
  }
  return catchCause2(self, (cause) => failCauseSync2(() => map6(cause, (issue) => new SchemaError(issue))));
}
function fromIssueExit(exit) {
  return isSuccess3(exit) ? exit : failCause2(map6(exit.cause, (issue) => new SchemaError(issue)));
}
var is2 = is;
function decodeUnknownEffect2(schema, options) {
  const parser = decodeUnknownEffect(schema, options);
  return (input, options) => {
    return fromIssueEffect(parser(input, options));
  };
}
var decodeUnknownOption2 = decodeUnknownOption;
function decodeUnknownResult2(schema, options) {
  const parser = decodeUnknownResult(schema, options);
  return (input, options) => {
    return mapError(parser(input, options), (issue) => new SchemaError(issue));
  };
}
function encodeUnknownEffect2(schema, options) {
  const parser = encodeUnknownEffect(schema, options);
  return (input, options) => {
    return fromIssueEffect(parser(input, options));
  };
}
var encodeEffect = encodeUnknownEffect2;
var make24 = make23;
function isSchema(u) {
  return hasProperty(u, TypeId27) && u[TypeId27] === TypeId27;
}
var optionalKey2 = /* @__PURE__ */ lambda((schema) => make24(optionalKey(schema.ast), {
  schema
}));
var optional2 = /* @__PURE__ */ lambda((self) => {
  const schema = UndefinedOr(self);
  return make24(optional(self.ast), {
    schema
  });
});
function Literal2(literal) {
  const out = make24(new Literal(literal), {
    literal,
    transform(to) {
      return out.pipe(decodeTo2(Literal2(to), {
        decode: transform(() => to),
        encode: transform(() => literal)
      }));
    }
  });
  return out;
}
var Unknown2 = /* @__PURE__ */ make24(unknown);
var Undefined2 = /* @__PURE__ */ make24(undefined_2);
var String5 = /* @__PURE__ */ make24(string2);
var Number6 = /* @__PURE__ */ make24(number2);
var Boolean3 = /* @__PURE__ */ make24(boolean);
function makeStruct(ast, fields) {
  return make24(ast, {
    fields,
    mapFields(f, options) {
      const fields = f(this.fields);
      return makeStruct(struct(fields, options?.unsafePreserveChecks ? this.ast.checks : undefined), fields);
    }
  });
}
function Struct(fields) {
  return makeStruct(struct(fields, undefined), fields);
}
function Record(key, value) {
  return make24(record(key.ast, value.ast), {
    key,
    value
  });
}
var ArraySchema = /* @__PURE__ */ lambda((schema) => make24(new Arrays(false, [], [schema.ast]), {
  value: schema
}));
function makeUnion(ast, members) {
  return make24(ast, {
    members,
    mapMembers(f, options) {
      const members = f(this.members);
      return makeUnion(union2(members, this.ast.options, options?.unsafePreserveChecks ? this.ast.checks : undefined), members);
    }
  });
}
function Union2(members, options) {
  return makeUnion(union2(members, options, undefined), members);
}
function Literals(literals) {
  const members = literals.map(Literal2);
  return make24(union2(members, undefined, undefined), {
    literals,
    members,
    mapMembers(f) {
      return Union2(f(this.members));
    },
    pick(literals) {
      return Literals(literals);
    },
    transform(to) {
      return Union2(members.map((member, index) => member.transform(to[index])));
    }
  });
}
var UndefinedOr = /* @__PURE__ */ lambda((self) => Union2([self, Undefined2]));
function decodeTo2(to, transformation) {
  return (from) => {
    return make24(decodeTo(from.ast, to.ast, transformation ? makeTransformation(transformation) : passthrough2()), {
      from,
      to
    });
  };
}
var TrueLiterals = /* @__PURE__ */ Literals(["true", "yes", "on", "1", "y"]);
var FalseLiterals = /* @__PURE__ */ Literals(["false", "no", "off", "0", "n"]);
var BooleanLiterals = /* @__PURE__ */ Literals([...TrueLiterals.literals, ...FalseLiterals.literals]).pipe(/* @__PURE__ */ decodeTo2(Boolean3, /* @__PURE__ */ transform2({
  decode: (value) => value === "true" || value === "yes" || value === "on" || value === "1" || value === "y",
  encode: (value) => value ? "true" : "false"
})));
function withConstructorDefault2(defaultValue) {
  return (schema) => make24(withConstructorDefault(schema.ast, defaultValue), {
    schema
  });
}
function tag(literal) {
  return Literal2(literal).pipe(withConstructorDefault2(succeed7(literal)));
}
function TaggedStruct(value, fields) {
  return Struct({
    _tag: tag(value),
    ...fields
  });
}
function instanceOf(constructor, annotations) {
  return declare((u) => u instanceof constructor, annotations);
}
function link() {
  return (encodeTo, transformation) => {
    return new Link(encodeTo.ast, makeTransformation(transformation));
  };
}
var makeFilter2 = makeFilter;
function isBase64(annotations) {
  const pattern = "^([0-9a-zA-Z+/]{4})*(([0-9a-zA-Z+/]{2}==)|([0-9a-zA-Z+/]{3}=))?$";
  const regExp = /^[0-9a-zA-Z+/]*={0,2}$/;
  return makeFilter2((s) => s.length % 4 === 0 && regExp.test(s), {
    expected: "a base64 encoded string",
    representation: {
      id: "effect/schema/isBase64",
      payload: null
    },
    toJsonSchema: () => ({
      pattern
    }),
    arbitraryConstraint: {
      patterns: [{
        source: pattern,
        flags: ""
      }]
    },
    toCode: () => ({
      runtime: "Schema.isBase64()"
    }),
    ...annotations
  });
}
var Finite = /* @__PURE__ */ make24(finite);
function isInt(annotations) {
  return makeFilter2((n) => globalThis.Number.isSafeInteger(n), {
    expected: "an integer",
    representation: {
      id: "effect/schema/isInt",
      payload: null
    },
    toJsonSchema: () => ({
      type: "integer",
      minimum: globalThis.Number.MIN_SAFE_INTEGER,
      maximum: globalThis.Number.MAX_SAFE_INTEGER
    }),
    toCode: () => ({
      runtime: "Schema.isInt()"
    }),
    arbitraryConstraint: {
      number: "integer"
    },
    ...annotations
  });
}
var Int = /* @__PURE__ */ Number6.check(/* @__PURE__ */ isInt());
var getErrorOptionsKey = (options) => (options?.includeStack === true ? 1 : 0) | (options?.excludeCause === true ? 2 : 0);
var getErrorOptions = (key) => {
  switch (key) {
    case 0:
      return;
    case 1:
      return {
        includeStack: true
      };
    case 2:
      return {
        excludeCause: true
      };
    case 3:
      return {
        includeStack: true,
        excludeCause: true
      };
  }
};
var defectSchemaCache = [];
function Defect(options) {
  const key = getErrorOptionsKey(options);
  const cached = defectSchemaCache[key];
  if (cached !== undefined) {
    return cached;
  }
  const schema = Json2.pipe(decodeTo2(Unknown2, defectFromJson(getErrorOptions(key))));
  defectSchemaCache[key] = schema;
  return schema;
}
var RegExp2 = /* @__PURE__ */ instanceOf(globalThis.RegExp, {
  representation: {
    id: "effect/schema/RegExp",
    payload: null
  },
  toCode: () => ({
    runtime: `Schema.RegExp`,
    Type: `globalThis.RegExp`
  }),
  expected: "RegExp",
  toCodecJson: () => link()(Struct({
    source: String5,
    flags: String5
  }), transformEffect2({
    decode: (e, options) => try_3({
      try: () => new globalThis.RegExp(e.source, e.flags),
      catch: () => new InvalidValue({
        expected: "valid RegExp source and flags"
      }, e, options)
    }),
    encode: (regExp) => succeed7({
      source: regExp.source,
      flags: regExp.flags
    })
  }))
});
var URLString = /* @__PURE__ */ String5.annotate({
  expected: "a string that will be decoded as a URL"
});
var URL2 = /* @__PURE__ */ instanceOf(globalThis.URL, {
  representation: {
    id: "effect/schema/URL",
    payload: null
  },
  toCode: () => ({
    runtime: `Schema.URL`,
    Type: `globalThis.URL`
  }),
  expected: "URL",
  toCodecJson: () => link()(URLString, urlFromString)
});
var JsonString = /* @__PURE__ */ String5.annotate({
  expected: "a string that will be decoded as JSON"
});
function fromJsonString2(schema, options) {
  return JsonString.pipe(decodeTo2(schema, fromJsonString(options)));
}
var File = /* @__PURE__ */ instanceOf(globalThis.File, {
  representation: {
    id: "effect/schema/File",
    payload: null
  },
  toCode: () => ({
    runtime: `Schema.File`,
    Type: `globalThis.File`
  }),
  expected: "File",
  toCodecJson: () => link()(Struct({
    data: String5.check(isBase64()),
    type: String5,
    name: String5,
    lastModified: Int
  }), transformEffect2({
    decode: (e, options) => match2(decode(e.data), {
      onFailure: () => fail7(new InvalidValue({
        expected: "a valid Base64 string"
      }, e.data, options)),
      onSuccess: (bytes) => {
        const buffer = new globalThis.Uint8Array(bytes);
        return succeed7(new globalThis.File([buffer], e.name, {
          type: e.type,
          lastModified: e.lastModified
        }));
      }
    }),
    encode: (file, options) => tryPromise2({
      try: async () => {
        const bytes = new globalThis.Uint8Array(await file.arrayBuffer());
        return {
          data: encode2(bytes),
          type: file.type,
          name: file.name,
          lastModified: file.lastModified
        };
      },
      catch: () => new InvalidValue({
        expected: "a readable File"
      }, file, options)
    })
  }))
});
var URLSearchParams2 = /* @__PURE__ */ instanceOf(globalThis.URLSearchParams, {
  representation: {
    id: "effect/schema/URLSearchParams",
    payload: null
  },
  toCode: () => ({
    runtime: `Schema.URLSearchParams`,
    Type: `globalThis.URLSearchParams`
  }),
  expected: "URLSearchParams",
  toCodecJson: () => link()(String5.annotate({
    expected: "a query string that will be decoded as URLSearchParams"
  }), transform2({
    decode: (e) => new globalThis.URLSearchParams(e),
    encode: (params) => params.toString()
  }))
});
var Base64String = /* @__PURE__ */ String5.annotate({
  expected: "a base64 encoded string that will be decoded as Uint8Array",
  format: "byte",
  contentEncoding: "base64"
});
var Uint8Array2 = /* @__PURE__ */ instanceOf(globalThis.Uint8Array, {
  representation: {
    id: "effect/schema/Uint8Array",
    payload: null
  },
  toCode: () => ({
    runtime: `Schema.Uint8Array`,
    Type: `globalThis.Uint8Array`
  }),
  expected: "Uint8Array",
  toCodecJson: () => link()(Base64String, uint8ArrayFromBase64String)
});
var arbitraryMinimumDateTimestamp = -8640000000000000;
var arbitraryMaximumDateTimestamp = 8640000000000000;
var arbitraryMinimumZonedDateTimeTimestamp = arbitraryMinimumDateTimestamp + 14 * 60 * 60 * 1000;
var arbitraryMaximumZonedDateTimeTimestamp = arbitraryMaximumDateTimestamp - 14 * 60 * 60 * 1000;
var arbitraryMinimumTimeZoneOffset = -12 * 60 * 60 * 1000;
var arbitraryMaximumTimeZoneOffset = 14 * 60 * 60 * 1000;
var immerable = /* @__PURE__ */ globalThis.Symbol.for("immer-draftable");
var payloadToken = {};
function makeClass(Inherited, identifier, struct2, annotations, proto) {
  const getClassSchema = getClassSchemaFactory(struct2, identifier, annotations);
  const ClassTypeId = getClassTypeId(identifier);
  const out = class extends Inherited {
    constructor(...[input, options]) {
      const internalOptions = options;
      const payload = internalOptions?.["~payload"];
      const value = payload?.token === payloadToken ? payload.value : struct2.make(input ?? {}, options);
      super(value, {
        ...options,
        disableChecks: true,
        "~payload": {
          token: payloadToken,
          value
        }
      });
    }
    static [TypeId27] = TypeId27;
    get [ClassTypeId]() {
      return ClassTypeId;
    }
    static [immerable] = true;
    static identifier = identifier;
    static fields = struct2.fields;
    static get ast() {
      return getClassSchema(this).ast;
    }
    static pipe() {
      return pipeArguments(this, arguments);
    }
    static rebuild(ast) {
      return getClassSchema(this).rebuild(ast);
    }
    static make(input, options) {
      return makeClassSync(toType(getClassSchema(this).ast), input ?? {}, options, (fields) => makeClassTrusted(this, fields));
    }
    static makeOption(input, options) {
      return makeOption(getClassSchema(this))(input ?? {}, options);
    }
    static makeEffect(input, options) {
      return getClassSchema(this).makeEffect(input ?? {}, options);
    }
    static annotate(annotations) {
      return this.rebuild(annotate(this.ast, annotations));
    }
    static annotateKey(annotations) {
      return this.rebuild(annotateKey(this.ast, annotations));
    }
    static check(...checks) {
      return this.rebuild(appendChecks(this.ast, checks));
    }
    static extend(identifier2) {
      return (schema, annotations) => {
        const extension = isStruct(schema) ? schema : Struct(schema);
        const fields = {
          ...struct2.fields,
          ...extension.fields
        };
        const ast = struct(fields, struct2.ast.checks, {
          identifier: identifier2
        });
        return makeClass(this, identifier2, makeStruct(appendChecks(ast, extension.ast.checks), fields), annotations, proto);
      };
    }
    static mapFields(f, options) {
      return struct2.mapFields(f, options);
    }
  };
  if (proto !== undefined) {
    Object.assign(out.prototype, proto(identifier));
  }
  return out;
}
function makeClassTrusted(self, fields) {
  return new self(fields, {
    "~payload": {
      token: payloadToken,
      value: fields
    }
  });
}
function getClassTransformation(self) {
  return new Transformation(transform((input) => makeClassTrusted(self, input)), passthrough());
}
function getClassTypeId(identifier) {
  return `~effect/Schema/Class/${identifier}`;
}
function getClassSchemaFactory(from, identifier, annotations) {
  let memo;
  return (self) => {
    if (memo !== undefined) {
      return memo;
    }
    const ClassTypeId = getClassTypeId(identifier);
    const isClassValue = (input) => input instanceof self || hasProperty(input, ClassTypeId);
    const transformation = getClassTransformation(self);
    const to = make24(new Declaration([from.ast], () => (input, ast, options) => {
      return isClassValue(input) ? succeed7(input) : fail7(new InvalidType(ast, input, options));
    }, {
      identifier,
      [CONSTRUCTOR_ANNOTATION_KEY]: ([from]) => ({
        isConstructed: isClassValue,
        link: new Link(from, transformation)
      }),
      toCodec: ([from]) => new Link(from.ast, transformation),
      toEquivalence: ([from]) => from,
      toFormatter: ([from]) => (t) => `${self.identifier}(${from(t)})`,
      [SENTINELS_ANNOTATION_KEY]: collectSentinels(from.ast),
      ...annotations
    }));
    return memo = decodeTo2(to, transformation)(from);
  };
}
function isStruct(schema) {
  return isSchema(schema);
}
var Error4 = (identifier) => (schema, annotations) => {
  const struct = isStruct(schema) ? schema : Struct(schema);
  const self = makeClass(Error2, identifier, struct, annotations, (identifier) => ({
    name: identifier
  }));
  return self;
};
var TaggedError3 = (identifier) => {
  return (tagValue, schema, annotations) => {
    const struct = isStruct(schema) ? schema.mapFields((fields) => ({
      _tag: tag(tagValue),
      ...fields
    }), {
      unsafePreserveChecks: true
    }) : TaggedStruct(tagValue, schema);
    return Error4(identifier ?? tagValue)(struct, annotations);
  };
};
var toCodecStringTree2 = toCodecStringTree;
var Json2 = /* @__PURE__ */ make24(/* @__PURE__ */ annotate(Json, {
  toCode: () => ({
    runtime: "Schema.Json",
    Type: "Schema.Json"
  })
}));

// node_modules/effect/dist/Config.js
var TypeId28 = "~effect/Config";
class ConfigError {
  _tag = "ConfigError";
  name = "ConfigError";
  cause;
  constructor(cause) {
    this.cause = cause;
  }
  get message() {
    return this.cause.toString();
  }
  toString() {
    return `ConfigError(${this.message})`;
  }
}
var Proto8 = {
  .../* @__PURE__ */ Prototype2({
    label: "Config",
    evaluate(fiber) {
      return this.parse(fiber.getRef(ConfigProvider));
    }
  }),
  [TypeId28]: TypeId28,
  toJSON() {
    return {
      _id: "Config"
    };
  }
};
function make25(evaluator) {
  const self = Object.create(Proto8);
  self.evaluator = evaluator;
  self.parse = (provider) => flatMapEager2(evaluator(provider, []), fromResult2);
  return self;
}
var evaluateAt = (self, provider, pathPrefix) => self.evaluator(provider, pathPrefix);
var map10 = /* @__PURE__ */ dual(2, (self, f) => {
  return make25((provider, pathPrefix) => map7(evaluateAt(self, provider, pathPrefix), map2(f)));
});
var withDefault3 = /* @__PURE__ */ dual(2, (self, defaultValue) => {
  return make25((provider, pathPrefix) => mapEager2(evaluateAt(self, provider, pathPrefix), (resolution) => isFailure2(resolution) ? succeed2(defaultValue) : resolution));
});
var option2 = (self) => self.pipe(map10(some2), withDefault3(none2()));
var isSourceError = (u) => isTagged(u, "SourceError");
var cursorToString = () => "<configuration>";
var mapReasons = (f) => (self) => catchCause2(self, (cause) => failCause3(fromReasons(cause.reasons.map(f))));
var loadCursor = (provider, path) => provider.load(path).pipe(mapReasons((reason) => isFailReason2(reason) ? makeDieReason(reason.error).annotate(reasonAnnotations2(reason)) : reason), mapEager2((node) => ({
  provider,
  path,
  node,
  toString: cursorToString
})));
var loadChildCursor = (cursor, segment) => loadCursor(cursor.provider, [...cursor.path, segment]);
var decodeFromCursor = (ast, decode) => decodeTo(unknown, ast, new Transformation(transformEffect((input) => decode(input)), passthrough()));
var isScalarInput = (ast) => {
  switch (ast._tag) {
    case "Union":
      return ast.types.every(isScalarInput);
    case "Objects":
    case "Arrays":
    case "Suspend":
      return false;
    default:
      return true;
  }
};
var hasProviderInput = (ast, node) => {
  switch (ast._tag) {
    case "Objects":
      return node?._tag === "Record";
    case "Arrays":
      return node?._tag === "Array";
    case "Union":
      return ast.types.some((ast) => hasProviderInput(ast, node));
    case "Suspend":
      return hasProviderInput(ast.thunk(), node);
    default:
      return node?.value !== undefined;
  }
};
var toConfigCursorAST = /* @__PURE__ */ memoize((root) => {
  const seen = new WeakSet;
  const recur = applyToSelfOrLastLinkEncoding((ast) => {
    seen.add(ast);
    switch (ast._tag) {
      case "Objects": {
        const materialize = fnUntraced2(function* (cursor) {
          if (cursor.node?._tag !== "Record") {
            return;
          }
          const node = cursor.node;
          const keys = new Set;
          for (const property of ast.propertySignatures) {
            if (typeof property.name === "string")
              keys.add(property.name);
          }
          if (ast.indexSignatures.length > 0) {
            const input = {};
            for (const key of node.keys) {
              assignProperty(input, key, undefined);
            }
            for (const is of ast.indexSignatures) {
              for (const key of getIndexSignatureKeys(input, is.parameter)) {
                if (typeof key === "string")
                  keys.add(key);
              }
            }
          }
          const out = {};
          for (const key of keys) {
            const child = yield* loadChildCursor(cursor, key);
            if (child.node !== undefined)
              assignProperty(out, key, child);
          }
          return out;
        });
        return decodeFromCursor(ast.recur(recur, (ast) => ast), materialize);
      }
      case "Arrays": {
        const materialize = fnUntraced2(function* (cursor) {
          if (cursor.node?._tag !== "Array") {
            return;
          }
          const out = [];
          for (let i = 0;i < cursor.node.length; i++) {
            out.push(yield* loadChildCursor(cursor, i));
          }
          return out;
        });
        return decodeFromCursor(ast.recur(recur), materialize);
      }
      case "Union":
        for (const member of ast.types) {
          recur(member);
        }
        if (!isScalarInput(ast))
          return ast.recur(recur);
        break;
      case "Suspend": {
        const target = ast.thunk();
        if (!seen.has(target))
          recur(target);
        return ast.recur(recur);
      }
      case "Declaration":
      case "Any":
        throw new globalThis.Error("Config.schema does not support opaque StringTree encodings", {
          cause: ast
        });
    }
    return decodeFromCursor(ast, (cursor) => succeed7(cursor.node?.value));
  });
  return recur(root);
});
function schema(codec, path) {
  const codecStringTree = toCodecStringTree2(codec);
  const encodedAst = toEncoded(codecStringTree.ast);
  const decodeCursor = decodeUnknownEffect(make24(toConfigCursorAST(codecStringTree.ast)));
  const localPath = typeof path === "string" ? [path] : path ?? [];
  return make25((provider, pathPrefix) => {
    const fullPath = [...pathPrefix, ...localPath];
    return loadCursor(provider, fullPath).pipe(flatMapEager2((cursor) => decodeCursor(cursor).pipe(mapEager2(succeed2), catchEager2((issue) => {
      const error = new ConfigError(new SchemaError(fullPath.length > 0 ? new Pointer(fullPath, issue) : issue));
      return hasProviderInput(encodedAst, cursor.node) ? fail7(error) : succeed7(fail2(error));
    }))), mapReasons((reason) => isDieReason2(reason) && isSourceError(reason.defect) ? makeFailReason(new ConfigError(reason.defect)).annotate(reasonAnnotations2(reason)) : reason));
  });
}
function String6(name) {
  return schema(String5, name);
}

// node_modules/effect/dist/http/Etag.js
var toString2 = (self) => {
  switch (self._tag) {
    case "Weak":
      return `W/"${self.value}"`;
    case "Strong":
      return `"${self.value}"`;
  }
};

class Generator extends (/* @__PURE__ */ Service()("effect/http/Etag/Generator")) {
}
var fromFileInfo = (info) => {
  const mtime = match(info.mtime, {
    onNone: () => "0",
    onSome: (mtime) => mtime.getTime().toString(16)
  });
  return `${info.size.toString(16)}-${mtime}`;
};
var fromFileWeb = (file) => {
  return `${file.size.toString(16)}-${file.lastModified.toString(16)}`;
};
var layer = /* @__PURE__ */ succeed6(Generator)({
  fromFileInfo(info) {
    return sync2(() => ({
      _tag: "Strong",
      value: fromFileInfo(info)
    }));
  },
  fromFileWeb(file) {
    return sync2(() => ({
      _tag: "Strong",
      value: fromFileWeb(file)
    }));
  }
});
var layerWeak = /* @__PURE__ */ succeed6(Generator)({
  fromFileInfo(info) {
    return sync2(() => ({
      _tag: "Weak",
      value: fromFileInfo(info)
    }));
  },
  fromFileWeb(file) {
    return sync2(() => ({
      _tag: "Weak",
      value: fromFileWeb(file)
    }));
  }
});

// node_modules/effect/dist/Ref.js
var TypeId29 = "~effect/Ref";
var RefProto = {
  [TypeId29]: {
    _A: identity
  },
  ...PipeInspectableProto,
  toJSON() {
    return {
      _id: "Ref",
      ref: this.ref
    };
  }
};
var makeUnsafe7 = (value) => {
  const self = Object.create(RefProto);
  self.ref = make9(value);
  return self;
};
var make26 = (value) => sync2(() => makeUnsafe7(value));
var get4 = (self) => sync2(() => self.ref.current);
var set4 = /* @__PURE__ */ dual(2, (self, value) => sync2(() => set(self.ref, value)));
var updateAndGet = /* @__PURE__ */ dual(2, (self, f) => sync2(() => self.ref.current = f(self.ref.current)));

// node_modules/effect/dist/http/HttpBody.js
var TypeId30 = "~effect/http/HttpBody";
class Proto9 {
  [TypeId30];
  constructor() {
    this[TypeId30] = TypeId30;
  }
  [NodeInspectSymbol]() {
    return this.toJSON();
  }
  toString() {
    return format(this, {
      ignoreToString: true
    });
  }
}

class Empty2 extends Proto9 {
  _tag = "Empty";
  toJSON() {
    return {
      _id: "effect/HttpBody",
      _tag: "Empty"
    };
  }
}
var empty8 = /* @__PURE__ */ new Empty2;

class Raw extends Proto9 {
  _tag = "Raw";
  body;
  contentType;
  contentLength;
  constructor(body, contentType, contentLength) {
    super();
    this.body = body;
    this.contentType = contentType;
    this.contentLength = contentLength;
  }
  toJSON() {
    return {
      _id: "effect/HttpBody",
      _tag: "Raw",
      body: this.body,
      contentType: this.contentType,
      contentLength: this.contentLength
    };
  }
}
var raw = (body, options) => new Raw(body, options?.contentType, options?.contentLength);

class Uint8Array3 extends Proto9 {
  _tag = "Uint8Array";
  contentType;
  contentLength;
  text;
  _body;
  constructor(body, contentType, contentLength, text) {
    super();
    this._body = body;
    this.text = text;
    this.contentType = contentType;
    this.contentLength = contentLength;
  }
  get body() {
    return this._body ??= encodeText(this.text);
  }
  toJSON() {
    const toString = this.contentType.startsWith("text/") || this.contentType.endsWith("json");
    return {
      _id: "effect/HttpBody",
      _tag: "Uint8Array",
      body: toString ? new TextDecoder().decode(this.body) : `Uint8Array(${this.body.length})`,
      contentType: this.contentType,
      contentLength: this.contentLength
    };
  }
}
var uint8Array = (body, contentType) => new Uint8Array3(body, contentType ?? "application/octet-stream", body.length);
var encoder3 = /* @__PURE__ */ new TextEncoder;
var buffer2 = globalThis.Buffer;
var encodeText = buffer2 !== undefined ? (body) => buffer2.from(body, "utf8") : (body) => encoder3.encode(body);
var text = (body, contentType) => {
  if (typeof body !== "string") {
    body = body === undefined ? "" : String(body);
  }
  if (buffer2 !== undefined) {
    return new Uint8Array3(undefined, contentType ?? "text/plain", buffer2.byteLength(body, "utf8"), body);
  }
  const bytes = encoder3.encode(body);
  return new Uint8Array3(bytes, contentType ?? "text/plain", bytes.length, body);
};
var jsonUnsafe = (body, contentType) => text(JSON.stringify(body), contentType ?? "application/json");
var urlParams = (urlParams, contentType) => text(toString(fromInput3(urlParams)), contentType ?? "application/x-www-form-urlencoded");
class Stream2 extends Proto9 {
  _tag = "Stream";
  stream;
  contentType;
  contentLength;
  constructor(stream, contentType, contentLength) {
    super();
    this.stream = stream;
    this.contentType = contentType;
    this.contentLength = contentLength;
  }
  toJSON() {
    return {
      _id: "effect/HttpBody",
      _tag: "Stream",
      contentType: this.contentType,
      contentLength: this.contentLength
    };
  }
}
var stream = (body, contentType, contentLength) => new Stream2(body, contentType ?? "application/octet-stream", contentLength);

// node_modules/effect/dist/http/HttpClientError.js
var TypeId31 = "~effect/http/HttpClientError";
class HttpClientError extends (/* @__PURE__ */ TaggedError2("HttpClientError")) {
  constructor(props) {
    if ("cause" in props.reason) {
      super({
        ...props,
        cause: props.reason.cause
      });
    } else {
      super(props);
    }
  }
  [TypeId31] = TypeId31;
  get request() {
    return this.reason.request;
  }
  get response() {
    return "response" in this.reason ? this.reason.response : undefined;
  }
  get message() {
    return this.reason.message;
  }
}
var formatReason = (tag) => tag.endsWith("Error") ? tag.slice(0, -5) : tag;
var formatMessage = (reason, description, info) => description ? `${reason}: ${description} (${info})` : `${reason} error (${info})`;
var formatMethodAndUrl = (request) => `${request.method} ${redactUrl(request.url)}`;
var reasonToJSON = (reason, json) => ({
  ...json,
  request: {
    _id: "HttpClientRequest",
    method: reason.request.method,
    url: redactUrl(reason.request.url)
  },
  ..."response" in reason ? {
    response: {
      _id: "HttpClientResponse",
      status: reason.response.status
    }
  } : undefined
});

class TransportError extends (/* @__PURE__ */ TaggedError2("TransportError")) {
  get methodAndUrl() {
    return formatMethodAndUrl(this.request);
  }
  get message() {
    return formatMessage(formatReason(this._tag), this.description, this.methodAndUrl);
  }
  toJSON() {
    return reasonToJSON(this, super.toJSON());
  }
}

class EncodeError extends (/* @__PURE__ */ TaggedError2("EncodeError")) {
  get methodAndUrl() {
    return formatMethodAndUrl(this.request);
  }
  get message() {
    return formatMessage(formatReason(this._tag), this.description, this.methodAndUrl);
  }
  toJSON() {
    return reasonToJSON(this, super.toJSON());
  }
}

class InvalidUrlError extends (/* @__PURE__ */ TaggedError2("InvalidUrlError")) {
  get methodAndUrl() {
    return formatMethodAndUrl(this.request);
  }
  get message() {
    return formatMessage(formatReason(this._tag), this.description, this.methodAndUrl);
  }
  toJSON() {
    return reasonToJSON(this, super.toJSON());
  }
}

class StatusCodeError extends (/* @__PURE__ */ TaggedError2("StatusCodeError")) {
  get methodAndUrl() {
    return formatMethodAndUrl(this.request);
  }
  get message() {
    const info = `${this.response.status} ${this.methodAndUrl}`;
    return formatMessage(formatReason(this._tag), this.description, info);
  }
  toJSON() {
    return reasonToJSON(this, super.toJSON());
  }
}

class DecodeError extends (/* @__PURE__ */ TaggedError2("DecodeError")) {
  get methodAndUrl() {
    return formatMethodAndUrl(this.request);
  }
  get message() {
    const info = `${this.response.status} ${this.methodAndUrl}`;
    return formatMessage(formatReason(this._tag), this.description, info);
  }
  toJSON() {
    return reasonToJSON(this, super.toJSON());
  }
}

class EmptyBodyError extends (/* @__PURE__ */ TaggedError2("EmptyBodyError")) {
  get methodAndUrl() {
    return formatMethodAndUrl(this.request);
  }
  get message() {
    const info = `${this.response.status} ${this.methodAndUrl}`;
    return formatMessage(formatReason(this._tag), this.description, info);
  }
  toJSON() {
    return reasonToJSON(this, super.toJSON());
  }
}

// node_modules/effect/dist/http/HttpMethod.js
var allShort = [["GET", "get"], ["POST", "post"], ["PUT", "put"], ["DELETE", "del"], ["PATCH", "patch"], ["HEAD", "head"], ["OPTIONS", "options"], ["TRACE", "trace"], ["QUERY", "query"]];

// node_modules/effect/dist/http/internal/httpBody.js
var updateHeaders = (headers, body) => {
  if (body._tag === "Empty" || body._tag === "FormData") {
    return remove2(remove2(headers, "content-type"), "content-length");
  }
  headers = body.contentType === undefined ? remove2(headers, "content-type") : set3(headers, "content-type", body.contentType);
  return updateContentLength(headers, body);
};
var updateContentLength = (headers, body) => {
  const contentLength = body._tag === "Empty" || body._tag === "FormData" ? undefined : body.contentLength;
  return contentLength === undefined ? remove2(headers, "content-length") : set3(headers, "content-length", contentLength.toString());
};

// node_modules/effect/dist/http/Url.js
class UrlError extends (/* @__PURE__ */ TaggedError2("UrlError")) {
}
var make27 = (url, params, hash) => try_({
  try: () => {
    const urlInstance = new URL(url, baseUrl());
    for (let i = 0;i < params.params.length; i++) {
      const [key, value] = params.params[i];
      if (value !== undefined) {
        urlInstance.searchParams.append(key, unwrap4(value));
      }
    }
    if (hash !== undefined) {
      urlInstance.hash = hash;
    }
    return urlInstance;
  },
  catch: (cause) => new UrlError({
    cause
  })
});
var baseUrl = () => {
  if ("location" in globalThis && globalThis.location !== undefined && globalThis.location.origin !== undefined && globalThis.location.pathname !== undefined) {
    const base = location.origin + location.pathname;
    try {
      return new URL(base).href;
    } catch {
      return;
    }
  }
  return;
};

// node_modules/effect/dist/http/HttpClientRequest.js
var TypeId32 = "~effect/http/HttpClientRequest";
var Proto10 = {
  [TypeId32]: TypeId32,
  ...BaseProto,
  toJSON() {
    return {
      _id: "HttpClientRequest",
      method: this.method,
      url: redactUrl(this.url),
      urlParams: this.urlParams,
      hash: this.hash,
      headers: redact(this.headers),
      body: this.body.toJSON()
    };
  },
  pipe() {
    return pipeArguments(this, arguments);
  }
};
function makeWith(method, url, urlParams, hash, headers, body) {
  const self = Object.create(Proto10);
  self.method = method;
  self.url = url;
  self.urlParams = urlParams;
  self.hash = hash;
  self.headers = headers;
  self.body = body;
  return self;
}
var empty9 = /* @__PURE__ */ makeWith("GET", "", empty7, /* @__PURE__ */ none2(), empty6, empty8);
var make28 = (method) => (url, options) => modify(empty9, {
  method,
  url,
  ...options ?? undefined
});
var get5 = /* @__PURE__ */ make28("GET");
var post = /* @__PURE__ */ make28("POST");
var modify = /* @__PURE__ */ dual(2, (self, options) => {
  let result = self;
  if (options.method) {
    result = setMethod(result, options.method);
  }
  if (options.url) {
    result = setUrl(result, options.url);
  }
  if (options.headers) {
    result = setHeaders(result, options.headers);
  }
  if (options.urlParams) {
    result = setUrlParams(result, options.urlParams);
  }
  if (options.hash) {
    result = setHash(result, options.hash);
  }
  if (options.body) {
    result = setBody(result, options.body);
  }
  if (options.accept) {
    result = accept(result, options.accept);
  }
  if (options.acceptJson) {
    result = acceptJson(result);
  }
  return result;
});
var setMethod = /* @__PURE__ */ dual(2, (self, method) => makeWith(method, self.url, self.urlParams, self.hash, self.headers, self.body));
var setHeader = /* @__PURE__ */ dual(3, (self, key, value) => makeWith(self.method, self.url, self.urlParams, self.hash, set3(self.headers, key, value), self.body));
var setHeaders = /* @__PURE__ */ dual(2, (self, input) => makeWith(self.method, self.url, self.urlParams, self.hash, setAll(self.headers, input), self.body));
var bearerToken = /* @__PURE__ */ dual(2, (self, token) => setHeader(self, "Authorization", `Bearer ${stringOrRedacted(token)}`));
var accept = /* @__PURE__ */ dual(2, (self, mediaType) => setHeader(self, "Accept", mediaType));
var acceptJson = /* @__PURE__ */ accept("application/json");
var setUrl = /* @__PURE__ */ dual(2, (self, url) => {
  if (typeof url === "string") {
    return makeWith(self.method, url, self.urlParams, self.hash, self.headers, self.body);
  }
  const clone = new URL(url.toString());
  const urlParams = fromInput3(clone.searchParams);
  const hash = fromNullishOr(clone.hash === "" ? undefined : clone.hash.slice(1));
  clone.search = "";
  clone.hash = "";
  return makeWith(self.method, clone.toString(), urlParams, hash, self.headers, self.body);
});
var prependUrl = /* @__PURE__ */ dual(2, (self, path) => {
  if (path === "")
    return self;
  return makeWith(self.method, joinSegments(path, self.url), self.urlParams, self.hash, self.headers, self.body);
});
var joinSegments = (first, second) => {
  const endsWithSlash = first.endsWith("/");
  const startsWithSlash = second.startsWith("/");
  const needsTrim = endsWithSlash && startsWithSlash;
  const needsSlash = !endsWithSlash && !startsWithSlash;
  return needsTrim ? first + second.slice(1) : needsSlash ? first + "/" + second : first + second;
};
var setUrlParams = /* @__PURE__ */ dual(2, (self, input) => makeWith(self.method, self.url, setAll2(self.urlParams, input), self.hash, self.headers, self.body));
var setHash = /* @__PURE__ */ dual(2, (self, hash) => makeWith(self.method, self.url, self.urlParams, some2(hash), self.headers, self.body));
var setBody = /* @__PURE__ */ dual(2, (self, body) => {
  return makeWith(self.method, self.url, self.urlParams, self.hash, updateHeaders(self.headers, body), body);
});
var bodyJsonUnsafe = /* @__PURE__ */ dual(2, (self, body) => setBody(self, jsonUnsafe(body)));
var bodyUrlParams = /* @__PURE__ */ dual(2, (self, input) => setBody(self, urlParams(fromInput3(input))));
function toUrl2(self) {
  const r = make27(self.url, self.urlParams, getOrUndefined(self.hash));
  if (isSuccess2(r)) {
    return some2(r.success);
  }
  return none2();
}

// node_modules/effect/dist/http/HttpIncomingMessage.js
var TypeId33 = "~effect/http/HttpIncomingMessage";
var MaxBodySize = /* @__PURE__ */ Reference("effect/http/HttpIncomingMessage/MaxBodySize", {
  defaultValue: () => {
    return;
  }
});
var inspect = (self, that) => {
  return {
    ...that,
    headers: redact(self.headers),
    remoteAddress: self.remoteAddress
  };
};

// node_modules/effect/dist/http/internal/webResponse.js
var WebSourceSymbol = "~effect/http/HttpClientResponse/WebSource";
var getWebSource = (self) => self[WebSourceSymbol]?.();

// node_modules/effect/dist/http/HttpClientResponse.js
var TypeId34 = "~effect/http/HttpClientResponse";
var fromWeb = (request, source) => new WebHttpClientResponse(request, source);
class WebHttpClientResponse extends Class2 {
  [TypeId33];
  [TypeId34];
  request;
  source;
  constructor(request, source) {
    super();
    this.request = request;
    this.source = source;
    this[TypeId33] = TypeId33;
    this[TypeId34] = TypeId34;
  }
  toJSON() {
    return inspect(this, {
      _id: "HttpClientResponse",
      request: this.request.toJSON(),
      status: this.status
    });
  }
  get status() {
    return this.source.status;
  }
  get url() {
    if (this.source.url)
      return this.source.url.split("#")[0];
    const url = toUrl2(this.request);
    if (isNone2(url))
      return "";
    url.value.hash = "";
    return url.value.href;
  }
  get headers() {
    return fromInput2(this.source.headers);
  }
  cachedCookies;
  get cookies() {
    if (this.cachedCookies) {
      return this.cachedCookies;
    }
    return this.cachedCookies = fromSetCookie(this.source.headers.getSetCookie?.() ?? []);
  }
  get remoteAddress() {
    return none2();
  }
  [WebSourceSymbol]() {
    return this.source;
  }
  get stream() {
    return this.source.body ? fromReadableStream2({
      evaluate: () => this.source.body,
      onError: (cause) => new HttpClientError({
        reason: new DecodeError({
          request: this.request,
          response: this,
          cause
        })
      })
    }) : fail9(new HttpClientError({
      reason: new EmptyBodyError({
        request: this.request,
        response: this,
        description: "can not create stream from empty body"
      })
    }));
  }
  get json() {
    return flatMap4(this.text, (text) => try_3({
      try: () => text === "" ? null : JSON.parse(text),
      catch: (cause) => new HttpClientError({
        reason: new DecodeError({
          request: this.request,
          response: this,
          cause
        })
      })
    }));
  }
  textBody;
  get text() {
    return this.textBody ??= map7(this.arrayBuffer, (_) => new TextDecoder().decode(_));
  }
  get urlParamsBody() {
    return flatMap4(this.arrayBuffer, (bytes) => try_3({
      try: () => fromInput3(fromBody(bytes)),
      catch: (cause) => new HttpClientError({
        reason: new DecodeError({
          request: this.request,
          response: this,
          cause
        })
      })
    }));
  }
  formDataBody;
  get formData() {
    return this.formDataBody ??= tryPromise2({
      try: () => this.source.formData(),
      catch: (cause) => new HttpClientError({
        reason: new DecodeError({
          request: this.request,
          response: this,
          cause
        })
      })
    }).pipe(cached2, runSync2);
  }
  arrayBufferBody;
  get arrayBuffer() {
    if (this.arrayBufferBody) {
      return this.arrayBufferBody;
    }
    this.arrayBufferBody = tryPromise2({
      try: () => this.source.arrayBuffer(),
      catch: (cause) => new HttpClientError({
        reason: new DecodeError({
          request: this.request,
          response: this,
          cause
        })
      })
    }).pipe(cached2, runSync2);
    return this.arrayBufferBody;
  }
  get upgrade() {
    return fail7(new HttpClientError({
      reason: new StatusCodeError({
        request: this.request,
        response: this,
        description: "fetch cannot hand over the connection of an HTTP upgrade"
      })
    }));
  }
  pipe() {
    return pipeArguments(this, arguments);
  }
}

// node_modules/effect/dist/http/HttpTraceContext.js
var toHeaders = (span) => fromRecordUnsafe({
  b3: `${span.traceId}-${span.spanId}-${span.sampled ? "1" : "0"}${match(span.parent, {
    onNone: () => "",
    onSome: (parent) => `-${parent.spanId}`
  })}`,
  traceparent: `00-${span.traceId.padStart(32, "0")}-${span.spanId}-${span.sampled ? "01" : "00"}`
});
var fromHeaders = (headers) => {
  let span = w3c(headers);
  if (isSome2(span)) {
    return span;
  }
  span = b3(headers);
  if (isSome2(span)) {
    return span;
  }
  return xb3(headers);
};
var b3 = (headers) => {
  if (!("b3" in headers)) {
    return none2();
  }
  const parts = headers["b3"].split("-");
  if (parts.length < 2 || isB3Ids(parts[0], parts[1]) === false) {
    return none2();
  }
  return some2(externalSpan({
    traceId: parts[0],
    spanId: parts[1],
    sampled: parts[2] ? parts[2] === "1" || parts[2] === "d" : true
  }));
};
var xb3 = (headers) => {
  if (!headers["x-b3-traceid"] || !headers["x-b3-spanid"] || isB3Ids(headers["x-b3-traceid"], headers["x-b3-spanid"]) === false) {
    return none2();
  }
  return some2(externalSpan({
    traceId: headers["x-b3-traceid"],
    spanId: headers["x-b3-spanid"],
    sampled: headers["x-b3-flags"] === "1" || (headers["x-b3-sampled"] ? headers["x-b3-sampled"] === "1" || headers["x-b3-sampled"] === "true" : true)
  }));
};
var w3cTraceId = /^[0-9a-f]{32}$/;
var w3cSpanId = /^[0-9a-f]{16}$/;
var w3cTraceFlags = /^[0-9a-f]{2}$/;
var b3TraceId = /^(?:[0-9a-f]{16}|[0-9a-f]{32})$/;
var isB3Ids = (traceId, spanId) => b3TraceId.test(traceId) && w3cSpanId.test(spanId);
var w3c = (headers) => {
  if (!headers["traceparent"]) {
    return none2();
  }
  const parts = headers["traceparent"].split("-");
  if (parts.length !== 4) {
    return none2();
  }
  const [version, traceId, spanId, flags] = parts;
  switch (version) {
    case "00": {
      if (w3cTraceId.test(traceId) === false || w3cSpanId.test(spanId) === false || w3cTraceFlags.test(flags) === false || traceId === "00000000000000000000000000000000" || spanId === "0000000000000000") {
        return none2();
      }
      return some2(externalSpan({
        traceId,
        spanId,
        sampled: (parseInt(flags, 16) & 1) === 1
      }));
    }
    default: {
      return none2();
    }
  }
};

// node_modules/effect/dist/http/internal/tracing.js
var defaultPorts = {
  "http:": 80,
  "https:": 443,
  "ws:": 80,
  "wss:": 443
};
var addUrlAttributes = (span, url, redactedNames) => {
  const query = redactUrl(url.search, redactedNames).slice(1);
  span.attribute("url.full", redactUrl(url.href, redactedNames));
  span.attribute("url.path", url.pathname);
  if (query !== "") {
    span.attribute("url.query", query);
  }
  span.attribute("url.scheme", url.protocol.slice(0, -1));
  if (url.hostname !== "") {
    span.attribute("server.address", url.hostname.startsWith("[") ? url.hostname.slice(1, -1) : url.hostname);
    const port = url.port === "" ? defaultPorts[url.protocol] : Number(url.port);
    if (port !== undefined) {
      span.attribute("server.port", port);
    }
  }
};
var redactedNames = (urlParams, redactedNames) => {
  const keys = urlParams.params.filter(([, value]) => typeof value !== "string").map(([key]) => key);
  return keys.length === 0 ? redactedNames : [...redactedNames, ...new URLSearchParams(keys.map((key) => [key, ""])).keys()];
};
var addHeaderAttributes = (span, phase, headers, filter, redactedNames) => {
  for (const name in headers) {
    if (!filter(name, phase))
      continue;
    span.attribute(`http.${phase}.header.${name}`, isRedactedName(name, redactedNames) ? "<redacted>" : headers[name]);
  }
};

// node_modules/effect/dist/http/HttpClient.js
var TypeId35 = "~effect/http/HttpClient";
var HttpClient = /* @__PURE__ */ Service("effect/HttpClient");
var transformResponse = /* @__PURE__ */ dual(2, (self, f) => makeWith2((request) => f(self.postprocess(request)), self.preprocess));
var makeWith2 = (postprocess, preprocess) => {
  const self = Object.create(Proto11);
  self.preprocess = preprocess;
  self.postprocess = postprocess;
  self.execute = function(request) {
    return postprocess(preprocess(request));
  };
  return self;
};
var Proto11 = {
  [TypeId35]: TypeId35,
  pipe() {
    return pipeArguments(this, arguments);
  },
  ...BaseProto,
  toJSON() {
    return {
      _id: "effect/HttpClient"
    };
  },
  .../* @__PURE__ */ Object.fromEntries(/* @__PURE__ */ allShort.map(([fullMethod, method]) => [method, function(url, options) {
    return this.execute(make28(fullMethod)(url, options));
  }]))
};
var make29 = (f) => makeWith2((effect) => flatMap4(effect, (request) => withFiber2((fiber) => {
  const scopedController = scopedRequests.get(request);
  const controller = scopedController ?? new AbortController;
  const urlResult = make27(request.url, request.urlParams, getOrUndefined(request.hash));
  if (isFailure2(urlResult)) {
    return fail7(new HttpClientError({
      reason: new InvalidUrlError({
        request,
        cause: urlResult.failure
      })
    }));
  }
  const url = urlResult.success;
  const tracerDisabled = fiber.getRef(DisablePropagation) || fiber.getRef(TracerDisabledWhen)(request);
  if (tracerDisabled) {
    const effect = f(request, url, controller.signal, fiber);
    if (scopedController)
      return map7(effect, (response) => trackResponse(response, controller, true));
    return uninterruptibleMask2((restore) => matchCauseEffect2(restore(effect), {
      onSuccess(response) {
        return succeed7(trackResponse(response, controller, false));
      },
      onFailure(cause) {
        if (hasInterrupts2(cause)) {
          controller.abort();
        }
        return failCause3(cause);
      }
    }));
  }
  return useSpan2(fiber.getRef(SpanNameGenerator)(request), {
    kind: "client"
  }, (span) => {
    span.attribute("http.request.method", request.method);
    const redactedHeaderNames = fiber.getRef(CurrentRedactedNames);
    addUrlAttributes(span, url, redactedNames(request.urlParams, fiber.getRef(CurrentRedactedNames2)));
    const headerFilter = fiber.getRef(TracerHeaderFilter);
    addHeaderAttributes(span, "request", request.headers, headerFilter, redactedHeaderNames);
    request = fiber.getRef(TracerPropagationEnabled) ? setHeaders(request, toHeaders(span)) : request;
    return uninterruptibleMask2((restore) => restore(f(request, url, controller.signal, fiber)).pipe(withParentSpan2(span, {
      captureStackTrace: false
    }), matchCauseEffect2({
      onSuccess: (response) => {
        span.attribute("http.response.status_code", response.status);
        addHeaderAttributes(span, "response", response.headers, headerFilter, redactedHeaderNames);
        if (response.status >= 400 && response.status < 600) {
          span.attribute("error.type", String(response.status));
          endSpanWithStatusError(fiber, span, request, response);
        }
        return succeed7(trackResponse(response, controller, scopedController !== undefined));
      },
      onFailure(cause) {
        if (!scopedController && hasInterrupts2(cause)) {
          controller.abort();
        }
        return failCause3(cause);
      }
    })));
  });
})), succeed7);
var endSpanWithStatusError = (fiber, span, request, response) => {
  const stackTraceLimit = getStackTraceLimit();
  setStackTraceLimit(0);
  let exit;
  try {
    exit = fail5(new HttpClientError({
      reason: new StatusCodeError({
        request,
        response
      })
    }));
  } finally {
    setStackTraceLimit(stackTraceLimit);
  }
  span.end(fiber.getRef(TracerTimingEnabled2) ? fiber.getRef(Clock).currentTimeNanosUnsafe() : BigInt(0), exit);
};
var mapRequest = /* @__PURE__ */ dual(2, (self, f) => makeWith2(self.postprocess, (request) => map7(self.preprocess(request), f)));
var TracerDisabledWhen = /* @__PURE__ */ Reference("effect/http/HttpClient/TracerDisabledWhen", {
  defaultValue: () => constFalse
});
var TracerHeaderFilter = /* @__PURE__ */ Reference("effect/http/HttpClient/TracerHeaderFilter", {
  defaultValue: () => constFalse
});
var TracerPropagationEnabled = /* @__PURE__ */ Reference("effect/http/HttpClient/TracerPropagationEnabled", {
  defaultValue: constTrue
});
var SpanNameGenerator = /* @__PURE__ */ Reference("effect/http/HttpClient/SpanNameGenerator", {
  defaultValue: () => (request) => request.method
});
var layerMergedContext = (effect2) => effect(HttpClient)(contextWith2((context) => map7(effect2, (client) => transformResponse(client, updateContext2((input) => merge(context, input))))));
var responseRegistry = /* @__PURE__ */ (() => {
  if ("FinalizationRegistry" in globalThis && globalThis.FinalizationRegistry) {
    const registry = /* @__PURE__ */ new FinalizationRegistry((controller) => {
      try {
        controller.abort();
      } catch {}
    });
    return {
      register(response, controller) {
        registry.register(response, controller, response);
      },
      unregister(response) {
        registry.unregister(response);
      }
    };
  }
  const timers = /* @__PURE__ */ new WeakMap;
  return {
    register(response, controller) {
      timers.set(response, setTimeout(() => controller.abort(), 5000));
    },
    unregister(response) {
      const timer = timers.get(response);
      if (timer === undefined)
        return;
      clearTimeout(timer);
      timers.delete(response);
    }
  };
})();
var scopedRequests = /* @__PURE__ */ new WeakMap;
var responseControllers = /* @__PURE__ */ new WeakMap;
var trackResponse = (response, controller, scoped) => {
  if (!scoped) {
    responseRegistry.register(response, controller);
    response = new InterruptibleResponse(response, controller);
  }
  responseControllers.set(response, controller);
  return response;
};

class InterruptibleResponse {
  original;
  controller;
  constructor(original, controller) {
    this.original = original;
    this.controller = controller;
  }
  [TypeId34] = TypeId34;
  [TypeId33] = TypeId33;
  applyInterrupt(effect) {
    return suspend2(() => {
      responseRegistry.unregister(this.original);
      return onInterrupt2(effect, () => sync2(() => {
        this.controller.abort();
      }));
    });
  }
  get request() {
    return this.original.request;
  }
  get url() {
    return this.original.url;
  }
  get status() {
    return this.original.status;
  }
  get headers() {
    return this.original.headers;
  }
  get cookies() {
    return this.original.cookies;
  }
  get remoteAddress() {
    return this.original.remoteAddress;
  }
  get formData() {
    return this.applyInterrupt(this.original.formData);
  }
  get text() {
    return this.applyInterrupt(this.original.text);
  }
  get json() {
    return this.applyInterrupt(this.original.json);
  }
  get urlParamsBody() {
    return this.applyInterrupt(this.original.urlParamsBody);
  }
  get arrayBuffer() {
    return this.applyInterrupt(this.original.arrayBuffer);
  }
  get upgrade() {
    return this.applyInterrupt(this.original.upgrade);
  }
  [WebSourceSymbol]() {
    const source = getWebSource(this.original);
    if (source !== undefined)
      responseRegistry.unregister(this.original);
    return source;
  }
  get stream() {
    return suspend4(() => {
      responseRegistry.unregister(this.original);
      return ensuring4(this.original.stream, sync2(() => {
        this.controller.abort();
      }));
    });
  }
  toJSON() {
    return this.original.toJSON();
  }
  [NodeInspectSymbol]() {
    return this.original[NodeInspectSymbol]();
  }
  pipe() {
    return pipeArguments(this, arguments);
  }
}

// node_modules/effect/dist/http/FetchHttpClient.js
var Fetch = /* @__PURE__ */ Reference("effect/http/FetchHttpClient/Fetch", {
  defaultValue: () => globalThis.fetch
});
var StreamBodyFallback = /* @__PURE__ */ Reference("effect/http/FetchHttpClient/StreamBodyFallback", {
  defaultValue: () => "error"
});

class RequestInit extends (/* @__PURE__ */ Service()("effect/http/FetchHttpClient/RequestInit")) {
}
var supportsStreamBody;
var detectStreamBodySupport = () => {
  try {
    let duplexAccessed = false;
    const body = new ReadableStream;
    const request = new Request("https://example.com", {
      method: "POST",
      body,
      get duplex() {
        duplexAccessed = true;
        return "half";
      }
    });
    return (duplexAccessed || request.body === body) && !request.headers.has("Content-Type");
  } catch {
    return false;
  }
};
var fetch = /* @__PURE__ */ make29((request, url, signal, fiber) => {
  if (request.headers.upgrade !== undefined) {
    return fail7(new HttpClientError({
      reason: new TransportError({
        request,
        description: "fetch cannot send an HTTP upgrade request"
      })
    }));
  }
  const fetch = fiber.getRef(Fetch);
  const options = getOrUndefined2(fiber.context, RequestInit) ?? {};
  let headers = options.headers ? merge4(fromInput2(options.headers), request.headers) : request.headers;
  if (headers["content-length"]) {
    headers = remove2(headers, "content-length");
  }
  let uploadController;
  const stopUpload = sync2(() => uploadController?.abort());
  const send = (body) => map7(tryPromise2({
    try: () => fetch(url, {
      ...options,
      method: request.method,
      headers,
      body,
      duplex: typeof ReadableStream !== "undefined" && body instanceof ReadableStream ? "half" : undefined,
      signal: uploadController ? AbortSignal.any([signal, uploadController.signal]) : signal
    }),
    catch: (cause) => new HttpClientError({
      reason: new TransportError({
        request,
        cause
      })
    })
  }), (response) => {
    const original = fromWeb(request, response);
    if (!uploadController)
      return original;
    if (response.body === null)
      uploadController.abort();
    return Object.create(original, {
      ...Object.fromEntries(["arrayBuffer", "text", "json", "formData", "urlParamsBody"].map((key) => [key, {
        get: () => ensuring2(original[key], stopUpload)
      }])),
      stream: {
        get: () => ensuring4(original.stream, stopUpload)
      }
    });
  }).pipe(onError2(() => stopUpload));
  switch (request.body._tag) {
    case "Raw":
    case "Uint8Array":
      return send(request.body.body);
    case "FormData":
      return send(request.body.formData);
    case "Stream": {
      supportsStreamBody ??= detectStreamBodySupport();
      if (supportsStreamBody) {
        uploadController = new AbortController;
        const uploadSignal = AbortSignal.any([signal, uploadController.signal]);
        const stopped = callback2((resume) => {
          const onAbort = () => resume(interrupt2);
          if (uploadSignal.aborted) {
            onAbort();
            return;
          }
          uploadSignal.addEventListener("abort", onAbort, {
            once: true
          });
          return sync2(() => uploadSignal.removeEventListener("abort", onAbort));
        });
        return flatMap4(toReadableStreamEffect(interruptWhen2(request.body.stream, stopped)), send);
      }
      if (fiber.getRef(StreamBodyFallback) === "error") {
        return fail7(new HttpClientError({
          reason: new EncodeError({
            request,
            description: "Streaming request bodies are not supported in this runtime"
          })
        }));
      }
      return mkArrayBuffer(request.body.stream).pipe(mapError3((cause) => new HttpClientError({
        reason: new EncodeError({
          request,
          cause
        })
      })), flatMap4(send));
    }
  }
  return send(undefined);
});
var layer2 = /* @__PURE__ */ layerMergedContext(/* @__PURE__ */ succeed7(fetch));

// node_modules/effect/dist/ErrorReporter.js
var ignore4 = "~effect/ErrorReporter/ignore";

// node_modules/effect/dist/http/internal/headers.js
var Proto12 = /* @__PURE__ */ Object.getPrototypeOf(empty6);
var emptyMutableUnsafe = () => Object.create(Proto12);

// node_modules/effect/dist/http/HttpServerResponse.js
var TypeId36 = "~effect/http/HttpServerResponse";
var isHttpServerResponse = (u) => hasProperty(u, TypeId36);
var empty10 = (options) => makeResponse({
  status: options?.status ?? 204,
  statusText: options?.statusText,
  headers: options?.headers ? fromInput2(options.headers) : undefined,
  cookies: options?.cookies
});
var getContentType = (options, headers) => {
  if (options?.contentType) {
    return options.contentType;
  } else if (options?.headers) {
    return headers["content-type"];
  }
};
var text2 = (body, options) => {
  const headers = options?.headers ? fromInput2(options.headers) : empty6;
  return makeResponse({
    status: options?.status ?? 200,
    statusText: options?.statusText,
    headers,
    cookies: options?.cookies ?? empty5,
    body: text(body, getContentType(options, headers))
  });
};
var raw2 = (body, options) => makeResponse({
  status: options?.status ?? 200,
  statusText: options?.statusText,
  headers: options?.headers && fromInput2(options.headers),
  cookies: options?.cookies,
  body: raw(body, {
    contentType: options?.contentType,
    contentLength: options?.contentLength
  })
});
var setHeader2 = /* @__PURE__ */ dual(3, (self, key, value) => makeResponse(self, set3(self.headers, key, value)));
var removeHeader2 = /* @__PURE__ */ dual(2, (self, key) => makeResponse({
  ...self,
  headers: remove2(self.headers, key)
}));
var setHeaders2 = /* @__PURE__ */ dual(2, (self, input) => makeResponse(self, setAll(self.headers, input)));
var setBody2 = /* @__PURE__ */ dual(2, (self, body) => makeResponse({
  ...self,
  body
}, updateHeaders(self.headers, body)));
var omitsBody = (response, withoutBody = false) => withoutBody || response.status === 204 || response.status === 205 || response.status === 304;
var Proto13 = {
  ...PipeInspectableProto,
  [TypeId36]: TypeId36,
  [ignore4]: true,
  toJSON() {
    return {
      _id: "HttpServerResponse",
      status: this.status,
      statusText: this.statusText,
      headers: redact(this.headers),
      cookies: this.cookies.toJSON(),
      body: this.body.toJSON()
    };
  }
};
var makeResponse = (options, ownedHeaders) => {
  const self = Object.create(Proto13);
  self.status = options.status;
  self.statusText = options.statusText;
  self.cookies = options.cookies ?? empty5;
  self.body = options.body ?? empty8;
  if (self.body._tag !== "Empty" && (self.body.contentType || self.body.contentLength !== undefined)) {
    const owned = ownedHeaders !== undefined;
    const newHeaders = owned ? ownedHeaders : options.headers === undefined || options.headers === empty6 ? emptyMutableUnsafe() : fromRecordUnsafe({
      ...options.headers
    });
    if (self.body.contentType && (!owned || newHeaders["content-type"] === undefined)) {
      newHeaders["content-type"] = self.body.contentType;
    }
    if (self.body.contentLength !== undefined && (!owned || newHeaders["content-length"] === undefined)) {
      newHeaders["content-length"] = self.body.contentLength.toString();
    }
    self.headers = newHeaders;
  } else {
    self.headers = ownedHeaders ?? options.headers ?? empty6;
  }
  return self;
};

// node_modules/effect/dist/http/internal/compression.js
var varyWith = (headers, dimension) => {
  const vary = headers["vary"];
  if (vary === undefined) {
    return dimension;
  }
  const members = vary.split(",").map((member) => member.trim().toLowerCase());
  return members.includes("*") || members.includes(dimension.toLowerCase()) ? vary : `${vary}, ${dimension}`;
};
var wrapCompression = (impl) => ({
  algorithms: impl.algorithms,
  compressResponse(response, algorithm, options) {
    return map7(impl.compressResponse(response, algorithm, options), (compressed) => {
      if (compressed === response) {
        return response;
      }
      const headers = {
        "content-encoding": algorithm,
        vary: varyWith(compressed.headers, "Accept-Encoding")
      };
      const etag = compressed.headers["etag"];
      if (etag !== undefined && !etag.startsWith("W/")) {
        headers["etag"] = `W/${etag}`;
      }
      return setHeaders2(compressed, headers);
    });
  }
});

// node_modules/effect/dist/http/internal/mimeTypes.js
var types = {
  "application/andrew-inset": ["ez"],
  "application/appinstaller": ["appinstaller"],
  "application/applixware": ["aw"],
  "application/appx": ["appx"],
  "application/appxbundle": ["appxbundle"],
  "application/atom+xml": ["atom"],
  "application/atomcat+xml": ["atomcat"],
  "application/atomdeleted+xml": ["atomdeleted"],
  "application/atomsvc+xml": ["atomsvc"],
  "application/atsc-dwd+xml": ["dwd"],
  "application/atsc-held+xml": ["held"],
  "application/atsc-rsat+xml": ["rsat"],
  "application/automationml-aml+xml": ["aml"],
  "application/automationml-amlx+zip": ["amlx"],
  "application/bdoc": ["bdoc"],
  "application/calendar+xml": ["xcs"],
  "application/ccxml+xml": ["ccxml"],
  "application/cdfx+xml": ["cdfx"],
  "application/cdmi-capability": ["cdmia"],
  "application/cdmi-container": ["cdmic"],
  "application/cdmi-domain": ["cdmid"],
  "application/cdmi-object": ["cdmio"],
  "application/cdmi-queue": ["cdmiq"],
  "application/cpl+xml": ["cpl"],
  "application/cu-seeme": ["cu"],
  "application/cwl": ["cwl"],
  "application/dash+xml": ["mpd"],
  "application/dash-patch+xml": ["mpp"],
  "application/davmount+xml": ["davmount"],
  "application/dicom": ["dcm"],
  "application/docbook+xml": ["dbk"],
  "application/dssc+der": ["dssc"],
  "application/dssc+xml": ["xdssc"],
  "application/ecmascript": ["ecma"],
  "application/emma+xml": ["emma"],
  "application/emotionml+xml": ["emotionml"],
  "application/epub+zip": ["epub"],
  "application/exi": ["exi"],
  "application/express": ["exp"],
  "application/fdf": ["fdf"],
  "application/fdt+xml": ["fdt"],
  "application/font-tdpfr": ["pfr"],
  "application/geo+json": ["geojson"],
  "application/gml+xml": ["gml"],
  "application/gpx+xml": ["gpx"],
  "application/gxf": ["gxf"],
  "application/gzip": ["gz"],
  "application/hjson": ["hjson"],
  "application/hyperstudio": ["stk"],
  "application/inkml+xml": ["ink", "inkml"],
  "application/ipfix": ["ipfix"],
  "application/its+xml": ["its"],
  "application/java-archive": ["jar", "war", "ear"],
  "application/java-serialized-object": ["ser"],
  "application/java-vm": ["class"],
  "application/javascript": ["*js"],
  "application/json": ["json", "map"],
  "application/json5": ["json5"],
  "application/jsonml+json": ["jsonml"],
  "application/ld+json": ["jsonld"],
  "application/lgr+xml": ["lgr"],
  "application/lost+xml": ["lostxml"],
  "application/mac-binhex40": ["hqx"],
  "application/mac-compactpro": ["cpt"],
  "application/mads+xml": ["mads"],
  "application/manifest+json": ["webmanifest"],
  "application/marc": ["mrc"],
  "application/marcxml+xml": ["mrcx"],
  "application/mathematica": ["ma", "nb", "mb"],
  "application/mathml+xml": ["mathml"],
  "application/mbox": ["mbox"],
  "application/media-policy-dataset+xml": ["mpf"],
  "application/mediaservercontrol+xml": ["mscml"],
  "application/metalink+xml": ["metalink"],
  "application/metalink4+xml": ["meta4"],
  "application/mets+xml": ["mets"],
  "application/mmt-aei+xml": ["maei"],
  "application/mmt-usd+xml": ["musd"],
  "application/mods+xml": ["mods"],
  "application/mp21": ["m21", "mp21"],
  "application/mp4": ["*mp4", "*mpg4", "mp4s", "m4p"],
  "application/msix": ["msix"],
  "application/msixbundle": ["msixbundle"],
  "application/msword": ["doc", "dot"],
  "application/mxf": ["mxf"],
  "application/n-quads": ["nq"],
  "application/n-triples": ["nt"],
  "application/node": ["cjs"],
  "application/octet-stream": ["bin", "dms", "lrf", "mar", "so", "dist", "distz", "pkg", "bpk", "dump", "elc", "deploy", "exe", "dll", "deb", "dmg", "iso", "img", "msi", "msp", "msm", "buffer"],
  "application/oda": ["oda"],
  "application/oebps-package+xml": ["opf"],
  "application/ogg": ["ogx"],
  "application/omdoc+xml": ["omdoc"],
  "application/onenote": ["onetoc", "onetoc2", "onetmp", "onepkg", "one", "onea"],
  "application/oxps": ["oxps"],
  "application/p2p-overlay+xml": ["relo"],
  "application/patch-ops-error+xml": ["xer"],
  "application/pdf": ["pdf"],
  "application/pgp-encrypted": ["pgp"],
  "application/pgp-keys": ["asc"],
  "application/pgp-signature": ["sig", "*asc"],
  "application/pics-rules": ["prf"],
  "application/pkcs10": ["p10"],
  "application/pkcs7-mime": ["p7m", "p7c"],
  "application/pkcs7-signature": ["p7s"],
  "application/pkcs8": ["p8"],
  "application/pkix-attr-cert": ["ac"],
  "application/pkix-cert": ["cer"],
  "application/pkix-crl": ["crl"],
  "application/pkix-pkipath": ["pkipath"],
  "application/pkixcmp": ["pki"],
  "application/pls+xml": ["pls"],
  "application/postscript": ["ai", "eps", "ps"],
  "application/provenance+xml": ["provx"],
  "application/pskc+xml": ["pskcxml"],
  "application/raml+yaml": ["raml"],
  "application/rdf+xml": ["rdf", "owl"],
  "application/reginfo+xml": ["rif"],
  "application/relax-ng-compact-syntax": ["rnc"],
  "application/resource-lists+xml": ["rl"],
  "application/resource-lists-diff+xml": ["rld"],
  "application/rls-services+xml": ["rs"],
  "application/route-apd+xml": ["rapd"],
  "application/route-s-tsid+xml": ["sls"],
  "application/route-usd+xml": ["rusd"],
  "application/rpki-ghostbusters": ["gbr"],
  "application/rpki-manifest": ["mft"],
  "application/rpki-roa": ["roa"],
  "application/rsd+xml": ["rsd"],
  "application/rss+xml": ["rss"],
  "application/rtf": ["rtf"],
  "application/sbml+xml": ["sbml"],
  "application/scvp-cv-request": ["scq"],
  "application/scvp-cv-response": ["scs"],
  "application/scvp-vp-request": ["spq"],
  "application/scvp-vp-response": ["spp"],
  "application/sdp": ["sdp"],
  "application/senml+xml": ["senmlx"],
  "application/sensml+xml": ["sensmlx"],
  "application/set-payment-initiation": ["setpay"],
  "application/set-registration-initiation": ["setreg"],
  "application/shf+xml": ["shf"],
  "application/sieve": ["siv", "sieve"],
  "application/smil+xml": ["smi", "smil"],
  "application/sparql-query": ["rq"],
  "application/sparql-results+xml": ["srx"],
  "application/sql": ["sql"],
  "application/srgs": ["gram"],
  "application/srgs+xml": ["grxml"],
  "application/sru+xml": ["sru"],
  "application/ssdl+xml": ["ssdl"],
  "application/ssml+xml": ["ssml"],
  "application/swid+xml": ["swidtag"],
  "application/tei+xml": ["tei", "teicorpus"],
  "application/thraud+xml": ["tfi"],
  "application/timestamped-data": ["tsd"],
  "application/toml": ["toml"],
  "application/trig": ["trig"],
  "application/ttml+xml": ["ttml"],
  "application/ubjson": ["ubj"],
  "application/urc-ressheet+xml": ["rsheet"],
  "application/urc-targetdesc+xml": ["td"],
  "application/voicexml+xml": ["vxml"],
  "application/wasm": ["wasm"],
  "application/watcherinfo+xml": ["wif"],
  "application/widget": ["wgt"],
  "application/winhlp": ["hlp"],
  "application/wsdl+xml": ["wsdl"],
  "application/wspolicy+xml": ["wspolicy"],
  "application/xaml+xml": ["xaml"],
  "application/xcap-att+xml": ["xav"],
  "application/xcap-caps+xml": ["xca"],
  "application/xcap-diff+xml": ["xdf"],
  "application/xcap-el+xml": ["xel"],
  "application/xcap-ns+xml": ["xns"],
  "application/xenc+xml": ["xenc"],
  "application/xfdf": ["xfdf"],
  "application/xhtml+xml": ["xhtml", "xht"],
  "application/xliff+xml": ["xlf"],
  "application/xml": ["xml", "xsl", "xsd", "rng"],
  "application/xml-dtd": ["dtd"],
  "application/xop+xml": ["xop"],
  "application/xproc+xml": ["xpl"],
  "application/xslt+xml": ["*xsl", "xslt"],
  "application/xspf+xml": ["xspf"],
  "application/xv+xml": ["mxml", "xhvml", "xvml", "xvm"],
  "application/yang": ["yang"],
  "application/yin+xml": ["yin"],
  "application/zip": ["zip"],
  "application/zip+dotlottie": ["lottie"],
  "audio/3gpp": ["*3gpp"],
  "audio/aac": ["adts", "aac"],
  "audio/adpcm": ["adp"],
  "audio/amr": ["amr"],
  "audio/basic": ["au", "snd"],
  "audio/midi": ["mid", "midi", "kar", "rmi"],
  "audio/mobile-xmf": ["mxmf"],
  "audio/mp3": ["*mp3"],
  "audio/mp4": ["m4a", "mp4a", "m4b"],
  "audio/mpeg": ["mpga", "mp2", "mp2a", "mp3", "m2a", "m3a"],
  "audio/ogg": ["oga", "ogg", "spx", "opus"],
  "audio/s3m": ["s3m"],
  "audio/silk": ["sil"],
  "audio/wav": ["wav"],
  "audio/wave": ["*wav"],
  "audio/webm": ["weba"],
  "audio/xm": ["xm"],
  "font/collection": ["ttc"],
  "font/otf": ["otf"],
  "font/ttf": ["ttf"],
  "font/woff": ["woff"],
  "font/woff2": ["woff2"],
  "image/aces": ["exr"],
  "image/apng": ["apng"],
  "image/avci": ["avci"],
  "image/avcs": ["avcs"],
  "image/avif": ["avif"],
  "image/bmp": ["bmp", "dib"],
  "image/cgm": ["cgm"],
  "image/dicom-rle": ["drle"],
  "image/dpx": ["dpx"],
  "image/emf": ["emf"],
  "image/fits": ["fits"],
  "image/g3fax": ["g3"],
  "image/gif": ["gif"],
  "image/heic": ["heic"],
  "image/heic-sequence": ["heics"],
  "image/heif": ["heif"],
  "image/heif-sequence": ["heifs"],
  "image/hej2k": ["hej2"],
  "image/ief": ["ief"],
  "image/jaii": ["jaii"],
  "image/jais": ["jais"],
  "image/jls": ["jls"],
  "image/jp2": ["jp2", "jpg2"],
  "image/jpeg": ["jpg", "jpeg", "jpe"],
  "image/jph": ["jph"],
  "image/jphc": ["jhc"],
  "image/jpm": ["jpm", "jpgm"],
  "image/jpx": ["jpx", "jpf"],
  "image/jxl": ["jxl"],
  "image/jxr": ["jxr"],
  "image/jxra": ["jxra"],
  "image/jxrs": ["jxrs"],
  "image/jxs": ["jxs"],
  "image/jxsc": ["jxsc"],
  "image/jxsi": ["jxsi"],
  "image/jxss": ["jxss"],
  "image/ktx": ["ktx"],
  "image/ktx2": ["ktx2"],
  "image/pjpeg": ["jfif"],
  "image/png": ["png"],
  "image/sgi": ["sgi"],
  "image/svg+xml": ["svg", "svgz"],
  "image/t38": ["t38"],
  "image/tiff": ["tif", "tiff"],
  "image/tiff-fx": ["tfx"],
  "image/webp": ["webp"],
  "image/wmf": ["wmf"],
  "message/disposition-notification": ["disposition-notification"],
  "message/global": ["u8msg"],
  "message/global-delivery-status": ["u8dsn"],
  "message/global-disposition-notification": ["u8mdn"],
  "message/global-headers": ["u8hdr"],
  "message/rfc822": ["eml", "mime", "mht", "mhtml"],
  "model/3mf": ["3mf"],
  "model/gltf+json": ["gltf"],
  "model/gltf-binary": ["glb"],
  "model/iges": ["igs", "iges"],
  "model/jt": ["jt"],
  "model/mesh": ["msh", "mesh", "silo"],
  "model/mtl": ["mtl"],
  "model/obj": ["obj"],
  "model/prc": ["prc"],
  "model/step": ["step", "stp", "stpnc", "p21", "210"],
  "model/step+xml": ["stpx"],
  "model/step+zip": ["stpz"],
  "model/step-xml+zip": ["stpxz"],
  "model/stl": ["stl"],
  "model/u3d": ["u3d"],
  "model/vrml": ["wrl", "vrml"],
  "model/x3d+binary": ["*x3db", "x3dbz"],
  "model/x3d+fastinfoset": ["x3db"],
  "model/x3d+vrml": ["*x3dv", "x3dvz"],
  "model/x3d+xml": ["x3d", "x3dz"],
  "model/x3d-vrml": ["x3dv"],
  "text/cache-manifest": ["appcache", "manifest"],
  "text/calendar": ["ics", "ifb"],
  "text/coffeescript": ["coffee", "litcoffee"],
  "text/css": ["css"],
  "text/csv": ["csv"],
  "text/html": ["html", "htm", "shtml"],
  "text/jade": ["jade"],
  "text/javascript": ["js", "mjs"],
  "text/jsx": ["jsx"],
  "text/less": ["less"],
  "text/markdown": ["md", "markdown"],
  "text/mathml": ["mml"],
  "text/mdx": ["mdx"],
  "text/n3": ["n3"],
  "text/plain": ["txt", "text", "conf", "def", "list", "log", "in", "ini"],
  "text/richtext": ["rtx"],
  "text/rtf": ["*rtf"],
  "text/sgml": ["sgml", "sgm"],
  "text/shex": ["shex"],
  "text/slim": ["slim", "slm"],
  "text/spdx": ["spdx"],
  "text/stylus": ["stylus", "styl"],
  "text/tab-separated-values": ["tsv"],
  "text/troff": ["t", "tr", "roff", "man", "me", "ms"],
  "text/turtle": ["ttl"],
  "text/uri-list": ["uri", "uris", "urls"],
  "text/vcard": ["vcard"],
  "text/vtt": ["vtt"],
  "text/wgsl": ["wgsl"],
  "text/x-python": ["py"],
  "text/x-shellscript": ["sh"],
  "text/xml": ["*xml"],
  "text/yaml": ["yaml", "yml"],
  "video/3gpp": ["3gp", "3gpp"],
  "video/3gpp2": ["3g2"],
  "video/h261": ["h261"],
  "video/h263": ["h263"],
  "video/h264": ["h264"],
  "video/iso.segment": ["m4s"],
  "video/jpeg": ["jpgv"],
  "video/jpm": ["*jpm", "*jpgm"],
  "video/mj2": ["mj2", "mjp2"],
  "video/mp2t": ["ts", "m2t", "m2ts", "mts"],
  "video/mp4": ["mp4", "mp4v", "mpg4"],
  "video/mpeg": ["mpeg", "mpg", "mpe", "m1v", "m2v"],
  "video/ogg": ["ogv"],
  "video/quicktime": ["qt", "mov"],
  "video/webm": ["webm"]
};
Object.freeze(types);
var mimeTypes_default = types;

// node_modules/effect/dist/http/Mime.js
var tables;
var getTables = () => {
  if (tables !== undefined) {
    return tables;
  }
  const extensionToType = new Map;
  const typeToExtension = new Map;
  const typeToExtensions = new Map;
  for (const [type, extensions] of Object.entries(mimeTypes_default)) {
    const allExtensions = new Set;
    typeToExtensions.set(type, allExtensions);
    for (let index = 0;index < extensions.length; index++) {
      let extension = extensions[index];
      const starred = extension.startsWith("*");
      if (starred) {
        extension = extension.slice(1);
      }
      allExtensions.add(extension);
      if (index === 0) {
        typeToExtension.set(type, extension);
      }
      if (!starred) {
        extensionToType.set(extension, type);
      }
    }
  }
  tables = {
    extensionToType,
    typeToExtension,
    typeToExtensions
  };
  return tables;
};
var getType = (path) => {
  if (typeof path !== "string") {
    return none2();
  }
  const last = path.replace(/^.*[/\\]/s, "").toLowerCase();
  const extension = last.replace(/^.*\./s, "").toLowerCase();
  const hasPath = last.length < path.length;
  const hasDot = extension.length < last.length - 1;
  if (!hasDot && hasPath) {
    return none2();
  }
  return fromUndefinedOr(getTables().extensionToType.get(extension));
};

// node_modules/effect/dist/http/HttpPlatform.js
class HttpPlatform extends (/* @__PURE__ */ Service()("effect/http/HttpPlatform")) {
}
var make30 = /* @__PURE__ */ fnUntraced2(function* (impl) {
  const fs = yield* FileSystem;
  const etagGen = yield* Generator;
  return HttpPlatform.of({
    platform: impl.platform,
    compression: wrapCompression(impl.compression),
    fileResponse: fnUntraced2(function* (path, options) {
      const info = yield* fs.stat(path);
      const etag = yield* etagGen.fromFileInfo(info);
      const requestedOffset = options?.offset === undefined ? zero2 : yield* fileResponseSize(options.offset, "offset");
      const offset = requestedOffset > info.size ? info.size : requestedOffset;
      const available = info.size - offset;
      const bytesToRead = options?.bytesToRead !== undefined ? yield* fileResponseSize(options.bytesToRead, "bytesToRead") : undefined;
      const contentLength = bytesToRead === undefined || bytesToRead > available ? available : bytesToRead;
      const limit = bytesToRead === undefined ? undefined : offset + contentLength;
      const start = yield* fileResponseNumber(offset, "offset");
      const end = limit === undefined ? undefined : yield* fileResponseNumber(limit, "end");
      const headers = set3(optionHeaders(options), "etag", toString2(etag));
      if (isSome2(info.mtime)) {
        headers["last-modified"] = info.mtime.value.toUTCString();
      }
      return impl.fileResponse(path, options?.status ?? 200, options?.statusText, headers, start, end, contentLength);
    }),
    fileWebResponse(file, options) {
      return map7(etagGen.fromFileWeb(file), (etag) => {
        const headers = merge4(optionHeaders(options), fromRecordUnsafe({
          etag: toString2(etag),
          "last-modified": new Date(file.lastModified).toUTCString()
        }));
        return impl.fileWebResponse(file, options?.status ?? 200, options?.statusText, headers, options);
      });
    }
  });
});
var optionHeaders = (options) => {
  const headers = options?.headers ? fromInput2(options.headers) : empty6;
  return options?.contentType ? set3(headers, "content-type", options.contentType) : headers;
};
var fileResponseSize = (input, field) => {
  const size = fromInput(input);
  return isSome2(size) ? succeed7(size.value) : fail7(badArgument({
    module: "HttpPlatform",
    method: "fileResponse",
    description: `Invalid ${field}: ${input}`
  }));
};
var fileResponseNumber = (value, field) => {
  const number = Number(value);
  return Number.isSafeInteger(number) ? succeed7(number) : fail7(badArgument({
    module: "HttpPlatform",
    method: "fileResponse",
    description: `${field} exceeds the safe integer range: ${value}`
  }));
};

// node_modules/effect/dist/http/HttpServerRespondable.js
var symbol4 = "~effect/http/HttpServerRespondable";
var isRespondable = (u) => hasProperty(u, symbol4);
var badRequest = /* @__PURE__ */ empty10({
  status: 400
});
var notFound2 = /* @__PURE__ */ empty10({
  status: 404
});
var toResponseOrElse = (u, orElse) => {
  if (isHttpServerResponse(u)) {
    return succeed7(u);
  } else if (isRespondable(u)) {
    return catchCause2(u[symbol4](), () => succeed7(orElse));
  } else if (isSchemaError(u)) {
    return succeed7(badRequest);
  } else if (isNoSuchElementError2(u)) {
    return succeed7(notFound2);
  }
  return succeed7(orElse);
};
var toResponseOrElseDefect = (u, orElse) => {
  if (isHttpServerResponse(u)) {
    return succeed7(u);
  } else if (isRespondable(u)) {
    return catchCause2(u[symbol4](), () => succeed7(orElse));
  }
  return succeed7(orElse);
};

// node_modules/effect/dist/http/HttpServerError.js
var TypeId37 = "~effect/http/HttpServerError";

class HttpServerError extends (/* @__PURE__ */ TaggedError2("HttpServerError")) {
  constructor(props) {
    if ("cause" in props.reason) {
      super({
        ...props,
        cause: props.reason.cause
      });
    } else {
      super(props);
    }
  }
  [TypeId37] = TypeId37;
  stack = `${this.name}: ${this.message}`;
  get request() {
    return this.reason.request;
  }
  get response() {
    return "response" in this.reason ? this.reason.response : undefined;
  }
  [symbol4]() {
    return this.reason[symbol4]();
  }
  get [ignore4]() {
    return this.reason[ignore4] ?? false;
  }
  get message() {
    return this.reason.message;
  }
}

class RequestParseError extends (/* @__PURE__ */ TaggedError2("RequestParseError")) {
  [symbol4]() {
    return succeed7(empty10({
      status: this.reason === "BodyTooLarge" ? 413 : 400
    }));
  }
  get methodAndUrl() {
    return formatMethodAndUrl2(this.request);
  }
  get message() {
    return formatRequestMessage(this._tag, this.description, this.methodAndUrl);
  }
}
class InternalError extends (/* @__PURE__ */ TaggedError2("InternalError")) {
  [symbol4]() {
    return succeed7(empty10({
      status: 500
    }));
  }
  get methodAndUrl() {
    return formatMethodAndUrl2(this.request);
  }
  get message() {
    return formatRequestMessage(this._tag, this.description, this.methodAndUrl);
  }
}
class ResponseError extends (/* @__PURE__ */ TaggedError2("ResponseError")) {
  [symbol4]() {
    return succeed7(empty10({
      status: 500
    }));
  }
  get methodAndUrl() {
    return formatMethodAndUrl2(this.request);
  }
  get message() {
    const info = `${this._tag} (${this.response.status} ${this.methodAndUrl})`;
    return this.description ? `${info}: ${this.description}` : info;
  }
}

class ServeError extends (/* @__PURE__ */ TaggedError2("ServeError")) {
}

class ClientAbort extends (/* @__PURE__ */ Service()("effect/http/HttpServerError/ClientAbort")) {
  static annotation = /* @__PURE__ */ this.context(true).pipe(/* @__PURE__ */ add(StackTrace, {
    name: "ClientAbort",
    stack: constUndefined,
    parent: undefined
  }));
}
var formatMethodAndUrl2 = (request) => `${request.method} ${toPath(request.url)}`;
var formatRequestMessage = (reason, description, info) => {
  const prefix = `${reason} (${info})`;
  return description ? `${prefix}: ${description}` : prefix;
};
var causeResponse = (cause) => {
  let response;
  let effect = succeedInternalServerError;
  const failures = [];
  let interrupts = [];
  let isClientInterrupt = false;
  for (let i = 0;i < cause.reasons.length; i++) {
    const reason = cause.reasons[i];
    switch (reason._tag) {
      case "Fail": {
        effect = toResponseOrElse(reason.error, internalServerError);
        failures.push(reason);
        break;
      }
      case "Die": {
        if (isHttpServerResponse(reason.defect)) {
          response = reason.defect;
        } else {
          effect = toResponseOrElseDefect(reason.defect, internalServerError);
          failures.push(reason);
        }
        break;
      }
      case "Interrupt": {
        isClientInterrupt = reason.annotations.has(ClientAbort.key);
        if (failures.length > 0)
          break;
        interrupts.push(reason);
        break;
      }
    }
  }
  if (response) {
    return succeed7([response, fromReasons(failures)]);
  } else if (interrupts.length > 0 && failures.length === 0) {
    failures.push(...interrupts);
    effect = isClientInterrupt ? clientAbortError : serverAbortError;
  }
  return mapEager2(effect, (response) => {
    failures.push(makeDieReason(response));
    return [response, fromReasons(failures)];
  });
};
var causeResponseStripped = (cause) => {
  let response;
  const failures = cause.reasons.filter((f) => {
    if (f._tag === "Die" && isHttpServerResponse(f.defect)) {
      response = f.defect;
      return false;
    }
    return true;
  });
  return [response ?? internalServerError, failures.length > 0 ? some2(fromReasons(failures)) : none2()];
};
var internalServerError = /* @__PURE__ */ empty10({
  status: 500
});
var succeedInternalServerError = /* @__PURE__ */ succeed7(internalServerError);
var clientAbortError = /* @__PURE__ */ succeed7(/* @__PURE__ */ empty10({
  status: 499
}));
var serverAbortError = /* @__PURE__ */ succeed7(/* @__PURE__ */ empty10({
  status: 503
}));

// node_modules/effect/dist/socket/Socket.js
var TypeId38 = "~effect/socket/Socket";
var Socket = /* @__PURE__ */ Service("effect/socket/Socket");
var make31 = (options) => Socket.of({
  [TypeId38]: TypeId38,
  reader: options.reader,
  writer: options.writer
});
var encoder4 = /* @__PURE__ */ new TextEncoder;
var CloseEventTypeId = "~effect/socket/Socket/CloseEvent";
var isCloseEvent = (u) => hasProperty(u, CloseEventTypeId);
var SocketErrorTypeId = "~effect/socket/Socket/SocketError";
var isSocketError = (u) => hasProperty(u, SocketErrorTypeId);

class SocketReadError extends (/* @__PURE__ */ Error4("effect/socket/Socket/SocketReadError")({
  _tag: /* @__PURE__ */ tag("SocketReadError"),
  cause: /* @__PURE__ */ Defect()
})) {
  message = `An error occurred during Read`;
}

class SocketWriteError extends (/* @__PURE__ */ Error4("effect/socket/Socket/SocketWriteError")({
  _tag: /* @__PURE__ */ tag("SocketWriteError"),
  cause: /* @__PURE__ */ Defect()
})) {
  message = `An error occurred during Write`;
}

class SocketOpenError extends (/* @__PURE__ */ Error4("effect/socket/Socket/SocketOpenError")({
  _tag: /* @__PURE__ */ tag("SocketOpenError"),
  kind: /* @__PURE__ */ Literals(["Unknown", "Timeout"]),
  cause: /* @__PURE__ */ Defect()
})) {
  get message() {
    return this.kind === "Timeout" ? `timeout waiting for "open"` : `An error occurred during Open`;
  }
}

class SocketUpgradeError extends (/* @__PURE__ */ Error4("effect/socket/Socket/SocketUpgradeError")({
  _tag: /* @__PURE__ */ tag("SocketUpgradeError"),
  cause: /* @__PURE__ */ optional2(/* @__PURE__ */ Defect())
})) {
  static unsupported = () => fail7(new SocketError({
    reason: new SocketUpgradeError({})
  }));
  get message() {
    return this.cause === undefined ? `Socket does not support TLS upgrade` : `An error occurred during TLS upgrade`;
  }
}

class SocketCloseError extends (/* @__PURE__ */ Error4("effect/socket/Socket/SocketCloseError")({
  _tag: /* @__PURE__ */ tag("SocketCloseError"),
  code: Int,
  closeReason: /* @__PURE__ */ optional2(String5)
})) {
  get message() {
    if (this.closeReason) {
      return `${this.code}: ${this.closeReason}`;
    }
    return `${this.code}`;
  }
}
var SocketErrorReason = /* @__PURE__ */ Union2([SocketReadError, SocketWriteError, SocketOpenError, SocketUpgradeError, SocketCloseError]);

class SocketError extends (/* @__PURE__ */ TaggedError3(SocketErrorTypeId)("SocketError", {
  _tag: /* @__PURE__ */ tag("SocketError"),
  reason: SocketErrorReason
})) {
  constructor(props) {
    if ("cause" in props.reason) {
      super({
        ...props,
        cause: props.reason.cause
      });
    } else {
      super(props);
    }
  }
  [SocketErrorTypeId] = SocketErrorTypeId;
  static is(u) {
    return isSocketError(u);
  }
  message = this.reason.message;
}
var closeError = (code, closeReason) => new SocketError({
  reason: new SocketCloseError({
    code,
    closeReason
  })
});
var isPausable = (ws) => ("pause" in ws) && typeof ws.pause === "function";
var fromWebSocket = (acquire, options) => withFiber2((fiber) => {
  let currentWS;
  const latch = makeUnsafe5(false);
  const acquireContext = fiber.context;
  const reader = gen2(function* () {
    const scope = yield* scope2;
    const ws = yield* provide(acquire, scope);
    if ("binaryType" in ws) {
      ws.binaryType = "arraybuffer";
    }
    const pausable = isPausable(ws);
    const highWaterMark = options?.highWaterMark ?? (pausable ? defaultHighWaterMark : undefined);
    let open = ws.readyState === 1;
    let paused = pausable && "isPaused" in ws && ws.isPaused === true;
    let buffer = [];
    let bufferSize = 0;
    let error;
    let waiter;
    let openWaiter;
    function pauseWebSocket() {
      if (!pausable || paused)
        return;
      ws.pause();
      paused = true;
    }
    function resumeWebSocket() {
      if (!pausable || !paused)
        return;
      ws.resume();
      paused = false;
    }
    function takeBuffer() {
      const chunk = buffer;
      buffer = [];
      bufferSize = 0;
      if (error === undefined)
        resumeWebSocket();
      return chunk;
    }
    function deliver() {
      if (waiter === undefined || buffer.length === 0)
        return;
      const resume = waiter;
      waiter = undefined;
      resume(succeed7(takeBuffer()));
    }
    function push(data) {
      buffer.push(data);
      if (highWaterMark !== undefined) {
        bufferSize += typeof data === "string" ? encoder4.encode(data).byteLength : data.byteLength;
      }
      if (waiter !== undefined)
        deliver();
      if (pausable && bufferSize >= highWaterMark) {
        pauseWebSocket();
      } else if (!pausable && !waiter && highWaterMark !== undefined && bufferSize > highWaterMark) {
        fail(new SocketError({
          reason: new SocketReadError({
            cause: new Error(`Socket highWaterMark of ${highWaterMark} bytes exceeded`)
          })
        }));
      }
    }
    function fail(err) {
      if (error === undefined)
        error = err;
      if (openWaiter !== undefined) {
        const {
          cleanup,
          resume
        } = openWaiter;
        openWaiter = undefined;
        cleanup();
        resume(fail7(error));
      }
      if (waiter !== undefined) {
        const resume = waiter;
        waiter = undefined;
        resume(buffer.length > 0 ? succeed7(takeBuffer()) : fail7(error));
      }
    }
    function onMessage(event) {
      const data = event.data;
      if (typeof Blob !== "undefined" && data instanceof Blob) {
        data.arrayBuffer().then((buf) => push(new Uint8Array(buf)), (cause) => {
          fail(new SocketError({
            reason: new SocketReadError({
              cause
            })
          }));
        });
        return;
      }
      push(data instanceof ArrayBuffer ? new Uint8Array(data) : data);
    }
    function onError(event) {
      fail(new SocketError({
        reason: open ? new SocketReadError({
          cause: event
        }) : new SocketOpenError({
          kind: "Unknown",
          cause: event
        })
      }));
    }
    function onClose(event) {
      fail(closeError(typeof event.code === "number" ? event.code : 1006, event.reason));
    }
    ws.addEventListener("message", onMessage);
    ws.addEventListener("error", onError, {
      once: true
    });
    ws.addEventListener("close", onClose, {
      once: true
    });
    yield* addFinalizer2(scope, sync2(() => {
      fail(closeError(1006));
      ws.removeEventListener("message", onMessage);
      ws.addEventListener("error", constVoid);
      ws.removeEventListener("error", onError);
      ws.removeEventListener("close", onClose);
      if (pausable) {
        try {
          resumeWebSocket();
        } catch {}
      }
      latch.closeUnsafe();
      currentWS = undefined;
    }));
    if (ws.readyState >= 2) {
      return yield* fail7(error ?? closeError(1006));
    }
    if (!open) {
      yield* callback2((resume) => {
        const onOpen = () => {
          if (openWaiter?.resume !== resume)
            return;
          openWaiter = undefined;
          ws.removeEventListener("open", onOpen);
          resume(void_3);
        };
        const cleanup = () => ws.removeEventListener("open", onOpen);
        openWaiter = {
          resume,
          cleanup
        };
        ws.addEventListener("open", onOpen, {
          once: true
        });
        return sync2(() => {
          if (openWaiter?.resume === resume)
            openWaiter = undefined;
          cleanup();
        });
      }).pipe(timeoutOrElse2({
        duration: options?.openTimeout ?? 1e4,
        orElse: () => fail7(new SocketError({
          reason: new SocketOpenError({
            kind: "Timeout",
            cause: new Error('timeout waiting for "open"')
          })
        }))
      }));
      open = true;
    }
    if (error === undefined && bufferSize < highWaterMark)
      resumeWebSocket();
    currentWS = ws;
    latch.openUnsafe();
    return {
      pull: callback2((resume) => {
        if (buffer.length > 0)
          return resume(succeed7(takeBuffer()));
        if (error !== undefined)
          return resume(fail7(error));
        waiter = resume;
        return sync2(() => {
          if (waiter === resume)
            waiter = undefined;
        });
      }),
      upgrade: SocketUpgradeError.unsupported
    };
  }).pipe(updateContext2((input) => merge(acquireContext, input)));
  const write = (chunk) => suspend2(() => {
    const ws = currentWS;
    if (ws === undefined)
      return latch.whenOpen(write(chunk));
    try {
      if (isCloseEvent(chunk)) {
        ws.close(chunk.code, chunk.reason);
      } else {
        ws.send(chunk);
      }
      return void_3;
    } catch (cause) {
      return fail7(new SocketError({
        reason: new SocketWriteError({
          cause
        })
      }));
    }
  });
  const writeAll = (chunks) => suspend2(() => {
    const ws = currentWS;
    if (ws === undefined)
      return latch.whenOpen(writeAll(chunks));
    try {
      for (let i = 0;i < chunks.length; i++) {
        ws.send(chunks[i]);
      }
      return void_3;
    } catch (cause) {
      return fail7(new SocketError({
        reason: new SocketWriteError({
          cause
        })
      }));
    }
  });
  const writer = succeed7({
    write,
    writeAll
  });
  return succeed7(make31({
    reader,
    writer
  }));
});
var defaultHighWaterMark = 64 * 1024;

// node_modules/effect/dist/http/MultipartParser/internal/contentType.js
var paramRE = /; *([!#$%&'*+.^\w`|~-]+)=("(?:[\t\u0020\u0021\u0023-\u005b\u005d-\u007e\u0080-\u{10ffff}]|\\[\t\u0020-\u{10ffff}])*"|[!#$%&'*+.^\w`|~-]+) */gu;
var dispositionParamRE = /;[ \t]*([!#$%&'*+.^\w`|~-]+)[ \t]*=[ \t]*("(?:[\t\u0020\u0021\u0023-\u005b\u005d-\u007e\u0080-\u{10ffff}]|\\[\t\u0020-\u{10ffff}])*"|[!#$%&'*+.^\w`|~-]+)[ \t]*/gu;
var quotedPairRE = /\\([\t\u0020-\u{10ffff}])/gu;
var mediaTypeRE = /^[!#$%&'*+.^\w|~-]+\/[!#$%&'*+.^\w|~-]+$/u;
var dispositionTypeRE = /^[!#$%&'*+.^\w|~-]+$/u;
var defaultContentType = {
  value: "",
  parameters: /* @__PURE__ */ Object.create(null)
};
function parse(header) {
  if (typeof header !== "string") {
    return defaultContentType;
  }
  let index = header.indexOf(";");
  const type = index !== -1 ? header.slice(0, index).trim() : header.trim();
  if (mediaTypeRE.test(type) === false) {
    return defaultContentType;
  }
  const result = {
    value: type.toLowerCase(),
    parameters: Object.create(null)
  };
  if (index === -1) {
    return result;
  }
  let key;
  let match;
  let value;
  paramRE.lastIndex = index;
  while (match = paramRE.exec(header)) {
    if (match.index !== index) {
      return defaultContentType;
    }
    index += match[0].length;
    key = match[1].toLowerCase();
    value = match[2];
    if (value[0] === '"') {
      value = value.slice(1, value.length - 1);
      if (quotedPairRE.test(value)) {
        value = value.replace(quotedPairRE, "$1");
      }
    }
    result.parameters[key] = value;
  }
  if (index !== header.length) {
    return defaultContentType;
  }
  return result;
}
var extValueRE = /^(utf-8|iso-8859-1)'[a-z0-9-]*'((?:%[0-9a-f]{2}|[!#$&+.^_`|~\w-])*)$/i;
var pctEncodedRE = /%([0-9a-f]{2})/gi;
function decodeExtValue(value) {
  const match = extValueRE.exec(value);
  if (match === null) {
    return;
  }
  if (match[1].toLowerCase() === "iso-8859-1") {
    return match[2].replace(pctEncodedRE, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
  }
  try {
    return decodeURIComponent(match[2]);
  } catch {
    return;
  }
}
var defaultContentDisposition = {
  ...defaultContentType,
  filename: undefined
};
function parseDisposition(header) {
  if (header === undefined) {
    return defaultContentDisposition;
  } else if (typeof header !== "string") {
    return;
  }
  let index = header.indexOf(";");
  const type = index !== -1 ? header.slice(0, index).trim() : header.trim();
  if (dispositionTypeRE.test(type) === false) {
    return;
  }
  const parameters = Object.create(null);
  if (index !== -1) {
    let match;
    dispositionParamRE.lastIndex = index;
    while (match = dispositionParamRE.exec(header)) {
      if (match.index !== index) {
        return;
      }
      index += match[0].length;
      const key = match[1].toLowerCase();
      const value = match[2];
      if (key in parameters) {
        return;
      }
      parameters[key] = value[0] === '"' ? value.slice(1, value.length - 1) : value;
    }
    if (index !== header.length) {
      return;
    }
  }
  let filename = parameters.filename;
  if ("filename*" in parameters) {
    filename = decodeExtValue(parameters["filename*"]);
    if (filename === undefined) {
      return;
    }
  }
  return {
    value: type.toLowerCase(),
    parameters,
    filename
  };
}

// node_modules/effect/dist/http/MultipartParser/internal/headers.js
var constMaxPairs = 100;
var constMaxSize = 16 * 1024;
var State = {
  key: 0,
  whitespace: 1,
  value: 2
};
var constContinue = {
  _tag: "Continue"
};
var constNameChars = /* @__PURE__ */ new Uint8Array(256);
for (const char of "!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") {
  constNameChars[/* @__PURE__ */ char.charCodeAt(0)] = 1;
}
var constValueChars = /* @__PURE__ */ new Uint8Array(256);
constValueChars[9] = 1;
for (let i = 32;i <= 126; i++) {
  constValueChars[i] = 1;
}
for (let i = 128;i <= 255; i++) {
  constValueChars[i] = 1;
}
function make32() {
  const decoder = new TextDecoder;
  const state = {
    state: State.key,
    headers: Object.create(null),
    key: "",
    value: undefined,
    crlf: 0,
    previousChunk: undefined,
    pairs: 0,
    size: 0
  };
  function reset(value) {
    state.state = State.key;
    state.headers = Object.create(null);
    state.key = "";
    state.value = undefined;
    state.crlf = 0;
    state.previousChunk = undefined;
    state.pairs = 0;
    state.size = 0;
    return value;
  }
  function error(reason) {
    return reset({
      _tag: "Failure",
      reason,
      headers: state.headers
    });
  }
  return function write(chunk, start) {
    let endOffset = 0;
    let previousCursor;
    if (state.previousChunk !== undefined) {
      endOffset = state.previousChunk.length;
      previousCursor = endOffset;
      chunk = concat([state.previousChunk, chunk]);
      state.previousChunk = undefined;
    }
    const end = chunk.length;
    outer:
      while (start < end) {
        if (state.state === State.key) {
          let i = start;
          for (;i < end; i++) {
            if (state.size++ > constMaxSize) {
              return error("HeaderTooLarge");
            }
            if (chunk[i] === 58) {
              state.key += decoder.decode(chunk.subarray(start, i)).toLowerCase();
              if (state.key.length === 0) {
                return error("InvalidHeaderName");
              }
              if (chunk[i + 1] === 32 && chunk[i + 2] !== 32 && chunk[i + 2] !== 9) {
                start = i + 2;
                state.state = State.value;
                state.size++;
              } else if (chunk[i + 1] !== 32 && chunk[i + 1] !== 9) {
                start = i + 1;
                state.state = State.value;
              } else {
                start = i + 1;
                state.state = State.whitespace;
              }
              break;
            } else if (constNameChars[chunk[i]] !== 1) {
              return error("InvalidHeaderName");
            }
          }
          if (i === end) {
            state.key += decoder.decode(chunk.subarray(start, end)).toLowerCase();
            return constContinue;
          }
        }
        if (state.state === State.whitespace) {
          for (;start < end; start++) {
            if (state.size++ > constMaxSize) {
              return error("HeaderTooLarge");
            }
            if (chunk[start] !== 32 && chunk[start] !== 9) {
              state.state = State.value;
              break;
            }
          }
          if (start === end) {
            return constContinue;
          }
        }
        if (state.state === State.value) {
          let i = start;
          if (previousCursor !== undefined) {
            i = previousCursor;
            previousCursor = undefined;
          }
          for (;i < end; i++) {
            if (state.size++ > constMaxSize) {
              return error("HeaderTooLarge");
            }
            if (chunk[i] === 13 || state.crlf > 0) {
              let byte = chunk[i];
              if (byte === 13 && state.crlf === 0) {
                state.crlf = 1;
                i++;
                state.size++;
                byte = chunk[i];
              }
              if (byte === 10 && state.crlf === 1) {
                state.crlf = 2;
                i++;
                state.size++;
                byte = chunk[i];
              }
              if (byte === 13 && state.crlf === 2) {
                state.crlf = 3;
                i++;
                state.size++;
                byte = chunk[i];
              }
              if (byte === 10 && state.crlf === 3) {
                state.crlf = 4;
                i++;
                state.size++;
              }
              if (state.crlf < 4 && i >= end) {
                state.previousChunk = chunk.subarray(start);
                return constContinue;
              } else if (state.crlf >= 2) {
                state.value = state.value === undefined ? chunk.subarray(start, i - state.crlf) : concat([state.value, chunk.subarray(start, i - state.crlf)]);
                const value = decoder.decode(state.value);
                if (state.headers[state.key] === undefined) {
                  state.headers[state.key] = value;
                } else if (typeof state.headers[state.key] === "string") {
                  state.headers[state.key] = [state.headers[state.key], value];
                } else {
                  state.headers[state.key].push(value);
                }
                start = i;
                state.size--;
                if (state.crlf !== 4 && state.pairs === constMaxPairs) {
                  return error("TooManyHeaders");
                } else if (state.crlf === 3) {
                  return error("InvalidHeaderValue");
                } else if (state.crlf === 4) {
                  return reset({
                    _tag: "Headers",
                    headers: state.headers,
                    endPosition: start - endOffset
                  });
                }
                state.pairs++;
                state.key = "";
                state.value = undefined;
                state.crlf = 0;
                state.state = State.key;
                continue outer;
              }
            } else if (constValueChars[chunk[i]] !== 1) {
              return error("InvalidHeaderValue");
            }
          }
          if (i === end) {
            state.value = state.value === undefined ? chunk.subarray(start, end) : concat([state.value, chunk.subarray(start, end)]);
            return constContinue;
          }
        }
      }
    if (start > end) {
      state.size += end - start;
    }
    return constContinue;
  };
}

// node_modules/effect/dist/http/MultipartParser/internal/search.js
function makeState(needle_) {
  const needle = new TextEncoder().encode(needle_);
  const needleLength = needle.length;
  const indexes = {};
  for (let i = 0;i < needleLength; i++) {
    const b = needle[i];
    if (indexes[b] === undefined)
      indexes[b] = [];
    indexes[b].push(i);
  }
  return {
    needle,
    needleLength,
    indexes,
    firstByte: needle[0],
    previousChunk: undefined,
    previousChunkLength: 0,
    matchIndex: 0
  };
}
function make33(needle, callback, seed, minimumChunkLength) {
  const state = makeState(needle);
  const minChunkLength = minimumChunkLength ?? state.needleLength;
  if (seed !== undefined) {
    state.previousChunk = seed;
    state.previousChunkLength = seed.length;
  }
  function makeIndexOf() {
    if ("Buffer" in globalThis && !(("Bun" in globalThis) || ("Deno" in globalThis))) {
      return function(chunk, needle, fromIndex) {
        return Buffer.prototype.indexOf.call(chunk, needle, fromIndex);
      };
    }
    const skipTable = new Uint8Array(256).fill(state.needle.length);
    for (let i = 0, lastIndex = state.needle.length - 1;i < lastIndex; ++i) {
      skipTable[state.needle[i]] = lastIndex - i;
    }
    return function(chunk, needle, fromIndex) {
      const lengthTotal = chunk.length;
      let i = fromIndex + state.needleLength - 1;
      while (i < lengthTotal) {
        for (let j = state.needleLength - 1, k = i;j >= 0 && chunk[k] === needle[j]; j--, k--) {
          if (j === 0)
            return k;
        }
        i += skipTable[chunk[i]];
      }
      return -1;
    };
  }
  const indexOf = makeIndexOf();
  function write(chunk) {
    let chunkLength = chunk.length;
    if (state.previousChunk !== undefined) {
      const newChunk = new Uint8Array(state.previousChunkLength + chunkLength);
      newChunk.set(state.previousChunk);
      newChunk.set(chunk, state.previousChunkLength);
      chunk = newChunk;
      chunkLength = state.previousChunkLength + chunkLength;
      state.previousChunk = undefined;
    }
    let pos = 0;
    while (pos < chunkLength) {
      const remaining = chunkLength - pos;
      if (remaining < minChunkLength) {
        state.previousChunk = chunk.subarray(pos);
        state.previousChunkLength = remaining;
        return;
      }
      const match = indexOf(chunk, state.needle, pos);
      if (match > -1) {
        if (match > pos) {
          callback(state.matchIndex, chunk.subarray(pos, match));
        }
        state.matchIndex += 1;
        pos = match + state.needleLength;
        continue;
      } else if (chunk[chunkLength - 1] in state.indexes) {
        const indexes = state.indexes[chunk[chunkLength - 1]];
        let earliestIndex = -1;
        for (let i = 0, len = indexes.length;i < len; i++) {
          const index = indexes[i];
          if (chunk[chunkLength - 1 - index] === state.firstByte && i > earliestIndex) {
            earliestIndex = index;
          }
        }
        if (earliestIndex === -1) {
          if (pos === 0) {
            callback(state.matchIndex, chunk);
          } else {
            callback(state.matchIndex, chunk.subarray(pos));
          }
        } else {
          if (chunkLength - 1 - earliestIndex > pos) {
            callback(state.matchIndex, chunk.subarray(pos, chunkLength - 1 - earliestIndex));
          }
          state.previousChunk = chunk.subarray(chunkLength - 1 - earliestIndex);
          state.previousChunkLength = earliestIndex + 1;
        }
      } else if (pos === 0) {
        callback(state.matchIndex, chunk);
      } else {
        callback(state.matchIndex, chunk.subarray(pos));
      }
      break;
    }
  }
  function end() {
    if (state.previousChunk !== undefined && state.previousChunk !== seed) {
      callback(state.matchIndex, state.previousChunk);
    }
    state.previousChunk = seed;
    state.previousChunkLength = seed?.length ?? 0;
    state.matchIndex = 0;
  }
  return {
    write,
    end
  };
}

// node_modules/effect/dist/http/MultipartParser/internal/multipart.js
var State2 = {
  headers: 0,
  body: 1
};
var errInvalidDisposition = {
  _tag: "InvalidDisposition"
};
var errEndNotReached = {
  _tag: "EndNotReached"
};
var errMaxParts = {
  _tag: "ReachedLimit",
  limit: "MaxParts"
};
var errMaxTotalSize = {
  _tag: "ReachedLimit",
  limit: "MaxTotalSize"
};
var errMaxPartSize = {
  _tag: "ReachedLimit",
  limit: "MaxPartSize"
};
var errMaxFieldSize = {
  _tag: "ReachedLimit",
  limit: "MaxFieldSize"
};
var constCR = /* @__PURE__ */ new TextEncoder().encode(`\r
`);
function defaultIsFile(info) {
  return info.filename !== undefined || info.contentType === "application/octet-stream";
}
function parseBoundary(headers) {
  const contentType = parse(headers["content-type"]);
  return contentType.parameters.boundary;
}
function noopOnChunk(_chunk) {}
var toLimit = (input) => input === Infinity ? Infinity : Number(fromInputUnsafe2(input));
function make34({
  headers,
  onFile: onPart,
  onField,
  onError,
  onDone,
  isFile = defaultIsFile,
  maxParts = Infinity,
  maxTotalSize: maxTotalSizeInput = Infinity,
  maxPartSize: maxPartSizeInput = Infinity,
  maxFieldSize: maxFieldSizeInput = 1024 * 1024
}) {
  const maxTotalSize = toLimit(maxTotalSizeInput);
  const maxPartSize = toLimit(maxPartSizeInput);
  const maxFieldSize = toLimit(maxFieldSizeInput);
  const boundary = parseBoundary(headers);
  if (boundary === undefined) {
    onError({
      _tag: "InvalidBoundary"
    });
    return {
      write: noopOnChunk,
      end() {}
    };
  }
  const state = {
    state: State2.headers,
    index: 0,
    parts: 0,
    onChunk: noopOnChunk,
    info: undefined,
    headerSkip: 0,
    partSize: 0,
    totalSize: 0,
    isFile: false,
    fieldChunks: [],
    fieldSize: 0,
    done: false,
    stopped: false
  };
  function skipBody() {
    state.state = State2.body;
    state.isFile = true;
    state.onChunk = noopOnChunk;
  }
  function stop(error) {
    state.stopped = true;
    if (state.state === State2.body && state.isFile) {
      state.onChunk(null);
    }
    onError(error);
  }
  const headerParser = make32();
  const split = make33(`\r
--${boundary}`, function(index, chunk) {
    if (state.stopped) {
      return;
    }
    if (index === 0) {
      skipBody();
      return;
    } else if (index !== state.index) {
      if (state.index > 0) {
        if (state.isFile) {
          state.onChunk(null);
        } else {
          onField(state.info, state.fieldChunks.length === 1 ? state.fieldChunks[0] : concat(state.fieldChunks));
          state.fieldSize = 0;
          state.fieldChunks = [];
        }
      }
      state.partSize = 0;
      state.state = State2.headers;
      state.index = index;
      state.headerSkip = 2;
      if (chunk[0] === 45 && chunk[1] === 45) {
        state.done = true;
        return onDone();
      }
      state.parts++;
      if (state.parts > maxParts) {
        return stop(errMaxParts);
      }
    }
    if ((state.partSize += chunk.length) > maxPartSize) {
      return stop(errMaxPartSize);
    }
    if (state.state === State2.headers) {
      const result = headerParser(chunk, state.headerSkip);
      state.headerSkip = 0;
      if (result._tag === "Continue") {
        return;
      } else if (result._tag === "Failure") {
        skipBody();
        return onError({
          _tag: "BadHeaders",
          error: result
        });
      }
      const contentType = parse(result.headers["content-type"]);
      const contentDisposition = parseDisposition(result.headers["content-disposition"]);
      if (contentDisposition === undefined || contentDisposition.value === "form-data" && !("name" in contentDisposition.parameters)) {
        skipBody();
        return onError(errInvalidDisposition);
      }
      state.info = {
        name: contentDisposition.parameters.name ?? "",
        filename: contentDisposition.filename,
        contentType: contentType.value === "" ? contentDisposition.filename !== undefined ? "application/octet-stream" : "text/plain" : contentType.value,
        contentTypeParameters: contentType.parameters,
        contentDisposition: contentDisposition.value,
        contentDispositionParameters: contentDisposition.parameters,
        headers: result.headers
      };
      state.state = State2.body;
      state.isFile = isFile(state.info);
      if (state.isFile) {
        state.onChunk = onPart(state.info);
      }
      if (result.endPosition < chunk.length) {
        if (state.isFile) {
          state.onChunk(chunk.subarray(result.endPosition));
        } else {
          const buf = chunk.subarray(result.endPosition);
          if ((state.fieldSize += buf.length) > maxFieldSize) {
            return stop(errMaxFieldSize);
          }
          state.fieldChunks.push(buf);
        }
      }
    } else if (state.isFile) {
      state.onChunk(chunk);
    } else {
      if ((state.fieldSize += chunk.length) > maxFieldSize) {
        return stop(errMaxFieldSize);
      }
      state.fieldChunks.push(chunk);
    }
  }, constCR, 2);
  return {
    write(chunk) {
      if (state.stopped) {
        return;
      }
      if ((state.totalSize += chunk.length) > maxTotalSize) {
        return stop(errMaxTotalSize);
      }
      return split.write(chunk);
    },
    end() {
      split.end();
      if (!state.done && !state.stopped) {
        stop(errEndNotReached);
      }
      state.state = State2.headers;
      state.index = 0;
      state.parts = 0;
      state.onChunk = noopOnChunk;
      state.info = undefined;
      state.totalSize = 0;
      state.partSize = 0;
      state.fieldChunks = [];
      state.fieldSize = 0;
      state.done = false;
      state.stopped = false;
    }
  };
}
var utf8Decoder = /* @__PURE__ */ new TextDecoder("utf-8");
function getDecoder(charset) {
  if (charset === "utf-8" || charset === "utf8" || charset === "") {
    return utf8Decoder;
  }
  try {
    return new TextDecoder(charset);
  } catch (error) {
    return utf8Decoder;
  }
}
function decodeField(info, value) {
  return getDecoder(info.contentTypeParameters.charset ?? "utf-8").decode(value);
}

// node_modules/effect/dist/http/MultipartParser.js
var make35 = make34;
var defaultIsFile2 = defaultIsFile;
var decodeField2 = decodeField;

// node_modules/effect/dist/http/Multipart.js
var TypeId39 = "~effect/http/Multipart";
var MultipartErrorTypeId = "~effect/http/Multipart/MultipartError";
var toMultipartError = (cause) => isMultipartError(cause) ? cause : MultipartError.fromReason("InternalError", cause);

class MultipartErrorReason extends Error3 {
}
var responseStatusByReason = {
  FileTooLarge: 413,
  FieldTooLarge: 413,
  BodyTooLarge: 413,
  TooManyParts: 413,
  InternalError: 500,
  Parse: 400
};

class MultipartError extends (/* @__PURE__ */ TaggedError2("MultipartError")) {
  static fromReason(reason, cause) {
    return new MultipartError({
      reason: new MultipartErrorReason({
        _tag: reason,
        cause
      })
    });
  }
  [MultipartErrorTypeId] = MultipartErrorTypeId;
  [ignore4] = true;
  [symbol4]() {
    return succeed7(empty10({
      status: responseStatusByReason[this.reason._tag]
    }));
  }
  get message() {
    return this.reason._tag;
  }
}
var isMultipartError = (u) => hasProperty(u, MultipartErrorTypeId);
var makeConfig = (headers) => withFiber2((fiber) => {
  const mimeTypes = get(fiber.context, FieldMimeTypes);
  return succeed7({
    headers,
    maxParts: fiber.getRef(MaxParts),
    maxFieldSize: fiber.getRef(MaxFieldSize),
    maxPartSize: fiber.getRef(MaxFileSize),
    maxTotalSize: fiber.getRef(MaxBodySize),
    isFile: mimeTypes.length === 0 ? undefined : (info) => !mimeTypes.some((_) => info.contentType.includes(_)) && defaultIsFile2(info)
  });
});
class PartBase extends Class2 {
  [TypeId39];
  constructor() {
    super();
    this[TypeId39] = TypeId39;
  }
}
var defaultWriteFile = (path, file) => flatMap4(FileSystem, (fs) => mapError3(run(file.content, fs.sink(path)), toMultipartError));
var maxPersistedFileNameBytes = 128;
var fileNameEncoder = /* @__PURE__ */ new TextEncoder;
var persistedFileName = (baseName) => {
  const codePoints = Array.from(baseName);
  let fileName = "";
  let bytes = 0;
  for (let i = codePoints.length - 1;i >= 0; i--) {
    bytes += fileNameEncoder.encode(codePoints[i]).length;
    if (bytes > maxPersistedFileNameBytes)
      break;
    fileName = codePoints[i] + fileName;
  }
  return fileName === "" || fileName === "." || fileName === ".." ? "file" : fileName;
};
var toPersisted = (stream, writeFile = defaultWriteFile) => gen2(function* () {
  const fs = yield* FileSystem;
  const path_ = yield* Path;
  const dir = yield* fs.makeTempDirectoryScoped();
  const persisted = Object.create(null);
  const usedPaths = new Set;
  let fileIndex = 0;
  yield* runForEach2(stream, (part) => {
    if (part._tag === "Field") {
      if (!(part.key in persisted)) {
        persisted[part.key] = part.value;
      } else if (typeof persisted[part.key] === "string") {
        persisted[part.key] = [persisted[part.key], part.value];
      } else {
        persisted[part.key].push(part.value);
      }
      return void_3;
    } else if (part.name === "") {
      return void_3;
    }
    const file = part;
    const fileName = persistedFileName(path_.basename(file.name));
    let path = path_.join(dir, fileName);
    while (usedPaths.has(path)) {
      path = path_.join(dir, `${fileIndex++}-${fileName}`);
    }
    usedPaths.add(path);
    const filePart = new PersistedFileImpl(file.key, file.name, file.contentType, path);
    if (Array.isArray(persisted[part.key])) {
      persisted[part.key].push(filePart);
    } else {
      persisted[part.key] = [filePart];
    }
    return writeFile(path, file);
  });
  return persisted;
}).pipe(catchTag2("PlatformError", (cause) => fail7(MultipartError.fromReason("InternalError", cause))));

class PersistedFileImpl extends PartBase {
  _tag = "PersistedFile";
  key;
  name;
  contentType;
  path;
  constructor(key, name, contentType, path) {
    super();
    this.key = key;
    this.name = name;
    this.contentType = contentType;
    this.path = path;
  }
  toJSON() {
    return {
      _id: "@effect/platform/Multipart/Part",
      _tag: "PersistedFile",
      key: this.key,
      name: this.name,
      contentType: this.contentType,
      path: this.path
    };
  }
}
var MaxParts = /* @__PURE__ */ Reference("effect/http/Multipart/MaxParts", {
  defaultValue: () => {
    return;
  }
});
var MaxFieldSize = /* @__PURE__ */ Reference("effect/http/Multipart/MaxFieldSize", {
  defaultValue: /* @__PURE__ */ constant(/* @__PURE__ */ mebibytes(10))
});
var MaxFileSize = /* @__PURE__ */ Reference("effect/http/Multipart/MaxFileSize", {
  defaultValue: () => {
    return;
  }
});
var FieldMimeTypes = /* @__PURE__ */ Reference("effect/http/Multipart/FieldMimeTypes", {
  defaultValue: /* @__PURE__ */ constant(["application/json"])
});

// node_modules/effect/dist/http/HttpServerRequest.js
var TypeId40 = "~effect/http/HttpServerRequest";
var HttpServerRequest = /* @__PURE__ */ Service("effect/http/HttpServerRequest");
var toURL = (self) => {
  const host = self.headers.host ?? "localhost";
  const protocol = self.protocol ?? (/^https:/i.test(self.originalUrl) ? "https" : "http");
  try {
    return some2(new URL(self.url.startsWith("/") ? `.${self.url}` : self.url, `${protocol}://${host}`));
  } catch {
    return none2();
  }
};

// node_modules/effect/dist/http/internal/requestState.js
class RequestState {
  preResponseHandler = undefined;
  loggerDisabled = false;
}
var CurrentRequestState = /* @__PURE__ */ Service("effect/http/HttpEffect/CurrentRequestState");

// node_modules/effect/dist/http/HttpMiddleware.js
var make36 = (middleware) => middleware;
var TracerDisabledWhen2 = /* @__PURE__ */ Reference("effect/http/HttpMiddleware/TracerDisabledWhen", {
  defaultValue: () => constFalse
});
var SpanNameGenerator2 = /* @__PURE__ */ Reference("@effect/platform/HttpMiddleware/SpanNameGenerator", {
  defaultValue: () => (request) => request.method
});
var TracerHeaderFilter2 = /* @__PURE__ */ Reference("effect/http/HttpMiddleware/TracerHeaderFilter", {
  defaultValue: () => constFalse
});
var isTracerDisabledUnsafe = (fiber, request) => !fiber.cache.tracerEnabled || fiber.getRef(TracerDisabledWhen2)(request);
var responseSpanExit = (fiber, request, response) => {
  if (!(response.status >= 500 && response.status < 600)) {
    return succeed4(response);
  }
  const url = redactUrl(request.url, fiber.getRef(CurrentRedactedNames2));
  if (url !== request.url) {
    request = request.modify({
      url
    });
  }
  const stackTraceLimit = getStackTraceLimit();
  setStackTraceLimit(0);
  try {
    return fail5(new HttpServerError({
      reason: new ResponseError({
        request,
        response
      })
    }));
  } finally {
    setStackTraceLimit(stackTraceLimit);
  }
};
var tracer2 = /* @__PURE__ */ make36((httpApp) => withFiber2((fiber) => {
  const request = getUnsafe(fiber.context, HttpServerRequest);
  if (isTracerDisabledUnsafe(fiber, request)) {
    return httpApp;
  }
  const nameGenerator = fiber.getRef(SpanNameGenerator2);
  const span = makeSpanUnsafe(fiber, nameGenerator(request), {
    parent: getOrUndefined(fromHeaders(request.headers)),
    kind: "server"
  });
  const prevServices = fiber.context;
  fiber.setContext(add(fiber.context, ParentSpan, span));
  onExitUnsafe(fiber, (exit) => {
    fiber.setContext(prevServices);
    const endTime = fiber.getRef(Clock).currentTimeNanosUnsafe();
    if (isSuccess3(exit) && (!span.sampled || fiber.getRef(Tracer) === nativeTracer)) {
      span.end(endTime, responseSpanExit(fiber, request, exit.value));
      return;
    }
    const redactedHeaderNames = fiber.getRef(CurrentRedactedNames);
    const headerFilter = fiber.getRef(TracerHeaderFilter2);
    let response;
    let spanExit;
    if (isFailure3(exit)) {
      const [failureResponse, cause] = causeResponseStripped(exit.cause);
      response = failureResponse;
      spanExit = isSome2(cause) && (response.status >= 500 || hasInterrupts2(cause.value)) ? failCause2(cause.value) : responseSpanExit(fiber, request, response);
    } else {
      response = exit.value;
      spanExit = responseSpanExit(fiber, request, response);
    }
    if (span.sampled) {
      span.attribute("http.request.method", request.method);
      const url = toURL(request);
      if (isSome2(url)) {
        addUrlAttributes(span, url.value, fiber.getRef(CurrentRedactedNames2));
      }
      if (request.headers["user-agent"] !== undefined) {
        span.attribute("user_agent.original", request.headers["user-agent"]);
      }
      addHeaderAttributes(span, "request", request.headers, headerFilter, redactedHeaderNames);
      if (isSome2(request.remoteAddress)) {
        span.attribute("client.address", request.remoteAddress.value);
      }
      span.attribute("http.response.status_code", response.status);
      addHeaderAttributes(span, "response", response.headers, headerFilter, redactedHeaderNames);
    }
    span.end(endTime, spanExit);
    return;
  });
  return httpApp;
}));

// node_modules/effect/dist/http/HttpEffect.js
var toHandled = (self, handleResponse, middleware) => {
  const handleCause = (frame, cause) => {
    frame.cause = cause;
    const request = frame.request;
    return flatMapEager2(causeResponse(cause), ([response, cause]) => {
      const fiber = getCurrent();
      reportCauseUnsafe(fiber, cause);
      const handler = frame.preResponseHandler;
      const cont = cause.reasons.length === 0 ? succeed7(response) : failCause3(cause);
      if (handler === undefined) {
        request[handledSymbol] = true;
        return flatMapEager2(handleResponse(request, response), () => cont);
      }
      return flatMapEager2(flatMapEager2(handler(request, response), (response) => {
        request[handledSymbol] = true;
        return handleResponse(request, response);
      }), () => cont);
    });
  };
  const sendResponse = (frame, response) => {
    const request = frame.request;
    const handler = frame.preResponseHandler;
    if (handler === undefined) {
      request[handledSymbol] = true;
      return mapEager2(handleResponse(request, response), (sent) => isHttpServerResponse(sent) ? sent : response);
    }
    return flatMapEager2(handler(request, response), (sentResponse) => {
      request[handledSymbol] = true;
      return mapEager2(handleResponse(request, sentResponse), (sent) => isHttpServerResponse(sent) ? sent : response);
    });
  };
  const withMiddleware = (frame) => {
    const request = frame.request;
    const responded = matchCauseEffect2(self, {
      onSuccess: (response) => sendResponse(frame, response),
      onFailure: (cause) => handleCause(frame, cause)
    });
    return middleware === undefined ? responded : matchCauseEffect2(tracer2(middleware(responded)), {
      onFailure(cause) {
        reportCauseUnsafe(getCurrent(), cause);
        if (handledSymbol in request)
          return void_3;
        return matchCauseEffectEager2(causeResponse(cause), {
          onFailure(_) {
            return handleResponse(request, empty10({
              status: 500
            }));
          },
          onSuccess([response]) {
            return handleResponse(request, response);
          }
        });
      },
      onSuccess(response) {
        return handledSymbol in request ? void_3 : handleResponse(request, response);
      }
    });
  };
  const traced = (frame) => tracer2(withMiddleware(frame));
  const finishAfter = (frame, fiber, effect) => onExitPrimitive2(effect, (exit) => {
    fiber.setContext(frame.prev);
    if (scopeEjected in frame.scope)
      return;
    return closeUnsafe(frame.scope, frame.cause === undefined ? exit : failCause2(frame.cause));
  }, true);

  class RequestFrame extends RequestState {
    scope;
    prev;
    request;
    cause;
    constructor(scope, prev, request) {
      super();
      this.scope = scope;
      this.prev = prev;
      this.request = request;
    }
    [contA](response, fiber) {
      const sent = sendResponse(this, response);
      if (effectIsExit(sent) && sent._tag === "Success") {
        fiber.setContext(this.prev);
        if (scopeEjected in this.scope)
          return sent;
        const closed = closeUnsafe(this.scope, sent);
        return closed === undefined ? sent : flatMap4(closed, () => sent);
      }
      return finishAfter(this, fiber, sent);
    }
    [contE](cause, fiber) {
      return finishAfter(this, fiber, handleCause(this, cause));
    }
  }
  return withFiber2((fiber) => {
    fiberEnterUninterruptibleUnsafe(fiber);
    const scope = makeUnsafe3();
    const frame = new RequestFrame(scope, fiber.context, getUnsafe(fiber.context, HttpServerRequest));
    fiber.setContext(add(add(frame.prev, Scope, scope), CurrentRequestState, frame));
    const currentTracer = fiber.cache.tracer;
    if (middleware === undefined && (currentTracer === undefined || currentTracer === nativeTracer || isTracerDisabledUnsafe(fiber, frame.request))) {
      fiber._stack.push(frame);
      return self;
    }
    return finishAfter(frame, fiber, middleware === undefined ? traced(frame) : withMiddleware(frame));
  });
};
var handledSymbol = /* @__PURE__ */ Symbol.for("effect/http/HttpEffect/handled");
var scopeEjected = /* @__PURE__ */ Symbol.for("effect/http/HttpEffect/scopeEjected");

// node_modules/effect/dist/http/HttpServer.js
class HttpServer extends (/* @__PURE__ */ Service()("effect/http/HttpServer")) {
}
var make37 = (options) => options;
var serveEffect = /* @__PURE__ */ dual((args) => isEffect2(args[0]), (effect, middleware) => HttpServer.use((server) => server.serve(effect, middleware)));
var makeTestClient = /* @__PURE__ */ gen2(function* () {
  const server = yield* HttpServer;
  const client = yield* HttpClient;
  const address = server.address;
  if (isUnixPathAddress(address)) {
    return yield* die3(new Error("HttpServer.layerTestClient: UnixPathAddress not supported"));
  }
  const url = yield* fromResult2(toUrl(address)).pipe(orDie2);
  if (isUnspecified(address.address)) {
    url.hostname = formatIp(ipv4Loopback);
  }
  return mapRequest(client, prependUrl(url.origin));
});
var layerTestClient = /* @__PURE__ */ effect(HttpClient)(makeTestClient);

// node_modules/@effect/platform-node/dist/NodeHttpServer.js
import { Readable as Readable5, Transform } from "node:stream";
import { pipeline as pipeline3 } from "node:stream/promises";

// node_modules/@effect/platform-node/dist/internal/http.js
import { createRequire as createRequire2 } from "node:module";
var __filename = "/home/zahid/work/labs/wonder-axi/node_modules/@effect/platform-node/dist/internal/http.js";
var require2 = /* @__PURE__ */ createRequire2(typeof __filename === "string" ? __filename : import.meta.url);
var http = /* @__PURE__ */ require2("node:http");

// node_modules/@effect/platform-node/dist/internal/upgrade.js
import { finished, Readable } from "node:stream";

// node_modules/@effect/platform-node-shared/dist/NodeSocket.js
import { Buffer as Buffer2 } from "node:buffer";
import * as Tls from "node:tls";
var isDeno = "Deno" in globalThis;
var readAvailable = (conn, chunkSize) => {
  const first = conn.read();
  if (first === null)
    return;
  const out = [first];
  let chunk;
  while (out.length < chunkSize && (chunk = conn.read()) !== null) {
    out.push(chunk);
  }
  return out;
};
var toBuffers = (input) => map4(ensure(input), (value) => Buffer2.from(value));
var fromDuplex = (open, options) => withFiber2((fiber) => {
  let currentSocket;
  const latch = makeUnsafe5(false);
  const openServices = fiber.context;
  const isServer = options?.tlsServer === true;
  const requestedChunkSize = options?.chunkSize ?? 4096;
  const chunkSize = requestedChunkSize === Infinity || Number.isSafeInteger(requestedChunkSize) && requestedChunkSize > 0 ? requestedChunkSize : 4096;
  const secureEvent = isServer ? "secure" : "secureConnect";
  const reader = gen2(function* () {
    const scope = yield* scope2;
    let conn = yield* provide(open, scope).pipe(options?.openTimeout !== undefined ? timeoutOrElse2({
      duration: options.openTimeout,
      orElse: () => fail7(new SocketError({
        reason: new SocketOpenError({
          kind: "Timeout",
          cause: new Error("Connection timed out")
        })
      }))
    }) : identity);
    let error;
    let waiter;
    let pending;
    let reading = false;
    let closed = false;
    let upgradeAvailable = true;
    function drain() {
      if (waiter === undefined || reading)
        return;
      let chunk = pending;
      pending = undefined;
      if (chunk === undefined && !closed) {
        reading = true;
        try {
          chunk = readAvailable(conn, chunkSize);
        } finally {
          reading = false;
        }
      }
      if (closed)
        chunk = undefined;
      if (waiter === undefined) {
        pending = chunk;
        return;
      }
      const result = chunk !== undefined ? succeed7(chunk) : error !== undefined ? fail7(error) : undefined;
      if (result === undefined)
        return;
      const resume = waiter;
      waiter = undefined;
      resume(result);
    }
    function end(err) {
      error ??= err;
      drain();
    }
    function close(err) {
      error ??= err;
      closed = true;
      pending = undefined;
      drain();
    }
    function onEnd() {
      end(new SocketError({
        reason: new SocketCloseError({
          code: 1000
        })
      }));
    }
    function onError(cause) {
      close(new SocketError({
        reason: new SocketReadError({
          cause
        })
      }));
    }
    function onClose(hadError) {
      const err = new SocketError({
        reason: new SocketCloseError({
          code: hadError ? 1006 : 1000
        })
      });
      if (hadError)
        close(err);
      else
        end(err);
    }
    function attachReadListeners(conn) {
      conn.on("readable", drain);
      conn.on("end", onEnd);
      conn.on("error", onError);
      conn.on("close", onClose);
    }
    function detachReadListeners(conn) {
      conn.off("readable", drain);
      conn.off("end", onEnd);
      conn.off("error", onError);
      conn.off("close", onClose);
    }
    if (!isDeno)
      conn.pause();
    attachReadListeners(conn);
    yield* addFinalizer2(scope, sync2(() => {
      close(new SocketError({
        reason: new SocketCloseError({
          code: 1006
        })
      }));
      detachReadListeners(conn);
      latch.closeUnsafe();
      currentSocket = undefined;
      upgradeAvailable = false;
    }));
    currentSocket = conn;
    latch.openUnsafe();
    const pull = callback2((resume) => {
      waiter = resume;
      drain();
      if (waiter !== resume)
        return;
      return sync2(() => {
        if (waiter === resume)
          waiter = undefined;
      });
    });
    const upgrade = (upgradeOptions = {}) => suspend2(() => {
      if (!upgradeAvailable) {
        return fail7(new SocketError({
          reason: new SocketUpgradeError({
            cause: new Error("socket is already upgraded or closed")
          })
        }));
      }
      const hasKey = upgradeOptions.key !== undefined;
      const hasCert = upgradeOptions.cert !== undefined;
      if (isServer && (!hasKey || !hasCert) || hasKey !== hasCert) {
        return fail7(new SocketError({
          reason: new SocketUpgradeError({
            cause: new Error(isServer ? "server TLS upgrade requires both key and cert" : "TLS upgrade credentials must include both key and cert")
          })
        }));
      }
      return callback2((resume) => {
        const raw = conn;
        detachReadListeners(raw);
        let tls;
        try {
          const secureContext = Tls.createSecureContext({
            key: upgradeOptions.key === undefined ? undefined : map4(ensure(upgradeOptions.key), (value) => Buffer2.from(value2(value))),
            cert: upgradeOptions.cert === undefined ? undefined : toBuffers(upgradeOptions.cert),
            ca: upgradeOptions.ca === undefined ? undefined : toBuffers(upgradeOptions.ca),
            passphrase: upgradeOptions.passphrase === undefined ? undefined : value2(upgradeOptions.passphrase)
          });
          const tlsOptions = {
            secureContext,
            ALPNProtocols: upgradeOptions.alpnProtocols === undefined ? undefined : [...upgradeOptions.alpnProtocols],
            requestCert: upgradeOptions.requestCert,
            rejectUnauthorized: upgradeOptions.rejectUnauthorized
          };
          tls = isServer ? new Tls.TLSSocket(raw, {
            ...tlsOptions,
            isServer: true
          }) : Tls.connect({
            ...tlsOptions,
            servername: upgradeOptions.servername,
            socket: raw
          });
        } catch (cause) {
          attachReadListeners(raw);
          resume(fail7(new SocketError({
            reason: new SocketUpgradeError({
              cause
            })
          })));
          return;
        }
        conn = tls;
        currentSocket = tls;
        tls.pause();
        function cleanup() {
          tls.off(secureEvent, succeed);
          tls.off("error", failUpgrade);
          tls.off("close", onUpgradeClose);
        }
        function succeed() {
          cleanup();
          upgradeAvailable = false;
          attachReadListeners(tls);
          resume(void_3);
        }
        function failUpgrade(cause) {
          cleanup();
          const upgradeError = new SocketError({
            reason: new SocketUpgradeError({
              cause
            })
          });
          close(upgradeError);
          resume(fail7(upgradeError));
        }
        function onUpgradeClose() {
          failUpgrade(new Error("socket closed during TLS upgrade"));
        }
        tls.once(secureEvent, succeed);
        tls.once("error", failUpgrade);
        tls.once("close", onUpgradeClose);
        return sync2(() => {
          cleanup();
          close(new SocketError({
            reason: new SocketCloseError({
              code: 1006
            })
          }));
          tls.destroy();
        });
      });
    });
    return {
      pull,
      upgrade
    };
  }).pipe(updateContext2((input) => merge(openServices, input)));
  const awaitDrain = (conn) => callback2((resume) => {
    function cleanup() {
      conn.off("drain", onDrain);
      conn.off("error", onError);
      conn.off("close", onClose);
    }
    function onDrain() {
      cleanup();
      resume(void_3);
    }
    function onError(cause) {
      cleanup();
      resume(fail7(new SocketError({
        reason: new SocketWriteError({
          cause
        })
      })));
    }
    function onClose() {
      cleanup();
      resume(fail7(new SocketError({
        reason: new SocketWriteError({
          cause: new Error("socket closed")
        })
      })));
    }
    conn.on("drain", onDrain);
    conn.on("error", onError);
    conn.on("close", onClose);
    return sync2(cleanup);
  });
  const write = (chunk) => suspend2(() => {
    const conn = currentSocket;
    if (conn === undefined)
      return latch.whenOpen(write(chunk));
    if (isCloseEvent(chunk)) {
      conn.destroy(chunk.code > 1000 ? new Error(`closed with code ${chunk.code}`) : undefined);
      return void_3;
    }
    try {
      return conn.write(chunk) ? void_3 : awaitDrain(conn);
    } catch (cause) {
      return fail7(new SocketError({
        reason: new SocketWriteError({
          cause
        })
      }));
    }
  });
  const writeAll = (chunks) => suspend2(() => {
    const conn = currentSocket;
    if (conn === undefined)
      return latch.whenOpen(writeAll(chunks));
    let needsDrain = false;
    try {
      if (chunks.length === 1) {
        needsDrain = !conn.write(chunks[0]);
      } else {
        conn.cork();
        try {
          for (let i = 0;i < chunks.length; i++) {
            needsDrain = !conn.write(chunks[i]) || needsDrain;
          }
        } finally {
          conn.uncork();
        }
      }
    } catch (cause) {
      return fail7(new SocketError({
        reason: new SocketWriteError({
          cause
        })
      }));
    }
    return needsDrain ? awaitDrain(conn) : void_3;
  });
  const writer = acquireRelease2(succeed7({
    write,
    writeAll
  }), () => sync2(() => {
    if (!currentSocket || currentSocket.writableEnded)
      return;
    currentSocket.end();
  }));
  return succeed7(make31({
    reader,
    writer
  }));
});

// node_modules/@effect/platform-node/dist/internal/upgrade.js
var toSocket = (conn, open = void_3) => fromDuplex(acquireRelease2(suspend2(() => conn.destroyed ? fail7(openError("The upgraded connection closed before it was read")) : as2(open, conn)), (conn) => sync2(() => {
  if (conn.destroyed)
    return;
  if ("destroySoon" in conn) {
    conn.destroySoon();
  } else {
    conn.destroy();
  }
})));
var openError = (cause) => new SocketError({
  reason: new SocketOpenError({
    kind: "Unknown",
    cause: typeof cause === "string" ? new Error(cause) : cause
  })
});
var frameBody = (conn, head, length) => {
  if (head.length >= length) {
    return {
      body: Readable.from(length === 0 ? [] : [head.subarray(0, length)], {
        objectMode: false
      }),
      rest: head.subarray(length)
    };
  }
  let remaining = length - head.length;
  let reading = false;
  const stop = () => {
    conn.off("readable", onReadable);
    conn.off("end", onEnd);
  };
  const onReadable = () => {
    while (remaining > 0) {
      let chunk = conn.read();
      if (chunk === null)
        return;
      if (chunk.length > remaining) {
        conn.unshift(chunk.subarray(remaining));
        chunk = chunk.subarray(0, remaining);
      }
      remaining -= chunk.length;
      if (!body.push(chunk) && remaining > 0)
        return;
    }
    stop();
    body.push(null);
  };
  const onEnd = () => {
    stop();
    body.destroy(new Error("The connection closed before the request body ended"));
  };
  const body = new Readable({
    read() {
      if (reading)
        return onReadable();
      reading = true;
      if (head.length > 0)
        this.push(head);
      if (conn.readableEnded)
        return onEnd();
      conn.on("readable", onReadable);
      conn.on("end", onEnd);
    },
    destroy(error, callback) {
      stop();
      callback(error);
    }
  });
  body.on("error", () => {});
  return {
    body,
    rest: Buffer.alloc(0)
  };
};
var drainBody = (body, maxBytes) => callback2((resume) => {
  if (body.readableEnded)
    return resume(void_3);
  let bytes = 0;
  const cleanup = finished(body, {
    readable: true,
    writable: false
  }, (error) => {
    done();
    resume(error ? fail7(openError(error)) : void_3);
  });
  function onData(chunk) {
    bytes += chunk.length;
    if (maxBytes !== undefined && bytes > maxBytes) {
      done();
      resume(fail7(openError("The request body exceeds the maximum body size")));
    }
  }
  function done() {
    body.off("data", onData);
    cleanup();
  }
  body.on("data", onData);
  body.resume();
  return sync2(done);
});

// node_modules/@effect/platform-node-shared/dist/NodeStream.js
import { finished as finished3, Readable as Readable2 } from "node:stream";

// node_modules/@effect/platform-node-shared/dist/internal/stream.js
var destroyStream = (stream, cleanup, onDestroyed) => {
  if (!("closed" in stream)) {
    cleanup();
    onDestroyed?.();
    return;
  }
  const onDone = () => {
    clearImmediate(immediate);
    stream.off("error", onDone);
    stream.off("close", onDone);
    onDestroyed?.();
  };
  const onClosed = () => {
    if (stream.closed) {
      onDone();
    } else {
      immediate = setImmediate(onClosed);
      if (!onDestroyed)
        immediate.unref();
    }
  };
  let immediate = setImmediate(onClosed);
  if (!onDestroyed)
    immediate.unref();
  stream.once("error", onDone);
  stream.once("close", onDone);
  cleanup();
  if (!stream.closed) {
    stream.destroy();
  }
};

// node_modules/@effect/platform-node-shared/dist/NodeSink.js
import { finished as finished2 } from "node:stream";
var fromWritable = (options) => fromChannel2(mapDone(fromWritableChannel(options), (_) => [_]));
var fromWritableChannel = (options) => fromTransform((pull) => {
  const writable = options.evaluate();
  const write = pullIntoWritable({
    ...options,
    writable,
    pull
  });
  return succeed7(options.endOnDone === false ? write : onError2(write, (cause) => isDoneCause(cause) ? void_3 : releaseWritable(writable)));
});
var releaseWritable = (writable) => callback2((resume) => {
  const onDestroyed = () => resume(void_3);
  if (writable.closed) {
    const cleanup = finished2(writable, {
      readable: false
    }, () => {
      cleanup();
      onDestroyed();
    });
  } else {
    destroyStream(writable, constVoid, onDestroyed);
  }
});
var pullIntoWritable = (options) => withFiber2((fiber) => {
  const dispatcher = fiber.currentDispatcher;
  const completion = makeUnsafe2();
  const failure = makeUnsafe2();
  const onError = (error) => doneUnsafe(failure, failSync2(() => options.onError(error)));
  let errorEmitted = false;
  const onStreamError = (error) => {
    errorEmitted = true;
    onError(error);
  };
  options.writable.on("error", onStreamError);
  const cleanup = finished2(options.writable, {
    readable: false
  }, (error) => {
    if (error) {
      onError(error);
    } else {
      doneUnsafe(completion, void_3);
    }
  });
  return options.pull.pipe(flatMap4((chunk) => {
    let i = 0;
    return callback2((resume) => {
      let cancelled = false;
      const onFinish = () => dispatcher.scheduleTask(loop, 0);
      const loop = () => {
        options.writable.off("drain", loop);
        options.writable.off("finish", onFinish);
        for (;i < chunk.length; ) {
          if (cancelled) {
            return;
          }
          const success = options.writable.write(chunk[i++], options.encoding, (error) => {
            if (!error)
              return;
            if (options.writable.closed) {
              onError(error);
            } else {
              dispatcher.scheduleTask(() => onError(error), 0);
            }
          });
          if (!success) {
            if (!cancelled) {
              options.writable.once("drain", loop);
              options.writable.once("finish", onFinish);
            }
            return;
          }
        }
        if (!cancelled) {
          resume(void_3);
        }
      };
      loop();
      return sync2(() => {
        cancelled = true;
        options.writable.off("drain", loop);
        options.writable.off("finish", onFinish);
      });
    });
  }), forever2({
    disableYield: true
  }), options.endOnDone !== false ? catchDone((_) => {
    options.writable.end();
    return andThen2(_await(completion), done2(_));
  }) : identity, raceFirst2(_await(failure)), ensuring2(sync2(() => {
    cleanup();
    const removeErrorListener = () => {
      options.writable.off("error", onStreamError);
      options.writable.off("error", removeErrorListener);
      options.writable.off("close", removeErrorListener);
    };
    if (options.writable.errored && !options.writable.closed && !errorEmitted) {
      options.writable.once("error", removeErrorListener);
      options.writable.once("close", removeErrorListener);
    } else {
      removeErrorListener();
    }
  })));
});

// node_modules/@effect/platform-node-shared/dist/NodeStream.js
class MaxBytesError extends Error {
  _tag = "MaxBytesError";
  maxBytes;
  constructor(maxBytes) {
    super("maxBytes exceeded");
    this.maxBytes = maxBytes;
  }
}
var fromReadable = (options) => fromChannel3(fromReadableChannel(options));
var fromReadableChannel = (options) => fromTransform((_, scope) => readableToPullUnsafe({
  scope,
  readable: options.evaluate(),
  onError: options.onError ?? defaultOnError,
  chunkSize: options.chunkSize,
  readSize: options.readSize,
  closeOnDone: options.closeOnDone
}));
var fromDuplex2 = (options) => fromTransform((upstream, scope) => {
  const duplex = options.evaluate();
  const exit = make9(undefined);
  const latch = makeUnsafe5(false);
  return pullIntoWritable({
    pull: upstream,
    writable: duplex,
    onError: options.onError ?? defaultOnError,
    endOnDone: options.endOnDone,
    encoding: options.encoding
  }).pipe(catchCause2((cause) => {
    if (isDoneCause(cause))
      return void_3;
    exit.current = failCause2(cause);
    latch.openUnsafe();
    return void_3;
  }), forkIn2(scope), flatMap4(() => readableToPullUnsafe({
    scope,
    exit,
    latch,
    readable: duplex,
    onError: options.onError ?? defaultOnError,
    chunkSize: options.chunkSize,
    readSize: options.readSize
  })));
});
var pipeThroughDuplex = /* @__PURE__ */ dual(2, (self, options) => pipeThroughChannelOrFail(self, fromDuplex2(options)));
var toArrayBuffer = (readable, options) => {
  const maxBytesNumber = toMaxBytes(options?.maxBytes);
  const onError = options?.onError ?? defaultOnError;
  return callback2((resume) => {
    const stream = readable();
    const buffers = [];
    let bytes = 0;
    const cleanup = finished3(stream, {
      readable: true,
      writable: false
    }, (err) => {
      stream.off("data", onData);
      if (err) {
        destroyStream(stream, cleanup);
        return resume(fail7(onError(err)));
      }
      cleanup();
      const buffer = buffers.length === 1 ? buffers[0] : Buffer.concat(buffers);
      if (buffer.byteOffset === 0 && buffer.buffer.byteLength === buffer.byteLength) {
        return resume(succeed7(buffer.buffer));
      }
      resume(succeed7(buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)));
    });
    function onData(chunk) {
      buffers.push(chunk);
      bytes += chunk.length;
      if (maxBytesNumber !== undefined && bytes > maxBytesNumber) {
        stream.off("data", onData);
        if (options?.destroyOnOverflow === false) {
          drainReadable(stream, cleanup);
        } else {
          destroyStream(stream, cleanup);
        }
        resume(fail7(onError(new MaxBytesError(maxBytesNumber))));
      }
    }
    stream.on("data", onData);
    return sync2(() => {
      stream.off("data", onData);
      destroyStream(stream, cleanup);
    });
  });
};
var toUint8Array = (readable, options) => map7(toArrayBuffer(readable, options), (buffer) => new Uint8Array(buffer));
var drainReadable = (stream, cleanup) => {
  cleanup();
  const done = finished3(stream, {
    readable: true,
    writable: false
  }, () => done());
  stream.resume();
};
var readableToPullUnsafe = (options) => {
  const readable = options.readable;
  const chunkSize = options.chunkSize ?? 4096;
  const closeOnDone = options.closeOnDone ?? true;
  const exit = options.exit ?? make9(undefined);
  const latch = options.latch ?? makeUnsafe5(false);
  function onReadable() {
    latch.openUnsafe();
  }
  readable.on("readable", onReadable);
  const cleanup = finished3(readable, {
    readable: true,
    writable: false
  }, (error) => {
    exit.current ??= fail5(error ? options.onError(error) : Done2());
    latch.openUnsafe();
  });
  const pull = suspend2(function loop() {
    let item = options.readable.read(options.readSize);
    if (item === null) {
      if (exit.current) {
        return exit.current;
      }
      if (readable.readableEnded) {
        return fail7(Done2());
      }
      latch.closeUnsafe();
      return flatMap4(latch.await, loop);
    }
    const chunk = of(item);
    while (chunk.length < chunkSize) {
      item = options.readable.read(options.readSize);
      if (item === null)
        break;
      chunk.push(item);
    }
    return succeed7(chunk);
  });
  return as2(addFinalizer2(options.scope, sync2(() => {
    readable.off("readable", onReadable);
    if (closeOnDone) {
      destroyStream(readable, cleanup);
    } else {
      cleanup();
    }
  })), pull);
};
var defaultOnError = (error) => new UnknownError2(error);
var toMaxBytes = (maxBytes) => maxBytes === undefined || maxBytes === Infinity ? undefined : Number(fromInputUnsafe2(maxBytes));

// node_modules/@effect/platform-node/dist/NodeHttpIncomingMessage.js
var textDecoder = /* @__PURE__ */ new TextDecoder;
var formDecoder = /* @__PURE__ */ new TextDecoder("utf-8", {
  ignoreBOM: true
});

class NodeHttpIncomingMessage extends Class2 {
  [TypeId33];
  source;
  onError;
  remoteAddressOverride;
  destroyOnOverflow = true;
  closeOnDone = true;
  constructor(source, onError, remoteAddressOverride) {
    super();
    this[TypeId33] = TypeId33;
    this.source = source;
    this.onError = onError;
    this.remoteAddressOverride = remoteAddressOverride;
  }
  get headers() {
    return fromInput2(this.source.headers);
  }
  get remoteAddress() {
    return this.remoteAddressOverride ?? fromNullishOr(this.source.socket?.remoteAddress);
  }
  textEffect;
  get text() {
    if (this.textEffect) {
      return this.textEffect;
    }
    this.textEffect = map7(this.arrayBuffer, (_) => textDecoder.decode(_));
    return this.textEffect;
  }
  get textUnsafe() {
    return runSync2(this.text);
  }
  get json() {
    return flatMap4(this.text, (text) => try_3({
      try: () => text === "" ? null : JSON.parse(text),
      catch: this.onError
    }));
  }
  get jsonUnsafe() {
    return runSync2(this.json);
  }
  get urlParamsBody() {
    return flatMap4(this.arrayBuffer, (bytes) => try_3({
      try: () => fromInput3(new URLSearchParams(formDecoder.decode(bytes).replace(/^\?/, "%3F"))),
      catch: this.onError
    }));
  }
  get bodySource() {
    return this.source;
  }
  get stream() {
    return fromReadable({
      evaluate: () => this.bodySource,
      onError: this.onError,
      closeOnDone: this.closeOnDone
    });
  }
  arrayBufferEffect;
  get arrayBuffer() {
    if (this.arrayBufferEffect) {
      return this.arrayBufferEffect;
    }
    this.arrayBufferEffect = withFiber2((fiber) => toArrayBuffer(() => this.bodySource, {
      onError: this.onError,
      destroyOnOverflow: this.destroyOnOverflow,
      maxBytes: fiber.getRef(MaxBodySize)
    })).pipe(cached2, runSync2);
    this.textEffect = map7(this.arrayBufferEffect, (_) => textDecoder.decode(_));
    return this.arrayBufferEffect;
  }
}

// node_modules/@effect/platform-node-shared/dist/NodeHttpCompression.js
import * as Zlib from "node:zlib";
var algorithms = /* @__PURE__ */ new Set(typeof Zlib.zstdCompress === "function" ? ["gzip", "deflate", "br", "zstd"] : ["gzip", "deflate", "br"]);
var brotliParams = (level, sizeHint) => {
  const params = {};
  if (level !== undefined) {
    params[Zlib.constants.BROTLI_PARAM_QUALITY] = level;
  }
  if (sizeHint !== undefined) {
    params[Zlib.constants.BROTLI_PARAM_SIZE_HINT] = sizeHint;
  }
  return {
    params
  };
};
var zstdParams = (level) => level === undefined || level === 3 ? undefined : {
  params: {
    [Zlib.constants.ZSTD_c_compressionLevel]: level
  }
};
var compress = (data, algorithm, options) => callback2((resume) => {
  const complete = (error, result) => resume(error === null ? succeed7(result) : die3(error));
  switch (algorithm) {
    case "gzip": {
      Zlib.gzip(data, {
        level: options?.level
      }, complete);
      break;
    }
    case "deflate": {
      Zlib.deflate(data, {
        level: options?.level
      }, complete);
      break;
    }
    case "br": {
      Zlib.brotliCompress(data, brotliParams(options?.level, data.byteLength), complete);
      break;
    }
    case "zstd": {
      const params = zstdParams(options?.level);
      if (params === undefined) {
        Zlib.zstdCompress(data, complete);
      } else {
        Zlib.zstdCompress(data, params, complete);
      }
      break;
    }
  }
});
var make38 = (fallback) => ({
  algorithms: fallback.algorithms,
  compressResponse(response, algorithm, options) {
    const body = response.body;
    if (body._tag !== "Uint8Array") {
      return fallback.compressResponse(response, algorithm, options);
    }
    return map7(compress(body.body, algorithm, options), (result) => setHeader2(setBody2(response, uint8Array(result, response.headers["content-type"] ?? body.contentType)), "content-length", result.byteLength.toString()));
  }
});
var compressTransform = (algorithm, options) => {
  switch (algorithm) {
    case "gzip": {
      return Zlib.createGzip({
        level: options?.level,
        flush: Zlib.constants.Z_SYNC_FLUSH
      });
    }
    case "deflate": {
      return Zlib.createDeflate({
        level: options?.level,
        flush: Zlib.constants.Z_SYNC_FLUSH
      });
    }
    case "br": {
      return Zlib.createBrotliCompress({
        ...brotliParams(options?.level),
        flush: Zlib.constants.BROTLI_OPERATION_FLUSH
      });
    }
    case "zstd": {
      return Zlib.createZstdCompress({
        ...zstdParams(options?.level),
        flush: Zlib.constants.ZSTD_e_flush
      });
    }
  }
};

// node_modules/@effect/platform-node/dist/NodeHttpPlatform.js
import * as Fs from "node:fs";
import { pipeline, Readable as Readable3 } from "node:stream";

// node_modules/@effect/platform-node-shared/dist/NodeFileSystem.js
import * as Crypto from "node:crypto";
import * as NFS from "node:fs";
import * as OS from "node:os";
import * as Path2 from "node:path";

// node_modules/@effect/platform-node-shared/dist/internal/utils.js
var handleErrnoException = (module, method) => (err, [path]) => {
  let reason = "Unknown";
  switch (err.code) {
    case "ENOENT":
      reason = "NotFound";
      break;
    case "EACCES":
    case "EPERM":
      reason = "PermissionDenied";
      break;
    case "EEXIST":
      reason = "AlreadyExists";
      break;
    case "EISDIR":
      reason = "BadResource";
      break;
    case "ENOTDIR":
      reason = "BadResource";
      break;
    case "EBUSY":
      reason = "Busy";
      break;
    case "ELOOP":
      reason = "BadResource";
      break;
  }
  return systemError({
    _tag: reason,
    module,
    method,
    pathOrDescriptor: path,
    syscall: err.syscall,
    cause: err
  });
};

// node_modules/@effect/platform-node-shared/dist/NodeFileSystem.js
var handleBadArgument = (method) => (err) => badArgument({
  module: "FileSystem",
  method,
  description: err.message ?? String(err)
});
var bigintToNumber = (value, field) => {
  const number = Number(value);
  if (!Number.isSafeInteger(number)) {
    throw new RangeError(`${field} exceeds the safe integer range: ${value}`);
  }
  return number;
};
var bigintToNumberOption = (value) => flatMap(fromNullishOr(value), toNumber);
var toUint64 = (value) => BigInt.asUintN(64, value);
var toUint64Option = (value) => map(fromNullishOr(value), toUint64);
var positionToNumber = (position, method) => try_3({
  try: () => bigintToNumber(position, "position"),
  catch: handleBadArgument(method)
});
var access2 = /* @__PURE__ */ (() => {
  const nodeAccess = /* @__PURE__ */ effectify(NFS.access, /* @__PURE__ */ handleErrnoException("FileSystem", "access"), /* @__PURE__ */ handleBadArgument("access"));
  return (path, options) => {
    let mode = NFS.constants.F_OK;
    if (options?.readable) {
      mode |= NFS.constants.R_OK;
    }
    if (options?.writable) {
      mode |= NFS.constants.W_OK;
    }
    return nodeAccess(path, mode);
  };
})();
var copy2 = /* @__PURE__ */ (() => {
  const nodeCp = /* @__PURE__ */ effectify(NFS.cp, /* @__PURE__ */ handleErrnoException("FileSystem", "copy"), /* @__PURE__ */ handleBadArgument("copy"));
  return (fromPath, toPath, options) => nodeCp(fromPath, toPath, {
    force: options?.overwrite ?? false,
    preserveTimestamps: options?.preserveTimestamps ?? false,
    recursive: true
  });
})();
var copyFile2 = /* @__PURE__ */ (() => {
  const nodeCopyFile = /* @__PURE__ */ effectify(NFS.copyFile, /* @__PURE__ */ handleErrnoException("FileSystem", "copyFile"), /* @__PURE__ */ handleBadArgument("copyFile"));
  return (fromPath, toPath) => nodeCopyFile(fromPath, toPath);
})();
var chmod2 = /* @__PURE__ */ (() => {
  const nodeChmod = /* @__PURE__ */ effectify(NFS.chmod, /* @__PURE__ */ handleErrnoException("FileSystem", "chmod"), /* @__PURE__ */ handleBadArgument("chmod"));
  return (path, mode) => nodeChmod(path, mode);
})();
var chown2 = /* @__PURE__ */ (() => {
  const nodeChown = /* @__PURE__ */ effectify(NFS.chown, /* @__PURE__ */ handleErrnoException("FileSystem", "chown"), /* @__PURE__ */ handleBadArgument("chown"));
  return (path, uid, gid) => nodeChown(path, uid, gid);
})();
var link3 = /* @__PURE__ */ (() => {
  const nodeLink = /* @__PURE__ */ effectify(NFS.link, /* @__PURE__ */ handleErrnoException("FileSystem", "link"), /* @__PURE__ */ handleBadArgument("link"));
  return (existingPath, newPath) => nodeLink(existingPath, newPath);
})();
var makeDirectory = /* @__PURE__ */ (() => {
  const nodeMkdir = /* @__PURE__ */ effectify(NFS.mkdir, /* @__PURE__ */ handleErrnoException("FileSystem", "makeDirectory"), /* @__PURE__ */ handleBadArgument("makeDirectory"));
  return (path, options) => nodeMkdir(path, {
    recursive: options?.recursive ?? false,
    mode: options?.mode
  });
})();
var makeTempDirectoryFactory = (method) => {
  const nodeMkdtemp = effectify(NFS.mkdtemp, handleErrnoException("FileSystem", method), handleBadArgument(method));
  return (options) => suspend2(() => {
    const prefix = options?.prefix ?? "";
    const directory = typeof options?.directory === "string" ? Path2.join(options.directory, ".") : OS.tmpdir();
    return nodeMkdtemp(prefix ? Path2.join(directory, prefix) : directory + "/");
  });
};
var makeTempDirectory = /* @__PURE__ */ makeTempDirectoryFactory("makeTempDirectory");
var removeFactory = (method) => {
  const nodeRm = effectify(NFS.rm, handleErrnoException("FileSystem", method), handleBadArgument(method));
  return (path, options) => nodeRm(path, {
    recursive: options?.recursive ?? false,
    force: options?.force ?? false
  });
};
var remove4 = /* @__PURE__ */ removeFactory("remove");
var makeTempDirectoryScoped = /* @__PURE__ */ (() => {
  const makeDirectory = /* @__PURE__ */ makeTempDirectoryFactory("makeTempDirectoryScoped");
  const removeDirectory = /* @__PURE__ */ removeFactory("makeTempDirectoryScoped");
  return (options) => acquireRelease2(makeDirectory(options), (directory) => orDie2(removeDirectory(directory, {
    recursive: true,
    force: true
  })));
})();
var openFlagBits = (flag) => {
  const {
    O_APPEND,
    O_CREAT,
    O_EXCL,
    O_RDONLY,
    O_RDWR,
    O_TRUNC,
    O_WRONLY
  } = NFS.constants;
  switch (flag) {
    case "r":
      return O_RDONLY;
    case "r+":
      return O_RDWR;
    case "w":
      return O_TRUNC | O_CREAT | O_WRONLY;
    case "wx":
      return O_TRUNC | O_CREAT | O_WRONLY | O_EXCL;
    case "w+":
      return O_TRUNC | O_CREAT | O_RDWR;
    case "wx+":
      return O_TRUNC | O_CREAT | O_RDWR | O_EXCL;
    case "a":
      return O_APPEND | O_CREAT | O_WRONLY;
    case "ax":
      return O_APPEND | O_CREAT | O_WRONLY | O_EXCL;
    case "a+":
      return O_APPEND | O_CREAT | O_RDWR;
    case "ax+":
      return O_APPEND | O_CREAT | O_RDWR | O_EXCL;
  }
};
var noFollowFlags = (method, flag) => suspend2(() => {
  const {
    O_NOFOLLOW
  } = NFS.constants;
  if (process.platform === "win32" || typeof O_NOFOLLOW !== "number" || O_NOFOLLOW === 0) {
    return fail7(badArgument({
      module: "FileSystem",
      method,
      description: "noFollow is not supported on this platform"
    }));
  }
  return succeed7(openFlagBits(flag) | O_NOFOLLOW);
});
var openFactory = (method) => {
  const nodeOpen = effectify(NFS.open, handleErrnoException("FileSystem", method), handleBadArgument(method));
  const nodeClose = effectify(NFS.close, handleErrnoException("FileSystem", method), handleBadArgument(method));
  return (path, options) => pipe(acquireRelease2(options?.noFollow === true ? flatMap4(noFollowFlags(method, options.flag ?? "r"), (flags) => nodeOpen(path, flags, options.mode)) : nodeOpen(path, options?.flag ?? "r", options?.mode), (fd) => orDie2(nodeClose(fd))), map7((fd) => makeFile(fd, options?.flag?.startsWith("a") ?? false)));
};
var open2 = /* @__PURE__ */ openFactory("open");
var negativePosition = (method) => badArgument({
  module: "FileSystem",
  method,
  description: "Cannot read before the start of the file"
});
var makeFile = /* @__PURE__ */ (() => {
  const nodeReadFactory = (method) => effectify(NFS.read, handleErrnoException("FileSystem", method), handleBadArgument(method));
  const nodeRead = /* @__PURE__ */ nodeReadFactory("read");
  const nodeReadAlloc = /* @__PURE__ */ nodeReadFactory("readAlloc");
  const nodeStat = /* @__PURE__ */ effectify(NFS.fstat, /* @__PURE__ */ handleErrnoException("FileSystem", "stat"), /* @__PURE__ */ handleBadArgument("stat"));
  const nodeTruncate = /* @__PURE__ */ effectify(NFS.ftruncate, /* @__PURE__ */ handleErrnoException("FileSystem", "truncate"), /* @__PURE__ */ handleBadArgument("truncate"));
  const nodeSync = /* @__PURE__ */ effectify(NFS.fsync, /* @__PURE__ */ handleErrnoException("FileSystem", "sync"), /* @__PURE__ */ handleBadArgument("sync"));
  const nodeWriteFactory = (method) => effectify(NFS.write, handleErrnoException("FileSystem", method), handleBadArgument(method));
  const nodeWrite = /* @__PURE__ */ nodeWriteFactory("write");
  const nodeWriteAll = /* @__PURE__ */ nodeWriteFactory("writeAll");

  class FileImpl {
    [FileTypeId];
    fd;
    append;
    position = /* @__PURE__ */ BigInt(0);
    constructor(fd, append) {
      this[FileTypeId] = FileTypeId;
      this.fd = fd;
      this.append = append;
    }
    get stat() {
      return flatMap4(nodeStat(this.fd, {
        bigint: true
      }), makeFileInfo);
    }
    get sync() {
      return nodeSync(this.fd);
    }
    seek(offset, from) {
      return suspend2(() => {
        const position = from === "start" ? offset : this.position + offset;
        if (position < BigInt(0)) {
          return fail7(badArgument({
            module: "FileSystem",
            method: "seek",
            description: "Cannot seek before the start of the file"
          }));
        }
        this.position = position;
        return succeed7(position);
      });
    }
    read(buffer, options) {
      return suspend2(() => {
        const explicit = options?.position;
        if (explicit !== undefined) {
          return explicit < BigInt(0) ? fail7(negativePosition("read")) : nodeRead(this.fd, {
            buffer,
            position: explicit
          });
        }
        const position = this.position;
        return map7(nodeRead(this.fd, {
          buffer,
          position
        }), (bytesRead) => {
          this.position = position + BigInt(bytesRead);
          return bytesRead;
        });
      });
    }
    readAlloc(size, options) {
      return suspend2(() => {
        try {
          if (!Number.isInteger(size) || size < 0) {
            throw new RangeError("size must be a non-negative integer");
          }
          const explicit = options?.position;
          if (explicit !== undefined && explicit < BigInt(0)) {
            return fail7(negativePosition("readAlloc"));
          }
          const buffer = Buffer.allocUnsafeSlow(size);
          const position = explicit ?? this.position;
          return map7(nodeReadAlloc(this.fd, {
            buffer,
            position
          }), (bytesRead) => {
            if (bytesRead === 0) {
              return none2();
            }
            if (explicit === undefined) {
              this.position = position + BigInt(bytesRead);
            }
            if (bytesRead === size) {
              return some2(buffer);
            }
            const dst = Buffer.allocUnsafeSlow(bytesRead);
            buffer.copy(dst, 0, 0, bytesRead);
            return some2(dst);
          });
        } catch (cause) {
          return fail7(handleBadArgument("readAlloc")(cause));
        }
      });
    }
    truncate(length) {
      return map7(nodeTruncate(this.fd, length || undefined), () => {
        if (!this.append) {
          const len = BigInt(length ?? 0);
          if (this.position > len) {
            this.position = len;
          }
        }
      });
    }
    write(buffer) {
      return suspend2(() => {
        const position = this.position;
        return flatMap4(this.append ? succeed7(undefined) : positionToNumber(position, "write"), (nodePosition) => map7(nodeWrite(this.fd, buffer, undefined, undefined, nodePosition), (bytesWritten) => {
          if (!this.append) {
            this.position = position + BigInt(bytesWritten);
          }
          return bytesWritten;
        }));
      });
    }
    writeAllChunk(buffer) {
      return suspend2(() => {
        const position = this.position;
        return flatMap4(this.append ? succeed7(undefined) : positionToNumber(position, "writeAll"), (nodePosition) => flatMap4(nodeWriteAll(this.fd, buffer, undefined, undefined, nodePosition), (bytesWritten) => {
          if (bytesWritten === 0) {
            return fail7(systemError({
              module: "FileSystem",
              method: "writeAll",
              _tag: "WriteZero",
              pathOrDescriptor: this.fd,
              description: "write returned 0 bytes written"
            }));
          }
          if (!this.append) {
            this.position = position + BigInt(bytesWritten);
          }
          return bytesWritten < buffer.length ? this.writeAllChunk(buffer.subarray(bytesWritten)) : void_3;
        }));
      });
    }
    writeAll(buffer) {
      return buffer.length === 0 ? void_3 : this.writeAllChunk(buffer);
    }
  }
  return (fd, append) => new FileImpl(fd, append);
})();
var makeTempFileFactory = (method) => {
  const makeDirectory = makeTempDirectoryFactory(method);
  return fnUntraced2(function* (options) {
    const directory = yield* makeDirectory(options);
    const random = Crypto.randomBytes(6).toString("hex");
    const name = Path2.join(directory, options?.suffix ? `${random}${options.suffix}` : random);
    yield* writeFile2(name, new Uint8Array(0));
    return name;
  });
};
var makeTempFile = /* @__PURE__ */ makeTempFileFactory("makeTempFile");
var makeTempFileScoped = /* @__PURE__ */ (() => {
  const makeFile = /* @__PURE__ */ makeTempFileFactory("makeTempFileScoped");
  const removeDirectory = /* @__PURE__ */ removeFactory("makeTempFileScoped");
  return (options) => acquireRelease2(makeFile(options), (file) => orDie2(removeDirectory(Path2.dirname(file), {
    recursive: true,
    force: true
  })));
})();
var readDirectory = (path, options) => tryPromise2({
  try: () => NFS.promises.readdir(path, options),
  catch: (err) => handleErrnoException("FileSystem", "readDirectory")(err, [path])
});
var readDirectoryEntries = (path) => tryPromise2({
  try: () => NFS.promises.readdir(path, {
    withFileTypes: true
  }),
  catch: (err) => handleErrnoException("FileSystem", "readDirectoryEntries")(err, [path])
}).pipe(map7((entries) => entries.map((entry) => ({
  name: entry.name,
  type: fileType(entry)
}))));
var readFile2 = (path) => callback2((resume, signal) => {
  try {
    NFS.readFile(path, {
      signal
    }, (err, data) => {
      if (err) {
        resume(fail7(handleErrnoException("FileSystem", "readFile")(err, [path])));
      } else {
        resume(succeed7(data));
      }
    });
  } catch (err) {
    resume(fail7(handleBadArgument("readFile")(err)));
  }
});
var readLink = /* @__PURE__ */ (() => {
  const nodeReadLink = /* @__PURE__ */ effectify(NFS.readlink, /* @__PURE__ */ handleErrnoException("FileSystem", "readLink"), /* @__PURE__ */ handleBadArgument("readLink"));
  return (path) => nodeReadLink(path);
})();
var realPath = /* @__PURE__ */ (() => {
  const nodeRealPath = /* @__PURE__ */ effectify(process.platform === "win32" ? NFS.realpath.native : NFS.realpath, /* @__PURE__ */ handleErrnoException("FileSystem", "realPath"), /* @__PURE__ */ handleBadArgument("realPath"));
  return (path) => nodeRealPath(path);
})();
var rename2 = /* @__PURE__ */ (() => {
  const nodeRename = /* @__PURE__ */ effectify(NFS.rename, /* @__PURE__ */ handleErrnoException("FileSystem", "rename"), /* @__PURE__ */ handleBadArgument("rename"));
  return (oldPath, newPath) => nodeRename(oldPath, newPath);
})();
var fileType = (stat) => stat.isFile() ? "File" : stat.isDirectory() ? "Directory" : stat.isSymbolicLink() ? "SymbolicLink" : stat.isBlockDevice() ? "BlockDevice" : stat.isCharacterDevice() ? "CharacterDevice" : stat.isFIFO() ? "FIFO" : stat.isSocket() ? "Socket" : "Unknown";
var makeFileInfo = (stat, method = "stat") => try_3({
  try: () => ({
    type: fileType(stat),
    mtime: fromNullishOr(stat.mtime),
    atime: fromNullishOr(stat.atime),
    birthtime: fromNullishOr(stat.birthtime),
    dev: toUint64(stat.dev),
    rdev: toUint64Option(stat.rdev),
    ino: toUint64Option(stat.ino),
    mode: bigintToNumber(stat.mode, "mode"),
    nlink: toUint64Option(stat.nlink),
    uid: bigintToNumberOption(stat.uid),
    gid: bigintToNumberOption(stat.gid),
    size: bytes(stat.size),
    blksize: stat.blksize !== undefined ? some2(bytes(stat.blksize)) : none2(),
    blocks: fromNullishOr(stat.blocks)
  }),
  catch: handleBadArgument(method)
});
var statFactory = (method, nodeStatFn) => {
  const nodeStat = effectify(nodeStatFn, handleErrnoException("FileSystem", method), handleBadArgument(method));
  return (path) => flatMap4(nodeStat(path, {
    bigint: true
  }), (stat) => makeFileInfo(stat, method));
};
var stat2 = /* @__PURE__ */ statFactory("stat", NFS.stat);
var lstat2 = /* @__PURE__ */ statFactory("lstat", NFS.lstat);
var symlink2 = /* @__PURE__ */ (() => {
  const nodeSymlink = /* @__PURE__ */ effectify(NFS.symlink, /* @__PURE__ */ handleErrnoException("FileSystem", "symlink"), /* @__PURE__ */ handleBadArgument("symlink"));
  return (target, path) => nodeSymlink(target, path);
})();
var truncate2 = /* @__PURE__ */ (() => {
  const nodeTruncate = /* @__PURE__ */ effectify(NFS.truncate, /* @__PURE__ */ handleErrnoException("FileSystem", "truncate"), /* @__PURE__ */ handleBadArgument("truncate"));
  return (path, length) => nodeTruncate(path, length);
})();
var utimes2 = /* @__PURE__ */ (() => {
  const nodeUtimes = /* @__PURE__ */ effectify(NFS.utimes, /* @__PURE__ */ handleErrnoException("FileSystem", "utime"), /* @__PURE__ */ handleBadArgument("utime"));
  return (path, atime, mtime) => nodeUtimes(path, atime, mtime);
})();
var watchNode = (path, info, options) => callback3((queue) => acquireRelease2(sync2(() => {
  const directory = info.type === "Directory" ? path : Path2.dirname(path);
  const watcher = NFS.watch(path, {
    recursive: options?.recursive ?? false
  }, (event, path) => {
    if (!path)
      return;
    switch (event) {
      case "rename": {
        runFork2(matchEffect3(stat2(Path2.resolve(directory, path)), {
          onSuccess: (_) => offer(queue, {
            _tag: "Create",
            path
          }),
          onFailure: (_) => offer(queue, {
            _tag: "Remove",
            path
          })
        }));
        return;
      }
      case "change": {
        offerUnsafe(queue, {
          _tag: "Update",
          path
        });
        return;
      }
    }
  });
  watcher.on("error", (error) => {
    failCauseUnsafe(queue, fail4(systemError({
      module: "FileSystem",
      _tag: "Unknown",
      method: "watch",
      pathOrDescriptor: path,
      cause: error
    })));
  });
  watcher.on("close", () => {
    endUnsafe(queue);
  });
  return watcher;
}), (watcher) => sync2(() => watcher.close())).pipe(tap2(() => sync2(() => options?.onReady?.()))));
var watch2 = (backend, path, options) => all3([stat2(path), serviceOption2(WatchBackend)]).pipe(map7(([stat, callerBackend]) => orElse(callerBackend, () => backend).pipe(flatMap((_) => _.register(path, stat, options)), getOrElse(() => watchNode(path, stat, options)))), unwrap3);
var writeFile2 = (path, data, options) => callback2((resume, signal) => {
  try {
    NFS.writeFile(path, data, {
      signal,
      flag: options?.flag,
      mode: options?.mode
    }, (err) => {
      if (err) {
        resume(fail7(handleErrnoException("FileSystem", "writeFile")(err, [path])));
      } else {
        resume(void_3);
      }
    });
  } catch (err) {
    resume(fail7(handleBadArgument("writeFile")(err)));
  }
});
var makeFileSystem = /* @__PURE__ */ map7(/* @__PURE__ */ serviceOption2(WatchBackend), (backend) => make14({
  access: access2,
  chmod: chmod2,
  chown: chown2,
  copy: copy2,
  copyFile: copyFile2,
  link: link3,
  lstat: lstat2,
  makeDirectory,
  makeTempDirectory,
  makeTempDirectoryScoped,
  makeTempFile,
  makeTempFileScoped,
  open: open2,
  readDirectory,
  readDirectoryEntries,
  readFile: readFile2,
  readLink,
  realPath,
  remove: remove4,
  rename: rename2,
  stat: stat2,
  symlink: symlink2,
  truncate: truncate2,
  utimes: utimes2,
  watch(path, options) {
    return watch2(backend, path, options);
  },
  writeFile: writeFile2
}));
var layer3 = /* @__PURE__ */ effect(FileSystem)(makeFileSystem);

// node_modules/@effect/platform-node/dist/NodeFileSystem.js
var layer4 = layer3;

// node_modules/@effect/platform-node/dist/NodeHttpPlatform.js
var lazyReadable = (evaluate, onDestroy) => {
  let source;
  return new Readable3({
    read() {
      if (source === undefined) {
        try {
          source = evaluate();
        } catch (cause) {
          this.destroy(cause instanceof Error ? cause : new Error(String(cause)));
          return;
        }
        source.on("data", (chunk) => {
          if (!this.push(chunk))
            source.pause();
        });
        source.once("end", () => this.push(null));
        source.once("error", (cause) => this.destroy(cause));
        source.once("close", () => {
          if (!source.readableEnded)
            this.destroy(new Error("Response stream closed prematurely"));
        });
      }
      source.resume();
    },
    destroy(error, callback) {
      source?.destroy(error ?? undefined);
      onDestroy?.();
      callback(error);
    }
  });
};
var compressedBody = (response, body) => removeHeader2(setBody2(response, body), "content-length");
var compression = /* @__PURE__ */ make38({
  algorithms,
  compressResponse(response, algorithm, options) {
    const body = response.body;
    switch (body._tag) {
      case "Stream": {
        return succeed7(compressedBody(response, stream(pipeThroughDuplex(body.stream, {
          evaluate: () => compressTransform(algorithm, options)
        }), response.headers["content-type"] ?? body.contentType)));
      }
      case "Raw": {
        let readable = body.body instanceof Readable3 ? body.body : undefined;
        const compressed = lazyReadable(() => {
          const transform = compressTransform(algorithm, options);
          readable ??= Readable3.fromWeb(new Response(body.body).body);
          return pipeline(readable, transform, (cause) => {
            if (cause)
              compressed.destroy(cause);
          });
        }, () => readable?.destroy());
        return succeed7(compressedBody(response, raw(compressed, {
          contentType: response.headers["content-type"] ?? body.contentType
        })));
      }
      default: {
        return succeed7(response);
      }
    }
  }
});
var make39 = /* @__PURE__ */ make30({
  platform: "node",
  compression,
  fileResponse(path, status, statusText, headers, start, end, contentLength) {
    const stream = lazyReadable(() => contentLength === BigInt(0) ? Readable3.from([]) : Fs.createReadStream(path, {
      start,
      end: end === undefined ? undefined : end - 1
    }));
    return raw2(stream, {
      headers: {
        ...headers,
        "content-type": headers["content-type"] ?? getOrElse(getType(path), () => "application/octet-stream"),
        "content-length": contentLength.toString()
      },
      status,
      statusText
    });
  },
  fileWebResponse(file, status, statusText, headers, _options) {
    return raw2(lazyReadable(() => Readable3.fromWeb(file.stream())), {
      headers: merge4(headers, fromRecordUnsafe({
        "content-type": headers["content-type"] ?? (file.type === "" ? getOrElse(getType(file.name), () => "application/octet-stream") : file.type),
        "content-length": file.size.toString()
      })),
      status,
      statusText
    });
  }
});
var layer5 = /* @__PURE__ */ pipe(/* @__PURE__ */ effect(HttpPlatform)(make39), /* @__PURE__ */ provide2(layer4), /* @__PURE__ */ provide2(layer));

// node_modules/@effect/platform-node/dist/NodeMultipart.js
import * as NFS2 from "node:fs";
import { finished as finished4 } from "node:stream";
import * as NodeStreamP from "node:stream/promises";

// node_modules/@effect/platform-node/dist/NodeMultipartParser.js
import { Duplex, Readable as Readable4 } from "node:stream";

class MultipartStream extends Duplex {
  _parser;
  _canWrite = true;
  _writeCallback;
  _currentFile;
  constructor(config) {
    super({
      readableObjectMode: true
    });
    let currentError;
    let initialized = false;
    this._parser = make35({
      ...config,
      onField: (info, value) => {
        if (currentError !== undefined)
          return;
        const field = {
          _tag: "Field",
          info,
          value
        };
        this.push(field);
        this.emit("field", field);
      },
      onFile: (info) => {
        if (currentError !== undefined)
          return (_) => {};
        const file = new FileStream(info, this);
        this._currentFile = file;
        this.push(file);
        this.emit("file", file);
        return (chunk) => {
          if (currentError !== undefined)
            return;
          this._canWrite = file.push(chunk);
          if (chunk === null && !this._canWrite) {
            this._currentFile = undefined;
            this._resume();
          }
        };
      },
      onError: (error) => {
        currentError = error;
        if (!initialized)
          return;
        this.emit("error", error);
        this._failCurrentFile(error);
      },
      onDone: () => {
        this.push(null);
      }
    });
    initialized = true;
    if (currentError !== undefined) {
      this.destroy(currentError);
    }
  }
  _resume() {
    this._canWrite = true;
    if (this._writeCallback !== undefined) {
      const callback = this._writeCallback;
      this._writeCallback = undefined;
      callback();
    }
  }
  _read(_size) {}
  _destroy(error, callback) {
    if (error)
      this._failCurrentFile(error);
    callback(error);
  }
  _failCurrentFile(error) {
    const file = this._currentFile;
    if (file === undefined)
      return;
    this._currentFile = undefined;
    file.on("error", constVoid);
    file.destroy(error);
  }
  _write(chunk, encoding, callback) {
    this._parser.write(chunk instanceof Uint8Array ? chunk : Buffer.from(chunk, encoding));
    if (this._canWrite) {
      callback();
    } else {
      this._writeCallback = callback;
    }
  }
  _final(callback) {
    this._parser.end();
    callback();
  }
}
var make40 = (config) => new MultipartStream(config);

class FileStream extends Readable4 {
  _tag = "File";
  filename;
  info;
  _parent;
  constructor(info, parent) {
    super();
    this.info = info;
    this._parent = parent;
    this.filename = info.filename;
  }
  _read(_size) {
    if (this._parent._canWrite === false) {
      this._parent._resume();
    }
  }
}

// node_modules/@effect/platform-node/dist/NodeMultipart.js
var stream2 = (source, headers) => makeConfig(headers).pipe(map7((config) => fromReadable({
  evaluate() {
    const parser = make40(config);
    const cleanup = finished4(source, {
      readable: true,
      writable: false
    }, (error) => {
      cleanup();
      if (error)
        parser.destroy(error);
    });
    source.pipe(parser);
    return parser;
  },
  onError: (error) => convertError(error)
})), unwrap3, map9(convertPart));
var persisted = (source, headers) => toPersisted(stream2(source, headers), (path, file) => tryPromise2({
  try: (signal) => NodeStreamP.pipeline(file.file, NFS2.createWriteStream(path), {
    signal
  }),
  catch: (cause) => MultipartError.fromReason("InternalError", cause)
}));
var convertPart = (part) => part._tag === "Field" ? new FieldImpl(part.info, part.value) : new FileImpl(part);

class PartBase2 extends Class2 {
  [TypeId39];
  constructor() {
    super();
    this[TypeId39] = TypeId39;
  }
}

class FieldImpl extends PartBase2 {
  _tag = "Field";
  key;
  contentType;
  value;
  constructor(info, value) {
    super();
    this.key = info.name;
    this.contentType = info.contentType;
    this.value = decodeField2(info, value);
  }
  toJSON() {
    return {
      _id: "@effect/platform/Multipart/Part",
      _tag: "Field",
      key: this.key,
      value: this.value,
      contentType: this.contentType
    };
  }
}

class FileImpl extends PartBase2 {
  _tag = "File";
  key;
  name;
  contentType;
  content;
  contentEffect;
  file;
  constructor(file) {
    super();
    this.file = file;
    this.key = file.info.name;
    this.name = file.filename ?? file.info.name;
    this.contentType = file.info.contentType;
    this.content = fromReadable({
      evaluate: () => file,
      onError: (cause) => MultipartError.fromReason("InternalError", cause)
    });
    this.contentEffect = toUint8Array(() => file, {
      onError: (cause) => MultipartError.fromReason("InternalError", cause)
    });
  }
  toJSON() {
    return {
      _id: "@effect/platform/Multipart/Part",
      _tag: "File",
      key: this.key,
      name: this.name,
      contentType: this.contentType
    };
  }
}
function convertError(cause) {
  switch (cause._tag) {
    case "ReachedLimit": {
      switch (cause.limit) {
        case "MaxParts": {
          return MultipartError.fromReason("TooManyParts", cause);
        }
        case "MaxFieldSize": {
          return MultipartError.fromReason("FieldTooLarge", cause);
        }
        case "MaxPartSize": {
          return MultipartError.fromReason("FileTooLarge", cause);
        }
        case "MaxTotalSize": {
          return MultipartError.fromReason("BodyTooLarge", cause);
        }
      }
    }
    default: {
      return MultipartError.fromReason("Parse", cause);
    }
  }
}

// node_modules/effect/dist/Brand.js
function nominal() {
  return Object.assign((input) => input, {
    option: (input) => some2(input),
    result: (input) => succeed2(input),
    is: (_) => true
  });
}

// node_modules/effect/dist/process/ChildProcessSpawner.js
var ExitCode = /* @__PURE__ */ nominal();
var ProcessId = /* @__PURE__ */ nominal();
var HandleTypeId = "~effect/process/ChildProcessSpawner/ChildProcessHandle";
var HandleProto = {
  [HandleTypeId]: HandleTypeId,
  ...BaseProto,
  toJSON() {
    return {
      _id: "ChildProcessHandle",
      pid: this.pid
    };
  }
};
var makeHandle = (params) => Object.setPrototypeOf({
  ...params
}, HandleProto);
var make41 = (spawn) => {
  const streamString = (command, options) => spawn(command).pipe(map7((handle) => decodeText2(options?.includeStderr === true ? handle.all : drainFork(handle.stdout, handle.stderr))), unwrap3);
  const streamLines = (command, options) => splitLines2(streamString(command, options));
  return ChildProcessSpawner.of({
    spawn,
    exitCode: (command) => scoped2(flatMap4(spawn(command), (handle) => raceFirst2(handle.exitCode, andThen2(runDrain2(handle.all), never2)))),
    streamString,
    streamLines,
    lines: (command, options) => runCollect(streamLines(command, options)),
    string: (command, options) => mkString(streamString(command, options))
  });
};

class ChildProcessSpawner extends (/* @__PURE__ */ Service()("effect/process/ChildProcessSpawner")) {
}

// node_modules/effect/dist/process/CommandLine.js
class CommandLineError extends (/* @__PURE__ */ TaggedError2("CommandLineError")) {
}
var display = (words, options) => {
  if (words.length === 0)
    return "";
  const format = options?.format ?? "posix";
  return joinWords(format, words.map((word, i) => isRedacted(word) ? "<redacted>" : escapeControlCharacters(quoteWord(format, word, i === 0))));
};
var template = (strings, ...expressions) => {
  const index = strings.findIndex((text) => text === undefined);
  if (index !== -1) {
    return fail2(new CommandLineError({
      reason: "InvalidEscape",
      message: `Template part ${index} contains an invalid escape sequence`
    }));
  }
  return parseTemplate(strings, expressions);
};
var unwrap5 = (item) => isRedacted(item) ? value2(item) : String(item);
var toWord = (item) => isRedacted(item) ? item : String(item);
var reservedWords = /* @__PURE__ */ new Set(["!", "{", "}", "and", "begin", "case", "coproc", "do", "done", "elif", "else", "end", "esac", "fi", "for", "foreach", "function", "if", "in", "nocorrect", "not", "or", "repeat", "select", "switch", "then", "time", "until", "while"]);
var quoteWord = (format, value, program) => {
  switch (format) {
    case "posix":
      return program && reservedWords.has(value) ? `'${value}'` : quotePosix(value);
    case "powershell":
      return quotePowerShell(value);
    case "windows":
      return program && !value.includes('"') ? quoteWindowsProgram(value) : quoteWindowsArgument(value);
  }
};
var joinWords = (format, quoted) => format === "powershell" ? `& ${quoted.join(" ")}` : quoted.join(" ");
var posixSafe = /^[A-Za-z0-9_@+:,./-]+$/;
var quotePosix = (value) => posixSafe.test(value) ? value : `'${value.replace(/['\\]/g, (char) => `'\\${char}'`)}'`;
var quotePowerShell = (value) => `'${value.replace(/['\u2018\u2019\u201A\u201B]/g, "$&$&")}'`;
var windowsNeedsQuotes = /[ \t\n\v"]/;
var quoteWindowsArgument = (value) => {
  if (value.length > 0 && !windowsNeedsQuotes.test(value))
    return value;
  let out = '"';
  let backslashes = 0;
  for (const char of value) {
    if (char === "\\") {
      backslashes++;
    } else if (char === '"') {
      out += "\\".repeat(backslashes * 2 + 1) + '"';
      backslashes = 0;
    } else {
      out += "\\".repeat(backslashes) + char;
      backslashes = 0;
    }
  }
  return out + "\\".repeat(backslashes * 2) + '"';
};
var quoteWindowsProgram = (value) => value.length > 0 && !/[ \t]/.test(value) ? value : `"${value}"`;
var controlCharacters = /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/g;
var controlEscapes = {
  "\n": "\\n",
  "\r": "\\r",
  "\t": "\\t"
};
var escapeControlCharacters = (value) => value.replace(controlCharacters, (char) => controlEscapes[char] ?? `\\u${char.charCodeAt(0).toString(16).padStart(4, "0")}`);
var operators = "|&;<>()";
var globs = "*?[";
var expansionStart = /[A-Za-z_0-9{(@*#?$!'"-]/;
var quotedExpansionStart = /[A-Za-z_0-9{(@*#?$!-]/;
var assignmentName = /^[A-Za-z_][A-Za-z0-9_]*\+?$/;
var isBlank = (char) => char === " " || char === "\t" || char === `
`;
var unsupportedSyntax = (char, next, wordStart) => {
  if (operators.includes(char))
    return `The operator "${char}" needs a shell`;
  if (char === "`")
    return "Command substitution needs a shell";
  if (char === "$" && next !== undefined && expansionStart.test(next))
    return `The expansion "$${next}" needs a shell`;
  if (globs.includes(char))
    return `The glob character "${char}" needs a shell`;
  if (!wordStart)
    return;
  if (char === "#")
    return "A comment needs a shell";
  if (char === "~")
    return "Tilde expansion needs a shell";
  if (char === "=" && next !== undefined && !isBlank(next))
    return 'A leading "=" is expanded by zsh';
  return;
};
var parseTemplate = (texts, values) => {
  const words = [];
  let word = [];
  let plain = true;
  let braceDepth = 0;
  let quote = undefined;
  let quotePart = 0;
  let part = 0;
  const error = (reason, message, at = part) => fail2(new CommandLineError({
    reason,
    message: `${message} in template part ${at}`
  }));
  const endWord = () => {
    if (word.length === 0)
      return;
    const value = word.map(unwrap5).join("");
    words.push(word.length === 1 && isRedacted(word[0]) ? word[0] : word.some(isRedacted) ? make19(value) : value);
    word = [];
    plain = true;
    braceDepth = 0;
  };
  const scan = (text) => {
    for (let i = 0;i < text.length; i++) {
      const char = text[i];
      const next = text[i + 1];
      if (quote === "'") {
        if (char === "'")
          quote = undefined;
        else
          word.push(char);
      } else if (quote === '"') {
        if (char === '"') {
          quote = undefined;
        } else if (char === "\\" && next !== undefined && '$`"\\\n'.includes(next)) {
          if (next !== `
`)
            word.push(next);
          i++;
        } else if (char === "`") {
          return error("UnsupportedSyntax", "Command substitution needs a shell");
        } else if (char === "$" && next !== undefined && quotedExpansionStart.test(next)) {
          return error("UnsupportedSyntax", `The expansion "$${next}" needs a shell`);
        } else {
          word.push(char);
        }
      } else if (isBlank(char)) {
        endWord();
      } else if (char === "\\") {
        if (next === undefined)
          return error("InvalidEscape", "A trailing backslash escapes nothing");
        if (next !== `
`) {
          word.push(next);
          plain = false;
        }
        i++;
      } else if (char === "'" || char === '"') {
        quote = char;
        quotePart = part;
        word.push("");
        plain = false;
      } else {
        const unsupported = unsupportedSyntax(char, next, word.length === 0);
        if (unsupported !== undefined)
          return error("UnsupportedSyntax", unsupported);
        if (char === "{") {
          braceDepth++;
        } else if (char === "}") {
          if (braceDepth > 0)
            braceDepth--;
        } else if (braceDepth > 0 && (char === "," || char === "." && next === ".")) {
          return error("UnsupportedSyntax", "Brace expansion needs a shell");
        } else if (char === "=" && words.length === 0 && plain && assignmentName.test(word.join(""))) {
          return error("UnsupportedSyntax", "A variable assignment needs a shell");
        }
        word.push(char);
      }
    }
    return;
  };
  for (;part < texts.length; part++) {
    const failure = scan(texts[part]);
    if (failure !== undefined)
      return failure;
    if (part === values.length)
      break;
    const expression = values[part];
    if (Array.isArray(expression)) {
      const after = texts[part + 1][0];
      if (word.length > 0 || (after === undefined ? part + 1 < values.length : !isBlank(after))) {
        return error("UnsupportedSyntax", "An interpolated array must be a separate argument");
      }
      for (const item of expression)
        words.push(toWord(item));
    } else {
      word.push(toWord(expression));
      plain = false;
    }
  }
  if (quote !== undefined)
    return error("UnterminatedQuote", "The quote is not closed", quotePart);
  endWord();
  return succeed2(words);
};

// node_modules/effect/dist/process/ChildProcess.js
var TypeId41 = "~effect/process/ChildProcess";
var Proto14 = {
  .../* @__PURE__ */ Prototype2({
    label: "Command",
    evaluate(fiber) {
      return getUnsafe(fiber.context, ChildProcessSpawner).spawn(this);
    }
  }),
  [TypeId41]: TypeId41
};
var makeStandardCommand = (command, args, options) => Object.assign(Object.create(Proto14), {
  _tag: "StandardCommand",
  command,
  args,
  options
});
var make42 = function make(...args) {
  if (isTemplateString(args[0])) {
    const [templates, ...expressions] = args;
    const [command, cmdArgs] = parseTemplate2(templates, expressions);
    return makeStandardCommand(command, cmdArgs, {});
  }
  if (typeof args[0] === "object" && !Array.isArray(args[0]) && !isTemplateString(args[0])) {
    const options = args[0];
    return function(templates, ...expressions) {
      if (options.shell !== undefined && options.shell !== false) {
        return makeStandardCommand(shellScript(templates, expressions), [], options);
      }
      const [command, cmdArgs] = parseTemplate2(templates, expressions);
      return makeStandardCommand(command, cmdArgs, options);
    };
  }
  if (typeof args[0] === "string" && !Array.isArray(args[1])) {
    const [command, options = {}] = args;
    return makeStandardCommand(command, [], options);
  }
  const [command, cmdArgs = [], options = {}] = args;
  return makeStandardCommand(command, cmdArgs, options);
};
var isTemplateString = (u) => Array.isArray(u) && ("raw" in u) && Array.isArray(u.raw);
var parseFdName = (name) => {
  const match = /^fd(\d+)$/.exec(name);
  if (match === null)
    return;
  const fd = parseInt(match[1], 10);
  return fd >= 3 ? fd : undefined;
};
var fdName = (fd) => `fd${fd}`;
var parseTemplate2 = (templates, expressions) => {
  const [program = "", ...args] = getOrThrow(template(templates, ...expressions));
  if (typeof program !== "string") {
    throw new CommandLineError({
      reason: "InvalidArgument",
      message: "The program name of a command cannot be Redacted"
    });
  }
  return [program, args];
};
var shellScript = (templates, expressions) => {
  if (expressions.length > 0) {
    throw new CommandLineError({
      reason: "UnsupportedSyntax",
      message: "A shell template cannot interpolate values; build the script with CommandLine.quote"
    });
  }
  const script = templates[0];
  if (script === undefined) {
    throw new CommandLineError({
      reason: "InvalidEscape",
      message: "Template part 0 contains an invalid escape sequence"
    });
  }
  return script;
};

// node_modules/@effect/platform-node-shared/dist/NodeChildProcessSpawner.js
import * as NodeChildProcess from "node:child_process";
import { PassThrough } from "node:stream";

// node_modules/@effect/platform-node-shared/dist/internal/nodeChildProcessSpawner.js
var buildSpawnOptions = (options, base, platform) => {
  const detached = options.detached ?? platform !== "win32";
  return {
    ...base,
    detached,
    shell: options.shell,
    windowsHide: options.windowsHide ?? !detached
  };
};

// node_modules/@effect/platform-node-shared/dist/NodeChildProcessSpawner.js
var toError = (error) => error instanceof globalThis.Error ? error : new globalThis.Error(String(error));
var toPlatformError = (method, error, command) => {
  const {
    commands
  } = flattenCommand(command);
  const format = globalThis.process.platform === "win32" ? "windows" : "posix";
  const commandStr = commands.map((cmd) => display([cmd.command, ...cmd.args], {
    format
  })).join(" | ");
  return handleErrnoException("ChildProcess", method)(error, [commandStr]);
};
var unwrap6 = (value) => isRedacted(value) ? value2(value) : value;
var findNulCharacter = (command) => {
  if (command.command.includes("\x00"))
    return "The command contains a NUL character";
  const index = command.args.findIndex((arg) => unwrap6(arg).includes("\x00"));
  if (index !== -1)
    return `Argument ${index + 1} contains a NUL character`;
  for (const [name, value] of Object.entries(command.options.env ?? {})) {
    if (value !== undefined && unwrap6(value).includes("\x00")) {
      return `The environment variable ${name} contains a NUL character`;
    }
  }
  return;
};
var processGroupGraceMillis = 1000;
var processGroupPollIntervalMillis = 10;
var handleInputErrors = (writable) => {
  const onError = () => {};
  writable.on("error", onError);
  writable.once("close", () => writable.off("error", onError));
};
var hasExited = (childProcess) => childProcess.exitCode !== null || childProcess.signalCode !== null;
var ownsProcessGroup = (command, childProcess) => command.options.detached !== false || !hasExited(childProcess);
var isUnsignallableProcessGroup = (error) => error?.code === "EPERM";
var isProcessAlive = (command, childProcess) => {
  if (!hasExited(childProcess)) {
    return true;
  }
  if (globalThis.process.platform === "win32" || !ownsProcessGroup(command, childProcess)) {
    return false;
  }
  try {
    globalThis.process.kill(-childProcess.pid, 0);
    return true;
  } catch (error) {
    return isUnsignallableProcessGroup(error);
  }
};
var taskkill = (childProcess, onExit = () => {}) => {
  if (hasExited(childProcess)) {
    onExit(null);
    return;
  }
  NodeChildProcess.execFile("taskkill", ["/pid", String(childProcess.pid), "/T", "/F"], {
    windowsHide: true
  }, onExit);
};
var make43 = /* @__PURE__ */ gen2(function* () {
  const fs = yield* FileSystem;
  const path = yield* Path;
  const resolveWorkingDirectory = fnUntraced2(function* (options) {
    if (isUndefined(options.cwd))
      return;
    yield* fs.access(options.cwd);
    return path.resolve(options.cwd);
  });
  const resolveEnvironment = (options) => {
    if (options.env === undefined)
      return;
    const env = options.extendEnv === true ? {
      ...globalThis.process.env
    } : {};
    for (const [key, value] of Object.entries(options.env)) {
      if (value === undefined)
        delete env[key];
      else
        env[key] = unwrap6(value);
    }
    return env;
  };
  const inputToStdioOption = (input) => isStream(input) ? "pipe" : input;
  const outputToStdioOption = (input) => isSink(input) ? "pipe" : input;
  const resolveStdinOption = (options) => {
    const defaultConfig = {
      stream: "pipe",
      encoding: "utf-8",
      endOnDone: true
    };
    if (isUndefined(options.stdin)) {
      return defaultConfig;
    }
    if (typeof options.stdin === "string") {
      return {
        ...defaultConfig,
        stream: options.stdin
      };
    }
    if (isStream(options.stdin)) {
      return {
        ...defaultConfig,
        stream: options.stdin
      };
    }
    return {
      stream: options.stdin.stream,
      encoding: options.stdin.encoding ?? defaultConfig.encoding,
      endOnDone: options.stdin.endOnDone ?? defaultConfig.endOnDone
    };
  };
  const resolveOutputOption = (options, streamName) => {
    const option = options[streamName];
    if (isUndefined(option)) {
      return {
        stream: "pipe"
      };
    }
    if (typeof option === "string") {
      return {
        stream: option
      };
    }
    if (isSink(option)) {
      return {
        stream: option
      };
    }
    return {
      stream: option.stream
    };
  };
  const resolveAdditionalFds = (options) => {
    if (isUndefined(options.additionalFds)) {
      return [];
    }
    const result = [];
    for (const [name, config] of Object.entries(options.additionalFds)) {
      const fd = parseFdName(name);
      if (isNotUndefined(fd)) {
        result.push({
          fd,
          config
        });
      }
    }
    return result.sort((a, b) => a.fd - b.fd);
  };
  const buildStdioArray = (stdinConfig, stdoutConfig, stderrConfig, additionalFds) => {
    const stdio = [inputToStdioOption(stdinConfig.stream), outputToStdioOption(stdoutConfig.stream), outputToStdioOption(stderrConfig.stream)];
    if (additionalFds.length === 0) {
      return stdio;
    }
    const maxFd = additionalFds.reduce((max, {
      fd
    }) => Math.max(max, fd), 2);
    for (let i = 3;i <= maxFd; i++) {
      stdio[i] = "ignore";
    }
    for (const {
      fd
    } of additionalFds) {
      stdio[fd] = "pipe";
    }
    return stdio;
  };
  const runInputStream = (stream, sink, inputFailure) => forkScoped2(run(tapError4(stream, (error) => fail6(inputFailure, error)), sink));
  const setupAdditionalFds = fnUntraced2(function* (command, childProcess, additionalFds, inputFailure) {
    if (additionalFds.length === 0) {
      return {
        getInputFd: () => drain,
        getOutputFd: () => empty4
      };
    }
    const inputSinks = new Map;
    const outputStreams = new Map;
    const setupInput = (fd, nodeStream, stream) => suspend2(() => {
      let sink = drain;
      if (nodeStream && "write" in nodeStream) {
        handleInputErrors(nodeStream);
        sink = fromWritable({
          evaluate: () => nodeStream,
          onError: (error) => toPlatformError(`fromWritable(fd${fd})`, toError(error), command)
        });
      }
      if (stream) {
        return map7(runInputStream(stream, sink, inputFailure), () => {
          inputSinks.set(fd, sink);
        });
      }
      inputSinks.set(fd, sink);
      return void_3;
    });
    const setupOutput = (fd, nodeStream, sink) => {
      let stream = empty4;
      if (nodeStream && "read" in nodeStream) {
        const passThrough = new PassThrough;
        let error = undefined;
        passThrough.on("error", (cause) => {
          error ??= cause;
        });
        nodeStream.on("error", (cause) => passThrough.destroy(cause));
        nodeStream.pipe(passThrough);
        const onError = (cause) => toPlatformError(`fromReadable(fd${fd})`, toError(cause), command);
        stream = suspend4(() => error === undefined ? fromReadable({
          evaluate: () => passThrough,
          onError
        }) : fail9(onError(error)));
      }
      if (sink) {
        stream = transduce(stream, sink);
      }
      outputStreams.set(fd, stream);
    };
    for (const {
      config,
      fd
    } of additionalFds) {
      const nodeStream = childProcess.stdio[fd];
      switch (config.type) {
        case "input": {
          yield* setupInput(fd, nodeStream, config.stream);
          break;
        }
        case "output": {
          setupOutput(fd, nodeStream, config.sink);
          break;
        }
        case "duplex": {
          if (nodeStream && "allowHalfOpen" in nodeStream) {
            nodeStream.allowHalfOpen = true;
          }
          setupOutput(fd, nodeStream, config.sink);
          yield* setupInput(fd, nodeStream, config.stream);
          break;
        }
      }
    }
    return {
      getInputFd: (fd) => inputSinks.get(fd) ?? drain,
      getOutputFd: (fd) => outputStreams.get(fd) ?? empty4
    };
  });
  const setupChildStdin = (command, childProcess, config, inputFailure) => suspend2(() => {
    let sink = drain;
    if (isNotNull(childProcess.stdin)) {
      handleInputErrors(childProcess.stdin);
      sink = fromWritable({
        evaluate: () => childProcess.stdin,
        onError: (error) => toPlatformError("fromWritable(stdin)", toError(error), command),
        endOnDone: config.endOnDone,
        encoding: config.encoding
      });
    }
    if (isStream(config.stream)) {
      return as2(runInputStream(config.stream, sink, inputFailure), sink);
    }
    return succeed7(sink);
  });
  const setupChildOutputStreams = (command, childProcess, stdoutConfig, stderrConfig) => {
    let stdout = childProcess.stdout ? (() => {
      const passThrough = new PassThrough;
      childProcess.stdout.on("error", (error) => passThrough.destroy(error));
      childProcess.stdout.pipe(passThrough);
      return fromReadable({
        evaluate: () => passThrough,
        onError: (error) => toPlatformError("fromReadable(stdout)", toError(error), command)
      });
    })() : empty4;
    let stderr = childProcess.stderr ? (() => {
      const passThrough = new PassThrough;
      childProcess.stderr.on("error", (error) => passThrough.destroy(error));
      childProcess.stderr.pipe(passThrough);
      return fromReadable({
        evaluate: () => passThrough,
        onError: (error) => toPlatformError("fromReadable(stderr)", toError(error), command)
      });
    })() : empty4;
    if (isSink(stdoutConfig.stream)) {
      stdout = transduce(stdout, stdoutConfig.stream);
    }
    if (isSink(stderrConfig.stream)) {
      stderr = transduce(stderr, stderrConfig.stream);
    }
    const all = merge3(stdout, stderr);
    return {
      stdout,
      stderr,
      all
    };
  };
  const spawn2 = (command, spawnOptions) => callback2((resume) => {
    const deferred = makeUnsafe2();
    const handle = NodeChildProcess.spawn(command.command, command.args.map(unwrap6), spawnOptions);
    handle.on("error", (error) => {
      if (command.args.some(isRedacted)) {
        Object.assign(error, {
          spawnargs: [command.command, ...command.args.map((arg) => isRedacted(arg) ? "<redacted>" : arg)]
        });
      }
      resume(fail7(toPlatformError("spawn", error, command)));
    });
    handle.on("exit", (...args) => {
      doneUnsafe(deferred, succeed4(args));
    });
    handle.on("spawn", () => {
      resume(succeed7([handle, deferred]));
    });
    return sync2(() => {
      handle.kill("SIGTERM");
    });
  });
  const killProcessGroup = (command, childProcess, signal) => {
    if (globalThis.process.platform === "win32") {
      return callback2((resume) => {
        taskkill(childProcess, (error) => {
          if (error) {
            resume(fail7(toPlatformError("kill", toError(error), command)));
          } else {
            resume(void_3);
          }
        });
      });
    }
    return try_3({
      try: () => {
        globalThis.process.kill(-childProcess.pid, signal);
      },
      catch: (error) => toPlatformError("kill", toError(error), command)
    });
  };
  const killProcessGroupOnExit = (childProcess, signal) => {
    if (globalThis.process.platform === "win32") {
      taskkill(childProcess);
      return;
    }
    try {
      globalThis.process.kill(-childProcess.pid, signal);
    } catch {}
  };
  const killProcess = (command, childProcess, signal) => suspend2(() => {
    const killed = childProcess.kill(signal);
    if (!killed) {
      const error = new globalThis.Error("Failed to kill child process");
      return fail7(toPlatformError("kill", error, command));
    }
    return void_3;
  });
  const awaitProcessExit = (command, childProcess, timeoutMillis) => callback2((resume) => {
    const deadline = Date.now() + timeoutMillis;
    let timer;
    const stop = () => {
      clearTimeout(timer);
      childProcess.removeListener("exit", poll);
    };
    const poll = () => {
      clearTimeout(timer);
      if (Date.now() >= deadline || !isProcessAlive(command, childProcess)) {
        stop();
        resume(void_3);
        return;
      }
      timer = setTimeout(poll, processGroupPollIntervalMillis);
    };
    childProcess.on("exit", poll);
    poll();
    return sync2(stop);
  });
  const terminateProcessGroup = fnUntraced2(function* (command, childProcess, exitSignal, options) {
    const signalGroup = (signal) => ownsProcessGroup(command, childProcess) ? killProcessGroup(command, childProcess, signal).pipe(catch_2((error) => hasExited(childProcess) && isUnsignallableProcessGroup(error.cause) ? void_3 : killProcess(command, childProcess, signal))) : killProcess(command, childProcess, signal);
    yield* signalGroup(options?.killSignal ?? "SIGTERM");
    if (isUndefined(options?.forceKillAfter)) {
      yield* _await(exitSignal);
      yield* awaitProcessExit(command, childProcess, processGroupGraceMillis);
    } else {
      yield* awaitProcessExit(command, childProcess, toMillis(options.forceKillAfter));
      if (isProcessAlive(command, childProcess)) {
        yield* signalGroup("SIGKILL");
        yield* awaitProcessExit(command, childProcess, processGroupGraceMillis);
      }
    }
    yield* _await(exitSignal);
  });
  const getSourceStream = (handle, from) => {
    const fromOption = from ?? "stdout";
    switch (fromOption) {
      case "stdout":
        return handle.stdout;
      case "stderr":
        return handle.stderr;
      case "all":
        return handle.all;
      default: {
        const fd = parseFdName(fromOption);
        if (isNotUndefined(fd)) {
          return handle.getOutputFd(fd);
        }
        return handle.stdout;
      }
    }
  };
  const spawnCommand = fnUntraced2(function* (cmd) {
    switch (cmd._tag) {
      case "StandardCommand": {
        if (cmd.options.shell !== undefined && cmd.options.shell !== false && cmd.args.length > 0) {
          return yield* fail7(badArgument({
            module: "ChildProcess",
            method: "spawn",
            description: "The shell option cannot be combined with arguments; pass the whole script as the command"
          }));
        }
        const nulCharacter = findNulCharacter(cmd);
        if (nulCharacter !== undefined) {
          return yield* fail7(badArgument({
            module: "ChildProcess",
            method: "spawn",
            description: nulCharacter
          }));
        }
        const stdinConfig = resolveStdinOption(cmd.options);
        const stdoutConfig = resolveOutputOption(cmd.options, "stdout");
        const stderrConfig = resolveOutputOption(cmd.options, "stderr");
        const resolvedAdditionalFds = resolveAdditionalFds(cmd.options);
        let isReferenced = true;
        const cwd = yield* resolveWorkingDirectory(cmd.options);
        const env = resolveEnvironment(cmd.options);
        const stdio = buildStdioArray(stdinConfig, stdoutConfig, stderrConfig, resolvedAdditionalFds);
        const [childProcess, exitSignal] = yield* acquireRelease2(spawn2(cmd, buildSpawnOptions(cmd.options, {
          cwd,
          env,
          stdio
        }, process.platform)), fnUntraced2(function* ([childProcess, exitSignal]) {
          const exited = yield* isDone3(exitSignal);
          if (exited) {
            const [code] = yield* _await(exitSignal);
            if (code !== 0 && isNotNull(code)) {
              yield* ignore2(terminateProcessGroup(cmd, childProcess, exitSignal, cmd.options));
            } else if (isReferenced && process.platform !== "win32" && cmd.options.detached !== false) {
              yield* ignore2(terminateProcessGroup(cmd, childProcess, exitSignal, cmd.options));
            }
            return;
          }
          if (!isReferenced) {
            return;
          }
          yield* ignore2(terminateProcessGroup(cmd, childProcess, exitSignal, cmd.options));
        }));
        const pid = ProcessId(childProcess.pid);
        if (cmd.options.detached !== false) {
          childProcess.on("exit", (code) => {
            if (code !== 0 && isNotNull(code)) {
              killProcessGroupOnExit(childProcess, cmd.options.killSignal ?? "SIGTERM");
            }
          });
        }
        const reref = sync2(() => {
          if (!isReferenced) {
            childProcess.ref();
            isReferenced = true;
          }
        });
        const unref = sync2(() => {
          if (isReferenced) {
            childProcess.unref();
            isReferenced = false;
          }
          return reref;
        });
        const inputFailure = makeUnsafe2();
        const stdin = yield* setupChildStdin(cmd, childProcess, stdinConfig, inputFailure);
        const {
          all,
          stderr,
          stdout
        } = setupChildOutputStreams(cmd, childProcess, stdoutConfig, stderrConfig);
        const {
          getInputFd,
          getOutputFd
        } = yield* setupAdditionalFds(cmd, childProcess, resolvedAdditionalFds, inputFailure);
        const isRunning = map7(isDone3(exitSignal), (done) => !done);
        const exitCode = flatMap4(raceFirst2(_await(exitSignal), _await(inputFailure)), ([code, signal]) => {
          if (isDoneUnsafe(inputFailure)) {
            return _await(inputFailure);
          }
          if (isNotNull(code)) {
            return succeed7(ExitCode(code));
          }
          const error = new globalThis.Error(`Process interrupted due to receipt of signal: '${signal}'`);
          return fail7(toPlatformError("exitCode", error, cmd));
        });
        const kill = (options) => terminateProcessGroup(cmd, childProcess, exitSignal, options);
        return makeHandle({
          pid,
          exitCode,
          isRunning,
          kill,
          stdin,
          stdout,
          stderr,
          all,
          getInputFd,
          getOutputFd,
          unref
        });
      }
      case "PipedCommand": {
        const {
          commands,
          pipeOptions
        } = flattenCommand(cmd);
        const [root, ...pipeline] = commands;
        const handles = [yield* spawnCommand(root)];
        for (let i = 0;i < pipeline.length; i++) {
          const command = pipeline[i];
          const options = pipeOptions[i] ?? {};
          const stdinConfig = resolveStdinOption(command.options);
          const sourceStream = unwrap3(succeed7(getSourceStream(handles[handles.length - 1], options.from)));
          const toOption = options.to ?? "stdin";
          if (toOption === "stdin") {
            handles.push(yield* spawnCommand(make42(command.command, command.args, {
              ...command.options,
              stdin: {
                ...stdinConfig,
                stream: sourceStream
              }
            })));
          } else {
            const fd = parseFdName(toOption);
            if (isNotUndefined(fd)) {
              const fdName2 = fdName(fd);
              const existingFds = command.options.additionalFds ?? {};
              const existingFd = existingFds[fdName2];
              const fdConfig = existingFd?.type === "duplex" ? {
                ...existingFd,
                stream: sourceStream
              } : {
                type: "input",
                stream: sourceStream
              };
              handles.push(yield* spawnCommand(make42(command.command, command.args, {
                ...command.options,
                additionalFds: {
                  ...existingFds,
                  [fdName2]: fdConfig
                }
              })));
            } else {
              handles.push(yield* spawnCommand(make42(command.command, command.args, {
                ...command.options,
                stdin: {
                  ...stdinConfig,
                  stream: sourceStream
                }
              })));
            }
          }
        }
        const handle = handles[handles.length - 1];
        const kill = (options) => forEach2([...handles].reverse(), (handle) => ignore2(handle.kill(options)), {
          discard: true
        });
        const unref = gen2(function* () {
          const rerefs = [];
          for (const handle of handles) {
            rerefs.push(yield* handle.unref);
          }
          return forEach2([...rerefs].reverse(), (reref) => reref, {
            discard: true
          });
        });
        return makeHandle({
          pid: handle.pid,
          exitCode: handle.exitCode,
          isRunning: handle.isRunning,
          kill,
          stdin: handles[0].stdin,
          stdout: handle.stdout,
          stderr: handle.stderr,
          all: handle.all,
          getInputFd: handle.getInputFd,
          getOutputFd: handle.getOutputFd,
          unref
        });
      }
    }
  });
  return make41(spawnCommand);
});
var layer6 = /* @__PURE__ */ effect(ChildProcessSpawner, make43);
var flattenCommand = (command) => {
  const commands = [];
  const pipeOptions = [];
  const flatten = (cmd) => {
    switch (cmd._tag) {
      case "StandardCommand": {
        commands.push(cmd);
        break;
      }
      case "PipedCommand": {
        flatten(cmd.left);
        pipeOptions.push(cmd.options);
        flatten(cmd.right);
        break;
      }
    }
  };
  flatten(command);
  if (commands.length === 0) {
    throw new Error("flattenCommand produced empty commands array");
  }
  const [first, ...rest] = commands;
  const nonEmptyCommands = [first, ...rest];
  return {
    commands: nonEmptyCommands,
    pipeOptions
  };
};

// node_modules/@effect/platform-node-shared/dist/NodePath.js
import * as NodePath from "node:path";
import * as NodeUrl from "node:url";
var fileUrlOps = (windows) => ({
  fromFileUrl: (url) => try_3({
    try: () => NodeUrl.fileURLToPath(url, {
      windows
    }),
    catch: (cause) => new BadArgument({
      module: "Path",
      method: "fromFileUrl",
      cause
    })
  }),
  toFileUrl: (path) => try_3({
    try: () => NodeUrl.pathToFileURL(path, {
      windows
    }),
    catch: (cause) => new BadArgument({
      module: "Path",
      method: "toFileUrl",
      cause
    })
  })
});
var layerPosix = /* @__PURE__ */ succeed6(Path)(/* @__PURE__ */ make15({
  ...NodePath.posix,
  .../* @__PURE__ */ fileUrlOps(false)
}));
var layerWin32 = /* @__PURE__ */ succeed6(Path)(/* @__PURE__ */ make15({
  ...NodePath.win32,
  .../* @__PURE__ */ fileUrlOps(true)
}));
var layer7 = /* @__PURE__ */ succeed6(Path)(/* @__PURE__ */ make15({
  ...NodePath,
  .../* @__PURE__ */ fileUrlOps(undefined)
}));

// node_modules/@effect/platform-node/dist/NodePath.js
var layer8 = layer7;

// node_modules/effect/dist/Stdio.js
var TypeId42 = "~effect/Stdio";
var Stdio = /* @__PURE__ */ Service(TypeId42);
var make44 = (options) => ({
  [TypeId42]: TypeId42,
  stdinIsTerminal: succeed7(false),
  stdoutIsTerminal: succeed7(false),
  stderrIsTerminal: succeed7(false),
  ...options
});

// node_modules/@effect/platform-node-shared/dist/NodeStdio.js
var layer9 = /* @__PURE__ */ succeed6(Stdio, /* @__PURE__ */ make44({
  args: /* @__PURE__ */ sync2(() => process.argv.slice(2)),
  stdinIsTerminal: /* @__PURE__ */ sync2(() => process.stdin.isTTY === true),
  stdoutIsTerminal: /* @__PURE__ */ sync2(() => process.stdout.isTTY === true),
  stderrIsTerminal: /* @__PURE__ */ sync2(() => process.stderr.isTTY === true),
  stdout: (options) => fromWritable({
    evaluate: () => process.stdout,
    onError: (cause) => systemError({
      module: "Stdio",
      method: "stdout",
      _tag: "Unknown",
      cause
    }),
    endOnDone: options?.endOnDone ?? false
  }),
  stderr: (options) => fromWritable({
    evaluate: () => process.stderr,
    onError: (cause) => systemError({
      module: "Stdio",
      method: "stderr",
      _tag: "Unknown",
      cause
    }),
    endOnDone: options?.endOnDone ?? false
  }),
  stdin: /* @__PURE__ */ fromReadable({
    evaluate: () => process.stdin,
    onError: (cause) => systemError({
      module: "Stdio",
      method: "stdin",
      _tag: "Unknown",
      cause
    }),
    closeOnDone: false
  })
}));

// node_modules/@effect/platform-node/dist/NodeStdio.js
var layer10 = layer9;

// node_modules/effect/dist/Terminal.js
var TypeId43 = "~effect/Terminal";
var QuitErrorTypeId = "~effect/Terminal/QuitError";

class QuitError extends (/* @__PURE__ */ Error4("QuitError")({
  _tag: /* @__PURE__ */ tag("QuitError")
})) {
  [QuitErrorTypeId] = QuitErrorTypeId;
}
var isQuitError = (u) => hasProperty(u, QuitErrorTypeId);
var Terminal = /* @__PURE__ */ Service("effect/Terminal");
var make45 = (impl) => Terminal.of({
  ...impl,
  [TypeId43]: TypeId43
});

// node_modules/@effect/platform-node-shared/dist/NodeTerminal.js
import * as readline from "node:readline";
var make46 = /* @__PURE__ */ fnUntraced2(function* (shouldQuit = defaultShouldQuit) {
  const stdin = process.stdin;
  const stdout = process.stdout;
  const lines = yield* make11();
  let inputEnded = stdin.readableEnded;
  let readlineActive = false;
  const onStdinEnd = () => {
    inputEnded = true;
    if (!readlineActive) {
      endUnsafe(lines);
    }
  };
  stdin.once("end", onStdinEnd);
  yield* addFinalizer3(() => sync2(() => stdin.off("end", onStdinEnd)));
  const rlRef = yield* make13({
    acquire: acquireRelease2(sync2(() => {
      const rl = readline.createInterface({
        input: stdin,
        escapeCodeTimeout: 50
      });
      const onLine = (line) => offerUnsafe(lines, line);
      const onClose = () => {
        readlineActive = false;
        endUnsafe(lines);
      };
      readlineActive = true;
      readline.emitKeypressEvents(stdin, rl);
      rl.on("line", onLine);
      rl.once("close", onClose);
      return {
        rl,
        onClose,
        onLine
      };
    }), ({
      rl,
      onClose,
      onLine
    }) => sync2(() => {
      readlineActive = false;
      rl.off("line", onLine);
      rl.off("close", onClose);
      rl.close();
      if (inputEnded) {
        endUnsafe(lines);
      }
    })),
    idleTimeToLive: "10 millis"
  });
  const setRawMode = (enabled) => sync2(() => {
    if (stdin.isTTY)
      stdin.setRawMode(enabled);
  });
  const rawModeRef = yield* make13({
    acquire: acquireRelease2(setRawMode(true), () => setRawMode(false)),
    idleTimeToLive: "10 millis"
  });
  const columns = sync2(() => stdout.columns ?? 0);
  const rows = sync2(() => stdout.rows ?? 0);
  const size = gen2(function* () {
    const queue = yield* sliding(1);
    const onResize = () => offerUnsafe(queue, {
      columns: stdout.columns ?? 0,
      rows: stdout.rows ?? 0
    });
    yield* addFinalizer3(() => sync2(() => {
      stdout.off("resize", onResize);
      endUnsafe(queue);
    }));
    stdout.on("resize", onResize);
    onResize();
    return queue;
  });
  const readInput = gen2(function* () {
    const queue = yield* make11();
    const handleKeypress = (s, k) => {
      const userInput = {
        input: fromUndefinedOr(s),
        key: {
          name: k.name ?? "",
          ctrl: !!k.ctrl,
          meta: !!k.meta,
          shift: !!k.shift
        }
      };
      offerUnsafe(queue, userInput);
      if (shouldQuit(userInput)) {
        endUnsafe(queue);
      }
    };
    const keepAlive = setInterval(() => {}, 2147483647);
    const handleEnd = () => {
      clearInterval(keepAlive);
      endUnsafe(queue);
    };
    yield* addFinalizer3(() => sync2(() => {
      clearInterval(keepAlive);
      stdin.off("keypress", handleKeypress);
      stdin.off("end", handleEnd);
    }));
    stdin.on("keypress", handleKeypress);
    if (inputEnded) {
      handleEnd();
    } else {
      yield* get3(rlRef);
      yield* get3(rawModeRef);
      stdin.once("end", handleEnd);
    }
    return queue;
  });
  const readLine = suspend2(() => poll(lines).pipe(flatMap4(match({
    onNone: () => scoped2(andThen2(get3(rlRef), take2(lines))),
    onSome: succeed7
  })), mapError3(() => new QuitError({}))));
  const display = (prompt) => uninterruptible2(callback2((resume) => {
    stdout.write(prompt, (err) => isNullish(err) ? resume(void_3) : resume(fail7(badArgument({
      module: "Terminal",
      method: "display",
      description: "Failed to write prompt to stdout",
      cause: err
    }))));
  }));
  return make45({
    columns,
    rows,
    size,
    readInput,
    readLine,
    display
  });
});
var layer11 = /* @__PURE__ */ effect(Terminal, /* @__PURE__ */ make46(defaultShouldQuit));
function defaultShouldQuit(input) {
  const key = input.key;
  return key.name === "escape" || key.ctrl && (key.name === "c" || key.name === "d");
}

// node_modules/@effect/platform-node/dist/NodeTerminal.js
var layer12 = layer11;

// node_modules/@effect/platform-node/dist/NodeServices.js
var layer13 = /* @__PURE__ */ provideMerge(layer6, /* @__PURE__ */ mergeAll2(layer4, layer8, layer10, layer12));

// node_modules/ws/wrapper.mjs
var import_stream3 = __toESM(require_stream(), 1);
var import_extension = __toESM(require_extension(), 1);
var import_permessage_deflate = __toESM(require_permessage_deflate(), 1);
var import_receiver = __toESM(require_receiver(), 1);
var import_sender = __toESM(require_sender(), 1);
var import_subprotocol = __toESM(require_subprotocol(), 1);
var import_websocket = __toESM(require_websocket(), 1);
var import_websocket_server = __toESM(require_websocket_server(), 1);

// node_modules/@effect/platform-node/dist/NodeHttpServer.js
var make47 = /* @__PURE__ */ fnUntraced2(function* (evaluate, options) {
  const scope = yield* scope2;
  const server = evaluate();
  let closing = false;
  const sessions = new Set;
  server.on("session", (session) => {
    if (closing) {
      return session.close();
    }
    sessions.add(session);
    session.once("close", () => sessions.delete(session));
  });
  const shutdown = yield* callback2((resume) => {
    if (!server.listening) {
      return resume(void_3);
    }
    server.close((error) => {
      if (error) {
        resume(die3(error));
      } else {
        resume(void_3);
      }
    });
    closing = true;
    for (const session of sessions) {
      session.close();
    }
  }).pipe(cached2);
  const preemptiveShutdown = options.disablePreemptiveShutdown ? void_3 : timeoutOrElse2(shutdown, {
    duration: options.gracefulShutdownTimeout ?? seconds(20),
    orElse: () => void_3
  });
  yield* addFinalizer2(scope, shutdown);
  yield* callback2((resume) => {
    function onError(cause) {
      resume(fail7(new ServeError({
        cause
      })));
    }
    server.on("error", onError);
    server.listen(options, () => {
      server.off("error", onError);
      resume(void_3);
    });
  });
  const address = server.address();
  const effectAddress = typeof address === "string" ? succeed7(unixPathAddress(address)) : fromResult2(inetAddressFromIpString(address.address, address.port)).pipe(mapError3((cause) => new ServeError({
    cause
  })));
  const boundAddress = yield* effectAddress;
  const wss = yield* acquireRelease2(sync2(() => new import_websocket_server.default({
    ...options.websocket,
    noServer: true
  })), (wss) => callback2((resume) => {
    wss.close(() => resume(void_3));
  })).pipe(provide(scope), cached2);
  return make37({
    address: boundAddress,
    serve: fnUntraced2(function* (httpApp, middleware) {
      const serveScope = yield* scope2;
      const scope = forkUnsafe2(serveScope, "parallel");
      const handler = yield* makeHandler(httpApp, {
        middleware,
        scope
      });
      const upgradeHandler = yield* makeUpgradeHandler(wss, httpApp, {
        middleware,
        scope
      });
      const checkContinueHandler = (request, response) => {
        pendingContinue.add(request);
        handler(request, response);
      };
      yield* addFinalizerExit(serveScope, () => ensuring2(preemptiveShutdown, sync2(() => {
        server.off("request", handler);
        server.off("checkContinue", checkContinueHandler);
        server.off("upgrade", upgradeHandler);
      })));
      server.on("request", handler);
      server.on("checkContinue", checkContinueHandler);
      server.on("upgrade", upgradeHandler);
    })
  });
});
var makeHandler = (httpEffect, options) => {
  const handled = toHandled(httpEffect, handleResponse, options.middleware);
  return withFiber2((parent) => {
    const services = parent.context;
    return succeed7(function handler(request, response) {
      const nodeRequest = request;
      const nodeResponse = response;
      if (nodeRequest.httpVersionMajor === 1) {
        nodeResponse.prependListener("finish", () => teardownBody(nodeRequest, nodeResponse));
      }
      const context = add(services, HttpServerRequest, new ServerRequestImpl(nodeRequest, nodeResponse));
      const fiber = runIn(runForkWith2(context)(handled), options.scope);
      if (fiber.pollUnsafe() === undefined) {
        nodeResponse.on("close", () => {
          if (!nodeResponse.writableEnded) {
            fiber.interruptUnsafe(parent.id, ClientAbort.annotation);
          }
        });
      }
    });
  });
};
var makeUpgradeHandler = (lazyWss, httpEffect, options) => {
  const handledApp = toHandled(httpEffect, handleResponse, options.middleware);
  return withFiber2((parent) => {
    const services = parent.context;
    return succeed7(function handler(nodeRequest, socket, head) {
      const protocol = nodeRequest.headers.upgrade;
      const isWebSocket = protocol === undefined || protocol.toLowerCase() === "websocket";
      let body;
      if (!isWebSocket && legacyUpgradeBody) {
        if (nodeRequest.headers["transfer-encoding"] !== undefined) {
          socket.on("error", () => {});
          socket.end(`HTTP/1.1 411 Length Required\r
Connection: close\r
Content-Length: 0\r
\r
`);
          return;
        }
        const framed = frameBody(socket, head, Number(nodeRequest.headers["content-length"] ?? 0));
        body = framed.body;
        head = framed.rest;
      }
      let nodeResponse_ = undefined;
      const nodeResponse = () => {
        if (nodeResponse_ === undefined) {
          nodeResponse_ = new http.ServerResponse(nodeRequest);
          if (state.response !== undefined || socket.destroyed) {
            nodeResponse_.end();
          } else {
            nodeResponse_.assignSocket(socket);
            nodeResponse_.on("finish", () => {
              socket.end();
            });
          }
        }
        return nodeResponse_;
      };
      const webSocketUpgrade = fromWebSocket(flatMap4(lazyWss, (wss) => acquireRelease2(callback2((resume) => {
        const onClose = () => resume(fail7(new SocketError({
          reason: new SocketOpenError({
            kind: "Unknown",
            cause: new Error("The socket closed before the upgrade")
          })
        })));
        if (socket.destroyed)
          return onClose();
        socket.once("close", onClose);
        wss.handleUpgrade(nodeRequest, socket, head, (ws) => {
          socket.off("close", onClose);
          state.response = upgradedResponse;
          resume(succeed7(ws));
        });
      }), (ws, exit) => sync2(() => ws.close(closeCode(exit))))));
      const state = {
        body,
        response: undefined,
        upgradeWith(request, headers) {
          if (isWebSocket) {
            return Object.keys(headers).length === 0 ? webSocketUpgrade : fail7(upgradeError(request, "WebSocket upgrades do not support custom response headers"));
          }
          const invalid = validateUpgradeHeaders(request, headers);
          if (invalid !== undefined)
            return fail7(invalid);
          return toSocket(socket, withFiber2((fiber) => {
            const maxBytes = fiber.getRef(MaxBodySize);
            return drainBody(body ?? nodeRequest, maxBytes === undefined ? undefined : Number(maxBytes));
          }).pipe(andThen2(sync2(() => {
            if (state.response !== undefined)
              return;
            state.response = empty10({
              status: 101,
              headers: {
                ...headers,
                connection: "Upgrade",
                upgrade: protocol
              }
            });
            if (head.length > 0)
              socket.unshift(head);
            let responseHead = `HTTP/1.1 101 Switching Protocols\r
Connection: Upgrade\r
Upgrade: ${protocol}\r
`;
            for (const name in headers)
              responseHead += `${name}: ${headers[name]}\r
`;
            socket.write(responseHead + `\r
`, "latin1");
          }))));
        }
      };
      const request = new ServerRequestImpl(nodeRequest, nodeResponse, state);
      const context = add(services, HttpServerRequest, request);
      const fiber = runIn(runForkWith2(context)(handledApp), options.scope);
      socket.on("error", () => {});
      socket.on("close", () => {
        if (!socket.writableEnded) {
          fiber.interruptUnsafe(parent.id, ClientAbort.annotation);
        }
      });
    });
  });
};
var legacyUpgradeBody = /* @__PURE__ */ Number(process.versions.node.split(".")[0]) < 26;
var reservedUpgradeHeaders = /* @__PURE__ */ new Set(["connection", "upgrade", "content-length", "transfer-encoding"]);
var validateUpgradeHeaders = (request, headers) => {
  for (const name in headers) {
    if (reservedUpgradeHeaders.has(name)) {
      return upgradeError(request, `Upgrade responses cannot set the ${name} header`);
    }
    try {
      http.validateHeaderName(name);
      http.validateHeaderValue(name, headers[name]);
    } catch (cause) {
      return upgradeError(request, `Invalid upgrade response header: ${name}`, cause);
    }
  }
};
var upgradeError = (request, description, cause) => new HttpServerError({
  reason: new InternalError({
    request,
    description,
    cause
  })
});
var isHttp2 = (request) => request?.httpVersionMajor === 2;
var requestHeaders = (source) => {
  if (!isHttp2(source)) {
    return source.headers;
  }
  const headers = {};
  for (const name in source.headers) {
    if (name[0] !== ":")
      headers[name] = source.headers[name];
  }
  const authority = source.headers[":authority"];
  if (typeof authority === "string")
    headers.host = authority;
  return headers;
};
var absoluteTarget = (url) => {
  if (!/^https?:\/\//i.test(url)) {
    return;
  }
  try {
    return new URL(url);
  } catch {
    return;
  }
};
var pendingContinue = /* @__PURE__ */ new WeakSet;
var bodyBytesRead = /* @__PURE__ */ new WeakMap;

class ServerRequestImpl extends NodeHttpIncomingMessage {
  destroyOnOverflow = false;
  closeOnDone = false;
  [TypeId40];
  response;
  upgradeState;
  url;
  headersOverride;
  peerAddressOverride;
  protocol;
  constructor(source, response, upgradeState, url, headersOverride, remoteAddressOverride, peerAddressOverride, protocol) {
    super(source, (cause) => new HttpServerError({
      reason: new RequestParseError({
        request: this,
        reason: cause instanceof MaxBytesError ? "BodyTooLarge" : "Parse",
        cause
      })
    }), remoteAddressOverride);
    this[TypeId40] = TypeId40;
    this.response = response;
    this.upgradeState = upgradeState;
    if (url === undefined) {
      const target = absoluteTarget(source.url);
      if (target === undefined) {
        url = source.url;
      } else {
        url = target.pathname + target.search;
        const headers = {
          ...requestHeaders(source),
          host: target.host
        };
        headersOverride = fromInput2(headers);
      }
    }
    this.url = url;
    this.headersOverride = headersOverride;
    this.peerAddressOverride = peerAddressOverride;
    this.protocol = protocol;
  }
  get peerAddress() {
    return this.peerAddressOverride ?? this.remoteAddress;
  }
  cachedCookies;
  get cookies() {
    if (this.cachedCookies) {
      return this.cachedCookies;
    }
    return this.cachedCookies = parseHeader(this.headers.cookie ?? "");
  }
  get resolvedResponse() {
    return typeof this.response === "function" ? this.response() : this.response;
  }
  writeContinue() {
    if (pendingContinue.delete(this.source) && !this.resolvedResponse.headersSent) {
      this.resolvedResponse.writeContinue();
    }
  }
  get stream() {
    return suspend4(() => {
      this.writeContinue();
      const source = this.source;
      return mapArray(super.stream, (chunk) => {
        let bytes = bodyBytesRead.get(source) ?? 0;
        for (const item of chunk)
          bytes += item.length;
        bodyBytesRead.set(source, bytes);
        return chunk;
      });
    });
  }
  get arrayBuffer() {
    return suspend2(() => {
      this.writeContinue();
      return super.arrayBuffer;
    });
  }
  modify(options) {
    return new ServerRequestImpl(this.source, this.response, this.upgradeState, options.url ?? this.url, options.headers ?? this.headersOverride, "remoteAddress" in options ? options.remoteAddress : this.remoteAddressOverride, options.peerAddress ?? this.peerAddressOverride, options.protocol ?? this.protocol);
  }
  get originalUrl() {
    const url = this.source.url;
    const headers = this.source.headers;
    const http2 = isHttp2(this.source);
    const host = (http2 ? headers[":authority"] : undefined) ?? headers.host;
    if (url[0] !== "/" || host === undefined) {
      return url;
    }
    const scheme = http2 ? headers[":scheme"] : undefined;
    const encrypted = this.source.socket?.encrypted;
    return `${scheme ?? (encrypted ? "https" : "http")}://${host}${url}`;
  }
  cachedMethod;
  get method() {
    return this.cachedMethod ??= this.source.method.toUpperCase();
  }
  get headers() {
    this.headersOverride ??= fromInput2(requestHeaders(this.source));
    return this.headersOverride;
  }
  multipartEffect;
  get multipart() {
    if (this.multipartEffect) {
      return this.multipartEffect;
    }
    this.multipartEffect = runSync2(cached2(suspend2(() => {
      this.writeContinue();
      return persisted(this.bodySource, this.source.headers);
    })));
    return this.multipartEffect;
  }
  get multipartStream() {
    return suspend4(() => {
      this.writeContinue();
      return stream2(this.bodySource, this.source.headers);
    });
  }
  get upgrade() {
    return this.upgradeWith({});
  }
  upgradeWith(options) {
    const state = this.upgradeState;
    if (state === undefined) {
      return fail7(new HttpServerError({
        reason: new RequestParseError({
          request: this,
          description: "not an upgradeable ServerRequest"
        })
      }));
    }
    return state.upgradeWith(this, fromInput2(options.headers));
  }
  get upgradedResponse() {
    return this.upgradeState?.response;
  }
  get bodySource() {
    return this.upgradeState?.body ?? this.source;
  }
  toString() {
    return `ServerRequest(${this.method} ${redactUrl(this.url)})`;
  }
  toJSON() {
    return inspect(this, {
      _id: "HttpServerRequest",
      method: this.method,
      url: redactUrl(this.originalUrl)
    });
  }
}
var layerServer = /* @__PURE__ */ flow(make47, /* @__PURE__ */ effect(HttpServer));
var layerHttpServices = /* @__PURE__ */ mergeAll2(layer5, layerWeak, layer13);
var layer14 = (evaluate, options) => mergeAll2(layerServer(evaluate, options), layerHttpServices);
var layerTest = /* @__PURE__ */ layerTestClient.pipe(/* @__PURE__ */ provide2(/* @__PURE__ */ fresh(layer2).pipe(/* @__PURE__ */ provide2(/* @__PURE__ */ succeed6(RequestInit)({
  keepalive: false
})))), /* @__PURE__ */ provideMerge(/* @__PURE__ */ layer14(http.createServer, {
  port: 0
})));
var closeCode = (exit) => isSuccess3(exit) ? 1000 : hasInterruptsOnly2(exit.cause) ? 1001 : 1011;
var upgradedResponse = /* @__PURE__ */ empty10({
  status: 101
});
var handleResponse = (request, response) => {
  const upgraded = request.upgradedResponse;
  if (upgraded !== undefined) {
    return succeed7(upgraded);
  }
  const nodeResponse = request.resolvedResponse;
  if (nodeResponse.writableEnded) {
    return void_3;
  }
  let headers = response.headers;
  if (response.status === 204 || response.status === 205) {
    const {
      "content-length": _,
      ...rest
    } = headers;
    headers = rest;
  }
  if (!isEmpty(response.cookies)) {
    headers = {
      ...headers
    };
    const toSet = toSetCookieHeaders(response.cookies);
    if (headers["set-cookie"] !== undefined) {
      toSet.push(headers["set-cookie"]);
    }
    headers["set-cookie"] = toSet;
  }
  try {
    return writeResponse(request, response, nodeResponse, headers);
  } catch (defect) {
    return andThen2(cancelResponseBody(response.body), handleCause(nodeResponse, response)(die2(defect)));
  }
};
var writeResponse = (request, response, nodeResponse, headers) => {
  if (nodeResponse.destroyed) {
    return cancelResponseBody(response.body);
  }
  if (request.method === "HEAD") {
    writeHead(nodeResponse, response, headers);
    return andThen2(cancelResponseBody(response.body), endAndAwait(nodeResponse));
  }
  if (omitsBody(response)) {
    writeHead(nodeResponse, response, headers);
    nodeResponse.end();
    return cancelResponseBody(response.body);
  }
  nodeResponse.strictContentLength = true;
  const body = response.body;
  switch (body._tag) {
    case "Empty": {
      writeHead(nodeResponse, response, headers);
      return end(nodeResponse, response);
    }
    case "Raw": {
      const piped = typeof body.body === "object" && body.body !== null && "pipe" in body.body && typeof body.body.pipe === "function";
      writeHead(nodeResponse, response, headers, piped);
      if (piped) {
        return tryPromise2({
          try: (signal) => pipeTo2(body.body, nodeResponse, headers, signal),
          catch: (cause) => new HttpServerError({
            reason: new ResponseError({
              request,
              response,
              description: "Error writing raw response",
              cause
            })
          })
        }).pipe(interruptible2, tapCause2(handleCause(nodeResponse, response)));
      }
      return endAndAwait(nodeResponse, body.body).pipe(tapCause2(handleCause(nodeResponse, response)));
    }
    case "Uint8Array": {
      writeHead(nodeResponse, response, headers);
      if (body.contentLength < 1024 * 1024) {
        return end(nodeResponse, response, body.text ?? body.body);
      }
      return endAndAwait(nodeResponse, body.text ?? body.body).pipe(tapCause2(handleCause(nodeResponse, response)));
    }
    case "FormData": {
      const r = new globalThis.Response(body.formData);
      const formHeaders = {
        ...headers,
        ...Object.fromEntries(r.headers)
      };
      writeHead(nodeResponse, response, formHeaders);
      return tryPromise2({
        try: (signal) => pipeTo2(Readable5.fromWeb(r.body), nodeResponse, formHeaders, signal),
        catch: (cause) => new HttpServerError({
          reason: new ResponseError({
            request,
            response,
            description: "Error writing FormData response",
            cause
          })
        })
      }).pipe(interruptible2, tapCause2(handleCause(nodeResponse, response)));
    }
    case "Stream": {
      writeHead(nodeResponse, response, headers, true);
      const drainLatch = makeUnsafe5();
      nodeResponse.on("drain", () => drainLatch.openUnsafe());
      return body.stream.pipe(orDie4, runForEachArray((array) => {
        const chunk = array.length > 1 ? Buffer.concat(array) : array[0];
        if (nodeResponse.write(chunk))
          return void_3;
        drainLatch.closeUnsafe();
        return drainLatch.await;
      }), interruptible2, matchCauseEffect2({
        onSuccess: () => end(nodeResponse, response),
        onFailure: handleCause(nodeResponse, response)
      }));
    }
  }
};
var isFinalStatus = (status) => status >= 200 && status <= 599;
var internalServerError2 = /* @__PURE__ */ empty10({
  status: 500
});
var invalidStatusError = (status) => new RangeError(`Invalid final response status: ${status}`);
var writeHead = (nodeResponse, response, headers, streaming = false) => {
  if (!isFinalStatus(response.status)) {
    throw invalidStatusError(response.status);
  }
  if (isHttp2(nodeResponse.req)) {
    nodeResponse.writeHead(response.status, withoutConnectionHeaders(headers));
    return;
  }
  nodeResponse.writeHead(response.status, response.statusText, closesConnection(nodeResponse.req, streaming) ? {
    ...headers,
    connection: "close"
  } : headers);
};
var connectionHeaders = /* @__PURE__ */ new Set(["connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "http2-settings"]);
var withoutConnectionHeaders = (headers) => {
  let result;
  for (const name in headers) {
    if (connectionHeaders.has(name) || name === "te" && headers[name] !== "trailers") {
      result ??= {
        ...headers
      };
      delete result[name];
    }
  }
  return result ?? headers;
};
var maxDrainBytes = 256 * 1024;
var closeGracePeriod = 500;
var closesConnection = (request, streaming) => {
  if (request === undefined || request.httpVersionMajor !== 1 || request.complete) {
    return false;
  }
  if (pendingContinue.has(request)) {
    return true;
  }
  const contentLength = request.headers["content-length"];
  return !streaming && contentLength !== undefined && Number(contentLength) - (bodyBytesRead.get(request) ?? 0) > maxDrainBytes;
};
var teardownBody = (request, response) => {
  if (request.complete || request.destroyed) {
    return;
  }
  const socket = request.socket;
  if (response._last) {
    request._consuming = true;
    response._last = false;
    return closeSocket(socket);
  }
  let bytes = 0;
  request.on("data", (chunk) => {
    bytes += chunk.length;
    if (bytes > maxDrainBytes) {
      request.pause();
      closeSocket(socket);
    }
  });
  request.resume();
};
var closeSocket = (socket) => {
  if (socket.destroyed) {
    return;
  }
  socket.end();
  const timer = setTimeout(() => socket.destroy(), closeGracePeriod);
  socket.once("close", () => clearTimeout(timer));
};
var cancelResponseBody = (body) => {
  const stream = body._tag === "Raw" ? body.body : undefined;
  if (stream instanceof Readable5) {
    return sync2(() => stream.destroy());
  }
  return void_3;
};
var endAndAwait = (nodeResponse, chunk) => callback2((resume) => {
  if (nodeResponse.destroyed)
    return resume(void_3);
  let completed = false;
  const done = () => {
    if (completed)
      return;
    completed = true;
    nodeResponse.off("close", done);
    resume(void_3);
  };
  nodeResponse.once("close", done);
  if (chunk === undefined) {
    nodeResponse.end(done);
  } else {
    nodeResponse.end(chunk, done);
  }
});
var end = (nodeResponse, response, chunk) => {
  try {
    nodeResponse.end(chunk);
    return void_3;
  } catch (defect) {
    return handleCause(nodeResponse, response)(die2(defect));
  }
};
var pipeTo2 = (readable, nodeResponse, headers, signal) => {
  const contentLength = headers["content-length"];
  if (typeof contentLength !== "string") {
    return pipeline3(readable, nodeResponse, {
      signal
    });
  }
  let remaining = Number(contentLength);
  const mismatch = () => new Error(`Response body doesn't match content-length of ${contentLength}`);
  return pipeline3(readable, new Transform({
    transform(chunk, _encoding, callback) {
      remaining -= chunk.length;
      callback(remaining < 0 ? mismatch() : null, chunk);
    },
    flush(callback) {
      callback(remaining > 0 ? mismatch() : null);
    }
  }), nodeResponse, {
    signal
  });
};
var handleCause = (nodeResponse, originalResponse) => (originalCause) => flatMap4(causeResponse(originalCause), ([response, cause]) => {
  const headersSent = nodeResponse.headersSent;
  let abort = headersSent;
  if (!headersSent) {
    if (!isFinalStatus(response.status)) {
      cause = combine2(cause, die2(invalidStatusError(response.status)));
      response = internalServerError2;
    }
    try {
      writeHead(nodeResponse, response, {});
    } catch {
      abort = true;
    }
  }
  if (!nodeResponse.writableEnded) {
    if (abort) {
      nodeResponse.destroy(isHttp2(nodeResponse.req) ? new Error("The response was aborted") : undefined);
    } else {
      nodeResponse.end();
    }
  }
  return failCause3(headersSent ? combine2(originalCause, die2(originalResponse)) : cause);
});
// node_modules/effect/dist/Runtime.js
var defaultTeardown = (exit, onExit) => {
  if (isSuccess3(exit))
    return onExit(0);
  if (hasInterruptsOnly2(exit.cause))
    return onExit(130);
  return onExit(getErrorExitCode(squash(exit.cause)));
};
var makeRunMain = (f) => dual((args) => isEffect2(args[0]), (effect, options) => {
  const main = options?.disableErrorReporting === true ? effect : tapCause2(effect, (cause) => {
    if (hasInterruptsOnly2(cause))
      return void_3;
    const isReported = getErrorReported(squash(cause));
    return isReported ? logError(cause) : void_3;
  });
  const teardown = options?.teardown ?? defaultTeardown;
  runFork2(withFiber2((fiber) => {
    try {
      const keepAlive = globalThis.setInterval(constVoid, 2147483647);
      fiber.addObserver(() => {
        clearInterval(keepAlive);
      });
    } catch {}
    f({
      fiber,
      teardown
    });
    return main;
  }));
});
var errorExitCode = "~effect/Runtime/errorExitCode";
var getErrorExitCode = (u) => {
  if (typeof u === "object" && u !== null && errorExitCode in u) {
    const code = u[errorExitCode];
    if (typeof code === "number") {
      return code;
    }
  }
  return 1;
};
var errorReported = "~effect/Runtime/errorReported";
var getErrorReported = (u) => {
  if (typeof u === "object" && u !== null && errorReported in u) {
    const isReported = u[errorReported];
    if (typeof isReported === "boolean") {
      return isReported;
    }
  }
  return true;
};

// node_modules/@effect/platform-node-shared/dist/NodeRuntime.js
var runMain = /* @__PURE__ */ makeRunMain(({
  fiber,
  teardown
}) => {
  let receivedSignal = false;
  fiber.addObserver((exit) => {
    process.removeListener("SIGINT", onSigint);
    process.removeListener("SIGTERM", onSigint);
    teardown(exit, (code) => {
      if (receivedSignal || code !== 0) {
        process.exit(code);
      }
    });
  });
  function onSigint() {
    receivedSignal = true;
    fiber.interruptUnsafe(fiber.id);
  }
  process.on("SIGINT", onSigint);
  process.on("SIGTERM", onSigint);
});

// node_modules/@effect/platform-node/dist/NodeRuntime.js
var runMain2 = runMain;
// node_modules/effect/dist/Console.js
var Console = ConsoleRef;
var consoleWith = (f) => withFiber((fiber) => f(fiber.getRef(Console)));
var error = (...args) => consoleWith((console) => sync(() => {
  console.error(...args);
}));
var log2 = (...args) => consoleWith((console) => sync(() => {
  console.log(...args);
}));
// node_modules/effect/dist/internal/matcher.js
var TypeId44 = "~effect/Match/Matcher";
var TypeMatcherProto = {
  [TypeId44]: {
    _input: identity,
    _filters: identity,
    _remaining: identity,
    _result: identity,
    _return: identity,
    _args: identity
  },
  _tag: "TypeMatcher",
  add(_case) {
    return makeTypeMatcher(this.select, [...this.cases, _case]);
  },
  pipe() {
    return pipeArguments(this, arguments);
  }
};
function makeTypeMatcher(select, cases) {
  const matcher = Object.create(TypeMatcherProto);
  matcher.select = select;
  matcher.cases = cases;
  return matcher;
}
var ValueMatcherProto = {
  [TypeId44]: {
    _input: identity,
    _filters: identity,
    _result: identity,
    _return: identity,
    _flavor: identity
  },
  _tag: "ValueMatcher",
  add(_case) {
    if (isSuccess2(this.value)) {
      return this;
    }
    if (_case._tag === "When" && _case.guard(this.provided) === true) {
      return makeValueMatcher(this.provided, succeed2(_case.evaluate(this.provided)));
    } else if (_case._tag === "Not" && _case.guard(this.provided) === false) {
      return makeValueMatcher(this.provided, succeed2(_case.evaluate(this.provided)));
    }
    return this;
  },
  pipe() {
    return pipeArguments(this, arguments);
  }
};
function makeValueMatcher(provided, value) {
  const matcher = Object.create(ValueMatcherProto);
  matcher.provided = provided;
  matcher.value = value;
  return matcher;
}
var makeWhen = (guard, evaluate) => ({
  _tag: "When",
  guard,
  evaluate
});
var makePredicate = (pattern) => {
  if (typeof pattern === "function") {
    return pattern;
  } else if (Array.isArray(pattern)) {
    const predicates = pattern.map(makePredicate);
    const len = predicates.length;
    return (u) => {
      if (!Array.isArray(u) || u.length < len) {
        return false;
      }
      for (let i = 0;i < len; i++) {
        if (predicates[i](u[i]) === false) {
          return false;
        }
      }
      return true;
    };
  } else if (pattern !== null && typeof pattern === "object") {
    const keysAndPredicates = Reflect.ownKeys(pattern).map((key) => [key, makePredicate(pattern[key])]);
    const len = keysAndPredicates.length;
    return (u) => {
      if (typeof u !== "object" || u === null) {
        return false;
      }
      for (let i = 0;i < len; i++) {
        const [key, predicate] = keysAndPredicates[i];
        if (!(key in u) || predicate(u[key]) === false) {
          return false;
        }
      }
      return true;
    };
  }
  return (u) => u === pattern;
};
var value3 = (i) => makeValueMatcher(i, fail2(i));
var when2 = (pattern, f) => (self) => self.add(makeWhen(makePredicate(pattern), f));
var discriminators = (field) => (fields) => {
  const predicate = makeWhen((arg) => arg != null && Object.hasOwn(fields, arg[field]), (data) => fields[data[field]](data));
  return (self) => self.add(predicate);
};
var discriminatorsExhaustive = (field) => (fields) => {
  const addCases = discriminators(field)(fields);
  return (matcher) => exhaustive(addCases(matcher));
};
var tagsExhaustive = /* @__PURE__ */ discriminatorsExhaustive("_tag");
var orElse2 = (f) => (self) => {
  const toResult = result2(self);
  if (isResult2(toResult)) {
    return toResult._tag === "Success" ? toResult.success : f(toResult.failure);
  }
  return (...args) => {
    const a = toResult(...args);
    return isSuccess2(a) ? a.success : f(a.failure, ...args);
  };
};
var result2 = (self) => {
  if (self._tag === "ValueMatcher") {
    return self.value;
  }
  const len = self.cases.length;
  if (len === 1) {
    const _case = self.cases[0];
    return (...args) => {
      const input = self.select(...args);
      if (_case._tag === "When" && _case.guard(input) === true) {
        return succeed2(_case.evaluate(input, ...args));
      } else if (_case._tag === "Not" && _case.guard(input) === false) {
        return succeed2(_case.evaluate(input, ...args));
      }
      return fail2(input);
    };
  }
  return (...args) => {
    const input = self.select(...args);
    for (let i = 0;i < len; i++) {
      const _case = self.cases[i];
      if (_case._tag === "When" && _case.guard(input) === true) {
        return succeed2(_case.evaluate(input, ...args));
      } else if (_case._tag === "Not" && _case.guard(input) === false) {
        return succeed2(_case.evaluate(input, ...args));
      }
    }
    return fail2(input);
  };
};
var getExhaustiveAbsurdErrorMessage = "effect/match/Match/exhaustive: absurd";
var exhaustive = (self) => {
  const toResult = result2(self);
  if (isResult2(toResult)) {
    if (isSuccess2(toResult)) {
      return toResult.success;
    }
    throw new Error(getExhaustiveAbsurdErrorMessage);
  }
  return (...args) => {
    const result = toResult(...args);
    if (isSuccess2(result)) {
      return result.success;
    }
    throw new Error(getExhaustiveAbsurdErrorMessage);
  };
};

// node_modules/effect/dist/Match.js
var value4 = value3;
var when3 = when2;
var tagsExhaustive2 = tagsExhaustive;
var string3 = isString;
var _undefined = isUndefined;
var orElse3 = orElse2;
// node_modules/effect/dist/cli/CliError.js
var TypeId45 = "~effect/cli/CliError";
var isCliError = (u) => hasProperty(u, TypeId45);
var userMessage = "~effect/cli/CliError/userMessage";
var getUserMessage = (error) => (userMessage in error) && typeof error[userMessage] === "string" ? error[userMessage] : undefined;

class UnrecognizedOption extends (/* @__PURE__ */ TaggedError3(`${TypeId45}/UnrecognizedOption`)("UnrecognizedOption", {
  option: String5,
  command: /* @__PURE__ */ optional2(/* @__PURE__ */ ArraySchema(String5)),
  suggestions: /* @__PURE__ */ ArraySchema(String5)
})) {
  [TypeId45] = TypeId45;
  get message() {
    const suggestionText = this.suggestions.length > 0 ? `

  Did you mean this?
    ${this.suggestions.join(`
    `)}` : "";
    const baseMessage = this.command ? `Unrecognized flag: ${this.option} in command ${this.command.join(" ")}` : `Unrecognized flag: ${this.option}`;
    return baseMessage + suggestionText;
  }
}

class DuplicateOption extends (/* @__PURE__ */ TaggedError3(`${TypeId45}/DuplicateOption`)("DuplicateOption", {
  option: String5,
  parentCommand: String5,
  childCommand: String5
})) {
  [TypeId45] = TypeId45;
  get message() {
    return `Duplicate flag name "${this.option}" in parent command "${this.parentCommand}" and subcommand "${this.childCommand}". ` + `Parent will always claim this flag (Mode A semantics). Consider renaming one of them to avoid confusion.`;
  }
}

class MissingOption extends (/* @__PURE__ */ TaggedError3(`${TypeId45}/MissingOption`)("MissingOption", {
  option: String5
})) {
  [TypeId45] = TypeId45;
  get message() {
    return `Missing required flag: --${this.option}`;
  }
}

class MissingArgument extends (/* @__PURE__ */ TaggedError3(`${TypeId45}/MissingArgument`)("MissingArgument", {
  argument: String5
})) {
  [TypeId45] = TypeId45;
  get message() {
    return `Missing required argument: ${this.argument}`;
  }
}

class UnexpectedArgument extends (/* @__PURE__ */ TaggedError3(`${TypeId45}/UnexpectedArgument`)("UnexpectedArgument", {
  arguments: /* @__PURE__ */ ArraySchema(String5)
})) {
  [TypeId45] = TypeId45;
  get message() {
    const label = this.arguments.length === 1 ? "argument" : "arguments";
    return `Unexpected positional ${label}: ${this.arguments.map((value) => JSON.stringify(value)).join(", ")}`;
  }
}

class InvalidValue2 extends (/* @__PURE__ */ TaggedError3(`${TypeId45}/InvalidValue`)("InvalidValue", {
  option: String5,
  value: String5,
  expected: String5,
  kind: /* @__PURE__ */ Union2([/* @__PURE__ */ Literal2("flag"), /* @__PURE__ */ Literal2("argument")])
})) {
  [TypeId45] = TypeId45;
  get message() {
    const expectation = this.expected.startsWith("Expected ") || this.expected.startsWith("Expected:") ? this.expected : `Expected: ${this.expected}`;
    if (this.kind === "argument") {
      return `Invalid value for argument <${this.option}>: "${this.value}". ${expectation}`;
    }
    if (this.value.length === 0) {
      return `Missing value for flag --${this.option}. ${expectation}`;
    }
    return `Invalid value for flag --${this.option}: "${this.value}". ${expectation}`;
  }
}

class UnknownSubcommand extends (/* @__PURE__ */ TaggedError3(`${TypeId45}/UnknownSubcommand`)("UnknownSubcommand", {
  subcommand: String5,
  parent: /* @__PURE__ */ optional2(/* @__PURE__ */ ArraySchema(String5)),
  suggestions: /* @__PURE__ */ ArraySchema(String5)
})) {
  [TypeId45] = TypeId45;
  get message() {
    const suggestionText = this.suggestions.length > 0 ? `

  Did you mean this?
    ${this.suggestions.join(`
    `)}` : "";
    return this.parent ? `Unknown subcommand "${this.subcommand}" for "${this.parent.join(" ")}"${suggestionText}` : `Unknown subcommand "${this.subcommand}"${suggestionText}`;
  }
}

class ValidationError extends (/* @__PURE__ */ TaggedError3(`${TypeId45}/ValidationError`)("ValidationError", {
  message: String5
})) {
  [TypeId45] = TypeId45;
}

class UserError extends (/* @__PURE__ */ TaggedError3(`${TypeId45}/UserError`)("UserError", {
  cause: /* @__PURE__ */ Defect(),
  userMessage: /* @__PURE__ */ optionalKey2(String5)
})) {
  [TypeId45] = TypeId45;
  [errorReported] = true;
  get message() {
    if (this.userMessage)
      return this.userMessage;
    if (typeof this.cause === "string" && this.cause)
      return this.cause;
    if (this.cause instanceof Error && this.cause.message)
      return this.cause.message;
    return "An error occurred";
  }
  get [userMessage]() {
    return this.message;
  }
}
var NonShowHelpErrors = /* @__PURE__ */ Union2([UnrecognizedOption, DuplicateOption, MissingOption, MissingArgument, UnexpectedArgument, InvalidValue2, UnknownSubcommand, ValidationError, UserError]);

class ShowHelp extends (/* @__PURE__ */ TaggedError3(`${TypeId45}/ShowHelp`)("ShowHelp", {
  commandPath: /* @__PURE__ */ ArraySchema(String5),
  errors: /* @__PURE__ */ ArraySchema(NonShowHelpErrors)
})) {
  [TypeId45] = TypeId45;
  [errorExitCode] = this.errors.length ? 1 : 0;
  [errorReported] = false;
  get message() {
    return "Help requested";
  }
}

// node_modules/effect/dist/cli/Primitive.js
var TypeId46 = "~effect/cli/Primitive";
var Proto15 = {
  [TypeId46]: {
    _A: identity
  }
};
var isTrueLiteral = /* @__PURE__ */ is2(TrueLiterals);
var isFalseLiteral = /* @__PURE__ */ is2(FalseLiterals);
var isBoolean2 = (p) => p._tag === "Boolean";
var makePrimitive2 = (tag, parse) => Object.assign(Object.create(Proto15), {
  _tag: tag,
  parse
});
var makeSchemaPrimitive = (tag, schema) => {
  const toCodecStringTree = toCodecStringTree2(schema);
  const decode = decodeUnknownEffect2(toCodecStringTree);
  return makePrimitive2(tag, (value) => mapError3(decode(value), (error) => error.message));
};
var Boolean4 = /* @__PURE__ */ makeSchemaPrimitive("Boolean", BooleanLiterals);
var Int2 = /* @__PURE__ */ makeSchemaPrimitive("Int", Int);
var String7 = /* @__PURE__ */ makePrimitive2("String", (value) => succeed7(value));
var Choice = (choices) => {
  const choiceMap = new Map(choices);
  const validChoices = choices.map(([key]) => format(key)).join(" | ");
  const primitive = makePrimitive2("Choice", (value) => {
    if (choiceMap.has(value)) {
      return succeed7(choiceMap.get(value));
    }
    return fail7(validChoices);
  });
  return Object.assign(primitive, {
    choiceKeys: choices.map(([key]) => key)
  });
};
var getTypeName = (primitive) => {
  switch (primitive._tag) {
    case "Boolean":
      return "boolean";
    case "String":
      return "string";
    case "Int":
      return "integer";
    case "Finite":
      return "number";
    case "Date":
      return "date";
    case "Path":
      return "path";
    case "Choice":
      return "choice";
    case "Redacted":
      return "string";
    case "FileText":
      return "file";
    case "FileParse":
      return "file";
    case "FileSchema":
      return "file";
    case "KeyValuePair":
      return "key=value";
    case "Never":
      return "none";
    default:
      return "value";
  }
};
var getChoiceKeys = (primitive) => primitive._tag === "Choice" ? primitive.choiceKeys : undefined;
var getPathType = (primitive) => primitive._tag === "Path" ? primitive.pathType : undefined;

// node_modules/effect/dist/cli/internal/ansi.js
var ESC = "\x1B[";
var BEL = "\x07";
var SEP = ";";
var reset = `${ESC}0m`;
var bold = `${ESC}1m`;
var italicized = `${ESC}3m`;
var underlined = `${ESC}4m`;
var strikethrough = `${ESC}9m`;
var cursorShow = `${ESC}?25h`;
var cursorHide = `${ESC}?25l`;
var cursorLeft = `${ESC}G`;
var cursorSavePosition = `${ESC}s`;
var cursorRestorePosition = `${ESC}u`;
var eraseLine = `${ESC}2K`;
var beep = BEL;
var red = `${ESC}31m`;
var green = `${ESC}32m`;
var magenta = `${ESC}35m`;
var white = `${ESC}37m`;
var blackBright = `${ESC}90m`;
var cyanBright = `${ESC}96m`;
var annotate2 = (colors, text, ...styles) => {
  if (!colors)
    return text;
  return `${styles.flat().join("")}${text}${reset}`;
};
var detectColors = () => {
  const globalProcess = globalThis.process;
  return typeof globalProcess === "object" && globalProcess !== null && typeof globalProcess.stdout === "object" && globalProcess.stdout !== null && globalProcess.stdout.isTTY === true && !globalProcess.env?.NO_COLOR;
};
var combine4 = (...styles) => styles;
var cursorTo = (column, row) => {
  if (row === undefined) {
    return `${ESC}${Math.max(column + 1, 0)}G`;
  }
  return `${ESC}${row + 1}${SEP}${Math.max(column + 1, 0)}H`;
};
var cursorDown = (lines = 1) => {
  return `${ESC}${lines}B`;
};
var cursorMove = (column, row = 0) => {
  let command = "";
  if (row < 0) {
    command += `${ESC}${-row}A`;
  }
  if (row > 0) {
    command += `${ESC}${row}B`;
  }
  if (column > 0) {
    command += `${ESC}${column}C`;
  }
  if (column < 0) {
    command += `${ESC}${-column}D`;
  }
  return command;
};
var eraseLines = (rows) => {
  let command = "";
  for (let i = 0;i < rows; i++) {
    command += `${ESC}2K` + (i < rows - 1 ? `${ESC}1A` : "");
  }
  if (rows > 0) {
    command += `${ESC}G`;
  }
  return command;
};

// node_modules/effect/dist/cli/Prompt.js
var TypeId47 = "~effect/cli/Prompt";
var defaultTheme = {
  prefix: "?",
  arrowUp: "↑",
  arrowDown: "↓",
  checkboxOn: "☒",
  checkboxOff: "☐",
  tick: "✔",
  ellipsis: "…",
  pointerSmall: "›",
  pointer: "❯",
  descriptionSeparator: "- ",
  passwordMask: "*",
  toggleSeparator: "/",
  primaryColor: cyanBright,
  mutedColor: blackBright,
  successColor: green,
  errorColor: red,
  submittedColor: white
};
var windowsTheme = {
  ...defaultTheme,
  checkboxOn: "[*]",
  checkboxOff: "[ ]",
  tick: "√",
  ellipsis: "...",
  pointerSmall: "»",
  pointer: ">"
};
var makeTheme = (options) => ({
  ...process.platform === "win32" ? windowsTheme : defaultTheme,
  ...options,
  colors: options?.colors ?? detectColors()
});
var Theme = /* @__PURE__ */ Reference("effect/cli/Prompt/Theme", {
  defaultValue: makeTheme
});
var getTheme = (options) => map7(Theme, (theme) => ({
  ...theme,
  ...options.theme
}));
var annotateErrorLine = (theme, line) => annotate2(theme.colors, line, combine4(italicized, theme.errorColor));
var annotateSymbol = (theme, symbol, ...styles) => symbol.length === 0 ? "" : annotate2(theme.colors, symbol, ...styles);
var separateSymbol = (symbol, text) => symbol.length === 0 ? text : symbol + " " + text;
var renderPagingPrefix = (theme, showArrowUp, showArrowDown) => {
  const width = Math.max(theme.arrowUp.length, theme.arrowDown.length);
  if (showArrowUp) {
    return theme.arrowUp.padEnd(width);
  }
  if (showArrowDown) {
    return theme.arrowDown.padEnd(width);
  }
  return " ".repeat(width);
};
var Confirm = (options) => {
  const opts = {
    initial: false,
    ...options,
    label: {
      confirm: "yes",
      deny: "no",
      ...options.label
    },
    placeholder: {
      defaultConfirm: "(Y/n)",
      defaultDeny: "(y/N)",
      ...options.placeholder
    }
  };
  const initialState = {
    value: opts.initial
  };
  return Custom(initialState, {
    render: handleConfirmRender(opts),
    process: (input) => handleConfirmProcess(input, opts.initial),
    clear: handleConfirmClear(opts)
  });
};
var Custom = (initialState, ...args) => {
  const [events, handlers] = args.length === 1 ? [undefined, args[0]] : [args[0], args[1]];
  const op = Object.create(proto);
  op._tag = "Loop";
  op.initialState = initialState;
  op.render = handlers.render;
  op.process = handlers.process;
  op.clear = handlers.clear;
  op.events = events;
  return op;
};
var Date4 = (options) => {
  const opts = {
    initial: new globalThis.Date,
    dateMask: "YYYY-MM-DD HH:mm:ss",
    validate: succeed7,
    ...options,
    locales: {
      ...defaultLocales,
      ...options.locales
    }
  };
  const dateParts = makeDateParts(opts.dateMask, opts.initial, opts.locales);
  const initialCursorPosition = dateParts.findIndex((part) => !part.isToken());
  const initialState = {
    dateParts,
    typed: "",
    cursor: initialCursorPosition,
    value: opts.initial,
    error: none2()
  };
  return Custom(initialState, {
    render: handleDateRender(opts),
    process: handleDateProcess(opts),
    clear: handleDateClear(opts)
  });
};
var flatMap6 = /* @__PURE__ */ dual(2, (self, f) => {
  const op = Object.create(proto);
  op._tag = "OnSuccess";
  op.prompt = self;
  op.onSuccess = f;
  return op;
});
var Number7 = (options) => {
  const opts = {
    default: 0,
    min: globalThis.Number.NEGATIVE_INFINITY,
    max: globalThis.Number.POSITIVE_INFINITY,
    incrementBy: 1,
    decrementBy: 1,
    precision: 2,
    validate: (n) => {
      if (n < opts.min) {
        return fail7(`${n} must be greater than or equal to ${opts.min}`);
      }
      if (n > opts.max) {
        return fail7(`${n} must be less than or equal to ${opts.max}`);
      }
      return succeed7(n);
    },
    ...options
  };
  const initialValue = options.default === undefined ? "" : `${opts.default}`;
  const initialState = {
    cursor: initialValue.length,
    value: initialValue,
    error: none2()
  };
  return Custom(initialState, {
    render: handleRenderFloat(opts),
    process: handleProcessFloat(opts),
    clear: handleNumberClear(opts)
  });
};
var Int3 = (options) => {
  const opts = {
    default: 0,
    min: globalThis.Number.NEGATIVE_INFINITY,
    max: globalThis.Number.POSITIVE_INFINITY,
    incrementBy: 1,
    decrementBy: 1,
    validate: (n) => {
      if (n < opts.min) {
        return fail7(`${n} must be greater than or equal to ${opts.min}`);
      }
      if (n > opts.max) {
        return fail7(`${n} must be less than or equal to ${opts.max}`);
      }
      return succeed7(n);
    },
    ...options
  };
  const initialValue = options.default === undefined ? "" : `${opts.default}`;
  const initialState = {
    cursor: initialValue.length,
    value: initialValue,
    error: none2()
  };
  return Custom(initialState, {
    render: handleRenderInteger(opts),
    process: handleProcessInteger(opts),
    clear: handleNumberClear(opts)
  });
};
var map11 = /* @__PURE__ */ dual(2, (self, f) => flatMap6(self, (a) => succeed10(f(a))));
var Password = (options) => basePrompt(options, "password").pipe(map11(make19));
var run3 = /* @__PURE__ */ fnUntraced2(function* (self) {
  const terminal = yield* Terminal;
  const input = yield* cached2(terminal.readInput);
  return yield* runWithInput(self, terminal, input);
}, /* @__PURE__ */ mapError3(() => new QuitError({})), scoped2);
var getSelectInitialIndex = (choices) => {
  let initialIndex = 0;
  let seenSelected = -1;
  for (let i = 0;i < choices.length; i++) {
    const choice = choices[i];
    if (choice.selected === true) {
      if (seenSelected !== -1) {
        throw new Error("InvalidArgumentException: only a single choice can be selected by default for Prompt.Select");
      }
      seenSelected = i;
    }
  }
  if (seenSelected !== -1) {
    initialIndex = seenSelected;
  }
  return initialIndex;
};
var Select = (options) => {
  const opts = {
    maxPerPage: 10,
    ...options
  };
  const initialIndex = getSelectInitialIndex(opts.choices);
  return Custom(initialIndex, {
    render: handleSelectRender(opts),
    process: handleSelectProcess(opts),
    clear: handleSelectClear(opts)
  });
};
var succeed10 = (value) => {
  const op = Object.create(proto);
  op._tag = "Succeed";
  op.value = value;
  return op;
};
var String8 = (options) => basePrompt(options, "text");
var Toggle = (options) => {
  const opts = {
    initial: false,
    active: "on",
    inactive: "off",
    ...options
  };
  return Custom(opts.initial, {
    render: handleToggleRender(opts),
    process: handleToggleProcess,
    clear: () => handleToggleClear(opts)
  });
};
var proto = {
  .../* @__PURE__ */ Prototype2({
    label: "Prompt",
    evaluate() {
      return run3(this);
    }
  }),
  [TypeId47]: {
    _Output: (_) => _
  }
};
var runWithInput = (prompt, terminal, input) => suspend2(() => {
  const op = prompt;
  switch (op._tag) {
    case "Loop": {
      return flatMap4(input, (queue) => runLoop(op, terminal, queue));
    }
    case "OnSuccess": {
      return flatMap4(runWithInput(op.prompt, terminal, input), (a) => runWithInput(op.onSuccess(a), terminal, input));
    }
    case "Succeed": {
      return succeed7(op.value);
    }
  }
});
var runLoop = /* @__PURE__ */ fnUntraced2(function* (loop, terminal, input) {
  let state = isEffect2(loop.initialState) ? yield* loop.initialState : loop.initialState;
  let action = Action.NextFrame({
    state
  });
  let clear = "";
  while (true) {
    const msg = yield* loop.render(state, action);
    yield* orDie2(terminal.display(clear + msg));
    clear = "";
    if (loop.events) {
      const takeInput = take2(input).pipe(map7((input) => ({
        _tag: "Input",
        input
      })));
      const result = yield* raceFirst2(takeInput, take2(loop.events).pipe(map7((value) => ({
        _tag: "Event",
        value
      }))));
      action = yield* loop.process(result, state);
    } else {
      const result = yield* take2(input);
      action = yield* loop.process(result, state);
    }
    switch (action._tag) {
      case "Beep":
        continue;
      case "NextFrame": {
        clear = yield* loop.clear(state, action);
        state = action.state;
        continue;
      }
      case "Submit": {
        clear = yield* loop.clear(state, action);
        const msg = yield* loop.render(state, action);
        yield* orDie2(terminal.display(clear + msg));
        return action.value;
      }
    }
  }
}, (effect, _, terminal) => ensuring2(effect, orDie2(terminal.display(cursorShow))));
var Action = /* @__PURE__ */ taggedEnum();
var eraseText = (text, columns) => {
  if (columns === 0) {
    return eraseLine + cursorTo(0);
  }
  let rows = 0;
  const lines = text.split(NEWLINE_REGEXP);
  for (const line of lines) {
    rows += 1 + Math.floor(Math.max(line.length - 1, 0) / columns);
  }
  return eraseLines(rows);
};
var lines = (prompt, columns) => {
  const lines = prompt.split(NEWLINE_REGEXP);
  return columns === 0 ? lines.length : pipe(map4(lines, (line) => Math.ceil(line.length / columns)), reduce(0, (left, right) => left + right));
};
var clearOutputWithError = (outputText, columns, errorText) => {
  if (errorText !== undefined && errorText.length > 0) {
    return cursorDown(lines(errorText, columns)) + eraseText(`
${errorText}`, columns) + eraseText(outputText, columns);
  }
  return eraseText(outputText, columns);
};
var renderBeep = beep;
var NEWLINE_REGEXP = /\r?\n/;
var handleConfirmClear = (options) => {
  return fnUntraced2(function* (state, _) {
    const terminal = yield* Terminal;
    const columns = yield* terminal.columns;
    const figures = yield* getTheme(options);
    const confirmMessage = state.value ? options.placeholder.defaultConfirm : options.placeholder.defaultDeny;
    const promptText = renderPrompt(confirmMessage, options.message, figures.prefix, figures.pointerSmall, figures, {
      plain: true
    });
    const clearOutput = eraseText(promptText, columns);
    const resetCurrentLine = eraseLine + cursorLeft;
    return clearOutput + resetCurrentLine;
  });
};
var renderConfirmNextFrame = /* @__PURE__ */ fnUntraced2(function* (state, options) {
  const figures = yield* getTheme(options);
  const leadingSymbol = annotateSymbol(figures, figures.prefix, figures.primaryColor);
  const trailingSymbol = annotateSymbol(figures, figures.pointerSmall, figures.mutedColor);
  const confirmMessage = state.value ? options.placeholder.defaultConfirm : options.placeholder.defaultDeny;
  const confirm = annotate2(figures.colors, confirmMessage, figures.mutedColor);
  const promptMsg = renderPrompt(confirm, options.message, leadingSymbol, trailingSymbol, figures);
  return cursorHide + promptMsg;
});
var renderConfirmSubmission = /* @__PURE__ */ fnUntraced2(function* (value, options) {
  const figures = yield* getTheme(options);
  const leadingSymbol = annotateSymbol(figures, figures.tick, figures.successColor);
  const trailingSymbol = annotateSymbol(figures, figures.ellipsis, figures.mutedColor);
  const confirmMessage = value ? options.label.confirm : options.label.deny;
  const promptMsg = renderPrompt(confirmMessage, options.message, leadingSymbol, trailingSymbol, figures);
  return promptMsg + `
`;
});
var handleConfirmRender = (options) => {
  return (_, action) => {
    return Action.$match(action, {
      Beep: () => succeed7(renderBeep),
      NextFrame: ({
        state
      }) => renderConfirmNextFrame(state, options),
      Submit: ({
        value
      }) => renderConfirmSubmission(value, options)
    });
  };
};
var TRUE_VALUE_REGEXP = /^y|t$/;
var FALSE_VALUE_REGEXP = /^n|f$/;
var handleConfirmProcess = (input, defaultValue) => {
  const value = getOrElse(input.input, () => "");
  if (input.key.name === "enter" || input.key.name === "return") {
    return succeed7(Action.Submit({
      value: defaultValue
    }));
  }
  if (TRUE_VALUE_REGEXP.test(value.toLowerCase())) {
    return succeed7(Action.Submit({
      value: true
    }));
  }
  if (FALSE_VALUE_REGEXP.test(value.toLowerCase())) {
    return succeed7(Action.Submit({
      value: false
    }));
  }
  return succeed7(Action.Beep());
};
var handleDateClear = (options) => {
  return fnUntraced2(function* (state, _) {
    const terminal = yield* Terminal;
    const columns = yield* terminal.columns;
    const figures = yield* getTheme(options);
    const resetCurrentLine = eraseLine + cursorLeft;
    const parts = reduce(state.dateParts, "", (doc, part) => doc + part.toString());
    const promptText = renderPrompt(parts, options.message, figures.prefix, figures.pointerSmall, figures, {
      plain: true
    });
    const errorText = isSome2(state.error) ? match3(state.error.value.split(NEWLINE_REGEXP), {
      onEmpty: () => "",
      onNonEmpty: (errorLines) => separateSymbol(figures.pointerSmall, errorLines.join(`
`))
    }) : "";
    const clearOutput = clearOutputWithError(promptText, columns, errorText);
    return clearOutput + resetCurrentLine;
  });
};
var renderDateError = (state, pointer, theme) => {
  if (isSome2(state.error)) {
    const errorLines = state.error.value.split(NEWLINE_REGEXP);
    if (isReadonlyArrayNonEmpty(errorLines)) {
      const prefix = annotateSymbol(theme, pointer, theme.errorColor);
      const lines = map4(errorLines, (str) => annotateErrorLine(theme, str));
      return cursorSavePosition + `
` + separateSymbol(prefix, lines.join(`
`)) + cursorRestorePosition;
    }
  }
  return "";
};
var renderParts = (state, theme, submitted = false) => {
  return reduce(state.dateParts, "", (doc, part, currentIndex) => {
    const partDoc = part.toString();
    if (currentIndex === state.cursor && !submitted) {
      const annotation = combine4(underlined, theme.primaryColor);
      return doc + annotate2(theme.colors, partDoc, annotation);
    }
    return doc + partDoc;
  });
};
var renderDateNextFrame = /* @__PURE__ */ fnUntraced2(function* (state, options) {
  const figures = yield* getTheme(options);
  const leadingSymbol = annotateSymbol(figures, figures.prefix, figures.primaryColor);
  const trailingSymbol = annotateSymbol(figures, figures.pointerSmall, figures.mutedColor);
  const parts = renderParts(state, figures);
  const promptMsg = renderPrompt(parts, options.message, leadingSymbol, trailingSymbol, figures);
  const errorMsg = renderDateError(state, figures.pointerSmall, figures);
  return cursorHide + promptMsg + errorMsg;
});
var renderDateSubmission = /* @__PURE__ */ fnUntraced2(function* (state, options) {
  const figures = yield* getTheme(options);
  const leadingSymbol = annotateSymbol(figures, figures.tick, figures.successColor);
  const trailingSymbol = annotateSymbol(figures, figures.ellipsis, figures.mutedColor);
  const parts = renderParts(state, figures, true);
  const promptMsg = renderPrompt(parts, options.message, leadingSymbol, trailingSymbol, figures);
  return promptMsg + `
`;
});
var processUp = (state) => {
  state.dateParts[state.cursor].increment();
  return Action.NextFrame({
    state: {
      ...state,
      typed: ""
    }
  });
};
var processDown = (state) => {
  state.dateParts[state.cursor].decrement();
  return Action.NextFrame({
    state: {
      ...state,
      typed: ""
    }
  });
};
var processDateCursorLeft = (state) => {
  const previous = state.dateParts[state.cursor].previousPart();
  if (isSome2(previous)) {
    return Action.NextFrame({
      state: {
        ...state,
        typed: "",
        cursor: state.dateParts.indexOf(previous.value)
      }
    });
  }
  return Action.Beep();
};
var processDateCursorRight = (state) => {
  const next = state.dateParts[state.cursor].nextPart();
  if (isSome2(next)) {
    return Action.NextFrame({
      state: {
        ...state,
        typed: "",
        cursor: state.dateParts.indexOf(next.value)
      }
    });
  }
  return Action.Beep();
};
var processDateNext = (state) => {
  const next = state.dateParts[state.cursor].nextPart();
  const cursor = match(next, {
    onNone: () => state.dateParts.findIndex((part) => !part.isToken()),
    onSome: (next) => state.dateParts.indexOf(next)
  });
  return Action.NextFrame({
    state: {
      ...state,
      typed: "",
      cursor
    }
  });
};
var defaultDateProcessor = (value, state) => {
  if (/\d/.test(value)) {
    const typed = state.typed + value;
    state.dateParts[state.cursor].setValue(typed);
    return Action.NextFrame({
      state: {
        ...state,
        typed
      }
    });
  }
  return Action.Beep();
};
var defaultLocales = {
  months: ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"],
  monthsShort: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
  weekdays: ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
  weekdaysShort: ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
};
var handleDateRender = (options) => {
  return (state, action) => {
    return Action.$match(action, {
      Beep: () => succeed7(renderBeep),
      NextFrame: ({
        state
      }) => renderDateNextFrame(state, options),
      Submit: () => renderDateSubmission(state, options)
    });
  };
};
var handleDateProcess = (options) => {
  return (input, state) => {
    switch (input.key.name) {
      case "left": {
        return succeed7(processDateCursorLeft(state));
      }
      case "right": {
        return succeed7(processDateCursorRight(state));
      }
      case "k":
      case "up": {
        return succeed7(processUp(state));
      }
      case "j":
      case "down": {
        return succeed7(processDown(state));
      }
      case "tab": {
        return succeed7(processDateNext(state));
      }
      case "enter":
      case "return": {
        return match6(options.validate(state.value), {
          onFailure: (error) => Action.NextFrame({
            state: {
              ...state,
              error: some2(error)
            }
          }),
          onSuccess: (value) => Action.Submit({
            value
          })
        });
      }
      default: {
        return succeed7(defaultDateProcessor(getOrElse(input.input, () => ""), state));
      }
    }
  };
};
var DATE_PART_REGEXP = /\\(.)|"((?:\\["\\]|[^"])+)"|(D[Do]?|d{3,4}|d)|(M{1,4})|(YY(?:YY)?)|([aA])|([Hh]{1,2})|(m{1,2})|(s{1,2})|(S{1,4})|./g;
var regExpGroups = {
  1: ({
    token,
    ...opts
  }) => new Token({
    token: token.replace(/\\(.)/g, "$1"),
    ...opts
  }),
  2: (opts) => new Day(opts),
  3: (opts) => new Month(opts),
  4: (opts) => new Year(opts),
  5: (opts) => new Meridiem(opts),
  6: (opts) => new Hours(opts),
  7: (opts) => new Minutes(opts),
  8: (opts) => new Seconds(opts),
  9: (opts) => new Milliseconds(opts)
};
var makeDateParts = (dateMask, date, locales) => {
  const parts = [];
  let result = null;
  while (result = DATE_PART_REGEXP.exec(dateMask)) {
    const match = result.shift();
    const index = result.findIndex((group) => group !== undefined);
    if (index in regExpGroups) {
      const token = result[index] || match;
      parts.push(regExpGroups[index]({
        token,
        date,
        parts,
        locales
      }));
    } else {
      parts.push(new Token({
        token: result[index] || match,
        date,
        parts,
        locales
      }));
    }
  }
  const orderedParts = parts.reduce((array, element) => {
    const lastElement = array[array.length - 1];
    if (element.isToken() && lastElement !== undefined && lastElement.isToken()) {
      lastElement.setValue(element.token);
    } else {
      array.push(element);
    }
    return array;
  }, empty2());
  parts.splice(0, parts.length, ...orderedParts);
  return parts;
};

class DatePart {
  token;
  date;
  parts;
  locales;
  constructor(params) {
    this.token = params.token;
    this.locales = params.locales;
    this.date = params.date || new globalThis.Date;
    this.parts = params.parts || [this];
  }
  isToken() {
    return false;
  }
  nextPart() {
    const currentPartIndex = getOrElse(findFirstIndex(this.parts, (part) => part === this), () => 0);
    return findFirst2(this.parts.slice(currentPartIndex + 1), (part) => !part.isToken());
  }
  previousPart() {
    const currentPartIndex = findFirstIndex(this.parts, (part) => part === this);
    if (isSome2(currentPartIndex)) {
      return findLast(this.parts.slice(0, currentPartIndex.value), (part) => !part.isToken());
    }
    return none2();
  }
  toString() {
    return globalThis.String(this.date);
  }
}

class Token extends DatePart {
  increment() {}
  decrement() {}
  setValue(value) {
    this.token = this.token + value;
  }
  isToken() {
    return true;
  }
  toString() {
    return this.token;
  }
}

class Milliseconds extends DatePart {
  increment() {
    this.date.setMilliseconds(this.date.getMilliseconds() + 1);
  }
  decrement() {
    this.date.setMilliseconds(this.date.getMilliseconds() - 1);
  }
  setValue(value) {
    this.date.setMilliseconds(globalThis.Number.parseInt(value.slice(-this.token.length)));
  }
  toString() {
    const millis = `${this.date.getMilliseconds()}`;
    return millis.padStart(4, "0").substring(0, this.token.length);
  }
}

class Seconds extends DatePart {
  increment() {
    this.date.setSeconds(this.date.getSeconds() + 1);
  }
  decrement() {
    this.date.setSeconds(this.date.getSeconds() - 1);
  }
  setValue(value) {
    this.date.setSeconds(globalThis.Number.parseInt(value.slice(-2)));
  }
  toString() {
    const seconds = `${this.date.getSeconds()}`;
    return this.token.length > 1 ? seconds.padStart(2, "0") : seconds;
  }
}

class Minutes extends DatePart {
  increment() {
    this.date.setMinutes(this.date.getMinutes() + 1);
  }
  decrement() {
    this.date.setMinutes(this.date.getMinutes() - 1);
  }
  setValue(value) {
    this.date.setMinutes(globalThis.Number.parseInt(value.slice(-2)));
  }
  toString() {
    const minutes = `${this.date.getMinutes()}`;
    return this.token.length > 1 ? minutes.padStart(2, "0") : minutes;
  }
}

class Hours extends DatePart {
  increment() {
    this.date.setHours(this.date.getHours() + 1);
  }
  decrement() {
    this.date.setHours(this.date.getHours() - 1);
  }
  setValue(value) {
    this.date.setHours(globalThis.Number.parseInt(value.slice(-2)));
  }
  toString() {
    const hours = /h/.test(this.token) ? this.date.getHours() % 12 || 12 : this.date.getHours();
    return this.token.length > 1 ? `${hours}`.padStart(2, "0") : `${hours}`;
  }
}

class Day extends DatePart {
  increment() {
    this.date.setDate(this.date.getDate() + 1);
  }
  decrement() {
    this.date.setDate(this.date.getDate() - 1);
  }
  setValue(value) {
    this.date.setDate(globalThis.Number.parseInt(value.slice(-2)));
  }
  toString() {
    const date = this.date.getDate();
    const day = this.date.getDay();
    switch (this.token) {
      case "DD":
        return `${date}`.padStart(2, "0");
      case "Do":
        return `${date}${this.ordinalIndicator(date)}`;
      case "d":
        return `${day + 1}`;
      case "ddd":
        return this.locales.weekdaysShort[day];
      case "dddd":
        return this.locales.weekdays[day];
      default:
        return `${date}`;
    }
  }
  ordinalIndicator(day) {
    if (day >= 11 && day <= 13) {
      return "th";
    }
    switch (day % 10) {
      case 1:
        return "st";
      case 2:
        return "nd";
      case 3:
        return "rd";
      default:
        return "th";
    }
  }
}

class Month extends DatePart {
  increment() {
    this.date.setMonth(this.date.getMonth() + 1);
  }
  decrement() {
    this.date.setMonth(this.date.getMonth() - 1);
  }
  setValue(value) {
    const month = globalThis.Number.parseInt(value.slice(-2)) - 1;
    this.date.setMonth(month < 0 ? 0 : month);
  }
  toString() {
    const month = this.date.getMonth();
    switch (this.token.length) {
      case 2:
        return `${month + 1}`.padStart(2, "0");
      case 3:
        return this.locales.monthsShort[month];
      case 4:
        return this.locales.months[month];
      default:
        return `${month + 1}`;
    }
  }
}

class Year extends DatePart {
  increment() {
    this.date.setFullYear(this.date.getFullYear() + 1);
  }
  decrement() {
    this.date.setFullYear(this.date.getFullYear() - 1);
  }
  setValue(value) {
    this.date.setFullYear(globalThis.Number.parseInt(value.slice(-4)));
  }
  toString() {
    const year = `${this.date.getFullYear()}`.padStart(4, "0");
    return this.token.length === 2 ? year.slice(-2) : year;
  }
}

class Meridiem extends DatePart {
  increment() {
    this.date.setHours((this.date.getHours() + 12) % 24);
  }
  decrement() {
    this.increment();
  }
  setValue(_value) {}
  toString() {
    const meridiem = this.date.getHours() >= 12 ? "pm" : "am";
    return /A/.test(this.token) ? meridiem.toUpperCase() : meridiem;
  }
}
var renderPrompt = (confirm, message, leadingSymbol, trailingSymbol, theme, options) => {
  const prefix = leadingSymbol.length === 0 ? "" : leadingSymbol + " ";
  const renderLine = (line) => {
    let output = prefix + line;
    if (trailingSymbol.length > 0) {
      output += " " + trailingSymbol;
    }
    if (confirm.length > 0) {
      output += " " + confirm;
    } else if (trailingSymbol.length > 0) {
      output += " ";
    }
    return output;
  };
  return match3(message.split(NEWLINE_REGEXP), {
    onEmpty: () => renderLine(""),
    onNonEmpty: (promptLines) => {
      const lines = options?.plain === true ? promptLines : map4(promptLines, (line) => annotate2(theme.colors, line, bold));
      return renderLine(lines.join(`
`));
    }
  });
};
var renderChoiceDescription = (choice, isActive, theme, renderOptions) => {
  if (!choice.disabled && choice.description && isActive) {
    const text = theme.descriptionSeparator + choice.description;
    return renderOptions?.plain === true ? text : annotate2(theme.colors, text, theme.mutedColor);
  }
  return "";
};
var handleNumberClear = (options) => {
  return fnUntraced2(function* (state, _) {
    const terminal = yield* Terminal;
    const columns = yield* terminal.columns;
    const figures = yield* getTheme(options);
    const resetCurrentLine = eraseLine + cursorLeft;
    const errorText = renderNumberError(state, figures.pointerSmall, figures, {
      plain: true
    });
    const promptText = renderNumberOutput(state, figures.prefix, figures.pointerSmall, options, figures, {
      plain: true
    });
    const clearOutput = clearOutputWithError(promptText, columns, errorText);
    return clearOutput + resetCurrentLine;
  });
};
var renderNumberInput = (state, submitted, theme, renderOptions) => {
  const value = state.value === "" ? "" : `${state.value}`;
  if (submitted || renderOptions?.plain === true) {
    return value;
  }
  const annotation = isSome2(state.error) ? theme.errorColor : combine4(underlined, theme.primaryColor);
  return annotate2(theme.colors, value, annotation);
};
var renderNumberError = (state, pointer, theme, renderOptions) => {
  if (isSome2(state.error)) {
    return match3(state.error.value.split(NEWLINE_REGEXP), {
      onEmpty: () => "",
      onNonEmpty: (errorLines) => {
        if (renderOptions?.plain === true) {
          return separateSymbol(pointer, errorLines.join(`
`));
        }
        const prefix = annotateSymbol(theme, pointer, theme.errorColor);
        const lines = map4(errorLines, (str) => annotateErrorLine(theme, str));
        return cursorSavePosition + `
` + separateSymbol(prefix, lines.join(`
`)) + cursorRestorePosition;
      }
    });
  }
  return "";
};
var renderNumberOutput = (state, leadingSymbol, trailingSymbol, options, theme, renderOptions, submitted = false) => {
  const value = renderNumberInput(state, submitted, theme, renderOptions);
  return renderPrompt(value, options.message, leadingSymbol, trailingSymbol, theme, renderOptions);
};
var renderNumberNextFrame = /* @__PURE__ */ fnUntraced2(function* (state, options) {
  const figures = yield* getTheme(options);
  const leadingSymbol = annotateSymbol(figures, figures.prefix, figures.primaryColor);
  const trailingSymbol = annotateSymbol(figures, figures.pointerSmall, figures.mutedColor);
  const errorMsg = renderNumberError(state, figures.pointerSmall, figures);
  const promptMsg = renderNumberOutput(state, leadingSymbol, trailingSymbol, options, figures);
  return promptMsg + errorMsg;
});
var renderNumberSubmission = /* @__PURE__ */ fnUntraced2(function* (nextState, options) {
  const figures = yield* getTheme(options);
  const leadingSymbol = annotateSymbol(figures, figures.tick, figures.successColor);
  const trailingSymbol = annotateSymbol(figures, figures.ellipsis, figures.mutedColor);
  const promptMsg = renderNumberOutput(nextState, leadingSymbol, trailingSymbol, options, figures, undefined, true);
  return promptMsg + `
`;
});
var processNumberBackspace = (state) => {
  if (state.value.length <= 0) {
    return succeed7(Action.Beep());
  }
  const value = state.value.slice(0, state.value.length - 1);
  return succeed7(Action.NextFrame({
    state: {
      ...state,
      value,
      error: none2()
    }
  }));
};
var processNumberClear = (state) => succeed7(Action.NextFrame({
  state: {
    ...state,
    cursor: 0,
    value: "",
    error: none2()
  }
}));
var defaultIntProcessor = (input, state) => {
  if (state.value.length === 0 && input === "-") {
    return succeed7(Action.NextFrame({
      state: {
        ...state,
        value: "-",
        error: none2()
      }
    }));
  }
  const parsed = globalThis.Number.parseInt(state.value + input);
  if (globalThis.Number.isNaN(parsed)) {
    return succeed7(Action.Beep());
  } else {
    return succeed7(Action.NextFrame({
      state: {
        ...state,
        value: `${parsed}`,
        error: none2()
      }
    }));
  }
};
var defaultFloatProcessor = (input, state) => {
  if (input === "." && state.value.includes(".")) {
    return succeed7(Action.Beep());
  }
  if (state.value.length === 0 && input === "-") {
    return succeed7(Action.NextFrame({
      state: {
        ...state,
        value: "-",
        error: none2()
      }
    }));
  }
  const parsed = globalThis.Number.parseFloat(state.value + input);
  if (globalThis.Number.isNaN(parsed)) {
    return succeed7(Action.Beep());
  } else {
    return succeed7(Action.NextFrame({
      state: {
        ...state,
        value: input === "." ? `${parsed}.` : state.value.includes(".") && /^\d$/.test(input) ? state.value + input : `${parsed}`,
        error: none2()
      }
    }));
  }
};
var handleRenderInteger = (options) => {
  return (state, action) => {
    return Action.$match(action, {
      Beep: () => succeed7(renderBeep),
      NextFrame: ({
        state
      }) => renderNumberNextFrame(state, options),
      Submit: () => renderNumberSubmission(state, options)
    });
  };
};
var handleProcessInteger = (options) => {
  return (input, state) => {
    if (input.key.ctrl && input.key.name === "u") {
      return processNumberClear(state);
    }
    switch (input.key.name) {
      case "backspace": {
        return processNumberBackspace(state);
      }
      case "k":
      case "up": {
        return succeed7(Action.NextFrame({
          state: {
            ...state,
            value: state.value === "" || state.value === "-" ? `${options.incrementBy}` : `${globalThis.Number.parseInt(state.value) + options.incrementBy}`,
            error: none2()
          }
        }));
      }
      case "j":
      case "down": {
        return succeed7(Action.NextFrame({
          state: {
            ...state,
            value: state.value === "" || state.value === "-" ? `-${options.decrementBy}` : `${globalThis.Number.parseInt(state.value) - options.decrementBy}`,
            error: none2()
          }
        }));
      }
      case "enter":
      case "return": {
        const parsed = globalThis.Number.parseInt(state.value);
        if (globalThis.Number.isNaN(parsed)) {
          return succeed7(Action.NextFrame({
            state: {
              ...state,
              error: some2("Must provide an integer value")
            }
          }));
        } else {
          return match6(options.validate(parsed), {
            onFailure: (error) => Action.NextFrame({
              state: {
                ...state,
                error: some2(error)
              }
            }),
            onSuccess: (value) => Action.Submit({
              value
            })
          });
        }
      }
      default: {
        return defaultIntProcessor(getOrElse(input.input, () => ""), state);
      }
    }
  };
};
var handleRenderFloat = (options) => {
  return (state, action) => {
    return Action.$match(action, {
      Beep: () => succeed7(renderBeep),
      NextFrame: ({
        state
      }) => renderNumberNextFrame(state, options),
      Submit: () => renderNumberSubmission(state, options)
    });
  };
};
var handleProcessFloat = (options) => {
  return (input, state) => {
    if (input.key.ctrl && input.key.name === "u") {
      return processNumberClear(state);
    }
    switch (input.key.name) {
      case "backspace": {
        return processNumberBackspace(state);
      }
      case "k":
      case "up": {
        return succeed7(Action.NextFrame({
          state: {
            ...state,
            value: state.value === "" || state.value === "-" ? `${options.incrementBy}` : `${globalThis.Number.parseFloat(state.value) + options.incrementBy}`,
            error: none2()
          }
        }));
      }
      case "j":
      case "down": {
        return succeed7(Action.NextFrame({
          state: {
            ...state,
            value: state.value === "" || state.value === "-" ? `-${options.decrementBy}` : `${globalThis.Number.parseFloat(state.value) - options.decrementBy}`,
            error: none2()
          }
        }));
      }
      case "enter":
      case "return": {
        const parsed = globalThis.Number.parseFloat(state.value);
        if (globalThis.Number.isNaN(parsed)) {
          return succeed7(Action.NextFrame({
            state: {
              ...state,
              error: some2("Must provide a floating point value")
            }
          }));
        } else {
          return flatMap4(sync2(() => round(parsed, options.precision)), (rounded) => match6(options.validate(rounded), {
            onFailure: (error) => Action.NextFrame({
              state: {
                ...state,
                error: some2(error)
              }
            }),
            onSuccess: (value) => Action.Submit({
              value
            })
          }));
        }
      }
      default: {
        return defaultFloatProcessor(getOrElse(input.input, () => ""), state);
      }
    }
  };
};
var renderSelectOutput = (leadingSymbol, trailingSymbol, options, theme, renderOptions) => options.message === undefined ? undefined : renderPrompt("", options.message, leadingSymbol, trailingSymbol, theme, renderOptions);
var withSelectHeader = (header, body) => header === undefined ? body : header + `
` + body;
var renderChoicePrefix = (state, choices, toDisplay, currentIndex, figures, renderOptions) => {
  const prefix = renderPagingPrefix(figures, currentIndex === toDisplay.startIndex && toDisplay.startIndex > 0, currentIndex === toDisplay.endIndex - 1 && toDisplay.endIndex < choices.length);
  if (renderOptions?.plain === true) {
    return state === currentIndex ? figures.pointer + prefix : prefix + " ".repeat(figures.pointer.length);
  }
  if (choices[currentIndex].disabled) {
    const annotation = combine4(bold, figures.mutedColor);
    return state === currentIndex ? annotateSymbol(figures, figures.pointer, annotation) + prefix : prefix + " ".repeat(figures.pointer.length);
  }
  return state === currentIndex ? annotateSymbol(figures, figures.pointer, figures.primaryColor) + prefix : prefix + " ".repeat(figures.pointer.length);
};
var renderChoiceTitle = (choice, isSelected, theme, renderOptions) => {
  if (renderOptions?.plain === true) {
    return choice.title;
  }
  const title = choice.title;
  if (isSelected) {
    return choice.disabled ? annotate2(theme.colors, title, combine4(underlined, theme.mutedColor)) : annotate2(theme.colors, title, combine4(underlined, theme.primaryColor));
  }
  return choice.disabled ? annotate2(theme.colors, title, combine4(strikethrough, theme.mutedColor)) : title;
};
var renderSelectChoices = (state, options, figures, renderOptions) => {
  const choices = options.choices;
  const toDisplay = entriesToDisplay(state, choices.length, options.maxPerPage);
  const documents = [];
  for (let index = toDisplay.startIndex;index < toDisplay.endIndex; index++) {
    const choice = choices[index];
    const isSelected = state === index;
    const prefix = renderChoicePrefix(state, choices, toDisplay, index, figures, renderOptions);
    const title = renderChoiceTitle(choice, isSelected, figures, renderOptions);
    const description = renderChoiceDescription(choice, isSelected, figures, renderOptions);
    documents.push(prefix + title + " " + description);
  }
  return documents.join(`
`);
};
var renderSelectNextFrame = /* @__PURE__ */ fnUntraced2(function* (state, options) {
  const figures = yield* getTheme(options);
  const choices = renderSelectChoices(state, options, figures);
  const leadingSymbol = annotateSymbol(figures, figures.prefix, figures.primaryColor);
  const trailingSymbol = annotateSymbol(figures, figures.pointerSmall, figures.mutedColor);
  const promptMsg = renderSelectOutput(leadingSymbol, trailingSymbol, options, figures);
  return cursorHide + withSelectHeader(promptMsg, choices);
});
var renderSelectSubmission = /* @__PURE__ */ fnUntraced2(function* (state, options) {
  const figures = yield* getTheme(options);
  const selected = options.choices[state].title;
  const leadingSymbol = annotateSymbol(figures, figures.tick, figures.successColor);
  const trailingSymbol = annotateSymbol(figures, figures.ellipsis, figures.mutedColor);
  const promptMsg = renderSelectOutput(leadingSymbol, trailingSymbol, options, figures) ?? leadingSymbol;
  return promptMsg + " " + annotate2(figures.colors, selected, figures.submittedColor) + `
`;
});
var processSelectCursorUp = (state, choices) => {
  if (state === 0) {
    return succeed7(Action.NextFrame({
      state: choices.length - 1
    }));
  }
  return succeed7(Action.NextFrame({
    state: state - 1
  }));
};
var processSelectCursorDown = (state, choices) => {
  if (state === choices.length - 1) {
    return succeed7(Action.NextFrame({
      state: 0
    }));
  }
  return succeed7(Action.NextFrame({
    state: state + 1
  }));
};
var processSelectNext = (state, choices) => {
  return succeed7(Action.NextFrame({
    state: (state + 1) % choices.length
  }));
};
var handleSelectRender = (options) => {
  return (state, action) => {
    return Action.$match(action, {
      Beep: () => succeed7(renderBeep),
      NextFrame: ({
        state
      }) => renderSelectNextFrame(state, options),
      Submit: () => renderSelectSubmission(state, options)
    });
  };
};
var handleSelectClear = (options) => fnUntraced2(function* (state, _) {
  const terminal = yield* Terminal;
  const columns = yield* terminal.columns;
  const figures = yield* getTheme(options);
  const clearPrompt = eraseLine + cursorLeft;
  const promptText = renderSelectOutput(figures.prefix, figures.pointerSmall, options, figures, {
    plain: true
  });
  const choicesText = renderSelectChoices(state, options, figures, {
    plain: true
  });
  const clearOutput = eraseText(withSelectHeader(promptText, choicesText), columns);
  return clearOutput + clearPrompt;
});
var handleSelectProcess = (options) => {
  return (input, state) => {
    switch (input.key.name) {
      case "k":
      case "up": {
        return processSelectCursorUp(state, options.choices);
      }
      case "j":
      case "down": {
        return processSelectCursorDown(state, options.choices);
      }
      case "tab": {
        return processSelectNext(state, options.choices);
      }
      case "enter":
      case "return": {
        const selected = options.choices[state];
        if (selected.disabled) {
          return succeed7(Action.Beep());
        }
        return succeed7(Action.Submit({
          value: selected.value
        }));
      }
      default: {
        return succeed7(Action.Beep());
      }
    }
  };
};
var renderClearScreen = /* @__PURE__ */ fnUntraced2(function* (state, options) {
  const terminal = yield* Terminal;
  const columns = yield* terminal.columns;
  const figures = yield* getTheme(options);
  const resetCurrentLine = eraseLine + cursorLeft;
  const errorText = renderTextError(state, figures.pointerSmall, figures, {
    plain: true
  });
  const clearOutput = clearOutputWithError(renderTextOutput(state, figures.prefix, figures.pointerSmall, options, figures, {
    plain: true
  }), columns, errorText);
  return clearOutput + resetCurrentLine;
});
var renderTextInput = (nextState, options, theme, submitted, renderOptions) => {
  const text = nextState.value;
  if (renderOptions?.plain === true) {
    switch (options.type) {
      case "hidden": {
        return "";
      }
      case "password": {
        return theme.passwordMask.repeat(text.length);
      }
      case "text": {
        return text;
      }
    }
  }
  if (text.length === 0) {
    return "";
  }
  const annotation = isSome2(nextState.error) ? theme.errorColor : submitted ? theme.submittedColor : combine4(underlined, theme.primaryColor);
  switch (options.type) {
    case "hidden": {
      return "";
    }
    case "password": {
      return annotateSymbol(theme, theme.passwordMask.repeat(text.length), annotation);
    }
    case "text": {
      return annotate2(theme.colors, text, annotation);
    }
  }
};
var renderTextError = (nextState, pointer, theme, renderOptions) => {
  if (isSome2(nextState.error)) {
    return match3(nextState.error.value.split(NEWLINE_REGEXP), {
      onEmpty: () => "",
      onNonEmpty: (errorLines) => {
        if (renderOptions?.plain === true) {
          return separateSymbol(pointer, errorLines.join(`
`));
        }
        const prefix = annotateSymbol(theme, pointer, theme.errorColor);
        const lines = map4(errorLines, (str) => annotateErrorLine(theme, str));
        return cursorSavePosition + `
` + separateSymbol(prefix, lines.join(`
`)) + cursorRestorePosition;
      }
    });
  }
  return "";
};
var renderTextOutput = (nextState, leadingSymbol, trailingSymbol, options, theme, renderOptions, submitted = false) => {
  const value = renderTextInput(nextState, options, theme, submitted, renderOptions);
  return renderPrompt(value, options.message, leadingSymbol, trailingSymbol, theme, renderOptions);
};
var renderTextNextFrame = /* @__PURE__ */ fnUntraced2(function* (state, options) {
  const figures = yield* getTheme(options);
  const leadingSymbol = annotateSymbol(figures, figures.prefix, figures.primaryColor);
  const trailingSymbol = annotateSymbol(figures, figures.pointerSmall, figures.mutedColor);
  const promptMsg = renderTextOutput(state, leadingSymbol, trailingSymbol, options, figures);
  const errorMsg = renderTextError(state, figures.pointerSmall, figures);
  const cursorWidth = options.type === "password" ? figures.passwordMask.length : 1;
  const offset = (state.cursor - state.value.length) * cursorWidth;
  return promptMsg + errorMsg + cursorMove(offset);
});
var renderTextSubmission = /* @__PURE__ */ fnUntraced2(function* (state, options) {
  const figures = yield* getTheme(options);
  const leadingSymbol = annotateSymbol(figures, figures.tick, figures.successColor);
  const trailingSymbol = annotateSymbol(figures, figures.ellipsis, figures.mutedColor);
  const promptMsg = renderTextOutput(state, leadingSymbol, trailingSymbol, options, figures, undefined, true);
  return promptMsg + `
`;
});
var processTextBackspace = (state) => {
  if (state.cursor <= 0) {
    return succeed7(Action.Beep());
  }
  const beforeCursor = state.value.slice(0, state.cursor - 1);
  const afterCursor = state.value.slice(state.cursor);
  const cursor = state.cursor - 1;
  const value = `${beforeCursor}${afterCursor}`;
  return succeed7(Action.NextFrame({
    state: {
      ...state,
      cursor,
      value,
      error: none2()
    }
  }));
};
var processTextClear = (state) => succeed7(Action.NextFrame({
  state: {
    ...state,
    cursor: 0,
    value: "",
    error: none2()
  }
}));
var processTextCursorLeft = (state) => {
  if (state.cursor <= 0) {
    return succeed7(Action.Beep());
  }
  const cursor = state.cursor - 1;
  return succeed7(Action.NextFrame({
    state: {
      ...state,
      cursor,
      error: none2()
    }
  }));
};
var processTextCursorRight = (state) => {
  if (state.cursor >= state.value.length) {
    return succeed7(Action.Beep());
  }
  const cursor = Math.min(state.cursor + 1, state.value.length);
  return succeed7(Action.NextFrame({
    state: {
      ...state,
      cursor,
      error: none2()
    }
  }));
};
var processTextCursorStart = (state) => succeed7(Action.NextFrame({
  state: {
    ...state,
    cursor: 0,
    error: none2()
  }
}));
var processTextCursorEnd = (state) => succeed7(Action.NextFrame({
  state: {
    ...state,
    cursor: state.value.length,
    error: none2()
  }
}));
var processTab = (state, options) => {
  if (state.value === options.default) {
    return succeed7(Action.Beep());
  }
  const value = state.value.length === 0 ? options.default : state.value;
  return succeed7(Action.NextFrame({
    state: {
      ...state,
      value,
      cursor: value.length,
      error: none2()
    }
  }));
};
var defaultTextProcessor = (input, state) => {
  const beforeCursor = state.value.slice(0, state.cursor);
  const afterCursor = state.value.slice(state.cursor);
  const value = `${beforeCursor}${input}${afterCursor}`;
  const cursor = state.cursor + input.length;
  return succeed7(Action.NextFrame({
    state: {
      ...state,
      cursor,
      value,
      error: none2()
    }
  }));
};
var handleTextRender = (options) => {
  return (state, action) => {
    return Action.$match(action, {
      Beep: () => succeed7(renderBeep),
      NextFrame: ({
        state
      }) => renderTextNextFrame(state, options),
      Submit: () => renderTextSubmission(state, options)
    });
  };
};
var handleTextProcess = (options) => {
  return (input, state) => {
    if (input.key.ctrl) {
      switch (input.key.name) {
        case "u": {
          return processTextClear(state);
        }
        case "a": {
          return processTextCursorStart(state);
        }
        case "e": {
          return processTextCursorEnd(state);
        }
        default: {
          return succeed7(Action.Beep());
        }
      }
    }
    switch (input.key.name) {
      case "backspace": {
        return processTextBackspace(state);
      }
      case "left": {
        return processTextCursorLeft(state);
      }
      case "right": {
        return processTextCursorRight(state);
      }
      case "home": {
        return processTextCursorStart(state);
      }
      case "end": {
        return processTextCursorEnd(state);
      }
      case "enter":
      case "return": {
        const value = state.value;
        return match6(options.validate(value), {
          onFailure: (error) => Action.NextFrame({
            state: {
              ...state,
              value,
              error: some2(error)
            }
          }),
          onSuccess: (value) => Action.Submit({
            value
          })
        });
      }
      case "tab": {
        return processTab(state, options);
      }
      default: {
        return defaultTextProcessor(getOrElse(input.input, () => ""), state);
      }
    }
  };
};
var handleTextClear = (options) => {
  return (state, _) => {
    return renderClearScreen(state, options);
  };
};
var basePrompt = (options, type) => {
  const opts = {
    default: "",
    type,
    validate: succeed7,
    ...options
  };
  const initialState = {
    cursor: opts.default.length,
    value: opts.default,
    error: none2()
  };
  return Custom(initialState, {
    render: handleTextRender(opts),
    process: handleTextProcess(opts),
    clear: handleTextClear(opts)
  });
};
var handleToggleClear = /* @__PURE__ */ fnUntraced2(function* (options) {
  const terminal = yield* Terminal;
  const columns = yield* terminal.columns;
  const figures = yield* getTheme(options);
  const clearPrompt = eraseLine + cursorLeft;
  const toggleText = options.active + " " + separateSymbol(figures.toggleSeparator, options.inactive);
  const promptText = renderPrompt(toggleText, options.message, figures.prefix, figures.pointerSmall, figures, {
    plain: true
  });
  const clearOutput = eraseText(promptText, columns);
  return clearOutput + clearPrompt;
});
var renderToggle = (value, options, theme, submitted = false) => {
  const separator = annotateSymbol(theme, theme.toggleSeparator, theme.mutedColor);
  const selectedAnnotation = combine4(underlined, submitted ? theme.submittedColor : theme.primaryColor);
  const inactive = value ? options.inactive : annotate2(theme.colors, options.inactive, selectedAnnotation);
  const active = value ? annotate2(theme.colors, options.active, selectedAnnotation) : options.active;
  return active + " " + separateSymbol(separator, inactive);
};
var renderToggleNextFrame = /* @__PURE__ */ fnUntraced2(function* (state, options) {
  const figures = yield* getTheme(options);
  const leadingSymbol = annotateSymbol(figures, figures.prefix, figures.primaryColor);
  const trailingSymbol = annotateSymbol(figures, figures.pointerSmall, figures.mutedColor);
  const toggle = renderToggle(state, options, figures);
  const promptMsg = renderPrompt(toggle, options.message, leadingSymbol, trailingSymbol, figures);
  return cursorHide + promptMsg;
});
var renderToggleSubmission = /* @__PURE__ */ fnUntraced2(function* (value, options) {
  const figures = yield* getTheme(options);
  const leadingSymbol = annotateSymbol(figures, figures.tick, figures.successColor);
  const trailingSymbol = annotateSymbol(figures, figures.ellipsis, figures.mutedColor);
  const toggle = renderToggle(value, options, figures, true);
  const promptMsg = renderPrompt(toggle, options.message, leadingSymbol, trailingSymbol, figures);
  return promptMsg + `
`;
});
var activate = /* @__PURE__ */ succeed7(/* @__PURE__ */ Action.NextFrame({
  state: true
}));
var deactivate = /* @__PURE__ */ succeed7(/* @__PURE__ */ Action.NextFrame({
  state: false
}));
var handleToggleRender = (options) => {
  return (state, action) => {
    switch (action._tag) {
      case "Beep": {
        return succeed7(renderBeep);
      }
      case "NextFrame": {
        return renderToggleNextFrame(state, options);
      }
      case "Submit": {
        return renderToggleSubmission(state, options);
      }
    }
  };
};
var handleToggleProcess = (input, state) => {
  switch (input.key.name) {
    case "0":
    case "j":
    case "delete":
    case "right":
    case "down": {
      return deactivate;
    }
    case "1":
    case "k":
    case "left":
    case "up": {
      return activate;
    }
    case " ":
    case "tab": {
      return state ? deactivate : activate;
    }
    case "enter":
    case "return": {
      return succeed7(Action.Submit({
        value: state
      }));
    }
    default: {
      return succeed7(Action.Beep());
    }
  }
};
var entriesToDisplay = (cursor, total, maxVisible) => {
  const max = maxVisible === undefined ? total : maxVisible;
  let startIndex = Math.min(total - max, cursor - Math.floor(max / 2));
  if (startIndex < 0) {
    startIndex = 0;
  }
  const endIndex = Math.min(startIndex + max, total);
  return {
    startIndex,
    endIndex
  };
};

// node_modules/effect/dist/cli/TerminalCapabilities.js
var TerminalCapabilities = /* @__PURE__ */ Reference("effect/cli/TerminalCapabilities", {
  defaultValue: () => fromProcess()
});
var isPresent = (value) => value !== undefined && value.length > 0;
var isForcingColor = (value) => isPresent(value) && value !== "0" && value.toLowerCase() !== "false";
var detect = (input) => {
  if (input.term === "dumb") {
    return {
      canColor: false,
      canAnimate: false,
      canPrompt: false
    };
  }
  return {
    canColor: !isPresent(input.noColor) && (isForcingColor(input.forceColor) || input.stdoutIsTerminal),
    canAnimate: input.stdoutIsTerminal,
    canPrompt: input.stdinIsTerminal && input.stdoutIsTerminal
  };
};
var fromProcess = () => {
  const process2 = globalThis.process;
  return detect({
    noColor: process2?.env?.NO_COLOR,
    forceColor: process2?.env?.FORCE_COLOR,
    term: process2?.env?.TERM,
    stdinIsTerminal: process2?.stdin?.isTTY === true,
    stdoutIsTerminal: process2?.stdout?.isTTY === true
  });
};

// node_modules/effect/dist/cli/Param.js
var TypeId48 = "~effect/cli/Param";
var argumentKind = "argument";
var flagKind = "flag";
var Proto16 = {
  [TypeId48]: {
    _A: identity
  },
  pipe() {
    return pipeArguments(this, arguments);
  }
};
var isParam = (u) => hasProperty(u, TypeId48);
var isFlagParam = (single) => single.kind === "flag";
var makeSingle2 = (params) => {
  const parse = (args) => params.kind === argumentKind ? parsePositional(params.name, params.primitiveType, args) : parseFlag(params.name, params.primitiveType, args);
  return Object.setPrototypeOf({
    _tag: "Single",
    ...params,
    description: params.description ?? none2(),
    aliases: params.aliases ?? [],
    hidden: params.hidden ?? false,
    parse
  }, Proto16);
};
var String9 = (kind, name) => makeSingle2({
  name,
  primitiveType: String7,
  kind
});
var Boolean5 = (kind, name) => makeSingle2({
  name,
  primitiveType: Boolean4,
  kind
});
var Int4 = (kind, name) => makeSingle2({
  name,
  primitiveType: Int2,
  kind
});
var ChoiceWithValue = (kind, name, choices) => makeSingle2({
  name,
  primitiveType: Choice(choices),
  kind
});
var Literals2 = (kind, name, choices) => {
  const mappedChoices = choices.map((value) => [value, value]);
  return ChoiceWithValue(kind, name, mappedChoices);
};
var FLAG_DASH_REGEXP = /^-+/;
var withAlias = /* @__PURE__ */ dual(2, (self, alias) => {
  return transformSingle(self, (single) => makeSingle2({
    ...single,
    aliases: [...single.aliases, alias.replace(FLAG_DASH_REGEXP, "")]
  }));
});
var withDescription = /* @__PURE__ */ dual(2, (self, description) => {
  return transformSingle(self, (single) => makeSingle2({
    ...single,
    description: some2(description)
  }));
});
var map12 = /* @__PURE__ */ dual(2, (self, f) => {
  const parse = (args) => map7(self.parse(args), ([operands, value]) => [operands, f(value)]);
  return Object.assign(Object.create(Proto16), {
    _tag: "Map",
    kind: self.kind,
    param: self,
    f,
    parse
  });
});
var transform3 = (self, f, alternatives = []) => {
  const alternativeParsers = alternatives.map((alternative) => () => alternative().parse);
  return Object.assign(Object.create(Proto16), {
    _tag: "Transform",
    kind: self.kind,
    param: self,
    alternatives,
    f,
    parse: f(self.parse, alternativeParsers)
  });
};
var mapEffect2 = /* @__PURE__ */ dual(2, (self, f) => transform3(self, (parse) => (args) => flatMap4(parse(args), ([leftover, a]) => f(a).pipe(map7((b) => [leftover, b])))));
var optional3 = (param) => {
  const parse = (args) => param.parse(args).pipe(map7(([leftover, value]) => [leftover, some2(value)]), catchTags2({
    MissingOption: () => succeed7([args.arguments, none2()]),
    MissingArgument: () => succeed7([args.arguments, none2()])
  }));
  return Object.assign(Object.create(Proto16), {
    _tag: "Optional",
    kind: param.kind,
    param,
    parse
  });
};
var withDefault4 = /* @__PURE__ */ dual(2, (self, defaultValue) => {
  if (!isEffect2(defaultValue)) {
    return map12(optional3(self), getOrElse(() => defaultValue));
  }
  return mapEffect2(optional3(self), match({
    onNone: () => defaultValue,
    onSome: succeed7
  }));
});
var variadic = (self, options) => {
  const single = getUnderlyingSingleOrThrow(self);
  const parse = (args) => {
    if (single.kind === "argument") {
      return parsePositionalVariadic(self, single, args, options);
    } else {
      return parseOptionVariadic(self, single, args, options);
    }
  };
  return Object.assign(Object.create(Proto16), {
    _tag: "Variadic",
    kind: self.kind,
    param: self,
    min: fromUndefinedOr(options?.min),
    max: fromUndefinedOr(options?.max),
    parse
  });
};
var atLeast = /* @__PURE__ */ dual(2, (self, min) => {
  if (min < 0) {
    throw new Error("atLeast: min must be non-negative");
  }
  return variadic(self, {
    min
  });
});
var withMetavar = /* @__PURE__ */ dual(2, (self, metavar) => transformSingle(self, (single) => makeSingle2({
  ...single,
  typeName: metavar
})));
var parsePositional = /* @__PURE__ */ fnUntraced2(function* (name, primitiveType, args) {
  if (args.arguments.length === 0) {
    return yield* new MissingArgument({
      argument: name
    });
  }
  const arg = args.arguments[0];
  const value = yield* mapError3(primitiveType.parse(arg), (error) => new InvalidValue2({
    option: name,
    value: arg,
    expected: error,
    kind: "argument"
  }));
  return [args.arguments.slice(1), value];
});
var parseFlag = /* @__PURE__ */ fnUntraced2(function* (name, primitiveType, args) {
  const providedValues = args.flags[name];
  if (providedValues === undefined || providedValues.length === 0) {
    return yield* new MissingOption({
      option: name
    });
  }
  if (providedValues.length > 1) {
    return yield* new InvalidValue2({
      option: name,
      value: `${providedValues.length} occurrences`,
      expected: "at most 1 value",
      kind: "flag"
    });
  }
  const arg = providedValues[0];
  const value = yield* mapError3(primitiveType.parse(arg), (error) => new InvalidValue2({
    option: name,
    value: arg,
    expected: error,
    kind: "flag"
  }));
  return [args.arguments, value];
});
var parsePositionalVariadic = /* @__PURE__ */ fnUntraced2(function* (self, single, args, options) {
  const results = [];
  const minValue = options?.min ?? 0;
  const maxValue = options?.max ?? Number.POSITIVE_INFINITY;
  let count = 0;
  let currentArgs = args.arguments;
  while (currentArgs.length > 0 && count < maxValue) {
    const [remainingArgs, value] = yield* self.parse({
      flags: args.flags,
      arguments: currentArgs
    });
    results.push(value);
    currentArgs = remainingArgs;
    count++;
  }
  if (count < minValue) {
    return yield* count === 0 ? new MissingArgument({
      argument: single.name
    }) : new InvalidValue2({
      option: single.name,
      value: `${count} values`,
      expected: `at least ${minValue} value${minValue === 1 ? "" : "s"}`,
      kind: single.kind
    });
  }
  return [currentArgs, results];
});
var parseOptionVariadic = /* @__PURE__ */ fnUntraced2(function* (self, single, args, options) {
  const results = [];
  const names = [single.name, ...single.aliases];
  const values = names.flatMap((name) => args.flags[name] ?? []);
  const count = values.length;
  if (isNotUndefined(options?.min) && count < options.min) {
    return yield* count === 0 ? new MissingOption({
      option: single.name
    }) : new InvalidValue2({
      option: single.name,
      value: `${count} occurrences`,
      expected: `at least ${options.min} value${options.min === 1 ? "" : "s"}`,
      kind: single.kind
    });
  }
  if (isNotUndefined(options?.max) && count > options.max) {
    return yield* new InvalidValue2({
      option: single.name,
      value: `${count} occurrences`,
      expected: `at most ${options.max} value${options.max === 1 ? "" : "s"}`,
      kind: single.kind
    });
  }
  for (const value of values) {
    const [, parsedValue] = yield* self.parse({
      flags: {
        [single.name]: [value]
      },
      arguments: []
    });
    results.push(parsedValue);
  }
  return [args.arguments, results];
});
var matchParam = (param, patterns) => {
  const p = param;
  switch (p._tag) {
    case "Single":
      return patterns.Single(p);
    case "Map":
      return patterns.Map(p);
    case "Transform":
      return patterns.Transform(p);
    case "Optional":
      return patterns.Optional(p);
    case "Variadic":
      return patterns.Variadic(p);
  }
};
var transformSingle = (param, f) => {
  return matchParam(param, {
    Single: (single) => f(single),
    Map: (mapped) => map12(transformSingle(mapped.param, f), mapped.f),
    Transform: (mapped) => transform3(transformSingle(mapped.param, f), mapped.f, mapped.alternatives.map((alternative) => () => transformSingle(alternative(), f))),
    Optional: (p) => optional3(transformSingle(p.param, f)),
    Variadic: (p) => variadic(transformSingle(p.param, f), {
      min: getOrUndefined(p.min),
      max: getOrUndefined(p.max)
    })
  });
};
var extractSingleParams = (param) => {
  return matchParam(param, {
    Single: (single) => [single],
    Map: (mapped) => extractSingleParams(mapped.param),
    Transform: (mapped) => [...extractSingleParams(mapped.param), ...mapped.alternatives.flatMap((alternative) => extractSingleParams(alternative()))],
    Optional: (optional) => extractSingleParams(optional.param),
    Variadic: (variadic) => extractSingleParams(variadic.param)
  });
};
var getUnderlyingSingleOrThrow = (param) => {
  const singles = extractSingleParams(param);
  if (singles.length === 0) {
    throw new Error("No Single param found in param structure");
  }
  if (singles.length > 1) {
    throw new Error(`Multiple Single params found: ${singles.map((s) => s.name).join(", ")}`);
  }
  return singles[0];
};
var getParamMetadata = (param) => {
  return matchParam(param, {
    Single: () => ({
      isOptional: false,
      isVariadic: false,
      variadicMin: none2(),
      variadicMax: none2()
    }),
    Map: (mapped) => getParamMetadata(mapped.param),
    Transform: (mapped) => getParamMetadata(mapped.param),
    Optional: (optional) => ({
      ...getParamMetadata(optional.param),
      isOptional: true
    }),
    Variadic: (variadic) => ({
      ...getParamMetadata(variadic.param),
      isVariadic: true,
      variadicMin: variadic.min,
      variadicMax: variadic.max
    })
  });
};

// node_modules/effect/dist/cli/Argument.js
var String10 = (name) => String9(argumentKind, name);
var optional4 = (arg) => optional3(arg);
var withDescription2 = /* @__PURE__ */ dual(2, (self, description) => withDescription(self, description));
var atLeast2 = /* @__PURE__ */ dual(2, (self, min) => atLeast(self, min));
// node_modules/effect/dist/cli/CliOutput.js
var Formatter = /* @__PURE__ */ Reference("effect/cli/CliOutput", {
  defaultValue: () => defaultFormatter2()
});
var defaultPresenter = {
  present: /* @__PURE__ */ fnUntraced2(function* (event) {
    const context = yield* context2();
    const formatter = getOrUndefined2(context, Formatter) ?? defaultFormatter2({
      colors: get(context, TerminalCapabilities).canColor
    });
    switch (event._tag) {
      case "Help":
        return yield* log2(formatter.formatHelpDoc(event.helpDoc));
      case "InvalidInvocation":
        yield* log2(formatter.formatHelpDoc(event.helpDoc));
        return yield* error(formatter.formatErrors(event.errors));
      case "Version":
        return yield* log2(formatter.formatVersion(event.name, event.version));
    }
  })
};
var Presenter = /* @__PURE__ */ Reference("effect/cli/CliOutput/Presenter", {
  defaultValue: () => defaultPresenter
});
var layer15 = (formatter) => succeed6(Formatter)(formatter);
var escapeControlCharacters2 = (text) => text.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, (character) => `\\x${character.charCodeAt(0).toString(16).padStart(2, "0")}`);
var defaultFormatter2 = (options) => {
  const useColor = options?.colors ?? detectColors();
  const helpOptions = {
    inheritedFlags: options?.inheritedFlags ?? "full",
    globalFlags: options?.globalFlags ?? "full"
  };
  const colors = useColor ? {
    bold: (text) => `\x1B[1m${text}\x1B[0m`,
    dim: (text) => `\x1B[2m${text}\x1B[0m`,
    cyan: (text) => `\x1B[36m${text}\x1B[0m`,
    green: (text) => `\x1B[32m${text}\x1B[0m`,
    blue: (text) => `\x1B[34m${text}\x1B[0m`,
    yellow: (text) => `\x1B[33m${text}\x1B[0m`,
    magenta: (text) => `\x1B[35m${text}\x1B[0m`
  } : {
    bold: (text) => text,
    dim: (text) => text,
    cyan: (text) => text,
    green: (text) => text,
    blue: (text) => text,
    yellow: (text) => text,
    magenta: (text) => text
  };
  const reset = useColor ? "\x1B[0m" : "";
  const red = useColor ? "\x1B[31m" : "";
  const bold = useColor ? "\x1B[1m" : "";
  return {
    formatHelpDoc: (doc) => formatHelpDocImpl(doc, colors, helpOptions),
    formatCliError: (error) => escapeControlCharacters2(error.message),
    formatError: (error) => {
      return `
${bold}${red}ERROR${reset}
  ${escapeControlCharacters2(error.message)}${reset}`;
    },
    formatErrors: (errors) => {
      if (errors.length === 0)
        return "";
      if (errors.length === 1) {
        return `
${bold}${red}ERROR${reset}
  ${escapeControlCharacters2(errors[0].message)}${reset}`;
      }
      const grouped = new Map;
      for (const error of errors) {
        const tag = error._tag ?? "Error";
        const group = grouped.get(tag) ?? [];
        group.push(error);
        grouped.set(tag, group);
      }
      const sections = [];
      sections.push(`
${bold}${red}ERRORS${reset}`);
      for (const [, group] of grouped) {
        for (const error of group) {
          sections.push(`  ${escapeControlCharacters2(error.message)}${reset}`);
        }
      }
      return sections.join(`
`);
    },
    formatVersion: (name, version) => `${colors.bold(name)} ${colors.dim("v")}${colors.bold(version)}`
  };
};
var stripAnsi = (text) => {
  return text.replace(/\u001B\[[0-9;]*m/g, "");
};
var graphemeSegmenter = /* @__PURE__ */ new Intl.Segmenter(undefined, {
  granularity: "grapheme"
});
var zeroWidthCodePoints = /[\p{Mark}\p{Control}\p{Default_Ignorable_Code_Point}]/gu;
var emojiPresentation = /\p{Emoji_Presentation}/u;
var isFullWidthCodePoint = (codePoint) => {
  if (codePoint < 4352)
    return false;
  return codePoint <= 4447 || codePoint === 9001 || codePoint === 9002 || codePoint >= 11904 && codePoint <= 12350 || codePoint >= 12352 && codePoint <= 42191 || codePoint >= 44032 && codePoint <= 55203 || codePoint >= 63744 && codePoint <= 64255 || codePoint >= 65040 && codePoint <= 65049 || codePoint >= 65072 && codePoint <= 65135 || codePoint >= 65280 && codePoint <= 65376 || codePoint >= 65504 && codePoint <= 65510 || codePoint >= 110592 && codePoint <= 111359 || codePoint >= 127488 && codePoint <= 127569 || codePoint >= 131072 && codePoint <= 262141;
};
var graphemeWidth = (grapheme) => {
  const visible = grapheme.replace(zeroWidthCodePoints, "");
  if (visible.length === 0)
    return 0;
  if (emojiPresentation.test(grapheme) || grapheme.includes("️"))
    return 2;
  return isFullWidthCodePoint(visible.codePointAt(0)) ? 2 : 1;
};
var displayWidth = (text) => {
  let length = 0;
  for (const {
    segment
  } of graphemeSegmenter.segment(stripAnsi(text))) {
    length += graphemeWidth(segment);
  }
  return length;
};
var pad = (s, width) => {
  const actualLength = displayWidth(s);
  const padding = Math.max(0, width - actualLength);
  return s + " ".repeat(padding);
};
var renderTable = (rows, widthCap) => {
  const maxColumn = Math.max(...rows.map((r) => displayWidth(r.left))) + 4;
  const col = widthCap === undefined ? maxColumn : Math.min(maxColumn, widthCap);
  return rows.map(({
    left,
    right
  }) => `  ${pad(left, Math.max(col, displayWidth(left) + 1))}${right}`).join(`
`);
};
var formatSubcommandName = (name, alias) => alias ? `${name}, ${alias}` : name;
var formatHelpDocImpl = (doc, colors, options) => {
  const sections = [];
  if (doc.description) {
    sections.push(colors.bold("DESCRIPTION"));
    sections.push(`  ${doc.description}`);
    sections.push("");
  }
  sections.push(colors.bold("USAGE"));
  sections.push(`  ${colors.cyan(doc.usage)}`);
  sections.push("");
  if (doc.args && doc.args.length > 0) {
    sections.push(colors.bold("ARGUMENTS"));
    const argRows = doc.args.map((arg) => {
      let name = arg.name;
      if (arg.variadic) {
        name += "...";
      }
      const coloredName = colors.green(name);
      const coloredType = colors.dim(arg.type);
      const nameType = `${coloredName} ${coloredType}`;
      const optionalSuffix = arg.required ? "" : colors.dim(" (optional)");
      const description = getOrElse(arg.description, () => "") + optionalSuffix;
      return {
        left: nameType,
        right: description
      };
    });
    sections.push(renderTable(argRows, 25));
    sections.push("");
  }
  const renderFlags = (title, flags) => {
    sections.push(colors.bold(title));
    sections.push(renderTable(flags.map((flag) => {
      const names = [`--${flag.name}`, ...flag.aliases].map(colors.green).join(", ");
      const typePart = flag.type !== "boolean" ? ` ${colors.dim(flag.type)}` : "";
      return {
        left: names + typePart,
        right: getOrElse(flag.description, () => "")
      };
    })));
    sections.push("");
  };
  const summaries = [];
  const renderFlagSection = (title, flags, mode, summaryPath) => {
    if (!flags || flags.length === 0 || mode === "none")
      return;
    if (mode === "summary" && summaryPath && summaryPath.length > 0) {
      summaries.push(`Run '${summaryPath.join(" ")} --help' for ${title.toLowerCase()}`);
    } else {
      renderFlags(title, flags);
    }
  };
  if (doc.flags.length > 0) {
    renderFlags("FLAGS", doc.flags);
  }
  const commandPath = doc.commandPath;
  renderFlagSection("INHERITED FLAGS", doc.inheritedFlags, options.inheritedFlags, commandPath?.slice(0, -1));
  const globalFlagsScope = doc.globalFlagsScope ?? commandPath?.slice(0, 1);
  const declaresGlobalFlags = commandPath === undefined || globalFlagsScope?.length === commandPath.length;
  renderFlagSection("GLOBAL FLAGS", doc.globalFlags, declaresGlobalFlags ? "full" : options.globalFlags, globalFlagsScope);
  if (summaries.length > 0) {
    sections.push(...summaries, "");
  }
  if (doc.subcommands && doc.subcommands.length > 0) {
    const ungrouped = doc.subcommands.find((group) => group.group === undefined);
    if (ungrouped) {
      sections.push(colors.bold("SUBCOMMANDS"));
      sections.push(renderTable(ungrouped.commands.map((sub) => ({
        left: colors.cyan(formatSubcommandName(sub.name, sub.alias)),
        right: sub.shortDescription ?? sub.description
      })), 20));
      if (doc.subcommands.length > 1) {
        sections.push("");
      }
    }
    for (const group of doc.subcommands) {
      if (group.group === undefined)
        continue;
      sections.push(colors.bold(`${group.group}:`));
      sections.push(renderTable(group.commands.map((sub) => ({
        left: colors.cyan(formatSubcommandName(sub.name, sub.alias)),
        right: sub.shortDescription ?? sub.description
      })), 20));
      sections.push("");
    }
  }
  if (doc.examples && doc.examples.length > 0) {
    sections.push(colors.bold("EXAMPLES"));
    let first = true;
    let previousHadDescription = false;
    for (const example of doc.examples) {
      if (example.description) {
        if (!first)
          sections.push("");
        sections.push(`  ${colors.dim(`# ${example.description}`)}`);
      } else if (previousHadDescription) {
        sections.push("");
      }
      sections.push(`  ${colors.cyan(example.command)}`);
      first = false;
      previousHadDescription = !!example.description;
    }
    sections.push("");
  }
  if (sections[sections.length - 1] === "") {
    sections.pop();
  }
  return sections.join(`
`);
};

// node_modules/effect/dist/cli/internal/config.js
var ConfigInternalTypeId = "~effect/cli/Command/Config/Internal";
var parseConfig = (config) => {
  const orderedParams = [];
  const flags = [];
  const args = [];
  function parse(config) {
    const tree = Object.create(null);
    for (const key of Object.keys(config)) {
      tree[key] = parseValue(config[key]);
    }
    return tree;
  }
  function parseValue(value) {
    if (Array.isArray(value)) {
      return {
        _tag: "Array",
        children: value.map((v) => parseValue(v))
      };
    } else if (isParam(value)) {
      const index = orderedParams.length;
      orderedParams.push(value);
      if (value.kind === "argument") {
        args.push(value);
      } else {
        flags.push(value);
      }
      return {
        _tag: "Param",
        index
      };
    } else {
      return {
        _tag: "Nested",
        tree: parse(value)
      };
    }
  }
  return {
    [ConfigInternalTypeId]: ConfigInternalTypeId,
    flags,
    arguments: args,
    orderedParams,
    tree: parse(config)
  };
};
var emptyConfig = /* @__PURE__ */ parseConfig({});
var reconstructTree = (tree, results) => {
  const output = {};
  for (const key of Object.keys(tree)) {
    assignProperty(output, key, nodeValue(tree[key]));
  }
  return output;
  function nodeValue(node) {
    switch (node._tag) {
      case "Param":
        return results[node.index];
      case "Array":
        return node.children.map((child) => nodeValue(child));
      case "Nested":
        return reconstructTree(node.tree, results);
    }
  }
};

// node_modules/effect/dist/cli/internal/command.js
var TypeId49 = "~effect/cli/Command";
var toImpl = (self) => self;
var Proto17 = {
  .../* @__PURE__ */ Prototype2({
    label: "Command",
    evaluate() {
      return toImpl(this).service;
    }
  })
};
var makeCommand = (options) => {
  const config = options.config;
  const contextConfig = options.contextConfig ?? emptyConfig;
  const service = options.service ?? Service(`${TypeId49}/${options.name}`);
  const annotations = options.annotations ?? empty();
  const globalFlags = options.globalFlags ?? [];
  const subcommands = options.subcommands ?? [];
  const resolveExamples = options.resolveExamples ?? noExamples;
  const handle = (input, commandPath) => isNotUndefined(options.handle) ? options.handle(input, commandPath) : fail7(new ShowHelp({
    commandPath,
    errors: []
  }));
  const parse = options.parse ?? makeParser(config);
  const parseContext = options.parseContext ?? makeParser(contextConfig, {
    allowLeftovers: true
  });
  const buildHelpDoc = (commandPath) => {
    const args = [];
    const flags = [];
    for (const arg of config.arguments) {
      const singles = extractSingleParams(arg);
      const metadata = getParamMetadata(arg);
      for (const single of singles) {
        args.push({
          name: single.name,
          type: single.typeName ?? getTypeName(single.primitiveType),
          description: single.description,
          required: !metadata.isOptional && (!metadata.isVariadic || exists(metadata.variadicMin, (min) => min > 0)),
          variadic: metadata.isVariadic
        });
      }
    }
    let usage = commandPath.length > 0 ? commandPath.join(" ") : options.name;
    if (subcommands.some((group) => group.commands.some((c) => !c.unlisted))) {
      usage += " <subcommand>";
    }
    usage += " [flags]";
    for (const arg of args) {
      const argName = arg.variadic ? `<${arg.name}...>` : `<${arg.name}>`;
      usage += ` ${arg.required ? argName : `[${argName}]`}`;
    }
    for (const option of config.flags) {
      const singles = extractSingleParams(option);
      const metadata = getParamMetadata(option);
      for (const single of singles) {
        if (single.hidden)
          continue;
        flags.push(toFlagDoc(single, metadata));
      }
    }
    const subcommandDocs = [];
    for (const group of subcommands) {
      const visible = group.commands.filter((c) => !c.unlisted);
      if (visible.length === 0)
        continue;
      subcommandDocs.push({
        group: group.group,
        commands: map4(visible, (subcommand) => ({
          name: subcommand.name,
          alias: subcommand.alias,
          shortDescription: subcommand.shortDescription,
          description: subcommand.description ?? ""
        }))
      });
    }
    const examples = resolveExamples(commandPath);
    return {
      description: options.description ?? "",
      usage,
      flags,
      annotations,
      ...args.length > 0 && {
        args
      },
      ...subcommandDocs.length > 0 && {
        subcommands: subcommandDocs
      },
      ...examples.length > 0 && {
        examples
      }
    };
  };
  return Object.assign(Object.create(Proto17), {
    [TypeId49]: TypeId49,
    name: options.name,
    examples: resolveExamples([options.name]),
    resolveExamples,
    annotations,
    globalFlags,
    subcommands,
    unlisted: options.unlisted ?? false,
    config,
    contextConfig,
    service,
    parse,
    parseContext,
    handle,
    buildHelpDoc,
    ...isNotUndefined(options.description) ? {
      description: options.description
    } : {},
    ...isNotUndefined(options.shortDescription) ? {
      shortDescription: options.shortDescription
    } : {},
    ...isNotUndefined(options.alias) ? {
      alias: options.alias
    } : {}
  });
};
var noExamples = () => [];
var toFlagDoc = (single, metadata) => {
  const formattedAliases = single.aliases.map((alias) => alias.length === 1 ? `-${alias}` : `--${alias}`);
  const type = single.typeName ?? getTypeName(single.primitiveType);
  return {
    name: single.name,
    aliases: formattedAliases,
    type: single.optionalValue === undefined ? type : `[=${type}]`,
    description: appendChoiceKeys(single.description, getChoiceKeys(single.primitiveType)),
    required: single.primitiveType._tag !== "Boolean" && !metadata.isOptional
  };
};
var appendChoiceKeys = (description, choiceKeys) => {
  if (choiceKeys === undefined || choiceKeys.length === 0) {
    return description;
  }
  const choiceSuffix = `(choices: ${choiceKeys.join(", ")})`;
  return match(description, {
    onNone: () => some2(choiceSuffix),
    onSome: (value) => some2(`${value} ${choiceSuffix}`)
  });
};
var makeParser = (cfg, options) => fnUntraced2(function* (input) {
  const parsedArgs = {
    flags: input.flags,
    arguments: input.arguments
  };
  const [remainingArguments, values] = yield* parseParams(parsedArgs, cfg.orderedParams);
  if (options?.allowLeftovers !== true && remainingArguments.length > 0) {
    return yield* new UnexpectedArgument({
      arguments: remainingArguments
    });
  }
  return reconstructTree(cfg.tree, values);
});
var parseParams = /* @__PURE__ */ fnUntraced2(function* (parsedArgs, params) {
  const results = [];
  let currentArguments = parsedArgs.arguments;
  for (const option of params) {
    const [remainingArguments, parsed] = yield* option.parse({
      flags: parsedArgs.flags,
      arguments: currentArguments
    });
    results.push(parsed);
    currentArguments = remainingArguments;
  }
  return [currentArguments, results];
});
var checkForDuplicateFlags = (parent, subcommands, options) => {
  const parentImpl = toImpl(parent);
  const parentOptionNames = new Set;
  const extractNames = (flags) => {
    for (const option of flags) {
      const singles = extractSingleParams(option);
      for (const single of singles) {
        parentOptionNames.add(single.name);
      }
    }
  };
  extractNames((options?.contextConfig ?? parentImpl.contextConfig).flags);
  for (const subcommand of subcommands) {
    const subImpl = toImpl(subcommand);
    for (const option of subImpl.config.flags) {
      const singles = extractSingleParams(option);
      for (const single of singles) {
        if (parentOptionNames.has(single.name)) {
          throw new DuplicateOption({
            option: single.name,
            parentCommand: parent.name,
            childCommand: subcommand.name
          });
        }
      }
    }
  }
};

// node_modules/effect/dist/cli/internal/completions/descriptor.js
var toFlagType = (single) => {
  const tag = single.primitiveType._tag;
  switch (tag) {
    case "Boolean":
      return {
        _tag: "Boolean"
      };
    case "Int":
      return {
        _tag: "Int"
      };
    case "Finite":
      return {
        _tag: "Finite"
      };
    case "Date":
      return {
        _tag: "Date"
      };
    case "Choice": {
      const keys = getChoiceKeys(single.primitiveType);
      return {
        _tag: "Choice",
        values: keys ?? []
      };
    }
    case "Path":
      return {
        _tag: "Path",
        pathType: getPathType(single.primitiveType) ?? "either"
      };
    case "FileText":
    case "FileParse":
    case "FileSchema":
      return {
        _tag: "Path",
        pathType: "file"
      };
    default:
      return {
        _tag: "String"
      };
  }
};
var toArgumentType = (single) => {
  const tag = single.primitiveType._tag;
  switch (tag) {
    case "Int":
      return {
        _tag: "Int"
      };
    case "Finite":
      return {
        _tag: "Finite"
      };
    case "Date":
      return {
        _tag: "Date"
      };
    case "Choice": {
      const keys = getChoiceKeys(single.primitiveType);
      return {
        _tag: "Choice",
        values: keys ?? []
      };
    }
    case "Path":
      return {
        _tag: "Path",
        pathType: getPathType(single.primitiveType) ?? "either"
      };
    case "FileText":
    case "FileParse":
    case "FileSchema":
      return {
        _tag: "Path",
        pathType: "file"
      };
    default:
      return {
        _tag: "String"
      };
  }
};
var fromCommand = (cmd, inheritedFlags = []) => {
  const impl = toImpl(cmd);
  const config = impl.config;
  const flags = [];
  const seen = new Set;
  for (const [index, flag] of [...config.flags, ...inheritedFlags].entries()) {
    const singles = extractSingleParams(flag);
    for (const single of singles) {
      if (single.kind !== "flag")
        continue;
      if (single.hidden)
        continue;
      if (index >= config.flags.length && seen.has(single.name))
        continue;
      seen.add(single.name);
      flags.push({
        name: single.name,
        aliases: single.aliases,
        description: getOrUndefined(single.description),
        type: toFlagType(single),
        optionalValue: single.optionalValue !== undefined,
        negation: single.negation
      });
    }
  }
  const args = [];
  for (const arg of config.arguments) {
    const singles = extractSingleParams(arg);
    const metadata = getParamMetadata(arg);
    for (const single of singles) {
      if (single.kind !== "argument")
        continue;
      args.push({
        name: single.name,
        description: getOrUndefined(single.description),
        required: !metadata.isOptional,
        variadic: metadata.isVariadic,
        type: toArgumentType(single)
      });
    }
  }
  const subcommands = [];
  const sharedFlags = [...inheritedFlags, ...impl.contextConfig.flags];
  for (const group of cmd.subcommands) {
    for (const subcommand of group.commands) {
      if (subcommand.unlisted)
        continue;
      const descriptor = fromCommand(subcommand, sharedFlags);
      subcommands.push(descriptor);
      if (subcommand.alias && subcommand.alias !== subcommand.name) {
        subcommands.push({
          ...descriptor,
          name: subcommand.alias
        });
      }
    }
  }
  return {
    name: cmd.name,
    description: cmd.shortDescription ?? cmd.description,
    flags,
    arguments: args,
    subcommands
  };
};
var takesSeparateValue = (flag) => flag.type._tag !== "Boolean" && flag.optionalValue !== true;
var negatedName = (flag) => flag.type._tag !== "Boolean" || flag.negation === false ? undefined : flag.negation ?? `no-${flag.name}`;

// node_modules/effect/dist/cli/internal/completions/bash.js
var escapeForBash = (s) => s.replace(/'/g, "'\\''");
var sanitizeFunctionName = (s) => s.replace(/[^a-zA-Z0-9_]/g, "_");
var flagNamesForWordlist = (flag) => {
  const names = [`--${flag.name}`];
  for (const alias of flag.aliases) {
    names.push(alias.length === 1 ? `-${alias}` : `--${alias}`);
  }
  const negation = negatedName(flag);
  if (negation !== undefined) {
    names.push(`--${negation}`);
  }
  return names;
};
var buildFlagGroupDeclarations = (flags, lines) => {
  if (flags.length === 0)
    return;
  const groups = flags.map(flagNamesForWordlist);
  lines.push(`  local ${groups.map((_, index) => `_used_${index}=""`).join(" ")}`);
  lines.push(`  for ((i = 1; i < cword; i++)); do`);
  lines.push(`    case "\${words[i]%%=*}" in`);
  groups.forEach((forms, index) => {
    lines.push(`      ${forms.join("|")}) _used_${index}=1 ;;`);
  });
  lines.push(`    esac`);
  lines.push(`  done`);
  lines.push(`  local _filtered_flags=""`);
  groups.forEach((forms, index) => {
    lines.push(`  [[ -n "$_used_${index}" ]] || _filtered_flags+=" ${forms.join(" ")}"`);
  });
  lines.push(``);
};
var removeQuotes = (variable) => [`\\\\`, `\\"`, `\\'`].map((quote) => `${variable}=\${${variable}//${quote}/}`).join("; ");
var choicesHelper = (helperName, lines) => {
  lines.push(`${helperName}()`);
  lines.push(`{`);
  lines.push(`  local _cur="$1" _word="$2"; shift 2`);
  lines.push(``);
  lines.push(`  local _head="\${_cur%"$_word"}"`);
  lines.push(`  local _open=""`);
  lines.push(`  case "$_head" in`);
  lines.push(`    *\\') _open="'" ;;`);
  lines.push(`    *\\") _open='"' ;;`);
  lines.push(`  esac`);
  lines.push(``);
  lines.push(`  local _prefix="$_cur" _committed="$_head"`);
  lines.push(`  ${removeQuotes("_prefix")}`);
  lines.push(`  ${removeQuotes("_committed")}`);
  lines.push(``);
  lines.push(`  COMPREPLY=()`);
  lines.push(`  local _choice _rest _match`);
  lines.push(`  for _choice in "$@"; do`);
  lines.push(`    [[ "$_choice" == "$_prefix"* ]] || continue`);
  lines.push(`    _rest="\${_choice#"$_committed"}"`);
  lines.push(`    case "$_open" in`);
  lines.push(`      "'")`);
  lines.push(`        if [[ "$_head" == "'" ]]; then`);
  lines.push(`          _match=\${_rest//\\'/\\'\\\\\\'\\'}`);
  lines.push(`        else`);
  lines.push(`          [[ "$_rest" == *\\'* ]] && continue`);
  lines.push(`          _match="$_rest"`);
  lines.push(`        fi`);
  lines.push(`        ;;`);
  lines.push(`      '"')`);
  lines.push(`        _match="\${_rest//\\\\/\\\\\\\\}"`);
  lines.push(`        _match="\${_match//\\$/\\\\$}"`);
  lines.push('        _match="${_match//\\`/\\\\\\`}"');
  lines.push(`        _match="\${_match//\\"/\\\\\\"}"`);
  lines.push(`        ;;`);
  lines.push(`      *)`);
  lines.push(`        printf -v _match '%q' "$_rest"`);
  lines.push(`        [[ -z "$_head" && "$_match" == '~'* ]] && _match="\\\\$_match"`);
  lines.push(`        ;;`);
  lines.push(`    esac`);
  lines.push(`    [[ -n "$_open" && "$_match" == *"$_open" ]] && _match+="$_open"`);
  lines.push(`    COMPREPLY+=("$_match")`);
  lines.push(`  done`);
  lines.push(`}`);
  lines.push(``);
};
var pathsHelper = (helpers, lines) => {
  lines.push(`${helpers.paths}()`);
  lines.push(`{`);
  lines.push(`  local _kind="$1" _cur="$2" _word="$3"`);
  lines.push(``);
  lines.push(`  local _prefix="$_cur" _committed="\${_cur%"$_word"}"`);
  lines.push(`  ${removeQuotes("_prefix")}`);
  lines.push(`  ${removeQuotes("_committed")}`);
  lines.push(``);
  lines.push(`  local _typed="" _dir="$_committed" _open="" _i`);
  lines.push(`  if [[ "$_word" == */* ]]; then`);
  lines.push(`    _typed="\${_word%/*}/"`);
  lines.push(`    _dir="\${_prefix%/*}/"`);
  lines.push(`    if [[ "$_cur" == "$_word" ]]; then`);
  lines.push(`      for ((_i = 0; _i < \${#_typed}; _i++)); do`);
  lines.push(`        case "$_open\${_typed:_i:1}" in`);
  lines.push(`          \\\\|\\"\\\\) ((_i += 1)) ;;`);
  lines.push(`          \\"|\\') _open="\${_typed:_i:1}" ;;`);
  lines.push(`          \\"\\"|\\'\\') _open="" ;;`);
  lines.push(`        esac`);
  lines.push(`      done`);
  lines.push(`    fi`);
  lines.push(`  fi`);
  lines.push(``);
  lines.push(`  local _path _names=() _rest="\${_word#"$_typed"}"`);
  lines.push(`  while IFS= read -r _path; do`);
  lines.push(`    if [[ "$_path" == "$_dir"* ]]; then`);
  lines.push(`      _names+=("$_committed\${_path#"$_dir"}")`);
  lines.push(`    elif [[ -z "$_committed" ]]; then`);
  lines.push(`      _names+=("$_path")`);
  lines.push(`      _typed="" _rest="" _open=""`);
  lines.push(`    fi`);
  lines.push(`  done < <(compgen "$_kind" -- "$_prefix")`);
  lines.push(`  if [[ -n "$_open" ]]; then`);
  lines.push(`    _typed+="$_open"`);
  lines.push(`    _rest="\${_prefix##*/}"`);
  lines.push(`  fi`);
  lines.push(``);
  lines.push(`  ${helpers.choices} "\${_cur%"$_word"}$_rest" "$_rest" "\${_names[@]}"`);
  lines.push(`  for ((_i = 0; _i < \${#COMPREPLY[@]}; _i++)); do`);
  lines.push(`    COMPREPLY[_i]="$_typed\${COMPREPLY[_i]}"`);
  lines.push(`  done`);
  lines.push(`}`);
  lines.push(``);
};
var valueCompletion = (type, helpers) => {
  switch (type._tag) {
    case "Choice": {
      const values = type.values.map((value) => `'${escapeForBash(value)}'`).join(" ");
      return `${helpers.choices} "$cur" "$_comp_word" ${values}`;
    }
    case "Path":
      return `${helpers.paths} ${type.pathType === "directory" ? "-d" : "-f"} "$cur" "$_comp_word"`;
    default:
      return;
  }
};
var generateFunction = (descriptor, parentPath, lines, helpers) => {
  const currentPath = [...parentPath, descriptor.name];
  const funcName = `_${currentPath.map(sanitizeFunctionName).join("_")}`;
  lines.push(`${funcName}()`);
  lines.push(`{`);
  lines.push(`  local cur prev words cword i`);
  lines.push(parentPath.length === 0 ? `  local _command_index=0` : `  local _command_index="$1"`);
  lines.push(`  _init_completion -n "$COMP_WORDBREAKS" || return`);
  if (parentPath.length === 0) {
    lines.push(`  local _comp_word="$2"`);
  }
  lines.push(``);
  const flagsWithValues = descriptor.flags.filter(takesSeparateValue);
  if (flagsWithValues.length > 0) {
    lines.push(`  # Flag value completions`);
    lines.push(`  case "$prev" in`);
    for (const flag of flagsWithValues) {
      const longNames = [`--${flag.name}`];
      for (const alias of flag.aliases) {
        longNames.push(alias.length === 1 ? `-${alias}` : `--${alias}`);
      }
      const completion = valueCompletion(flag.type, helpers);
      if (completion) {
        lines.push(`    ${longNames.join("|")})`);
        lines.push(`      ${completion}`);
        lines.push(`      return`);
        lines.push(`      ;;`);
      }
    }
    lines.push(`  esac`);
    lines.push(``);
  }
  if (descriptor.subcommands.length > 0) {
    lines.push(`  # Subcommand dispatch`);
    lines.push(`  local cmd _skip_next=0`);
    lines.push(`  for ((i = _command_index + 1; i < cword; i++)); do`);
    lines.push(`    if (( _skip_next )); then`);
    lines.push(`      _skip_next=0`);
    lines.push(`      continue`);
    lines.push(`    fi`);
    lines.push(`    case "\${words[i]}" in`);
    for (const flag of descriptor.flags) {
      if (!takesSeparateValue(flag))
        continue;
      const forms = flagNamesForWordlist(flag);
      lines.push(`      ${forms.join("|")}) _skip_next=1 ;;`);
      lines.push(`      ${forms.map((form) => `${form}=*`).join("|")}) ;;`);
    }
    for (const sub of descriptor.subcommands) {
      const subFuncName = `_${[...currentPath, sub.name].map(sanitizeFunctionName).join("_")}`;
      lines.push(`      ${sub.name})`);
      lines.push(`        ${subFuncName} "$i"`);
      lines.push(`        return`);
      lines.push(`        ;;`);
    }
    lines.push(`    esac`);
    lines.push(`  done`);
    lines.push(``);
  }
  buildFlagGroupDeclarations(descriptor.flags, lines);
  if (descriptor.flags.length > 0 || descriptor.subcommands.length > 0) {
    lines.push(`  # Complete flags (filtered) and subcommands`);
    lines.push(`  if [[ "$cur" == -* ]]; then`);
    if (descriptor.flags.length > 0) {
      lines.push(`    COMPREPLY=( $(compgen -W "$_filtered_flags" -- "$cur") )`);
    }
    lines.push(`    return`);
    lines.push(`  fi`);
    lines.push(``);
  }
  const argsWithCompletions = descriptor.arguments.flatMap((argument, index) => {
    const completion = valueCompletion(argument.type, helpers);
    return completion === undefined ? [] : [{
      argument,
      completion,
      index
    }];
  });
  if (argsWithCompletions.length > 0) {
    lines.push(`  # Positional argument completions`);
    lines.push(`  local _position=0 _skip_next=0 _end_of_options=0`);
    lines.push(`  for ((i = _command_index + 1; i < cword; i++)); do`);
    lines.push(`    if (( _skip_next )); then`);
    lines.push(`      _skip_next=0`);
    lines.push(`      continue`);
    lines.push(`    fi`);
    lines.push(`    if (( _end_of_options )); then`);
    lines.push(`      ((_position += 1))`);
    lines.push(`      continue`);
    lines.push(`    fi`);
    lines.push(`    case "\${words[i]}" in`);
    lines.push(`      --) _end_of_options=1 ;;`);
    for (const flag of descriptor.flags) {
      const forms = flagNamesForWordlist(flag);
      if (!takesSeparateValue(flag)) {
        lines.push(`      ${forms.join("|")}) ;;`);
      } else {
        lines.push(`      ${forms.join("|")}) _skip_next=1 ;;`);
        lines.push(`      ${forms.map((form) => `${form}=*`).join("|")}) ;;`);
      }
    }
    lines.push(`      -*) ;;`);
    lines.push(`      *) ((_position += 1)) ;;`);
    lines.push(`    esac`);
    lines.push(`  done`);
    lines.push(`  case "$_position" in`);
    for (const {
      argument,
      completion,
      index
    } of argsWithCompletions) {
      if (argument.variadic)
        continue;
      lines.push(`    ${index})`);
      lines.push(`      ${completion}`);
      lines.push(`      return`);
      lines.push(`      ;;`);
    }
    lines.push(`  esac`);
    const variadic = argsWithCompletions.find(({
      argument
    }) => argument.variadic);
    if (variadic !== undefined) {
      lines.push(`  if (( _position >= ${variadic.index} )); then`);
      lines.push(`    ${variadic.completion}`);
      lines.push(`    return`);
      lines.push(`  fi`);
    }
  } else if (descriptor.subcommands.length > 0) {
    const subNames = descriptor.subcommands.map((s) => s.name);
    lines.push(`  COMPREPLY=( $(compgen -W '${subNames.join(" ")}' -- "$cur") )`);
  }
  lines.push(`}`);
  lines.push(``);
  for (const sub of descriptor.subcommands) {
    generateFunction(sub, currentPath, lines, helpers);
  }
};
var generate = (executableName, descriptor) => {
  const lines = [];
  const safeName = sanitizeFunctionName(executableName);
  const helpers = {
    choices: `_${safeName}--choices`,
    paths: `_${safeName}--paths`
  };
  lines.push(`###-begin-${escapeForBash(executableName)}-completions-###`);
  lines.push(`#`);
  lines.push(`# Static completion script for Bash`);
  lines.push(`#`);
  lines.push(`# Installation:`);
  lines.push(`#   ${escapeForBash(executableName)} --completions bash >> ~/.bashrc`);
  lines.push(`#`);
  lines.push(``);
  lines.push(`if ! type _init_completion &>/dev/null; then`);
  lines.push(`  _init_completion()`);
  lines.push(`  {`);
  lines.push(`    COMPREPLY=()`);
  lines.push(`    local _i _j=0 _piece _line="$COMP_LINE"`);
  lines.push(`    words=("\${COMP_WORDS[0]}")`);
  lines.push(`    cword=0`);
  lines.push(`    _line="\${_line#*"\${COMP_WORDS[0]}"}"`);
  lines.push(`    for ((_i = 1; _i < \${#COMP_WORDS[@]}; _i++)); do`);
  lines.push(`      _piece="\${COMP_WORDS[_i]}"`);
  lines.push(`      if [[ "$_line" == [[:blank:]]* ]]; then`);
  lines.push(`        ((_j++))`);
  lines.push(`        words[_j]="$_piece"`);
  lines.push(`      else`);
  lines.push(`        words[_j]="\${words[_j]}$_piece"`);
  lines.push(`      fi`);
  lines.push(`      ((_i == COMP_CWORD)) && cword=$_j`);
  lines.push(`      _line="\${_line#*"$_piece"}"`);
  lines.push(`    done`);
  lines.push(`    cur="\${words[cword]}"`);
  lines.push(`    prev=""`);
  lines.push(`    ((cword > 0)) && prev="\${words[cword-1]}"`);
  lines.push(`    return 0`);
  lines.push(`  }`);
  lines.push(`fi`);
  lines.push(``);
  choicesHelper(helpers.choices, lines);
  pathsHelper(helpers, lines);
  generateFunction(descriptor, [], lines, helpers);
  lines.push(`complete -F _${safeName} ${escapeForBash(executableName)}`);
  lines.push(`###-end-${escapeForBash(executableName)}-completions-###`);
  return lines.join(`
`);
};

// node_modules/effect/dist/cli/internal/completions/fish.js
var escapeFishString = (s) => s.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
var escapeFishChoice = (s) => escapeFishString(s.replace(/[^A-Za-z0-9_.,/@%+-]/gu, "\\$&"));
var subcommandCondition = (parentPath, childSubcommandNames) => {
  if (parentPath.length === 0) {
    if (childSubcommandNames.length > 0) {
      return `__fish_use_subcommand`;
    }
    return ``;
  }
  const parentCondition = parentPath.map((parent) => `__fish_seen_subcommand_from ${parent}`).join("; and ");
  if (childSubcommandNames.length > 0) {
    return `${parentCondition}; and not __fish_seen_subcommand_from ${childSubcommandNames.join(" ")}`;
  }
  return parentCondition;
};
var flagContainsOptCondition = (flag) => {
  const optArgs = [];
  for (const alias of flag.aliases) {
    if (alias.length === 1) {
      optArgs.push(`-s ${alias}`);
    }
  }
  optArgs.push(flag.name);
  for (const alias of flag.aliases) {
    if (alias.length > 1) {
      optArgs.push(alias);
    }
  }
  const negation = negatedName(flag);
  if (negation !== undefined) {
    optArgs.push(negation);
  }
  return `not __fish_contains_opt ${optArgs.join(" ")}`;
};
var valueFlagDedupCondition = (flag) => {
  const forms = [`--${flag.name}`];
  for (const alias of flag.aliases) {
    forms.push(alias.length === 1 ? `-${alias}` : `--${alias}`);
  }
  return `begin; ${flagContainsOptCondition(flag)}; or contains -- (commandline -poc)[-1] ${forms.join(" ")}; end`;
};
var flagCompletionArgs = (flag) => {
  const args = [`-l ${flag.name}`];
  for (const alias of flag.aliases) {
    if (alias.length === 1) {
      args.push(`-s ${alias}`);
    } else {
      args.push(`-l ${alias}`);
    }
  }
  if (flag.description) {
    args.push(`-d '${escapeFishString(flag.description)}'`);
  }
  const valueArgs = takesSeparateValue(flag) ? flagValueArgs(flag.type) : undefined;
  if (valueArgs) {
    args.push(valueArgs);
  }
  return args;
};
var flagValueArgs = (type) => {
  switch (type._tag) {
    case "Boolean":
      return;
    case "Choice":
      return `-r -f -a '${type.values.map(escapeFishChoice).join(" ")}'`;
    case "Path":
      if (type.pathType === "directory")
        return `-r -F`;
      return `-r -F`;
    default:
      return `-r -f`;
  }
};
var argValueArgs = (type) => {
  switch (type._tag) {
    case "Choice":
      return `-r -f -a '${type.values.map(escapeFishChoice).join(" ")}'`;
    case "Path":
      return `-r -F`;
    default:
      return;
  }
};
var generateCompletions = (executableName, descriptor, parentPath, lines) => {
  const allSubNames = descriptor.subcommands.map((s) => s.name);
  const condition = subcommandCondition(parentPath, allSubNames);
  const conditionArg = condition ? `-n '${condition}'` : ``;
  const hasPathArgs = descriptor.arguments.some((a) => a.type._tag === "Path");
  if (!hasPathArgs) {
    const parts = [`complete -c ${executableName}`];
    if (conditionArg)
      parts.push(conditionArg);
    parts.push(`-f`);
    lines.push(parts.join(" "));
  }
  for (const sub of descriptor.subcommands) {
    const parts = [`complete -c ${executableName}`];
    if (conditionArg)
      parts.push(conditionArg);
    parts.push(`-f -a '${escapeFishString(sub.name)}'`);
    if (sub.description) {
      parts.push(`-d '${escapeFishString(sub.description)}'`);
    }
    lines.push(parts.join(" "));
  }
  for (const flag of descriptor.flags) {
    const dedup = takesSeparateValue(flag) ? valueFlagDedupCondition(flag) : flagContainsOptCondition(flag);
    const flagCondition = condition ? `${condition}; and ${dedup}` : dedup;
    const flagCondArg = flagCondition ? `-n '${flagCondition}'` : ``;
    const parts = [`complete -c ${executableName}`];
    if (flagCondArg)
      parts.push(flagCondArg);
    parts.push(...flagCompletionArgs(flag));
    lines.push(parts.join(" "));
    const negation = negatedName(flag);
    if (negation !== undefined) {
      const negParts = [`complete -c ${executableName}`];
      if (flagCondArg)
        negParts.push(flagCondArg);
      negParts.push(`-l ${negation}`);
      if (flag.description) {
        negParts.push(`-d '${escapeFishString(`Disable ${flag.name}`)}'`);
      }
      lines.push(negParts.join(" "));
    }
  }
  if (descriptor.flags.length > 0) {
    const notDash = `not string match -q -- "-*" (commandline -ct)`;
    const bareBase = condition ? `${condition}; and ${notDash}` : notDash;
    for (const flag of descriptor.flags) {
      const bareCondition = `${bareBase}; and ${flagContainsOptCondition(flag)}`;
      const negation = negatedName(flag);
      const parts = [`complete -c ${executableName}`];
      parts.push(`-n '${bareCondition}'`);
      parts.push(`-f -a '--${flag.name}'`);
      if (flag.description) {
        parts.push(`-d '${escapeFishString(flag.description)}'`);
      }
      lines.push(parts.join(" "));
      if (negation !== undefined) {
        const negParts = [`complete -c ${executableName}`];
        negParts.push(`-n '${bareCondition}'`);
        negParts.push(`-f -a '--${negation}'`);
        if (flag.description) {
          negParts.push(`-d '${escapeFishString(`Disable ${flag.name}`)}'`);
        }
        lines.push(negParts.join(" "));
      }
    }
  }
  for (const arg of descriptor.arguments) {
    const valueArg = argValueArgs(arg.type);
    if (valueArg) {
      const parts = [`complete -c ${executableName}`];
      if (conditionArg)
        parts.push(conditionArg);
      parts.push(valueArg);
      if (arg.description) {
        parts.push(`-d '${escapeFishString(arg.description)}'`);
      }
      lines.push(parts.join(" "));
    }
  }
  for (const sub of descriptor.subcommands) {
    generateCompletions(executableName, sub, [...parentPath, sub.name], lines);
  }
};
var generate2 = (executableName, descriptor) => {
  const lines = [];
  lines.push(`###-begin-${executableName}-completions-###`);
  lines.push(`#`);
  lines.push(`# Static completion script for Fish`);
  lines.push(`#`);
  lines.push(`# Installation:`);
  lines.push(`#   ${executableName} --completions fish > ~/.config/fish/completions/${executableName}.fish`);
  lines.push(`#`);
  lines.push(``);
  generateCompletions(executableName, descriptor, [], lines);
  lines.push(``);
  lines.push(`###-end-${executableName}-completions-###`);
  return lines.join(`
`);
};

// node_modules/effect/dist/cli/internal/completions/powershell.js
var quotePs = (s) => {
  const parts = [];
  let literal = "";
  for (let i = 0;i < s.length; i++) {
    const code = s.charCodeAt(i);
    if (code >= 32 && code < 128) {
      literal += s[i] === "'" ? "''" : s[i];
      continue;
    }
    if (literal !== "" || parts.length === 0)
      parts.push(`'${literal}'`);
    literal = "";
    parts.push(`[char]0x${code.toString(16).toUpperCase().padStart(4, "0")}`);
  }
  if (literal !== "" || parts.length === 0)
    parts.push(`'${literal}'`);
  return parts.length === 1 ? parts[0] : `(${parts.join(" + ")})`;
};
var psArray = (items) => `@(${items.map(quotePs).join(", ")})`;
var psOptional = (s) => s ? quotePs(s) : "$null";
var sanitize = (s) => s.replace(/[^a-zA-Z0-9_]/g, "_");
var flagForms = (flag) => [`--${flag.name}`, ...flag.aliases.map((alias) => alias.length === 1 ? `-${alias}` : `--${alias}`)];
var valueFields = (type) => `values = ${psArray(type._tag === "Choice" ? type.values : [])}; ` + `pathType = ${type._tag === "Path" ? quotePs(type.pathType) : "$null"}`;
var flagEntry = (flag) => {
  const negation = negatedName(flag);
  return `@{ name = ${quotePs(flag.name)}; forms = ${psArray(flagForms(flag))}; ` + `takesValue = $${flag.type._tag !== "Boolean"}; ` + (negation === undefined ? "" : `negation = ${quotePs(`--${negation}`)}; `) + (flag.optionalValue === true ? `optionalValue = $true; ` : "") + `${valueFields(flag.type)}; ` + `description = ${psOptional(flag.description)} }`;
};
var argumentEntry = (argument) => `@{ name = ${quotePs(argument.name)}; variadic = $${argument.variadic}; ${valueFields(argument.type)}; ` + `description = ${psOptional(argument.description)} }`;
var pushList = (lines, field, entries) => {
  if (entries.length === 0) {
    lines.push(`  ${field} = @()`);
    return;
  }
  lines.push(`  ${field} = @(`);
  for (const entry of entries) {
    lines.push(`    ${entry}`);
  }
  lines.push(`  )`);
};
var generateContexts = (dataName, descriptor, path, lines, seen) => {
  const key = path.join(" ");
  if (seen.has(key))
    return;
  seen.add(key);
  lines.push(`${dataName}[${quotePs(key)}] = @{`);
  pushList(lines, "subcommands", descriptor.subcommands.map((sub) => `@{ name = ${quotePs(sub.name)}; description = ${psOptional(sub.description)} }`));
  pushList(lines, "flags", descriptor.flags.map(flagEntry));
  pushList(lines, "arguments", descriptor.arguments.map(argumentEntry));
  lines.push(`}`);
  for (const sub of descriptor.subcommands) {
    generateContexts(dataName, sub, [...path, sub.name], lines, seen);
  }
};
var completer = (dataName) => String.raw`  param($wordToComplete, $commandAst, $cursorPosition)

  $spec = ${dataName}
  $results = [System.Collections.Generic.List[System.Management.Automation.CompletionResult]]::new()
  $booleanLiterals = @('true', 'yes', 'on', '1', 'y', 'false', 'no', 'off', '0', 'n')

  $index = {
    param($context)
    $lookup = [hashtable]::new([System.StringComparer]::Ordinal)
    foreach ($flag in $context.flags) {
      foreach ($form in $flag.forms) { $lookup[$form] = $flag }
    }
    $lookup
  }

  $add = {
    param([string]$value, [string]$type, $tooltip, [string]$typed, [string]$textPrefix)
    if (-not $value.StartsWith($typed, [System.StringComparison]::OrdinalIgnoreCase)) { return }
    $text = $value
    if ($value -notmatch '^[\w./:%+=\\-]+$') { $text = "'" + ($value -replace "['\u2018-\u201b]", '$0$0') + "'" }
    $text = $textPrefix + $text
    $label = $value
    if (-not $label) { $label = $text }
    if (-not $tooltip) { $tooltip = $label }
    $results.Add([System.Management.Automation.CompletionResult]::new($text, $label, $type, $tooltip))
  }

  $addValues = {
    param($entry, [string]$typed, [string]$textPrefix)
    foreach ($value in $entry.values) {
      if ($textPrefix -eq '' -and (& $optionLike $value)) { continue }
      & $add $value 'ParameterValue' $entry.description $typed $textPrefix
    }
    if ($entry.pathType) {
      foreach ($file in [System.Management.Automation.CompletionCompleters]::CompleteFilename($typed)) {
        if ($entry.pathType -eq 'directory' -and $file.ResultType -ne 'ProviderContainer') { continue }
        $results.Add([System.Management.Automation.CompletionResult]::new($textPrefix + $file.CompletionText, $file.ListItemText, $file.ResultType, $file.ToolTip))
      }
    }
  }

  $addFlags = {
    param([string]$typed)
    foreach ($flag in $context.flags) {
      if ($used.ContainsKey($flag.name)) { continue }
      foreach ($form in $flag.forms) {
        & $add $form 'ParameterName' $flag.description $typed ''
      }
      if ($flag.negation) {
        $tooltip = $null
        if ($flag.description) { $tooltip = 'Disable ' + $flag.name }
        & $add $flag.negation 'ParameterName' $tooltip $typed ''
      }
    }
  }

  # As in the CLI lexer, nothing after '--' is an option, and negative numbers
  # and a lone '-' are values.
  $optionLike = {
    param([string]$word)
    -not $endOfOptions -and $word -match '^-.' -and $word -notmatch '^-(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$'
  }

  # A typed value that has no candidates completes to itself, so the engine
  # does not fall back to listing files.
  $keepTyped = {
    if ($results.Count -eq 0 -and $current) {
      $results.Add([System.Management.Automation.CompletionResult]::new($current, $current, 'ParameterValue', $current))
    }
  }

  $path = ''
  $context = $spec[$path]
  $lookup = & $index $context
  $used = [hashtable]::new([System.StringComparer]::Ordinal)
  $pending = $null
  $endOfOptions = $false
  $position = 0

  # The engine strips the quotes from a quoted word.
  $typed = [string]$wordToComplete
  if ($typed -match '^([''"])(.*?)\1?$') { $typed = $Matches[2] }
  $current = $null

  $elements = $commandAst.CommandElements
  for ($i = 1; $i -lt $elements.Count; $i++) {
    $element = $elements[$i]
    $word = $element.Extent.Text
    if ($element -is [System.Management.Automation.Language.StringConstantExpressionAst]) { $word = $element.Value }
    if ($element.Extent.EndOffset -ge $cursorPosition) {
      if ($element -is [System.Management.Automation.Language.ConstantExpressionAst] -and $element.Extent.StartOffset -le $cursorPosition) {
        $current = $element.Extent.Text
        if ($element.Extent.EndOffset -eq $cursorPosition) { $typed = $word }
      }
      break
    }

    # A flag takes the next word as its value unless it is an option or '--';
    # a boolean flag only takes a boolean literal.
    $isOption = & $optionLike $word
    if ($null -ne $pending) {
      $flag = $pending
      $pending = $null
      if ($flag.takesValue) {
        if (-not $isOption) { continue }
      } elseif ($booleanLiterals -ccontains $word) { continue }
    }
    if ($endOfOptions) { $position++; continue }
    if ($word -ceq '--') { $endOfOptions = $true; continue }

    if ($isOption) {
      $hasValue = $word.Contains('=')
      if ($hasValue) {
        $forms = @($word.Substring(0, $word.IndexOf('=')))
      } elseif ($word.StartsWith('--')) {
        $forms = @($word)
      } else {
        $forms = @(foreach ($char in $word.Substring(1).ToCharArray()) { '-' + $char })
      }
      foreach ($form in $forms) {
        $pending = $null
        $flag = $lookup[$form]
        # As in the CLI parser, a negation wins over an alias but not over a
        # canonical name.
        if ($null -eq $flag -or $form -cne ('--' + $flag.name)) {
          $negated = $null
          foreach ($candidate in $context.flags) {
            if ($candidate.negation -ceq $form) { $negated = $candidate }
          }
          if ($null -ne $negated) {
            $used[$negated.name] = $true
            continue
          }
        }
        if ($null -eq $flag) { continue }
        $used[$flag.name] = $true
        # A flag with an optional value only takes a value attached with '='.
        if (-not $hasValue -and -not $flag.optionalValue) { $pending = $flag }
      }
      continue
    }

    if ($position -eq 0 -and $context.subcommands.name -ccontains $word) {
      if ($path) { $path = $path + ' ' + $word } else { $path = $word }
      $context = $spec[$path]
      $lookup = & $index $context
      $used = [hashtable]::new([System.StringComparer]::Ordinal)
      continue
    }
    $position++
  }

  $typedIsOption = & $optionLike $typed
  if ($null -ne $pending -and $pending.takesValue -and -not $typedIsOption) {
    & $addValues $pending $typed ''
    & $keepTyped
    return $results
  }

  if ($typedIsOption) {
    if ($typed -match '^(-[^=]+)=(.*)$') {
      $form = $Matches[1]
      $flag = $lookup[$form]
      if ($null -ne $flag -and $flag.takesValue) {
        & $addValues $flag $Matches[2] ($form + '=')
        & $keepTyped
      }
    } else {
      & $addFlags $typed
    }
    return $results
  }

  if (-not $endOfOptions -and $position -eq 0) {
    foreach ($sub in $context.subcommands) {
      & $add $sub.name 'Command' $sub.description $typed ''
    }
  }

  $arguments = $context.arguments
  if ($position -lt $arguments.Count) {
    & $addValues $arguments[$position] $typed ''
  } elseif ($arguments.Count -gt 0 -and $arguments[-1].variadic) {
    & $addValues $arguments[-1] $typed ''
  }

  if (-not $endOfOptions) {
    & $addFlags $typed
  }
  return $results`;
var generate3 = (executableName, descriptor) => {
  const dataName = `$global:_${sanitize(executableName)}_Completions`;
  const lines = [];
  lines.push(`###-begin-${executableName}-completions-###`);
  lines.push(`#`);
  lines.push(`# Static completion script for PowerShell`);
  lines.push(`#`);
  lines.push(`# Installation:`);
  lines.push(`#   ${executableName} --completions powershell > ${executableName}-completion.ps1`);
  lines.push(`#   then add this line to your $PROFILE (use the full path to the file):`);
  lines.push(`#   . <PATH>\\${executableName}-completion.ps1`);
  lines.push(`#`);
  lines.push(``);
  lines.push(`${dataName} = [hashtable]::new([System.StringComparer]::Ordinal)`);
  generateContexts(dataName, descriptor, [], lines, new Set);
  lines.push(``);
  lines.push(`Register-ArgumentCompleter -Native -CommandName ${quotePs(executableName)} -ScriptBlock {`);
  lines.push(completer(dataName));
  lines.push(`}`);
  lines.push(``);
  lines.push(`###-end-${executableName}-completions-###`);
  return lines.join(`
`);
};

// node_modules/effect/dist/cli/internal/completions/zsh.js
var escapeZsh = (s) => s.replace(/\\/g, "\\\\").replace(/'/g, "'\\''").replace(/:/g, "\\:");
var escapeZshExplanation = (s) => escapeZsh(s).replace(/]/g, "\\]");
var escapeZshChoice = (s) => s.replace(/[^A-Za-z0-9_.,/@%+-]/gu, "\\$&").replace(/'/g, "'\\''");
var sanitize2 = (s) => s.replace(/[^a-zA-Z0-9_]/g, "_");
var allForms = (flag) => {
  const forms = [`--${flag.name}`];
  for (const alias of flag.aliases) {
    forms.push(alias.length === 1 ? `-${alias}` : `--${alias}`);
  }
  const negation = negatedName(flag);
  if (negation !== undefined) {
    forms.push(`--${negation}`);
  }
  return forms;
};
var valueAction = (type) => {
  switch (type._tag) {
    case "Boolean":
      return "";
    case "Choice":
      return `:value:(${type.values.map(escapeZshChoice).join(" ")})`;
    case "Path":
      return type.pathType === "directory" ? `:directory:_directories` : `:file:_files`;
    case "Int":
      return `:integer:`;
    case "Finite":
      return `:float:`;
    case "Date":
      return `:date:`;
    default:
      return `:value:`;
  }
};
var argAction = (type) => {
  switch (type._tag) {
    case "Choice":
      return `(${type.values.map(escapeZshChoice).join(" ")})`;
    case "Path":
      return type.pathType === "directory" ? `_directories` : `_files`;
    default:
      return ``;
  }
};
var flagSpecs = (flag) => {
  const specs = [];
  const desc = flag.description ? `[${escapeZshExplanation(flag.description)}]` : "";
  const optional = flag.optionalValue === true && flag.type._tag !== "Boolean";
  const valueSuffix = optional ? "=-" : "";
  const action = optional ? `:${valueAction(flag.type)}` : valueAction(flag.type);
  const excl = `(${allForms(flag).join(" ")})`;
  specs.push(`'${excl}--${flag.name}${valueSuffix}${desc}${action}'`);
  for (const alias of flag.aliases) {
    const prefix = alias.length === 1 ? "-" : "--";
    specs.push(`'${excl}${prefix}${alias}${valueSuffix}${desc}${action}'`);
  }
  const negation = negatedName(flag);
  if (negation !== undefined) {
    const negDesc = flag.description ? `[${escapeZshExplanation(`Disable ${flag.name}`)}]` : "";
    specs.push(`'${excl}--${negation}${negDesc}'`);
  }
  return specs;
};
var argSpec = (arg) => {
  const desc = arg.description ? escapeZsh(arg.description) : arg.name;
  const action = argAction(arg.type);
  const prefix = arg.variadic ? "*" : "";
  return `'${prefix}:${desc}:${action}'`;
};
var generateFunction2 = (descriptor, parentPath, lines) => {
  const currentPath = [...parentPath, descriptor.name];
  const funcName = `_${currentPath.map(sanitize2).join("_")}`;
  lines.push(`${funcName}() {`);
  if (descriptor.subcommands.length > 0) {
    lines.push(`  local context state state_descr line`);
    lines.push(`  typeset -A opt_args`);
    lines.push(``);
    lines.push(`  local -a commands`);
    lines.push(`  commands=(`);
    for (const sub of descriptor.subcommands) {
      const desc = sub.description ? escapeZsh(sub.description) : "";
      lines.push(`    '${sub.name}:${desc}'`);
    }
    lines.push(`  )`);
    lines.push(``);
    lines.push(`  local -a specs`);
    lines.push(`  specs=(`);
    for (const flag of descriptor.flags) {
      for (const spec of flagSpecs(flag)) {
        lines.push(`    ${spec}`);
      }
    }
    if (descriptor.arguments.length > 0) {
      lines.push(`    -`);
      lines.push(`    parent-arguments`);
      for (const arg of descriptor.arguments) {
        lines.push(`    ${argSpec(arg)}`);
      }
      lines.push(`    -`);
      lines.push(`    subcommands`);
    }
    lines.push(`    '1:command:->command'`);
    lines.push(`    '*::arg:->args'`);
    lines.push(`  )`);
    lines.push(``);
    lines.push(`  _arguments -C "\${specs[@]}"`);
    lines.push(``);
    lines.push(`  case "$state" in`);
    lines.push(`    command)`);
    lines.push(`      _describe -t commands 'commands' commands`);
    lines.push(`      ;;`);
    lines.push(`    args)`);
    lines.push(`      case "$words[1]" in`);
    for (const sub of descriptor.subcommands) {
      const subFunc = `_${[...currentPath, sub.name].map(sanitize2).join("_")}`;
      lines.push(`        ${sub.name})`);
      lines.push(`          ${subFunc}`);
      lines.push(`          ;;`);
    }
    lines.push(`      esac`);
    lines.push(`      ;;`);
    lines.push(`  esac`);
  } else {
    const allSpecs = [];
    for (const flag of descriptor.flags) {
      allSpecs.push(...flagSpecs(flag));
    }
    for (const arg of descriptor.arguments) {
      allSpecs.push(argSpec(arg));
    }
    if (allSpecs.length > 0) {
      lines.push(`  local -a specs`);
      lines.push(`  specs=(`);
      for (const spec of allSpecs) {
        lines.push(`    ${spec}`);
      }
      lines.push(`  )`);
      lines.push(`  _arguments "\${specs[@]}"`);
    }
  }
  lines.push(`}`);
  lines.push(``);
  for (const sub of descriptor.subcommands) {
    generateFunction2(sub, currentPath, lines);
  }
};
var generate4 = (executableName, descriptor) => {
  const lines = [];
  const safeName = sanitize2(executableName);
  lines.push(`#compdef ${executableName}`);
  lines.push(`###-begin-${executableName}-completions-###`);
  lines.push(`#`);
  lines.push(`# Static completion script for Zsh`);
  lines.push(`#`);
  lines.push(`# Installation:`);
  lines.push(`#   ${executableName} --completions zsh > ~/.zsh/completions/_${executableName}`);
  lines.push(`#   then add ~/.zsh/completions to your fpath`);
  lines.push(`#`);
  lines.push(``);
  generateFunction2(descriptor, [], lines);
  lines.push(`# Handle both direct invocation and autoload`);
  lines.push(`if [[ "\${zsh_eval_context[-1]}" == "loadautofunc" ]]; then`);
  lines.push(`  _${safeName} "$@"`);
  lines.push(`else`);
  lines.push(`  compdef _${safeName} ${executableName}`);
  lines.push(`fi`);
  lines.push(`###-end-${executableName}-completions-###`);
  return lines.join(`
`);
};

// node_modules/effect/dist/cli/Completions.js
var fromCommand2 = (command) => fromCommand(command);
var generate5 = (executableName, shell, descriptor) => {
  switch (shell) {
    case "bash":
      return generate(executableName, descriptor);
    case "zsh":
      return generate4(executableName, descriptor);
    case "fish":
      return generate2(executableName, descriptor);
    case "powershell":
      return generate3(executableName, descriptor);
  }
};

// node_modules/effect/dist/cli/Flag.js
var String11 = (name) => String9(flagKind, name);
var Boolean6 = (name) => Boolean5(flagKind, name);
var Int5 = (name) => Int4(flagKind, name);
var ChoiceWithValue2 = (name, choices) => ChoiceWithValue(flagKind, name, choices);
var Literals3 = (name, literals) => Literals2(flagKind, name, literals);
var withAlias2 = /* @__PURE__ */ dual(2, (self, alias) => withAlias(self, alias));
var withDescription3 = /* @__PURE__ */ dual(2, (self, description) => withDescription(self, description));
var withMetavar2 = /* @__PURE__ */ dual(2, (self, metavar) => withMetavar(self, metavar));
var optional5 = (param) => optional3(param);
var withDefault5 = withDefault4;
var atLeast3 = /* @__PURE__ */ dual(2, (self, min) => atLeast(self, min));

// node_modules/effect/dist/cli/internal/help.js
var dedupeGlobalFlags = (flags) => {
  const seen = new Set;
  const deduped = [];
  for (const flag of flags) {
    if (seen.has(flag)) {
      continue;
    }
    seen.add(flag);
    deduped.push(flag);
  }
  return deduped;
};
var getCommandsForCommandPath = (command, commandPath) => {
  const commands = [command];
  let currentCommand = command;
  for (let i = 1;i < commandPath.length; i++) {
    const subcommandName = commandPath[i];
    let subcommand = undefined;
    for (const group of currentCommand.subcommands) {
      subcommand = group.commands.find((sub) => sub.name === subcommandName);
      if (subcommand) {
        break;
      }
    }
    if (!subcommand) {
      break;
    }
    commands.push(subcommand);
    currentCommand = subcommand;
  }
  return commands;
};
var getGlobalFlagsForCommandPath = (command, commandPath, builtIns) => {
  const commands = getCommandsForCommandPath(command, commandPath);
  const declared = commands.flatMap((current) => toImpl(current).globalFlags);
  return dedupeGlobalFlags([...builtIns, ...declared]);
};
var collectDeclaredGlobalFlags = (command) => {
  const collected = [];
  const visit = (current) => {
    const impl = toImpl(current);
    for (const flag of impl.globalFlags) {
      collected.push(flag);
    }
    for (const group of current.subcommands) {
      for (const subcommand of group.commands) {
        visit(subcommand);
      }
    }
  };
  visit(command);
  return dedupeGlobalFlags(collected);
};
var getInheritedFlagsForCommandPath = (commands, currentFlags) => {
  if (commands.length <= 1) {
    return [];
  }
  const seen = new Set(currentFlags.map((flag) => flag.name));
  const inheritedFlags = [];
  for (const ancestor of commands.slice(0, -1)) {
    const ancestorImpl = toImpl(ancestor);
    for (const flag of ancestorImpl.contextConfig.flags) {
      const singles = extractSingleParams(flag);
      const metadata = getParamMetadata(flag);
      for (const single of singles) {
        if (seen.has(single.name)) {
          continue;
        }
        if (single.hidden) {
          continue;
        }
        seen.add(single.name);
        inheritedFlags.push(toFlagDoc(single, metadata));
      }
    }
  }
  return inheritedFlags;
};
var getGlobalFlagsForCommandTree = (command, builtIns) => dedupeGlobalFlags([...builtIns, ...collectDeclaredGlobalFlags(command)]);
var getHelpForCommandPath = (command, commandPath, builtIns) => gen2(function* () {
  const commands = getCommandsForCommandPath(command, commandPath);
  const currentCommand = commands.length > 0 ? commands[commands.length - 1] : command;
  const baseDoc = toImpl(currentCommand).buildHelpDoc(commandPath);
  const inheritedFlags = getInheritedFlagsForCommandPath(commands, baseDoc.flags);
  const flags = getGlobalFlagsForCommandPath(command, commandPath, builtIns);
  const globalFlagDocs = [];
  for (const flag of flags) {
    const singles = extractSingleParams(flag.flag);
    const metadata = getParamMetadata(flag.flag);
    for (const single of singles) {
      if (single.hidden)
        continue;
      globalFlagDocs.push({
        ...toFlagDoc(single, metadata),
        required: false
      });
    }
  }
  let scopeLength = 1;
  for (let i = commands.length - 1;i > 0; i--) {
    if (toImpl(commands[i]).globalFlags.length > 0) {
      scopeLength = i + 1;
      break;
    }
  }
  return {
    ...baseDoc,
    inheritedFlags,
    globalFlags: globalFlagDocs,
    globalFlagsScope: commandPath.slice(0, scopeLength),
    commandPath
  };
});

// node_modules/effect/dist/cli/GlobalFlag.js
var Action2 = (options) => ({
  _tag: "Action",
  flag: options.flag,
  run: options.run
});
var Setting = (id) => (options) => {
  settingIdCounter += 1;
  const ref = Service(`effect/cli/GlobalFlag/${id}/${settingIdCounter}`);
  return Object.assign(ref, {
    _tag: "Setting",
    id,
    flag: options.flag
  });
};
var settingIdCounter = 0;
var Help = /* @__PURE__ */ Action2({
  flag: /* @__PURE__ */ Boolean6("help").pipe(/* @__PURE__ */ withAlias2("h"), /* @__PURE__ */ withDescription3("Show help information"), /* @__PURE__ */ withDefault5(false)),
  run: /* @__PURE__ */ fnUntraced2(function* (_, {
    builtIns,
    command,
    commandPath
  }) {
    const presenter = yield* Presenter;
    const helpDoc = yield* getHelpForCommandPath(command, commandPath, builtIns);
    yield* presenter.present({
      _tag: "Help",
      reason: "Requested",
      commandPath,
      helpDoc
    });
  })
});
var Version = /* @__PURE__ */ Action2({
  flag: /* @__PURE__ */ Boolean6("version").pipe(/* @__PURE__ */ withAlias2("v"), /* @__PURE__ */ withDescription3("Show version information"), /* @__PURE__ */ withDefault5(false)),
  run: /* @__PURE__ */ fnUntraced2(function* (_, {
    command,
    version
  }) {
    const presenter = yield* Presenter;
    yield* presenter.present({
      _tag: "Version",
      name: command.name,
      version
    });
  })
});
var Wizard = /* @__PURE__ */ Action2({
  flag: /* @__PURE__ */ Boolean6("wizard").pipe(/* @__PURE__ */ withDescription3("Start wizard mode for a command"), /* @__PURE__ */ withDefault5(false)),
  run: () => void_3
});
var Completions = /* @__PURE__ */ Action2({
  flag: /* @__PURE__ */ ChoiceWithValue2("completions", [["bash", "bash"], ["zsh", "zsh"], ["fish", "fish"], ["powershell", "powershell"], ["sh", "bash"], ["pwsh", "powershell"]]).pipe(optional5, /* @__PURE__ */ withMetavar2("<bash|zsh|fish|powershell|sh|pwsh>"), /* @__PURE__ */ withDescription3("Print shell completion script")),
  run: /* @__PURE__ */ fnUntraced2(function* (shell, {
    command
  }) {
    if (isNone2(shell))
      return;
    const descriptor = fromCommand2(command);
    yield* log2(generate5(command.name, shell.value, descriptor));
  })
});
var LogLevel = /* @__PURE__ */ Setting("log-level")({
  flag: /* @__PURE__ */ ChoiceWithValue2("log-level", [["all", "All"], ["trace", "Trace"], ["debug", "Debug"], ["info", "Info"], ["warn", "Warn"], ["warning", "Warn"], ["error", "Error"], ["fatal", "Fatal"], ["none", "None"]]).pipe(optional5, /* @__PURE__ */ withDescription3("Sets the minimum log level"), /* @__PURE__ */ withMetavar2("<all|trace|debug|info|warn|warning|error|fatal|none>"))
});
var BuiltIns = [Help, Version, Wizard, Completions, LogLevel];

// node_modules/effect/dist/cli/CliConfig.js
class CliConfig extends (/* @__PURE__ */ Reference("effect/cli/CliConfig", {
  defaultValue: () => defaults
})) {
}
var defaults = {
  builtIns: BuiltIns
};
// node_modules/effect/dist/cli/internal/formatter.js
var resolveFormatter = /* @__PURE__ */ contextWith2((context) => succeed7(getOrUndefined2(context, Formatter) ?? defaultFormatter2({
  colors: get(context, TerminalCapabilities).canColor
})));

// node_modules/effect/dist/cli/internal/lexer.js
function lex(argv) {
  const endIndex = argv.indexOf("--");
  if (endIndex === -1) {
    return {
      tokens: lexTokens(argv),
      trailingOperands: []
    };
  }
  return {
    tokens: lexTokens(argv.slice(0, endIndex)),
    trailingOperands: argv.slice(endIndex + 1)
  };
}
var negativeNumberRegex = /^-(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$/;
var lexTokens = (args) => {
  const tokens = [];
  for (const arg of args) {
    if (!arg.startsWith("-") || negativeNumberRegex.test(arg)) {
      tokens.push({
        _tag: "Value",
        value: arg
      });
    } else if (arg.startsWith("--")) {
      const equalIndex = arg.indexOf("=");
      if (equalIndex !== -1) {
        const name = arg.slice(2, equalIndex);
        if (name === "") {
          tokens.push({
            _tag: "Value",
            value: arg
          });
        } else {
          const value = arg.slice(equalIndex + 1);
          tokens.push({
            _tag: "LongOption",
            name,
            raw: arg,
            value
          });
        }
      } else {
        tokens.push({
          _tag: "LongOption",
          name: arg.slice(2),
          raw: arg
        });
      }
    } else if (arg.length > 1) {
      const flags = arg.slice(1);
      const equalIndex = flags.indexOf("=");
      if (equalIndex !== -1) {
        const chars = Array.from(flags.slice(0, equalIndex));
        const flag = chars.pop() ?? "";
        for (const ch of chars) {
          tokens.push({
            _tag: "ShortOption",
            flag: ch,
            raw: `-${ch}`
          });
        }
        tokens.push({
          _tag: "ShortOption",
          flag,
          raw: `-${flag}`,
          value: flags.slice(equalIndex + 1)
        });
      } else {
        for (const ch of flags) {
          tokens.push({
            _tag: "ShortOption",
            flag: ch,
            raw: `-${ch}`
          });
        }
      }
    } else {
      tokens.push({
        _tag: "Value",
        value: arg
      });
    }
  }
  return tokens;
};

// node_modules/effect/dist/cli/internal/auto-suggest.js
var levenshtein = (a, b) => {
  const m = a.length;
  const n = b.length;
  const dp = Array.from({
    length: m + 1
  }, () => new Array(n + 1).fill(0));
  for (let i = 0;i <= m; i++)
    dp[i][0] = i;
  for (let j = 0;j <= n; j++)
    dp[0][j] = j;
  for (let i = 1;i <= m; i++) {
    for (let j = 1;j <= n; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost);
    }
  }
  return dp[m][n];
};
var suggest = (input, candidates) => {
  const distances = candidates.map((c) => [levenshtein(input, c), c]).filter(([d]) => d <= 2).sort(([a], [b]) => a - b);
  if (distances.length === 0)
    return [];
  const minDistance = distances[0][0];
  return distances.filter(([d]) => d === minDistance).map(([, c]) => c);
};

// node_modules/effect/dist/cli/internal/parser.js
var getCommandPath = (parsedInput) => match(parsedInput.subcommand, {
  onNone: () => [],
  onSome: (subcommand) => [subcommand.name, ...getCommandPath(subcommand.parsedInput)]
});
var parseArgs = (lexResult, command, commandPath = []) => gen2(function* () {
  const {
    tokens,
    trailingOperands: afterEndOfOptions
  } = lexResult;
  const newCommandPath = [...commandPath, command.name];
  const commandImpl = toImpl(command);
  const singles = commandImpl.config.flags.flatMap(extractSingleParams);
  const flagParams = singles.filter(isFlagParam);
  const flagRegistry = createFlagRegistry(flagParams);
  const inheritedSingles = commandImpl.contextConfig.flags.flatMap(extractSingleParams);
  const inheritedFlagParams = inheritedSingles.filter(isFlagParam);
  const inheritedFlagRegistry = createFlagRegistry(inheritedFlagParams);
  const inheritedNames = new Set(inheritedFlagParams.map((param) => param.name));
  const context = {
    command,
    commandPath: newCommandPath,
    flagRegistry,
    inheritedFlagRegistry,
    localFlagNames: flagParams.filter((param) => !inheritedNames.has(param.name)).map((param) => param.name)
  };
  const result = scanCommandLevel(tokens, context);
  if (result._tag === "Leaf") {
    return {
      flags: result.flags,
      arguments: [...result.arguments, ...afterEndOfOptions],
      subcommand: none2(),
      ...result.errors.length > 0 && {
        errors: result.errors
      }
    };
  }
  const subLex = {
    tokens: result.childTokens,
    trailingOperands: afterEndOfOptions
  };
  const subParsed = yield* parseArgs(subLex, result.sub, newCommandPath);
  const allErrors = [...result.errors, ...subParsed.errors ?? []];
  return {
    flags: result.flags,
    arguments: [],
    subcommand: some2({
      name: result.sub.name,
      parsedInput: subParsed
    }),
    ...allErrors.length > 0 && {
      errors: allErrors
    }
  };
});
var makeCursor = (tokens) => {
  let i = 0;
  return {
    peek: () => tokens[i],
    take: () => tokens[i++],
    rest: () => tokens.slice(i)
  };
};
var negatedName2 = (param) => !isBoolean2(param.primitiveType) || param.negation === false ? undefined : param.negation ?? `no-${param.name}`;
var createFlagRegistry = (params) => {
  const index = new Map;
  const negations = new Map;
  for (const param of params) {
    if (index.has(param.name)) {
      throw new Error(`Duplicate flag name "${param.name}" in command definition`);
    }
    index.set(param.name, param);
    for (const alias of param.aliases) {
      if (index.has(alias)) {
        throw new Error(`Duplicate flag/alias "${alias}" in command definition (conflicts with "${index.get(alias).name}")`);
      }
      index.set(alias, param);
    }
    const negation = negatedName2(param);
    if (negation !== undefined) {
      if (negations.has(negation)) {
        throw new Error(`Duplicate negation "${negation}" in command definition (conflicts with "${negations.get(negation).name}")`);
      }
      negations.set(negation, param);
    }
  }
  return {
    params,
    index,
    negations
  };
};
var buildSubcommandIndex = (subcommands) => {
  const index = new Map;
  const setKey = (key, command) => {
    const existing = index.get(key);
    if (existing && existing !== command) {
      throw new Error(`Duplicate subcommand name/alias "${key}" in command definition (conflicts with "${existing.name}")`);
    }
    index.set(key, command);
  };
  for (const group of subcommands) {
    for (const subcommand of group.commands) {
      setKey(subcommand.name, subcommand);
      if (subcommand.alias && subcommand.alias !== subcommand.name) {
        setKey(subcommand.alias, subcommand);
      }
    }
  }
  return index;
};
var createEmptyFlagMap = (params) => Object.fromEntries(params.map((p) => [p.name, []]));
var createFlagAccumulator = (params) => {
  const map = createEmptyFlagMap(params);
  return {
    add: (name, raw) => {
      if (raw !== undefined)
        map[name].push(raw);
    },
    merge: (from) => {
      for (const key in from) {
        const values = from[key];
        if (values?.length) {
          for (let i = 0;i < values.length; i++) {
            map[key].push(values[i]);
          }
        }
      }
    },
    snapshot: () => map
  };
};
var isFlagToken = (t) => t._tag === "LongOption" || t._tag === "ShortOption";
var getFlagName = (t) => t._tag === "LongOption" ? t.name : t.flag;
var resolveFlag = (token, registry) => {
  const tokenName = getFlagName(token);
  const direct = registry.index.get(tokenName);
  if (direct && direct.name === tokenName) {
    return {
      param: direct,
      negated: false
    };
  }
  const negated = token._tag === "LongOption" ? registry.negations.get(token.name) : undefined;
  if (negated) {
    return {
      param: negated,
      negated: true
    };
  }
  if (direct) {
    return {
      param: direct,
      negated: false
    };
  }
  return;
};
var invalidNegatedFlagValue = (token, spec, value) => new InvalidValue2({
  option: spec.name,
  value,
  expected: `omit the value and use ${token.raw} by itself to set --${spec.name} to false`,
  kind: "flag"
});
var missingFlagValue = (spec) => {
  const choices = getChoiceKeys(spec.primitiveType);
  return new InvalidValue2({
    option: spec.name,
    value: "",
    expected: choices === undefined ? spec.typeName ?? getTypeName(spec.primitiveType) : choices.join(" | "),
    kind: "flag"
  });
};
var asBooleanLiteral = (token) => token?._tag === "Value" && (isTrueLiteral(token.value) || isFalseLiteral(token.value)) ? token.value : undefined;
var consumeFlagValue = (cursor, token, spec, negated = false) => {
  const consumed = consumeFlagValueWithTokens(cursor, token, spec, negated);
  switch (consumed._tag) {
    case "Value":
      return {
        _tag: "Value",
        value: consumed.value
      };
    case "Error":
      return {
        _tag: "Error",
        error: consumed.error
      };
  }
};
var consumeFlagValueWithTokens = (cursor, token, spec, negated = false) => {
  if (negated) {
    if (token.value !== undefined) {
      return {
        _tag: "Error",
        error: invalidNegatedFlagValue(token, spec, token.value),
        tokens: []
      };
    }
    const literal = spec.optionalValue === undefined ? asBooleanLiteral(cursor.peek()) : undefined;
    if (literal !== undefined) {
      const literalToken = cursor.take();
      return {
        _tag: "Error",
        error: invalidNegatedFlagValue(token, spec, literal),
        tokens: literalToken === undefined ? [] : [literalToken]
      };
    }
    return {
      _tag: "Value",
      value: "false",
      tokens: []
    };
  }
  if (token.value !== undefined) {
    return {
      _tag: "Value",
      value: token.value,
      tokens: []
    };
  }
  if (spec.optionalValue !== undefined) {
    return {
      _tag: "Value",
      value: spec.optionalValue,
      tokens: []
    };
  }
  if (isBoolean2(spec.primitiveType)) {
    const literal = asBooleanLiteral(cursor.peek());
    const literalToken = literal !== undefined ? cursor.take() : undefined;
    return {
      _tag: "Value",
      value: literal ?? "true",
      tokens: literalToken === undefined ? [] : [literalToken]
    };
  }
  const next = cursor.peek();
  if (next?._tag === "Value") {
    const valueToken = cursor.take();
    return {
      _tag: "Value",
      value: next.value,
      tokens: valueToken === undefined ? [] : [valueToken]
    };
  }
  return {
    _tag: "Error",
    error: missingFlagValue(spec),
    tokens: []
  };
};
var consumeKnownFlags = (tokens, registry) => {
  const flagMap = createEmptyFlagMap(registry.params);
  const remainder = [];
  const errors = [];
  const cursor = makeCursor(tokens);
  for (let t = cursor.take();t; t = cursor.take()) {
    if (!isFlagToken(t)) {
      remainder.push(t);
      continue;
    }
    const resolved = resolveFlag(t, registry);
    if (!resolved) {
      remainder.push(t);
      continue;
    }
    const consumed = consumeFlagValue(cursor, t, resolved.param, resolved.negated);
    if (consumed._tag === "Error") {
      errors.push(consumed.error);
      continue;
    }
    if (consumed.value !== undefined) {
      flagMap[resolved.param.name].push(consumed.value);
    }
  }
  return {
    flagMap,
    remainder,
    errors
  };
};
var extractFlagParams = (command) => {
  const commandImpl = toImpl(command);
  const singles = commandImpl.config.flags.flatMap(extractSingleParams);
  return singles.filter(isFlagParam);
};
var extractContextFlagParams = (command) => {
  const commandImpl = toImpl(command);
  const singles = commandImpl.contextConfig.flags.flatMap(extractSingleParams);
  return singles.filter(isFlagParam);
};
var resolveFromRegistries = (token, registries) => {
  for (const registry of registries) {
    const resolved = resolveFlag(token, registry);
    if (resolved !== undefined) {
      return resolved;
    }
  }
  return;
};
var preserveFlag = (remainder, cursor, token, resolved) => {
  remainder.push(token);
  const consumed = consumeFlagValueWithTokens(cursor, token, resolved.param, resolved.negated);
  remainder.push(...consumed.tokens);
};
var localFlagWouldPrecedeSubcommand = (token, remainingTokens, resolved, subIndex, registries) => {
  const cursor = makeCursor(remainingTokens);
  consumeFlagValueWithTokens(cursor, token, resolved.param, resolved.negated);
  for (let token = cursor.take();token; token = cursor.take()) {
    if (isFlagToken(token)) {
      const known = resolveFromRegistries(token, registries);
      if (known !== undefined) {
        consumeFlagValueWithTokens(cursor, token, known.param, known.negated);
      }
      continue;
    }
    if (token._tag === "Value") {
      return subIndex.has(token.value);
    }
  }
  return false;
};
var consumeGlobalFlags = (tokens, command, registry) => {
  const flagMap = createEmptyFlagMap(registry.params);
  const errors = [];
  const consumeLevel = (tokens, command, ignoredRegistries) => {
    const localRegistry = createFlagRegistry(extractFlagParams(command));
    const inheritedRegistry = createFlagRegistry(extractContextFlagParams(command));
    const subIndex = buildSubcommandIndex(command.subcommands);
    const cursor = makeCursor(tokens);
    const remainder = [];
    let awaitingFirstValue = true;
    for (let token = cursor.take();token; token = cursor.take()) {
      if (isFlagToken(token)) {
        const ignored = resolveFromRegistries(token, ignoredRegistries);
        if (ignored !== undefined) {
          preserveFlag(remainder, cursor, token, ignored);
          continue;
        }
        const inherited = resolveFlag(token, inheritedRegistry);
        if (inherited !== undefined) {
          preserveFlag(remainder, cursor, token, inherited);
          continue;
        }
        const local = resolveFlag(token, localRegistry);
        const global = resolveFlag(token, registry);
        if (local !== undefined) {
          if (global === undefined || !awaitingFirstValue || !localFlagWouldPrecedeSubcommand(token, cursor.rest(), local, subIndex, [localRegistry, inheritedRegistry, registry])) {
            preserveFlag(remainder, cursor, token, local);
            continue;
          }
        }
        if (global !== undefined) {
          const consumed = consumeFlagValueWithTokens(cursor, token, global.param, global.negated);
          if (consumed._tag === "Error") {
            errors.push(consumed.error);
            continue;
          }
          if (consumed.value !== undefined) {
            flagMap[global.param.name].push(consumed.value);
          }
          continue;
        }
        remainder.push(token);
        continue;
      }
      if (token._tag === "Value" && awaitingFirstValue) {
        const sub = subIndex.get(token.value);
        if (sub !== undefined) {
          remainder.push(token);
          remainder.push(...consumeLevel(cursor.rest(), sub, [...ignoredRegistries, inheritedRegistry]));
          return remainder;
        }
        awaitingFirstValue = false;
      }
      remainder.push(token);
    }
    return remainder;
  };
  return {
    flagMap,
    remainder: consumeLevel(tokens, command, []),
    errors
  };
};
var createUnrecognizedFlagError = (token, params, commandPath) => {
  const printable = token._tag === "LongOption" ? `--${token.name}` : `-${token.flag}`;
  const validNames = [];
  for (const p of params) {
    if (p.hidden)
      continue;
    validNames.push(p.name);
    const negation = negatedName2(p);
    if (negation !== undefined) {
      validNames.push(negation);
    }
    for (const alias of p.aliases) {
      validNames.push(alias);
    }
  }
  const suggestions = suggest(getFlagName(token), validNames).map((n) => n.length === 1 ? `-${n}` : `--${n}`);
  return new UnrecognizedOption({
    option: printable,
    suggestions,
    command: commandPath
  });
};
var createParseState = (registry) => ({
  flags: createFlagAccumulator(registry.params),
  arguments: [],
  errors: [],
  mode: {
    _tag: "AwaitingFirstValue"
  }
});
var toLeafResult = (state) => ({
  _tag: "Leaf",
  flags: state.flags.snapshot(),
  arguments: state.arguments,
  errors: state.errors
});
var resolveFirstValue = (value, cursor, context, state) => {
  const {
    command,
    commandPath,
    inheritedFlagRegistry,
    localFlagNames
  } = context;
  const subIndex = buildSubcommandIndex(command.subcommands);
  const sub = subIndex.get(value);
  if (sub) {
    const selectedPath = [...commandPath, sub.name];
    const parentFlags = state.flags.snapshot();
    for (const localFlagName of localFlagNames) {
      const values = parentFlags[localFlagName];
      if (values !== undefined && values.length > 0) {
        state.errors.push(new UnrecognizedOption({
          option: `--${localFlagName}`,
          suggestions: [],
          command: selectedPath
        }));
      }
    }
    const tail = consumeKnownFlags(cursor.rest(), inheritedFlagRegistry);
    state.flags.merge(tail.flagMap);
    state.errors.push(...tail.errors);
    return {
      _tag: "Subcommand",
      result: {
        _tag: "Sub",
        flags: state.flags.snapshot(),
        sub,
        childTokens: tail.remainder,
        errors: state.errors
      }
    };
  }
  const expectsArgs = toImpl(command).config.arguments.length > 0;
  if (!expectsArgs && subIndex.size > 0) {
    const visibleKeys = [];
    for (const [key, sub] of subIndex) {
      if (!sub.unlisted)
        visibleKeys.push(key);
    }
    const suggestions = suggest(value, visibleKeys);
    state.errors.push(new UnknownSubcommand({
      subcommand: value,
      parent: commandPath,
      suggestions
    }));
  }
  return {
    _tag: "Argument"
  };
};
var processFlag = (token, cursor, context, state) => {
  const {
    commandPath,
    flagRegistry
  } = context;
  const resolved = resolveFlag(token, flagRegistry);
  if (!resolved) {
    state.errors.push(createUnrecognizedFlagError(token, flagRegistry.params, commandPath));
    return;
  }
  const consumed = consumeFlagValue(cursor, token, resolved.param, resolved.negated);
  if (consumed._tag === "Error") {
    state.errors.push(consumed.error);
    return;
  }
  state.flags.add(resolved.param.name, consumed.value);
};
var processValue = (value, cursor, context, state) => {
  if (state.mode._tag === "AwaitingFirstValue") {
    const result = resolveFirstValue(value, cursor, context, state);
    if (result._tag === "Subcommand") {
      return result.result;
    }
    state.mode = {
      _tag: "CollectingArguments"
    };
  }
  state.arguments.push(value);
  return;
};
var scanCommandLevel = (tokens, context) => {
  const cursor = makeCursor(tokens);
  const state = createParseState(context.flagRegistry);
  for (let token = cursor.take();token; token = cursor.take()) {
    if (isFlagToken(token)) {
      processFlag(token, cursor, context, state);
      continue;
    }
    if (token._tag === "Value") {
      const subResult = processValue(token.value, cursor, context, state);
      if (subResult)
        return subResult;
    }
  }
  return toLeafResult(state);
};

// node_modules/effect/dist/cli/internal/wizard.js
var run4 = /* @__PURE__ */ fnUntraced2(function* (command, options) {
  const commandPath = options?.commandPath ?? [command.name];
  const selected = getCommandAtPath(command, commandPath);
  const commandLine = (options?.prefix ?? commandPath).map((value) => commandLineArg(value));
  const {
    colors
  } = yield* Theme;
  yield* logCurrentCommand(commandLine, colors);
  yield* promptCommand(selected, commandLine, selected === command ? "ROOT" : selected.name, colors);
  return {
    args: commandLine.map((arg) => arg.value),
    displayArgs: commandLine.map((arg) => arg.displayValue)
  };
});
var getCommandAtPath = (command, commandPath) => {
  let current = command;
  for (const name of commandPath.slice(1)) {
    const child = current.subcommands.flatMap((group) => group.commands).find((candidate) => candidate.name === name || candidate.alias === name);
    if (child === undefined) {
      break;
    }
    current = child;
  }
  return current;
};
var promptCommand = /* @__PURE__ */ fnUntraced2(function* (command, commandLine, sectionName, colors) {
  const impl = toImpl(command);
  const visibleSubcommands = command.subcommands.flatMap((group) => group.commands.filter((child) => !child.unlisted));
  const config = visibleSubcommands.length === 0 ? impl.config : impl.contextConfig;
  if (config.flags.length > 0) {
    yield* log2(renderSection(sectionName, "FLAGS", colors));
    for (const param of config.flags) {
      commandLine.push(...yield* promptParam(param));
    }
    if (config.arguments.length > 0 || visibleSubcommands.length > 0) {
      yield* logCurrentCommand(commandLine, colors);
    }
  }
  if (config.arguments.length > 0) {
    yield* log2(renderSection(sectionName, "ARGUMENTS", colors));
    for (const param of config.arguments) {
      commandLine.push(...yield* promptParam(param));
    }
    if (visibleSubcommands.length > 0) {
      yield* logCurrentCommand(commandLine, colors);
    }
  }
  if (visibleSubcommands.length === 0) {
    return;
  }
  const child = yield* run3(Select({
    message: "Command",
    choices: visibleSubcommands.map((command) => ({
      title: command.name,
      value: command,
      ...command.shortDescription !== undefined ? {
        description: command.shortDescription
      } : command.description !== undefined ? {
        description: command.description
      } : {}
    }))
  }));
  yield* log2();
  commandLine.push(commandLineArg(child.name));
  if (hasWizardSteps(child)) {
    yield* logCurrentCommand(commandLine, colors);
  }
  yield* promptCommand(child, commandLine, child.name, colors);
});
var hasWizardSteps = (command) => {
  const hasVisibleSubcommands = command.subcommands.some((group) => group.commands.some((child) => !child.unlisted));
  return hasVisibleSubcommands || toImpl(command).config.orderedParams.length > 0;
};
var promptParam = /* @__PURE__ */ fnUntraced2(function* (param) {
  const single = getUnderlyingSingleOrThrow(param);
  const metadata = getParamMetadata(param);
  if (metadata.isOptional) {
    const include = yield* run3(Confirm({
      message: `Set ${renderParamLabel(single)}?`,
      initial: false
    }));
    if (!include) {
      yield* log2();
      return [];
    }
  }
  const count = !metadata.isVariadic ? 1 : yield* run3(Int3({
    message: `${renderParamLabel(single)} count`,
    default: getOrElse(metadata.variadicMin, () => 0),
    min: getOrElse(metadata.variadicMin, () => 0),
    ...isSome2(metadata.variadicMax) ? {
      max: metadata.variadicMax.value
    } : {}
  }));
  const values = [];
  for (let i = 0;i < count; i++) {
    values.push(yield* promptSingle(single));
  }
  const parsed = single.kind === flagKind ? {
    flags: {
      [single.name]: values.map((arg) => arg.value)
    },
    arguments: []
  } : {
    flags: {},
    arguments: values.map((arg) => arg.value)
  };
  yield* param.parse(parsed);
  yield* log2();
  if (single.kind === argumentKind) {
    return values;
  }
  return values.flatMap((value) => single.optionalValue !== undefined || value.value.startsWith("-") && value.value.length > 1 ? [commandLineArg(`--${single.name}=${value.value}`, `--${single.name}=${value.displayValue}`)] : [commandLineArg(`--${single.name}`), value]);
});
var promptSingle = (single) => {
  const message = renderParamMessage(single);
  switch (single.primitiveType._tag) {
    case "Boolean":
      return map7(run3(Confirm({
        message,
        label: {
          confirm: "true",
          deny: "false"
        },
        placeholder: {
          defaultConfirm: "(T/f)",
          defaultDeny: "(t/F)"
        }
      })), (value) => commandLineArg(String(value)));
    case "Choice": {
      const choices = getChoiceKeys(single.primitiveType) ?? [];
      return map7(run3(Select({
        message,
        choices: choices.map((choice) => ({
          title: choice,
          value: choice
        }))
      })), commandLineArg);
    }
    case "Date":
      return map7(run3(Date4({
        message
      })), (date) => commandLineArg(date.toISOString()));
    case "Finite":
      return map7(run3(Number7({
        message
      })), (value) => commandLineArg(String(value)));
    case "Int":
      return map7(run3(Int3({
        message
      })), (value) => commandLineArg(String(value)));
    case "Redacted":
      return map7(run3(Password({
        message
      })), (value) => commandLineArg(value2(value), "<redacted>"));
    default:
      return map7(run3(String8({
        message
      })), commandLineArg);
  }
};
var commandLineArg = (value, displayValue = value) => ({
  value,
  displayValue
});
var formatName = (single) => single.kind === flagKind ? `--${single.name}` : single.name;
var renderParamMessage = (single) => renderParamLabel(single);
var renderParamLabel = (single) => {
  const description = getOrUndefined(single.description)?.trim();
  const label = single.kind === flagKind && description !== undefined && description.length <= 32 ? description : humanize(single.name);
  return single.kind === flagKind ? `${label} (${formatName(single)})` : label;
};
var humanize = (name) => {
  const words = name.split(/[-_]+/).filter((word) => word.length > 0);
  if (words.length === 0)
    return name;
  return [words[0][0].toUpperCase() + words[0].slice(1), ...words.slice(1)].join(" ");
};
var logCurrentCommand = (commandLine, colors) => log2(renderCommandBlock("Current command", commandLine.map((arg) => arg.displayValue), colors, magenta));
var renderSection = (commandName, section, colors) => `${annotate2(colors, commandName.toUpperCase(), bold, cyanBright)} ${annotate2(colors, "·", blackBright)} ${annotate2(colors, section, bold, white)}`;
var renderIntroduction = (name, version, summary, colors) => {
  const title = `${annotate2(colors, name, bold, cyanBright)} ${annotate2(colors, `v${version}`, white)} ${annotate2(colors, "· Command wizard", bold, white)}`;
  return [title, ...summary === undefined || summary.length === 0 ? [] : [summary], annotate2(colors, "Build a command interactively. Press Ctrl+C to cancel.", blackBright), ""].join(`
`);
};
var renderCompletion = (commandLine, colors) => renderCommandBlock("Command ready", commandLine, colors, cyanBright, green);
var renderQuit = (colors) => `
${annotate2(colors, "Wizard cancelled.", red)}`;
var renderCommandBlock = (label, commandLine, colors, commandColor, labelColor = white) => {
  const lines = wrapCommand(commandLine);
  return [annotate2(colors, label, bold, labelColor), ...lines.map((line) => annotate2(colors, line, commandColor)), ""].join(`
`);
};
var wrapCommand = (commandLine) => {
  const width = 88;
  const firstIndent = "  $ ";
  const continuationIndent = "    ";
  const args = commandLine.map(formatShellArg);
  const lines = [];
  let current = firstIndent;
  for (const arg of args) {
    const separator = current === firstIndent || current === continuationIndent ? "" : " ";
    if (current.length + separator.length + arg.length > width && current !== firstIndent) {
      lines.push(`${current} \\`);
      current = `${continuationIndent}${arg}`;
    } else {
      current += `${separator}${arg}`;
    }
  }
  if (current !== firstIndent) {
    lines.push(current);
  }
  return lines;
};
var formatShellArg = (arg) => /^[A-Za-z0-9_./:@%+=,-]+$/.test(arg) ? arg : `'${arg.replaceAll("'", `'"'"'`)}'`;

// node_modules/effect/dist/cli/Command.js
var isCommand = (u) => hasProperty(u, TypeId49);
var make48 = (name, config, handler) => {
  const parsedConfig = parseConfig(config ?? {});
  return makeCommand({
    name,
    config: parsedConfig,
    ...isNotUndefined(handler) ? {
      handle: handler
    } : {}
  });
};
var normalizeSubcommandEntries = (entries) => {
  const flat = [];
  const grouped = new Map;
  const addToGroup = (group, command) => {
    flat.push(command);
    const existing = grouped.get(group);
    if (existing) {
      existing.push(command);
    } else {
      grouped.set(group, [command]);
    }
  };
  for (const entry of entries) {
    if (isCommand(entry)) {
      addToGroup(undefined, entry);
      continue;
    }
    for (const command of entry.commands) {
      addToGroup(entry.group, command);
    }
  }
  const groups = [];
  const ungroupedCommands = grouped.get(undefined);
  if (ungroupedCommands && ungroupedCommands.length > 0) {
    groups.push({
      group: undefined,
      commands: ungroupedCommands
    });
  }
  for (const [group, commands] of grouped) {
    if (group === undefined) {
      continue;
    }
    groups.push({
      group,
      commands
    });
  }
  return {
    flat,
    groups
  };
};
var withSubcommands = /* @__PURE__ */ dual(2, (self, subcommands) => {
  const normalized = normalizeSubcommandEntries(subcommands);
  checkForDuplicateFlags(self, normalized.flat);
  const impl = toImpl(self);
  const byName = new Map(normalized.flat.map((s) => [s.name, toImpl(s)]));
  const SubcommandStateSymbol = Symbol("effect/cli/SubcommandState");
  const parse = fnUntraced2(function* (raw) {
    if (isNone2(raw.subcommand)) {
      return yield* impl.parse(raw);
    }
    const sub = byName.get(raw.subcommand.value.name);
    if (!sub) {
      return yield* impl.parse(raw);
    }
    const context = yield* impl.parseContext(raw);
    const result = yield* sub.parse(raw.subcommand.value.parsedInput);
    return {
      ...context,
      [SubcommandStateSymbol]: {
        name: sub.name,
        result
      }
    };
  });
  const handle = fnUntraced2(function* (input, path) {
    const internal = input;
    const selectedSubcommand = internal[SubcommandStateSymbol];
    if (selectedSubcommand) {
      const child = byName.get(selectedSubcommand.name);
      if (!child) {
        return yield* new ShowHelp({
          commandPath: path,
          errors: []
        });
      }
      return yield* child.handle(selectedSubcommand.result, [...path, child.name]).pipe(provideService2(impl.service, input));
    }
    return yield* impl.handle(input, path);
  });
  return makeCommand({
    name: impl.name,
    config: impl.config,
    contextConfig: impl.contextConfig,
    description: impl.description,
    shortDescription: impl.shortDescription,
    alias: impl.alias,
    unlisted: impl.unlisted,
    annotations: impl.annotations,
    globalFlags: impl.globalFlags,
    resolveExamples: impl.resolveExamples,
    service: impl.service,
    subcommands: normalized.groups,
    parse,
    parseContext: impl.parseContext,
    handle
  });
});
var withDescription4 = /* @__PURE__ */ dual(2, (self, description) => makeCommand({
  ...toImpl(self),
  description
}));
var getOutOfScopeGlobalFlagErrors = (allFlags, activeFlags, flagMap, commandPath) => {
  const activeSet = new Set(activeFlags);
  const errors = [];
  const seen = new Set;
  for (const flag of allFlags) {
    if (activeSet.has(flag)) {
      continue;
    }
    const singles = extractSingleParams(flag.flag);
    for (const single of singles) {
      const entries = flagMap[single.name];
      if (!entries || entries.length === 0) {
        continue;
      }
      const option = `--${single.name}`;
      if (seen.has(option)) {
        continue;
      }
      seen.add(option);
      errors.push(new UnrecognizedOption({
        option,
        suggestions: [],
        command: commandPath
      }));
    }
  }
  return errors;
};
var showHelp = (command, error, renderErrors) => gen2(function* () {
  const {
    builtIns
  } = yield* CliConfig;
  const presenter = yield* Presenter;
  const {
    commandPath,
    errors
  } = error;
  const helpDoc = yield* getHelpForCommandPath(command, commandPath, builtIns);
  yield* presenter.present(renderErrors && errors.length > 0 ? {
    _tag: "InvalidInvocation",
    commandPath,
    helpDoc,
    errors
  } : {
    _tag: "Help",
    reason: "Implicit",
    commandPath,
    helpDoc
  });
});
var showUserError = (error2, userMessage) => gen2(function* () {
  const formatter = yield* resolveFormatter;
  const rendered = error2 instanceof UserError ? error2 : new UserError({
    cause: error2,
    userMessage
  });
  yield* error(formatter.formatError(rendered));
  Object.assign(error2, {
    [errorReported]: false
  });
});
var run5 = /* @__PURE__ */ dual(2, (command, config) => Stdio.use(({
  args
}) => flatMap4(args, (args) => runWith2(command, config)(args))));
var runWith2 = (command, config) => {
  const commandImpl = toImpl(command);
  return fnUntraced2(function* (args) {
    const {
      builtIns
    } = yield* CliConfig;
    const {
      tokens,
      trailingOperands
    } = lex(args);
    const allFlags = getGlobalFlagsForCommandTree(command, builtIns);
    const allFlagParams = allFlags.flatMap((f) => extractSingleParams(f.flag));
    const globalRegistry = createFlagRegistry(allFlagParams.filter(isFlagParam));
    const {
      flagMap,
      remainder,
      errors: globalFlagErrors
    } = consumeGlobalFlags(tokens, command, globalRegistry);
    const emptyArgs = {
      flags: flagMap,
      arguments: []
    };
    const parsedArgs = yield* parseArgs({
      tokens: remainder,
      trailingOperands
    }, command);
    const commandPath = [command.name, ...getCommandPath(parsedArgs)];
    const toShowHelp = mapError3((error) => error._tag === "ShowHelp" ? error : new ShowHelp({
      commandPath,
      errors: [error]
    }));
    const handlerCtx = {
      builtIns,
      command,
      commandPath,
      version: config.version
    };
    const activeFlags = getGlobalFlagsForCommandPath(command, commandPath, builtIns);
    const outOfScopeErrors = getOutOfScopeGlobalFlagErrors(allFlags, activeFlags, flagMap, commandPath);
    if (outOfScopeErrors.length > 0 || globalFlagErrors.length > 0) {
      const parseErrors = parsedArgs.errors ?? [];
      return yield* new ShowHelp({
        commandPath,
        errors: [...globalFlagErrors, ...outOfScopeErrors, ...parseErrors]
      });
    }
    for (const flag of activeFlags) {
      if (flag._tag !== "Action")
        continue;
      const singles = extractSingleParams(flag.flag);
      const hasEntry = singles.some((s) => {
        const entries = flagMap[s.name];
        return entries !== undefined && entries.length > 0;
      });
      if (!hasEntry)
        continue;
      const [, value] = yield* flag.flag.parse(emptyArgs).pipe(toShowHelp);
      if (flag === Wizard) {
        const {
          colors
        } = yield* Theme;
        return yield* gen2(function* () {
          yield* log2(renderIntroduction(command.name, config.version, command.description, colors));
          const prefix = [command.name, ...args.filter((arg) => arg !== "--wizard" && !arg.startsWith("--wizard="))];
          const wizardResult = yield* run4(command, {
            commandPath,
            prefix
          });
          yield* log2(renderCompletion(wizardResult.displayArgs, colors));
          const shouldRun = yield* run3(Toggle({
            message: "Run this command?",
            initial: true,
            active: "yes",
            inactive: "no"
          }));
          if (shouldRun) {
            yield* log2();
            yield* runWith2(command, {
              ...config,
              renderErrors: false
            })(wizardResult.args.slice(1));
          }
        }).pipe(catchTag2("QuitError", () => log2(renderQuit(colors))));
      }
      yield* flag.run(value, handlerCtx);
      return;
    }
    if (parsedArgs.errors && parsedArgs.errors.length > 0) {
      return yield* new ShowHelp({
        commandPath,
        errors: parsedArgs.errors
      });
    }
    const input = yield* commandImpl.parse(parsedArgs).pipe(toShowHelp);
    let program = commandImpl.handle(input, [command.name]);
    const logLevel = activeFlags.includes(LogLevel) ? (yield* LogLevel.flag.parse(emptyArgs).pipe(toShowHelp))[1] : none2();
    program = provideService2(program, LogLevel, logLevel);
    for (const flag of activeFlags) {
      if (flag._tag !== "Setting" || flag === LogLevel)
        continue;
      const [, value] = yield* flag.flag.parse(emptyArgs).pipe(toShowHelp);
      program = provideService2(program, flag, value);
    }
    const services = match(logLevel, {
      onNone: () => empty(),
      onSome: (level) => make3(MinimumLogLevel2, level)
    });
    yield* provideContext2(program, services);
  }, catchFilter2((error) => isCliError(error) && error._tag === "ShowHelp" ? succeed2(error) : fail2(error), (error) => andThen2(showHelp(command, error, config.renderErrors !== false), fail7(error))), catchFilter2((error) => {
    if (config.renderErrors === false || !isObjectKeyword(error))
      return fail2(error);
    const userMessage = getUserMessage(error);
    return userMessage === undefined ? fail2(error) : succeed2({
      error,
      userMessage
    });
  }, ({
    error,
    userMessage
  }) => andThen2(showUserError(error, userMessage), fail7(error))), catchFilter2((e) => isQuitError(e) ? succeed2(e) : fail2(e), (_) => interrupt2));
};
// src/domain/credentials.ts
var REFRESH_WINDOW_MS = 5 * 60 * 1000;
var MS_PER_MINUTE = 60000;
var MINUTES_PER_DAY = 1440;
var MINUTES_PER_HOUR = 60;
var CredentialsSchema = Struct({
  accessToken: String5,
  clientId: String5,
  expiresAt: Finite,
  redirectUri: String5,
  refreshToken: String5
});
var TokenResponseSchema = Struct({
  access_token: String5,
  expires_in: Finite,
  id_token: optionalKey2(String5),
  refresh_token: optionalKey2(String5)
});
var RegistrationResponseSchema = Struct({
  client_id: String5
});
var IdTokenClaimsSchema = Struct({
  email: optionalKey2(String5)
});
var UserInfoSchema = Struct({
  email: optionalKey2(String5)
});
var needsRefresh = (expiresAt, nowMs) => nowMs >= expiresAt - REFRESH_WINDOW_MS;
var spanLabel = (minutes) => {
  const days = Math.floor(minutes / MINUTES_PER_DAY);
  const hours = Math.floor(minutes % MINUTES_PER_DAY / MINUTES_PER_HOUR);
  if (days > 0) {
    return `${days}d ${hours}h`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes % MINUTES_PER_HOUR}m`;
  }
  return `${minutes}m`;
};
var describeExpiry = (expiresAt, nowMs) => {
  const remainingMs = expiresAt - nowMs;
  const span = spanLabel(Math.floor(Math.abs(remainingMs) / MS_PER_MINUTE));
  return {
    expiresAt: formatIso2(makeUnsafe6(expiresAt)),
    expiresIn: remainingMs >= 0 ? span : `expired ${span} ago`
  };
};
var credentialsFromToken = (previous, token, nowMs) => {
  const refreshToken = token.refresh_token ?? previous.refreshToken;
  return refreshToken === undefined ? none2() : some2({
    accessToken: token.access_token,
    clientId: previous.clientId,
    expiresAt: nowMs + token.expires_in * 1000,
    redirectUri: previous.redirectUri,
    refreshToken
  });
};
var emailFromIdToken = (idToken) => {
  const [, payload] = idToken.split(".");
  if (payload === undefined) {
    return none2();
  }
  return getSuccess2(decodeString(payload)).pipe(flatMap((json) => getSuccess2(decodeUnknownResult2(fromJsonString2(IdTokenClaimsSchema))(json))), flatMapNullishOr((claims) => claims.email));
};

// src/domain/mcp.ts
var MCP_URL = "https://mcp.wonder.so/mcp";
var PROTOCOL_VERSION = "2025-06-18";
var TextContentSchema = Struct({
  text: String5,
  type: Literal2("text")
});
var ImageContentSchema = Struct({
  data: String5,
  mimeType: String5,
  type: Literal2("image")
});
var OtherContentSchema = Struct({ type: String5 });
var ToolResultSchema = Struct({
  content: ArraySchema(Union2([TextContentSchema, ImageContentSchema, OtherContentSchema])),
  isError: optionalKey2(Boolean3),
  structuredContent: optionalKey2(Json2)
});
var ToolDefSchema = Struct({
  annotations: optionalKey2(Struct({ readOnlyHint: optionalKey2(Boolean3) })),
  description: optionalKey2(String5),
  inputSchema: optionalKey2(Record(String5, Unknown2)),
  name: String5
});
var ToolsPageSchema = Struct({
  nextCursor: optionalKey2(String5),
  tools: ArraySchema(ToolDefSchema)
});
var RpcMessageSchema = Struct({
  error: optionalKey2(Struct({ code: Finite, message: String5 })),
  id: optionalKey2(Union2([Finite, String5])),
  result: optionalKey2(Json2)
});
var decodeRpcMessage = decodeUnknownResult2(fromJsonString2(RpcMessageSchema));
var sseDataPayloads = (body) => body.replaceAll(`\r
`, `
`).split(`

`).map((event) => event.split(`
`).filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trimStart()).join(`
`)).filter((payload) => payload.length > 0);
var parseRpcResponse = (body, requestId) => {
  const payloads = body.trimStart().startsWith("{") ? [body] : sseDataPayloads(body);
  return findFirst2(filterMap(payloads, (payload) => decodeRpcMessage(payload)), (message) => message.id === requestId);
};
var splitContent = (result) => ({
  images: filterMap(result.content, (item) => ("data" in item) && item.type === "image" ? succeed2({ data: item.data, mimeType: item.mimeType }) : failVoid),
  texts: filterMap(result.content, (item) => ("text" in item) && item.type === "text" ? succeed2(item.text) : failVoid)
});

// src/domain/oauth.ts
var AUTH_BASE = "https://oauth.wonder.so";
var REGISTER_URL = `${AUTH_BASE}/oauth2/register`;
var AUTHORIZE_URL = `${AUTH_BASE}/oauth2/authorize`;
var TOKEN_URL = `${AUTH_BASE}/oauth2/token`;
var USERINFO_URL = `${AUTH_BASE}/oauth2/userinfo`;
var SCOPE = "openid email profile offline_access";
var DEFAULT_PORT = 53682;
var DEFAULT_LOGIN_TIMEOUT_SECONDS = 600;
var redirectUriFor = (port) => `http://127.0.0.1:${port}/callback`;
var authorizeUrl = (parameters) => {
  const url = new URL(AUTHORIZE_URL);
  url.search = new URLSearchParams({
    client_id: parameters.clientId,
    code_challenge: parameters.codeChallenge,
    code_challenge_method: "S256",
    redirect_uri: parameters.redirectUri,
    resource: MCP_URL,
    response_type: "code",
    scope: SCOPE,
    state: parameters.state
  }).toString();
  return url.toString();
};
var Callback = taggedEnum();
var interpretCallback = (requestUrl, expectedState) => {
  const url = new URL(requestUrl, "http://127.0.0.1");
  if (url.pathname !== "/callback") {
    return Callback.Ignored({ path: url.pathname });
  }
  const error = fromNullishOr(url.searchParams.get("error"));
  if (isSome2(error)) {
    return Callback.Denied({
      reason: url.searchParams.get("error_description") ?? error.value
    });
  }
  const received = url.searchParams.get("state");
  if (received !== expectedState) {
    return Callback.StateMismatch({ received });
  }
  const code = url.searchParams.get("code");
  return code === null ? Callback.Ignored({ path: url.pathname }) : Callback.Code({ code });
};

// node_modules/@toon-format/toon/dist/index.mjs
var NULL_LITERAL = "null";
var DELIMITERS = {
  comma: ",",
  tab: "\t",
  pipe: "|"
};
var DEFAULT_DELIMITER = DELIMITERS.comma;
function escapeString(value) {
  return value.replace(/\\/g, `\\\\`).replace(/"/g, `\\"`).replace(/\n/g, `\\n`).replace(/\r/g, `\\r`).replace(/\t/g, `\\t`).replace(/[\u0000-\u001F]/g, (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`);
}
var FETCH_LINE = Symbol("fetch-line");
function isBooleanOrNullLiteral(token) {
  return token === "true" || token === "false" || token === "null";
}
function setOwnProperty(target, key, value) {
  if (key === "__proto__") {
    Object.defineProperty(target, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true
    });
    return;
  }
  target[key] = value;
}
var COMMENT_LINE_PATTERN = new RegExp(`(?:^\uFEFF?|\\n) *#`);
var RawString = class {
  constructor(value) {
    if (COMMENT_LINE_PATTERN.test(value))
      throw new TypeError(`Raw string must not contain a line starting with "#": ${JSON.stringify(value)}`);
    this.value = value;
  }
};
function isRawString(value) {
  return value instanceof RawString;
}
var SURROGATE_PATTERN = /[\uD800-\uDFFF]/;
function normalizeValue(value) {
  if (value === null)
    return null;
  if (isRawString(value))
    return value;
  if (typeof value === "object" && value !== null && "toJSON" in value && typeof value.toJSON === "function") {
    const next = value.toJSON();
    if (next !== value)
      return normalizeValue(next);
  }
  if (typeof value === "string") {
    assertNoLoneSurrogate(value, "string value");
    return value;
  }
  if (typeof value === "boolean")
    return value;
  if (typeof value === "number") {
    if (Object.is(value, -0))
      return 0;
    if (!Number.isFinite(value))
      return null;
    return value;
  }
  if (typeof value === "bigint") {
    if (value >= Number.MIN_SAFE_INTEGER && value <= Number.MAX_SAFE_INTEGER)
      return Number(value);
    return value.toString();
  }
  if (value instanceof Date)
    return value.toISOString();
  if (Array.isArray(value))
    return Array.from(value, normalizeValue);
  if (value instanceof Set)
    return Array.from(value).map(normalizeValue);
  if (value instanceof Map)
    return Object.fromEntries(Array.from(value, ([k, v]) => [String(k), normalizeValue(v)]));
  if (isPlainObject(value)) {
    const encodedValues = {};
    for (const key in value)
      if (Object.hasOwn(value, key)) {
        assertNoLoneSurrogate(key, "object key");
        setOwnProperty(encodedValues, key, normalizeValue(value[key]));
      }
    return encodedValues;
  }
  return null;
}
function assertNoLoneSurrogate(value, context) {
  if (!SURROGATE_PATTERN.test(value))
    return;
  for (let index = 0;index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code < 55296 || code > 57343)
      continue;
    const isHighSurrogate = code <= 56319;
    const next = value.charCodeAt(index + 1);
    if (isHighSurrogate && next >= 56320 && next <= 57343) {
      index++;
      continue;
    }
    throw new TypeError(`Cannot encode ${context} containing an unpaired surrogate U+${code.toString(16).toUpperCase()} at index ${index}`);
  }
}
function isJsonPrimitive(value) {
  return value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean";
}
function isEncodablePrimitive(value) {
  return isJsonPrimitive(value) || isRawString(value);
}
function isJsonArray(value) {
  return Array.isArray(value);
}
function isJsonObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value) && !isRawString(value);
}
function isEmptyObject(value) {
  return Object.keys(value).length === 0;
}
function isPlainObject(value) {
  if (value === null || typeof value !== "object")
    return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === null || prototype === Object.prototype;
}
function isArrayOfPrimitives(value) {
  return value.length === 0 || value.every((item) => isEncodablePrimitive(item));
}
function isArrayOfArrays(value) {
  return value.length === 0 || value.every((item) => isJsonArray(item));
}
function isArrayOfObjects(value) {
  return value.length === 0 || value.every((item) => isJsonObject(item));
}
var NUMERIC_LIKE_PATTERN = /^[+-]?\d+(?:\.\d+)?(?:e[+-]?\d+)?$/i;
function assertValidDelimiter(delimiter) {
  if (!Object.values(DELIMITERS).includes(delimiter))
    throw new TypeError(`Invalid delimiter ${JSON.stringify(delimiter)}. Valid delimiters are: comma (,), tab (\\t), pipe (|)`);
}
function isValidUnquotedKey(key) {
  return /^[A-Z_][\w.]*$/i.test(key);
}
function isSafeUnquoted(value, delimiter = DEFAULT_DELIMITER) {
  if (!value)
    return false;
  if (/^[ \t]|[ \t]$/.test(value))
    return false;
  if (isBooleanOrNullLiteral(value) || isNumericLike(value))
    return false;
  if (value.includes(":"))
    return false;
  if (value.includes('"') || value.includes("\\"))
    return false;
  if (/[[\]{}]/.test(value))
    return false;
  if (/[\u0000-\u001F]/.test(value))
    return false;
  if (value.includes(delimiter))
    return false;
  if (value.startsWith("-"))
    return false;
  if (value.startsWith("#"))
    return false;
  return true;
}
function isNumericLike(value) {
  return NUMERIC_LIKE_PATTERN.test(value);
}
function encodePrimitive(value, delimiter) {
  if (isRawString(value))
    return value.value;
  if (value === null)
    return NULL_LITERAL;
  if (typeof value === "boolean")
    return String(value);
  if (typeof value === "number")
    return String(value);
  return encodeStringLiteral(value, delimiter);
}
function encodeStringLiteral(value, delimiter = DEFAULT_DELIMITER) {
  if (isSafeUnquoted(value, delimiter))
    return value;
  return `"${escapeString(value)}"`;
}
function encodeKey(key) {
  if (isValidUnquotedKey(key))
    return key;
  return `"${escapeString(key)}"`;
}
function encodeAndJoinPrimitives(values, delimiter = DEFAULT_DELIMITER) {
  return values.map((v) => encodePrimitive(v, delimiter)).join(delimiter);
}
function formatHeader(length, options) {
  const key = options?.key;
  const fields = options?.fields;
  const delimiter = options?.delimiter ?? ",";
  let header = "";
  if (key != null)
    header += encodeKey(key);
  header += `[${length}${options?.keyed ? ":" : ""}${delimiter !== DEFAULT_DELIMITER ? delimiter : ""}]`;
  if (fields)
    header += `{${formatFieldSegment(fields, delimiter)}}`;
  header += ":";
  return header;
}
function formatFieldSegment(fields, delimiter) {
  return fields.map((field) => encodeKey(field.name) + (field.children ? `{${formatFieldSegment(field.children, delimiter)}}` : "")).join(delimiter);
}
function extractTabularFields(rows) {
  if (rows.length === 0)
    return;
  const firstKeys = Object.keys(rows[0]);
  if (firstKeys.length === 0)
    return;
  for (const row of rows) {
    if (Object.keys(row).length !== firstKeys.length)
      return;
    for (const key of firstKeys)
      if (!Object.hasOwn(row, key))
        return;
  }
  const fieldNodes = [];
  for (const key of firstKeys) {
    const fieldNode = classifyColumn(key, rows.map((row) => row[key]));
    if (!fieldNode)
      return;
    fieldNodes.push(fieldNode);
  }
  return fieldNodes;
}
function extractKeyedTabularFields(value) {
  const entryValues = Object.values(value);
  if (entryValues.length < 2)
    return;
  if (!entryValues.every((entryValue) => isJsonObject(entryValue) && !isEmptyObject(entryValue)))
    return;
  return extractTabularFields(entryValues);
}
function collectRowLeaves(row, fields) {
  const leaves = [];
  collectLeafValues(row, fields, leaves);
  return leaves;
}
function classifyColumn(name, values) {
  if (values.every((value) => isEncodablePrimitive(value)))
    return { name };
  if (!values.every((value) => isJsonObject(value) && !isEmptyObject(value)))
    return;
  const children = extractTabularFields(values);
  if (!children)
    return;
  return {
    name,
    children
  };
}
function collectLeafValues(row, fields, leaves) {
  for (const field of fields) {
    const value = row[field.name];
    if (field.children)
      collectLeafValues(value, field.children, leaves);
    else
      leaves.push(value);
  }
}
function* encodeJsonValue(value, options, depth) {
  if (isEncodablePrimitive(value)) {
    const encodedPrimitive = typeof value === "string" && value.startsWith("\uFEFF") ? `"${escapeString(value)}"` : encodePrimitive(value, options.delimiter);
    if (encodedPrimitive !== "")
      yield encodedPrimitive;
    return;
  }
  if (isJsonArray(value))
    yield* encodeArrayLines(undefined, value, depth, options);
  else if (isJsonObject(value)) {
    const keyedFields = extractKeyedTabularFields(value);
    if (keyedFields) {
      yield* encodeKeyedObjectLines(undefined, value, keyedFields, depth, options);
      return;
    }
    yield* encodeObjectLines(value, depth, options);
  }
}
function* encodeObjectLines(value, depth, options) {
  for (const [key, val] of Object.entries(value))
    yield* encodeKeyValuePairLines(key, val, depth, options);
}
function* encodeKeyValuePairLines(key, value, depth, options) {
  const encodedKey = encodeKey(key);
  if (isEncodablePrimitive(value))
    yield indentedLine(depth, `${encodedKey}: ${encodePrimitive(value, options.delimiter)}`, options.indentSize);
  else if (isJsonArray(value))
    yield* encodeArrayLines(key, value, depth, options);
  else if (isJsonObject(value)) {
    const keyedFields = extractKeyedTabularFields(value);
    if (keyedFields) {
      yield* encodeKeyedObjectLines(key, value, keyedFields, depth, options);
      return;
    }
    yield indentedLine(depth, `${encodedKey}:`, options.indentSize);
    if (!isEmptyObject(value))
      yield* encodeObjectLines(value, depth + 1, options);
  }
}
function* encodeKeyedObjectLines(key, value, fields, depth, options) {
  const entries = Object.entries(value);
  yield indentedLine(depth, formatHeader(entries.length, {
    key,
    fields,
    delimiter: options.delimiter,
    keyed: true
  }), options.indentSize);
  yield* encodeKeyedEntryRowsLines(entries, fields, depth + 1, options);
}
function* encodeKeyedEntryRowsLines(entries, fields, depth, options) {
  for (const [entryKey, entryValue] of entries) {
    const leaves = collectRowLeaves(entryValue, fields);
    yield indentedLine(depth, `${encodeKey(entryKey)}: ${encodeAndJoinPrimitives(leaves, options.delimiter)}`, options.indentSize);
  }
}
function* encodeArrayLines(key, value, depth, options) {
  if (value.length === 0) {
    yield indentedLine(depth, key != null ? `${encodeKey(key)}: []` : "[]", options.indentSize);
    return;
  }
  if (isArrayOfPrimitives(value)) {
    yield indentedLine(depth, encodeInlineArrayLine(value, options.delimiter, key), options.indentSize);
    return;
  }
  if (isArrayOfArrays(value)) {
    if (value.every((arr) => isArrayOfPrimitives(arr))) {
      yield* encodeArrayOfArraysAsListItemsLines(key, value, depth, options);
      return;
    }
  }
  if (isArrayOfObjects(value)) {
    const fields = extractTabularFields(value);
    if (fields)
      yield* encodeArrayOfObjectsAsTabularLines(key, value, fields, depth, options);
    else
      yield* encodeMixedArrayAsListItemsLines(key, value, depth, options);
    return;
  }
  yield* encodeMixedArrayAsListItemsLines(key, value, depth, options);
}
function* encodeArrayOfArraysAsListItemsLines(prefix, values, depth, options) {
  yield indentedLine(depth, formatHeader(values.length, {
    key: prefix,
    delimiter: options.delimiter
  }), options.indentSize);
  for (const arr of values)
    if (isArrayOfPrimitives(arr)) {
      const arrayLine = encodeInlineArrayLine(arr, options.delimiter);
      yield indentedListItem(depth + 1, arrayLine, options.indentSize);
    }
}
function encodeInlineArrayLine(values, delimiter, prefix) {
  const header = formatHeader(values.length, {
    key: prefix,
    delimiter
  });
  const joinedValue = encodeAndJoinPrimitives(values, delimiter);
  if (values.length === 0)
    return header;
  return `${header} ${joinedValue}`;
}
function* encodeArrayOfObjectsAsTabularLines(prefix, rows, fields, depth, options) {
  yield indentedLine(depth, formatHeader(rows.length, {
    key: prefix,
    fields,
    delimiter: options.delimiter
  }), options.indentSize);
  yield* writeTabularRowsLines(rows, fields, depth + 1, options);
}
function* writeTabularRowsLines(rows, fields, depth, options) {
  for (const row of rows)
    yield indentedLine(depth, encodeAndJoinPrimitives(collectRowLeaves(row, fields), options.delimiter), options.indentSize);
}
function* encodeMixedArrayAsListItemsLines(prefix, items, depth, options) {
  yield indentedLine(depth, formatHeader(items.length, {
    key: prefix,
    delimiter: options.delimiter
  }), options.indentSize);
  for (const item of items)
    yield* encodeListItemValueLines(item, depth + 1, options);
}
function* encodeObjectAsListItemLines(obj, depth, options) {
  if (isEmptyObject(obj)) {
    yield indentedLine(depth, "-", options.indentSize);
    return;
  }
  const entries = Object.entries(obj);
  const [firstKey, firstValue] = entries[0];
  const restEntries = entries.slice(1);
  if (isJsonArray(firstValue) && isArrayOfObjects(firstValue)) {
    const fields = extractTabularFields(firstValue);
    if (fields) {
      yield indentedListItem(depth, formatHeader(firstValue.length, {
        key: firstKey,
        fields,
        delimiter: options.delimiter
      }), options.indentSize);
      yield* writeTabularRowsLines(firstValue, fields, depth + 2, options);
      if (restEntries.length > 0)
        yield* encodeObjectLines(Object.fromEntries(restEntries), depth + 1, options);
      return;
    }
  }
  if (isJsonObject(firstValue)) {
    const keyedFields = extractKeyedTabularFields(firstValue);
    if (keyedFields) {
      const keyedEntries = Object.entries(firstValue);
      yield indentedListItem(depth, formatHeader(keyedEntries.length, {
        key: firstKey,
        fields: keyedFields,
        delimiter: options.delimiter,
        keyed: true
      }), options.indentSize);
      yield* encodeKeyedEntryRowsLines(keyedEntries, keyedFields, depth + 2, options);
      if (restEntries.length > 0)
        yield* encodeObjectLines(Object.fromEntries(restEntries), depth + 1, options);
      return;
    }
  }
  const encodedKey = encodeKey(firstKey);
  if (isEncodablePrimitive(firstValue))
    yield indentedListItem(depth, `${encodedKey}: ${encodePrimitive(firstValue, options.delimiter)}`, options.indentSize);
  else if (isJsonArray(firstValue)) {
    if (firstValue.length === 0)
      yield indentedListItem(depth, `${encodedKey}: []`, options.indentSize);
    else if (isArrayOfPrimitives(firstValue))
      yield indentedListItem(depth, `${encodedKey}${encodeInlineArrayLine(firstValue, options.delimiter)}`, options.indentSize);
    else {
      yield indentedListItem(depth, `${encodedKey}${formatHeader(firstValue.length, { delimiter: options.delimiter })}`, options.indentSize);
      for (const item of firstValue)
        yield* encodeListItemValueLines(item, depth + 2, options);
    }
  } else if (isJsonObject(firstValue)) {
    yield indentedListItem(depth, `${encodedKey}:`, options.indentSize);
    if (!isEmptyObject(firstValue))
      yield* encodeObjectLines(firstValue, depth + 2, options);
  }
  if (restEntries.length > 0)
    yield* encodeObjectLines(Object.fromEntries(restEntries), depth + 1, options);
}
function* encodeListItemValueLines(value, depth, options) {
  if (isEncodablePrimitive(value))
    yield indentedListItem(depth, encodePrimitive(value, options.delimiter), options.indentSize);
  else if (isJsonArray(value)) {
    if (isArrayOfPrimitives(value))
      yield indentedListItem(depth, encodeInlineArrayLine(value, options.delimiter), options.indentSize);
    else {
      yield indentedListItem(depth, formatHeader(value.length, { delimiter: options.delimiter }), options.indentSize);
      for (const item of value)
        yield* encodeListItemValueLines(item, depth + 1, options);
    }
  } else if (isJsonObject(value))
    yield* encodeObjectAsListItemLines(value, depth, options);
}
function indentedLine(depth, content, indentSize) {
  return " ".repeat(indentSize * depth) + content;
}
function indentedListItem(depth, content, indentSize) {
  return indentedLine(depth, "- " + content, indentSize);
}
function applyReplacer(root, replacer) {
  const replacedRoot = replacer("", root, []);
  if (replacedRoot === undefined)
    return transformChildren(root, replacer, []);
  return transformReplaced(root, replacedRoot, replacer, []);
}
function transformReplaced(original, replaced, replacer, path) {
  if (isRawString(replaced) && !isEncodablePrimitive(original))
    return transformChildren(original, replacer, path);
  return transformChildren(normalizeValue(replaced), replacer, path);
}
function transformChildren(value, replacer, path) {
  if (isJsonObject(value))
    return transformObject(value, replacer, path);
  if (isJsonArray(value))
    return transformArray(value, replacer, path);
  return value;
}
function transformObject(obj, replacer, path) {
  const result = {};
  for (const [key, value] of Object.entries(obj)) {
    const childPath = [...path, key];
    const replacedValue = replacer(key, value, childPath);
    if (replacedValue === undefined)
      continue;
    setOwnProperty(result, key, transformReplaced(value, replacedValue, replacer, childPath));
  }
  return result;
}
function transformArray(arr, replacer, path) {
  const result = [];
  for (let i = 0;i < arr.length; i++) {
    const value = arr[i];
    const childPath = [...path, i];
    const replacedValue = replacer(String(i), value, childPath);
    if (replacedValue === undefined)
      continue;
    result.push(transformReplaced(value, replacedValue, replacer, childPath));
  }
  return result;
}
function encode4(input, options) {
  return Array.from(encodeLines(input, options)).join(`
`);
}
function encodeLines(input, options) {
  const normalizedValue = normalizeValue(input);
  const resolvedOptions = resolveOptions(options);
  return encodeJsonValue(resolvedOptions.replacer ? applyReplacer(normalizedValue, resolvedOptions.replacer) : normalizedValue, resolvedOptions, 0);
}
function resolveOptions(options) {
  const delimiter = options?.delimiter ?? DEFAULT_DELIMITER;
  assertValidDelimiter(delimiter);
  return {
    indentSize: options?.indentSize ?? options?.indent ?? 2,
    delimiter,
    replacer: options?.replacer
  };
}

// src/domain/render.ts
var DEFAULT_LINE_CAP = 120;
var toon = (value) => encode4(value);
var withHints = (value, help) => help.length === 0 ? value : Object.fromEntries([...Object.entries(value), ["help", help]]);
var withHelp = (value, help) => toon(withHints(value, help));
var format4 = (value, help, json) => json ? JSON.stringify(withHints(value, help), null, 2) : withHelp(value, help);
var truncateLines = (text, cap) => {
  const lines = text.split(`
`);
  return lines.length <= cap ? { omittedLines: 0, text } : {
    omittedLines: lines.length - cap,
    text: lines.slice(0, cap).join(`
`)
  };
};
var capped = (text, cap, fullHint) => {
  const cut = truncateLines(text, cap);
  return cut.omittedLines === 0 ? { text, truncated: false } : {
    text: `${cut.text}
... ${cut.omittedLines} more lines (${text.length} chars total). ${fullHint}`,
    truncated: true
  };
};
var firstSentence = (description) => {
  const flat = description.replaceAll(/\s+/gu, " ").trim();
  const end = flat.search(/[.!](?:\s|$)/u);
  return end === -1 ? flat : flat.slice(0, end + 1);
};
var oneLine = (text, max) => {
  const flat = text.replaceAll(/\s+/gu, " ").trim();
  return flat.length <= max ? flat : `${flat.slice(0, max - 1)}…`;
};
var TreesReplySchema = Struct({
  trees: ArraySchema(String5)
});
var JsonReplySchema = fromJsonString2(Record(String5, Unknown2));
var unwrapTrees = (text) => {
  const reply = decodeUnknownOption2(JsonReplySchema)(text);
  const trees = reply.pipe(flatMap(decodeUnknownOption2(TreesReplySchema)));
  if (isNone2(trees) || isNone2(reply)) {
    return text;
  }
  const rest = Object.fromEntries(Object.entries(reply.value).filter(([key]) => key !== "trees"));
  const body = trees.value.trees.join(`
`);
  return Object.keys(rest).length === 0 ? body : `${body}
${JSON.stringify(rest)}`;
};

// src/domain/errors/auth-error.ts
class AuthError extends TaggedError3()("AuthError", {
  fix: String5,
  message: String5
}) {
  kind = "auth";
  [errorExitCode] = 1;
}

// src/services/callback-server.ts
import { createServer } from "node:http";

// src/domain/errors/usage-error.ts
class UsageError extends TaggedError3()("UsageError", {
  fix: String5,
  message: String5
}) {
  kind = "usage";
  [errorExitCode] = 2;
}

// src/services/callback-server.ts
var page = (message, status) => text2(`<!doctype html><meta charset="utf-8"><title>wonder-axi</title><body style="font-family:system-ui;max-width:32rem;margin:4rem auto"><h1>wonder-axi</h1><p>${message}</p></body>`, { contentType: "text/html", status });

class CallbackServer extends Service()("wonder-axi/CallbackServer") {
  static layer = succeed6(CallbackServer, CallbackServer.of({
    awaitCode: ({ onListening, port, state, timeoutSeconds }) => gen2(function* listen() {
      const result = yield* make6();
      const handler = gen2(function* respond() {
        const request = yield* HttpServerRequest;
        return yield* value4(interpretCallback(request.url, state)).pipe(tagsExhaustive2({
          Code: ({ code }) => succeed5(result, code).pipe(as2(page("Signed in. You can close this tab and return to your terminal.", 200))),
          Denied: ({ reason }) => fail6(result, new AuthError({
            fix: "Run `wonder-axi auth login` again and approve the request.",
            message: `Wonder denied the sign-in: ${reason}`
          })).pipe(as2(page(`Sign-in denied: ${reason}`, 200))),
          Ignored: () => succeed7(text2("Not found", { status: 404 })),
          StateMismatch: () => fail6(result, new AuthError({
            fix: "Run `wonder-axi auth login` again and open the newest URL only.",
            message: "The callback state did not match this login attempt."
          })).pipe(as2(page("State mismatch.", 400)))
        }));
      });
      yield* serveEffect(handler);
      yield* onListening;
      return yield* _await(result).pipe(timeoutOrElse2({
        duration: seconds(timeoutSeconds),
        orElse: () => fail7(new AuthError({
          fix: "Run `wonder-axi auth login` again and finish the browser step sooner.",
          message: `No sign-in callback arrived within ${timeoutSeconds}s.`
        }))
      }));
    }).pipe(scoped2, provide4(layer14(createServer, { host: "127.0.0.1", port })), catchTag2("ServeError", () => fail7(new UsageError({
      fix: "Pick a free port with `--port <n>`.",
      message: `Could not listen on 127.0.0.1:${port}.`
    }))))
  }));
}

// src/domain/errors/network-error.ts
class NetworkError extends TaggedError3()("NetworkError", {
  fix: String5,
  message: String5
}) {
  kind = "network";
  [errorExitCode] = 1;
}

// src/services/http-reply.ts
var networkError = (url) => (cause) => new NetworkError({
  fix: "Check your network connection and retry.",
  message: `Request to ${url} failed: ${cause.message}`
});
var send = (client, request, url) => gen2(function* exchange() {
  const response = yield* client.execute(request);
  const text = yield* response.text;
  return {
    contentType: response.headers["content-type"] ?? "",
    sessionId: response.headers["mcp-session-id"],
    status: response.status,
    text
  };
}).pipe(mapError3(networkError(url)));

// src/domain/errors/storage-error.ts
class StorageError extends TaggedError3()("StorageError", {
  fix: String5,
  message: String5
}) {
  kind = "storage";
  [errorExitCode] = 1;
}

// src/domain/state.ts
var SessionStateSchema = Struct({
  fileId: optionalKey2(String5),
  fileName: optionalKey2(String5),
  pageId: String5
});

// src/services/store.ts
var FILE_MODE = 384;
var DIR_MODE = 448;
var storageFailure = (action, file) => mapError3(() => new StorageError({
  fix: "Check that the path is readable and writable, or delete it and retry.",
  message: `Could not ${action} ${file}.`
}));

class Store extends Service()("wonder-axi/Store") {
  static layer = effect(Store, gen2(function* build() {
    const fs = yield* FileSystem;
    const path = yield* Path;
    const home = yield* String6("HOME");
    const configHome = yield* String6("XDG_CONFIG_HOME").pipe(withDefault3(path.join(home, ".config")));
    const stateHome = yield* String6("XDG_STATE_HOME").pipe(withDefault3(path.join(home, ".local", "state")));
    const credentialsPath = path.join(configHome, "wonder-axi", "credentials.json");
    const statePath = path.join(stateHome, "wonder-axi", "state.json");
    const readJson = (file, schema) => gen2(function* load() {
      if (!(yield* fs.exists(file))) {
        return none2();
      }
      const text = yield* fs.readFileString(file);
      const decoded = yield* decodeUnknownEffect2(fromJsonString2(schema))(text);
      return some2(decoded);
    }).pipe(storageFailure("read", file));
    const writeJson = (file, schema, value) => gen2(function* save() {
      yield* fs.makeDirectory(path.dirname(file), {
        mode: DIR_MODE,
        recursive: true
      });
      const text = yield* encodeEffect(fromJsonString2(schema))(value);
      yield* fs.writeFileString(file, `${text}
`, { mode: FILE_MODE });
      yield* fs.chmod(file, FILE_MODE);
    }).pipe(storageFailure("write", file));
    return Store.of({
      credentialsPath,
      deleteCredentials: gen2(function* remove() {
        if (!(yield* fs.exists(credentialsPath))) {
          return false;
        }
        yield* fs.remove(credentialsPath);
        return true;
      }).pipe(storageFailure("delete", credentialsPath)),
      readCredentials: readJson(credentialsPath, CredentialsSchema),
      readState: readJson(statePath, SessionStateSchema),
      writeCredentials: (credentials) => writeJson(credentialsPath, CredentialsSchema, credentials),
      writeState: (state) => writeJson(statePath, SessionStateSchema, state)
    });
  }).pipe(mapError3((cause) => new StorageError({
    fix: "Set HOME or XDG_CONFIG_HOME and XDG_STATE_HOME.",
    message: `Could not resolve config directories: ${cause.message}`
  }))));
}

// src/services/auth.ts
var PKCE_VERIFIER_BYTES = 32;
var STATE_BYTES = 16;
var notSignedIn = () => new AuthError({
  fix: "Run `wonder-axi auth login`, or set WONDER_TOKEN to a Wonder personal access token.",
  message: "Not signed in to Wonder."
});
var decodeBody = (schema, text) => decodeUnknownEffect2(fromJsonString2(schema))(text).pipe(mapError3(() => new AuthError({
  fix: "Retry; if it persists, run `wonder-axi auth login` again.",
  message: "Wonder's auth server returned an unexpected response."
})));

class Auth extends Service()("wonder-axi/Auth") {
  static layer = effect(Auth, gen2(function* build() {
    const client = yield* HttpClient;
    const store = yield* Store;
    const callback = yield* CallbackServer;
    const random = yield* SecureRandom;
    const digest = yield* Digest;
    const envToken = yield* String6("WONDER_TOKEN").pipe(option2, map7(filter((token) => token.length > 0)), orDie2);
    const postForm = (url, fields) => gen2(function* exchange() {
      const reply = yield* send(client, post(url).pipe(acceptJson, bodyUrlParams(fields)), url);
      if (reply.status < 200 || reply.status >= 300) {
        return yield* new AuthError({
          fix: "Run `wonder-axi auth login` to sign in again.",
          message: `Wonder's auth server answered ${reply.status}: ${reply.text.slice(0, 300)}`
        });
      }
      return reply.text;
    });
    const refresh = gen2(function* renew() {
      const credentials = yield* store.readCredentials;
      if (isNone2(credentials)) {
        return yield* notSignedIn();
      }
      const text = yield* postForm(TOKEN_URL, {
        client_id: credentials.value.clientId,
        grant_type: "refresh_token",
        refresh_token: credentials.value.refreshToken,
        resource: MCP_URL
      });
      const token = yield* decodeBody(TokenResponseSchema, text);
      const now = yield* currentTimeMillis2;
      const next = credentialsFromToken(credentials.value, token, now);
      if (isNone2(next)) {
        return yield* notSignedIn();
      }
      yield* store.writeCredentials(next.value);
      return next.value.accessToken;
    });
    const accessToken = gen2(function* current() {
      if (isSome2(envToken)) {
        return envToken.value;
      }
      const credentials = yield* store.readCredentials;
      if (isNone2(credentials)) {
        return yield* notSignedIn();
      }
      const now = yield* currentTimeMillis2;
      return needsRefresh(credentials.value.expiresAt, now) ? yield* refresh : credentials.value.accessToken;
    });
    const refreshAccessToken = isSome2(envToken) ? fail7(new AuthError({
      fix: "Create a new personal access token in Wonder and update WONDER_TOKEN.",
      message: "WONDER_TOKEN was rejected by Wonder."
    })) : refresh;
    const status = gen2(function* inspect() {
      if (isSome2(envToken)) {
        return {
          credentialsPath: store.credentialsPath,
          expiresAt: none2(),
          source: "env"
        };
      }
      const credentials = yield* store.readCredentials;
      if (isNone2(credentials)) {
        return yield* notSignedIn();
      }
      return {
        credentialsPath: store.credentialsPath,
        expiresAt: some2(credentials.value.expiresAt),
        source: "file"
      };
    });
    const registerClient = (redirectUri) => gen2(function* enroll() {
      const reply = yield* send(client, post(REGISTER_URL).pipe(acceptJson, bodyJsonUnsafe({
        client_name: "wonder-axi",
        grant_types: ["authorization_code", "refresh_token"],
        redirect_uris: [redirectUri],
        response_types: ["code"],
        scope: SCOPE,
        token_endpoint_auth_method: "none"
      })), REGISTER_URL);
      if (reply.status < 200 || reply.status >= 300) {
        return yield* new AuthError({
          fix: "Retry later, or set WONDER_TOKEN to a Wonder personal access token.",
          message: `Client registration failed with ${reply.status}: ${reply.text.slice(0, 300)}`
        });
      }
      const registration = yield* decodeBody(RegistrationResponseSchema, reply.text);
      return registration.client_id;
    });
    const randomToken = (size) => random.randomBytes(size).pipe(map7(encode3));
    const login = fn2("Auth.login")(function* signIn(options) {
      const redirectUri = redirectUriFor(options.port);
      const existing = yield* store.readCredentials;
      const reusable = existing.pipe(filter((stored) => stored.redirectUri === redirectUri));
      const clientId = isSome2(reusable) ? reusable.value.clientId : yield* registerClient(redirectUri);
      const verifier = yield* randomToken(PKCE_VERIFIER_BYTES);
      const state = yield* randomToken(STATE_BYTES);
      const challenge = encode3(yield* digest.digest(sha256, new TextEncoder().encode(verifier)));
      const code = yield* callback.awaitCode({
        onListening: options.onUrl(authorizeUrl({
          clientId,
          codeChallenge: challenge,
          redirectUri,
          state
        })),
        port: options.port,
        state,
        timeoutSeconds: options.timeoutSeconds
      });
      const text = yield* postForm(TOKEN_URL, {
        client_id: clientId,
        code,
        code_verifier: verifier,
        grant_type: "authorization_code",
        redirect_uri: redirectUri,
        resource: MCP_URL
      });
      const token = yield* decodeBody(TokenResponseSchema, text);
      const now = yield* currentTimeMillis2;
      const credentials = credentialsFromToken({ clientId, redirectUri }, token, now);
      if (isNone2(credentials)) {
        return yield* new AuthError({
          fix: "Run `wonder-axi auth login` again and approve offline access.",
          message: "Wonder did not return a refresh token."
        });
      }
      yield* store.writeCredentials(credentials.value);
      return {
        email: fromUndefinedOr(token.id_token).pipe(flatMap(emailFromIdToken))
      };
    });
    const accountEmail = gen2(function* who() {
      const token = yield* accessToken;
      const reply = yield* send(client, get5(USERINFO_URL).pipe(acceptJson, bearerToken(token)), USERINFO_URL);
      if (reply.status < 200 || reply.status >= 300) {
        return none2();
      }
      return decodeUnknownOption2(fromJsonString2(UserInfoSchema))(reply.text).pipe(flatMapNullishOr((info) => info.email));
    });
    return Auth.of({
      accessToken,
      accountEmail,
      login,
      logout: store.deleteCredentials,
      refreshAccessToken,
      status
    });
  }));
}

// src/domain/context.ts
var SECTION_TITLES = [
  "# Basic info",
  "# Artboards (active page)",
  "# Design guidelines"
];
var splitSections = (text) => {
  const lines = text.split(`
`);
  const starts = lines.flatMap((line, index) => {
    const title = SECTION_TITLES.find((candidate) => candidate === line.trimEnd());
    return title === undefined ? [] : [[title, index]];
  });
  return Object.fromEntries(starts.map(([title, start], position) => [
    title,
    lines.slice(start + 1, starts[position + 1]?.[1] ?? lines.length).join(`
`).trim()
  ]));
};
var NamedSchema = Struct({ name: String5 });
var AppContextSchema = Struct({
  currentBranchName: optionalKey2(String5),
  currentFileId: optionalKey2(String5),
  currentFileName: optionalKey2(String5),
  currentOrganizationName: optionalKey2(String5),
  currentPageId: optionalKey2(String5),
  currentPageName: optionalKey2(String5),
  pages: optionalKey2(ArraySchema(Struct({ pageId: String5, pageName: String5 }))),
  selectedElements: optionalKey2(ArraySchema(Record(String5, Json2)))
});
var BasicInfoSchema = Struct({
  currentApplicationContext: optionalKey2(AppContextSchema),
  designTokens: optionalKey2(ArraySchema(NamedSchema)),
  icons: optionalKey2(ArraySchema(NamedSchema)),
  user: optionalKey2(Struct({ email: optionalKey2(String5) }))
});
var ArtboardsSchema = ArraySchema(Struct({
  dataNodeId: String5,
  dataNodeLabel: optionalKey2(String5),
  height: optionalKey2(Finite),
  width: optionalKey2(Finite)
}));
var decodeBasicInfo = (json) => getSuccess2(decodeUnknownResult2(fromJsonString2(BasicInfoSchema))(json));
var ACCOUNT_PATTERN = /Connected Wonder account: "(?<email>[^"]+)"/u;
var accountFromText = (text) => fromNullishOr(ACCOUNT_PATTERN.exec(text)?.groups?.email);
var TOKEN_PREVIEW = 6;
var labelled = (name, id) => `${name ?? "?"} (${id ?? "?"})`;
var artboardRows = (artboardsText) => getSuccess2(decodeUnknownResult2(fromJsonString2(ArtboardsSchema))(artboardsText)).pipe(map((list) => list.map((artboard) => ({
  id: artboard.dataNodeId,
  label: artboard.dataNodeLabel ?? "",
  size: `${artboard.width ?? "?"}x${artboard.height ?? "?"}`
}))));
var SELECTION_CAP = 10;
var selectionRow = (element) => {
  const { dataNodeId, dataNodeLabel, ...rest } = element;
  return { id: dataNodeId ?? null, label: dataNodeLabel ?? "", ...rest };
};
var describeApp = (info) => {
  const app = info?.currentApplicationContext;
  return {
    file: app === undefined ? null : labelled(app.currentFileName, app.currentFileId),
    branch: app?.currentBranchName ?? null,
    page: app === undefined ? null : labelled(app.currentPageName, app.currentPageId),
    pages: (app?.pages ?? []).map((page) => ({
      id: page.pageId,
      name: page.pageName
    })),
    selection: (app?.selectedElements ?? []).map(selectionRow)
  };
};
var sessionOf = (app) => {
  if (app?.currentPageId === undefined) {
    return;
  }
  const { currentFileId, currentFileName, currentPageId } = app;
  if (currentFileId === undefined || currentFileName === undefined) {
    return { pageId: currentPageId };
  }
  return {
    fileId: currentFileId,
    fileName: currentFileName,
    pageId: currentPageId
  };
};
var selectionFields = (selected, full) => {
  if (selected.length === 0) {
    return { selection: "none" };
  }
  const shown = full ? selected : selected.slice(0, SELECTION_CAP);
  const omitted = selected.length - shown.length;
  if (omitted === 0) {
    return { selectedCount: selected.length, selection: shown };
  }
  return {
    selectedCount: selected.length,
    selection: shown,
    selectionNote: `${omitted} more. Pass --full for all.`
  };
};
var describeHere = (info, full) => {
  const app = info?.currentApplicationContext;
  if (app?.currentFileId === undefined) {
    return { state: undefined, summary: { file: "none open in Wonder" } };
  }
  const place = {
    file: app.currentFileName ?? null,
    fileId: app.currentFileId,
    page: app.currentPageName ?? null,
    pageId: app.currentPageId ?? null,
    branch: app.currentBranchName ?? null
  };
  return {
    state: sessionOf(app),
    summary: Object.assign(place, selectionFields((app.selectedElements ?? []).map(selectionRow), full))
  };
};
var describeLibrary = (info) => {
  const tokens = info?.designTokens ?? [];
  return {
    tokens: tokens.length,
    tokenNames: tokens.slice(0, TOKEN_PREVIEW).map((token) => token.name),
    icons: (info?.icons ?? []).length
  };
};
var summarizeContext = (text, guidelinesLineCap, fullHint) => {
  const sections = splitSections(text);
  const info = getOrUndefined(decodeBasicInfo(sections["# Basic info"] ?? ""));
  const artboardsText = sections["# Artboards (active page)"] ?? "";
  const artboards = artboardRows(artboardsText);
  const guidelines = sections["# Design guidelines"] ?? "";
  const body = toon({
    ...describeApp(info),
    artboards: getOrElse(artboards, () => []),
    ...describeLibrary(info)
  });
  const unavailable = isNone2(artboards) && artboardsText.length > 0 ? `
artboardsNote: ${oneLine(artboardsText, 200)}` : "";
  const guidelinesBlock = guidelines.length === 0 ? "" : `

${capped(guidelines, guidelinesLineCap, fullHint).text}`;
  return `${body}${unavailable}${guidelinesBlock}`;
};

// src/domain/errors/refused-error.ts
class RefusedError extends TaggedError3()("RefusedError", {
  fix: String5,
  message: String5
}) {
  kind = "refused";
  [errorExitCode] = 1;
}

// src/services/wonder.ts
var Step = taggedEnum();
var REQUEST_TIMEOUT = seconds(120);
var MAX_RETRIES = 1;
var CLIENT_VERSION = "0.1.0";
var unexpected = (detail) => new NetworkError({
  fix: "Retry; if it persists, run `wonder-axi guide` to confirm the server is reachable.",
  message: `Wonder's MCP server returned an unexpected response: ${detail}`
});
var httpFailure = (reply) => new NetworkError({
  fix: reply.status === 429 ? "Rate limited. Wait a minute and retry." : "Retry; if it persists, check your network.",
  message: `Wonder's MCP server answered ${reply.status}: ${reply.text.slice(0, 300)}`
});
var isSessionGone = (reply) => reply.status === 404 || reply.status === 400 && /session/iu.test(reply.text);
var decodeResult = (schema, value) => decodeUnknownEffect2(schema)(value).pipe(mapError3(() => unexpected("result did not match the MCP schema.")));

class Wonder extends Service()("wonder-axi/Wonder") {
  static layer = effect(Wonder, gen2(function* build() {
    const client = yield* HttpClient;
    const auth = yield* Auth;
    const sessionRef = yield* make26(none2());
    const idRef = yield* make26(0);
    const post2 = (token, sessionId, body) => send(client, post(MCP_URL).pipe(bearerToken(token), setHeader("accept", "application/json, text/event-stream"), setHeader("mcp-protocol-version", PROTOCOL_VERSION), isSome2(sessionId) ? setHeader("mcp-session-id", sessionId.value) : setHeaders({}), bodyJsonUnsafe(body)), MCP_URL).pipe(timeoutOrElse2({
      duration: REQUEST_TIMEOUT,
      orElse: () => fail7(new NetworkError({
        fix: "Retry the command.",
        message: `Wonder did not answer within ${toSeconds(REQUEST_TIMEOUT)}s.`
      }))
    }));
    const nextId = updateAndGet(idRef, (id) => id + 1);
    const startSession = (token) => gen2(function* start() {
      const id = yield* nextId;
      const reply = yield* post2(token, none2(), {
        id,
        jsonrpc: "2.0",
        method: "initialize",
        params: {
          capabilities: {},
          clientInfo: { name: "wonder-axi", version: CLIENT_VERSION },
          protocolVersion: PROTOCOL_VERSION
        }
      });
      if (reply.status === 401) {
        return none2();
      }
      if (reply.status < 200 || reply.status >= 300) {
        return yield* httpFailure(reply);
      }
      if (reply.sessionId === undefined) {
        return yield* unexpected("initialize returned no mcp-session-id.");
      }
      yield* post2(token, some2(reply.sessionId), {
        jsonrpc: "2.0",
        method: "notifications/initialized"
      });
      yield* set4(sessionRef, some2(reply.sessionId));
      return some2(reply.sessionId);
    });
    const attempt = (token, method, params) => gen2(function* run() {
      const known = yield* get4(sessionRef);
      const sessionId = isSome2(known) ? known : yield* startSession(token);
      if (isNone2(sessionId)) {
        return Step.Unauthorized({ status: 401 });
      }
      const id = yield* nextId;
      const reply = yield* post2(token, sessionId, {
        id,
        jsonrpc: "2.0",
        method,
        params
      });
      if (reply.status === 401) {
        return Step.Unauthorized({ status: reply.status });
      }
      if (isSessionGone(reply)) {
        return Step.SessionGone({ status: reply.status });
      }
      if (reply.status < 200 || reply.status >= 300) {
        return yield* httpFailure(reply);
      }
      const message = parseRpcResponse(reply.text, id);
      if (isNone2(message)) {
        return yield* unexpected(`no reply to ${method}.`);
      }
      if (message.value.error !== undefined) {
        return yield* new NetworkError({
          fix: "Run `wonder-axi guide <tool>` to check the arguments.",
          message: `Wonder rejected ${method}: ${message.value.error.message}`
        });
      }
      return Step.Ok({ result: message.value.result ?? null });
    });
    const rpc = (method, params) => {
      const go = (getToken, retries) => gen2(function* step() {
        const token = yield* getToken;
        const outcome = yield* attempt(token, method, params);
        return yield* value4(outcome).pipe(tagsExhaustive2({
          Ok: ({ result }) => succeed7(result),
          SessionGone: () => retries > 0 ? set4(sessionRef, none2()).pipe(andThen2(go(auth.accessToken, retries - 1))) : fail7(unexpected("the MCP session expired twice in a row.")),
          Unauthorized: () => retries > 0 ? go(auth.refreshAccessToken, retries - 1) : fail7(new AuthError({
            fix: "Run `wonder-axi auth login` to sign in again.",
            message: "Wonder rejected the access token."
          }))
        }));
      });
      return go(auth.accessToken, MAX_RETRIES);
    };
    const callTool = fn2("Wonder.callTool")(function* invoke(name, args) {
      const result = yield* decodeResult(ToolResultSchema, yield* rpc("tools/call", { arguments: args, name }));
      if (result.isError === true) {
        return yield* new RefusedError({
          fix: "Fix the call as the message says, then retry. `wonder-axi guide <tool>` shows the arguments.",
          message: splitContent(result).texts.join(`
`) || `${name} failed.`
        });
      }
      return result;
    });
    const listPage = (cursor) => gen2(function* page() {
      const listed = yield* decodeResult(ToolsPageSchema, yield* rpc("tools/list", isSome2(cursor) ? { cursor: cursor.value } : {}));
      if (listed.nextCursor === undefined) {
        return listed.tools;
      }
      const rest = yield* listPage(some2(listed.nextCursor));
      return [...listed.tools, ...rest];
    });
    return Wonder.of({ callTool, listTools: listPage(none2()) });
  }));
}

// src/commands/shared.ts
var pageFlag = String11("page").pipe(optional5, withDescription3("Page id. Defaults to the page set by `use` or `open`."));
var fullFlag = Boolean6("full").pipe(withDefault5(false), withDescription3("Print the complete text without truncation."));
var jsonFlag = Boolean6("json").pipe(withDefault5(false), withDescription3("Print JSON instead of TOON."));
var fileFlag = String11("file").pipe(optional5, withDescription3("Read input from this file instead of stdin."));
var FilesSchema = Struct({
  files: ArraySchema(Struct({
    fileId: String5,
    fileName: String5,
    organizationName: optionalKey2(String5)
  }))
});
var textOf = (result) => splitContent(result).texts.join(`
`);
var decodeText3 = (schema, result) => getSuccess2(decodeUnknownResult2(fromJsonString2(schema))(textOf(result)));
var withOptional = (base, key, value) => match(value, {
  onNone: () => base,
  onSome: (present) => ({ ...base, [key]: present })
});
var resolvePage = (page) => gen2(function* lookup() {
  if (isSome2(page)) {
    return page.value;
  }
  const store = yield* Store;
  const state = yield* store.readState;
  if (isNone2(state)) {
    return yield* new UsageError({
      fix: "Run `wonder-axi open <fileId>` or `wonder-axi use <pageId>`, or pass --page <pageId>.",
      message: "No page selected."
    });
  }
  return state.value.pageId;
});
var emit = (value, help, json) => log2(format4(value, help, json));
var printText = (text, full, hint = "Pass --full for the complete text.") => log2(full ? text : capped(text, DEFAULT_LINE_CAP, hint).text);
var readInput = (file) => gen2(function* ingest() {
  if (isSome2(file)) {
    const fs = yield* FileSystem;
    return yield* fs.readFileString(file.value);
  }
  const stdio = yield* Stdio;
  if (yield* stdio.stdinIsTerminal) {
    return yield* new UsageError({
      fix: "Pipe the input (`wonder-axi <command> < file`) or pass --file <path>.",
      message: "No input: stdin is a terminal and --file was not given."
    });
  }
  return yield* mkString(decodeText2(stdio.stdin));
}).pipe(catchTag2("PlatformError", (cause) => fail7(new UsageError({
  fix: "Check the --file path or the piped input.",
  message: `Could not read input: ${cause.message}`
}))));
var callBasicInfo = gen2(function* basicInfo() {
  const wonder = yield* Wonder;
  const store = yield* Store;
  const state = yield* store.readState;
  return yield* wonder.callTool("get_basic_info", match(state, {
    onNone: () => ({}),
    onSome: (current) => ({ pageId: current.pageId })
  }));
});
var lookupAccount = gen2(function* lookup() {
  const auth = yield* Auth;
  const basic = yield* callBasicInfo.pipe(map7((result) => {
    const text = textOf(result);
    return { info: decodeBasicInfo(text), text };
  }), catchTag2("RefusedError", (refused) => succeed7({
    info: none2(),
    text: refused.message
  })));
  const direct = yield* auth.accountEmail.pipe(orElseSucceed2(() => none2()));
  const email = direct.pipe(orElse(() => flatMapNullishOr(basic.info, (found) => found.user?.email)), orElse(() => accountFromText(basic.text)));
  return { email, info: basic.info };
});

// src/commands/auth.ts
var emailOrNone = lookupAccount.pipe(map7((account) => account.email), orElseSucceed2(() => none2()));
var login = make48("login", {
  json: jsonFlag,
  port: Int5("port").pipe(withDefault5(DEFAULT_PORT), withDescription3("Loopback port for the OAuth callback.")),
  timeout: Int5("timeout").pipe(withDefault5(DEFAULT_LOGIN_TIMEOUT_SECONDS), withDescription3("Seconds to wait for the browser callback."))
}, fn2(function* runLogin({ json, port, timeout }) {
  const auth = yield* Auth;
  const result = yield* auth.login({
    onUrl: (url) => log2([
      "Open this URL in a browser to sign in to Wonder:",
      url,
      `Waiting on http://127.0.0.1:${port}/callback for up to ${timeout}s.`,
      `From another machine: ssh -R ${port}:127.0.0.1:${port} <this-host>, then open the URL there.`
    ].join(`
`)),
    port,
    timeoutSeconds: timeout
  });
  const email = isSome2(result.email) ? result.email : yield* emailOrNone;
  yield* log2(format4({ signedIn: getOrElse(email, () => "unknown") }, ["Run `wonder-axi` to see your files."], json));
})).pipe(withDescription4("Sign in to Wonder with OAuth (authorization code + PKCE)."));
var status = make48("status", { json: jsonFlag }, fn2(function* runStatus({ json }) {
  const auth = yield* Auth;
  const email = yield* emailOrNone;
  const current = yield* auth.status;
  const now = yield* currentTimeMillis2;
  const expiry = match(current.expiresAt, {
    onNone: () => ({ expires: "never (personal access token)" }),
    onSome: (at) => describeExpiry(at, now)
  });
  const location2 = current.source === "file" ? current.credentialsPath : "WONDER_TOKEN";
  yield* log2(format4({
    email: getOrElse(email, () => "unknown (the token was not accepted)"),
    source: current.source,
    ...expiry,
    credentials: location2
  }, isNone2(email) ? ["Run `wonder-axi auth login` to sign in again."] : [], json));
})).pipe(withDescription4("Show the signed-in account, token source and expiry."));
var logout = make48("logout", { json: jsonFlag }, fn2(function* runLogout({ json }) {
  const auth = yield* Auth;
  const removed = yield* auth.logout;
  yield* log2(format4({
    loggedOut: removed,
    note: removed ? "credentials deleted" : "no credentials were stored"
  }, [], json));
})).pipe(withDescription4("Delete the stored credentials."));
var auth = make48("auth").pipe(withDescription4("Sign in, inspect or sign out."), withSubcommands([login, status, logout]));

// src/domain/tool-args.ts
var JsonObjectSchema = Record(String5, Unknown2);
var decodeJsonObject = decodeUnknownResult2(fromJsonString2(JsonObjectSchema));
var decodeJsonValue = decodeUnknownResult2(fromJsonString2(Unknown2));
var DeclaredParamsSchema = Struct({
  properties: Record(String5, Struct({ type: optionalKey2(String5) }))
});
var parseJsonObject = (text) => mapError(decodeJsonObject(text), () => "Arguments must be a JSON object.");
var propertyTypes = (inputSchema) => {
  const declared = getOrUndefined3(decodeUnknownResult2(DeclaredParamsSchema)(inputSchema ?? {}));
  return new Map(Object.entries(declared?.properties ?? {}).map(([key, value]) => [
    key,
    value.type ?? "string"
  ]));
};
var declaresParam = (inputSchema, name) => propertyTypes(inputSchema).has(name);
var coerce2 = (key, raw, type) => {
  if (type === "string") {
    return succeed2(raw);
  }
  return mapError(decodeJsonValue(raw), () => `--arg ${key}: expected ${type} (JSON), got "${raw}".`);
};
var parsePair = (pair, types) => {
  const index = pair.indexOf("=");
  if (index <= 0) {
    return fail2(`--arg expects key=value, got "${pair}".`);
  }
  const key = pair.slice(0, index);
  const type = types.get(key);
  if (types.size > 0 && type === undefined) {
    return fail2(`Unknown argument "${key}". Known: ${[...types.keys()].join(", ")}.`);
  }
  return map2(coerce2(key, pair.slice(index + 1), type ?? "string"), (value) => [key, value]);
};
var parseArgPairs = (pairs, inputSchema) => {
  const types = propertyTypes(inputSchema);
  return map2(all(pairs.map((pair) => parsePair(pair, types))), (entries) => Object.fromEntries(entries));
};

// src/domain/image.ts
var EXTENSIONS = new Map([
  ["image/gif", "gif"],
  ["image/jpeg", "jpg"],
  ["image/jpg", "jpg"],
  ["image/png", "png"],
  ["image/svg+xml", "svg"],
  ["image/webp", "webp"]
]);
var PNG_MAGIC = [137, 80, 78, 71];
var PNG_HEADER_BYTES = 24;
var JPEG_MARKER_BYTES = 9;
var NON_FRAME_MARKERS = new Set([196, 200, 204]);
var viewOf = (bytes) => new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
var pngSize = (bytes) => {
  const isPng = bytes.length >= PNG_HEADER_BYTES && PNG_MAGIC.every((value, index) => bytes[index] === value);
  if (!isPng) {
    return none2();
  }
  const view = viewOf(bytes);
  return some2([view.getUint32(16), view.getUint32(20)]);
};
var isStartOfFrame = (marker) => marker >= 192 && marker <= 207 && !NON_FRAME_MARKERS.has(marker);
var jpegSize = (bytes) => {
  const none = none2();
  if (bytes[0] !== 255 || bytes[1] !== 216) {
    return none;
  }
  const view = viewOf(bytes);
  const scan = (offset) => {
    if (offset + JPEG_MARKER_BYTES > bytes.length || bytes[offset] !== 255) {
      return none;
    }
    return isStartOfFrame(view.getUint8(offset + 1)) ? some2([
      view.getUint16(offset + 7),
      view.getUint16(offset + 5)
    ]) : scan(offset + 2 + view.getUint16(offset + 2));
  };
  return scan(2);
};
var imageSize = (bytes) => orElse(pngSize(bytes), () => jpegSize(bytes));
var decodeImage = (data, mimeType) => getSuccess2(decode(data)).pipe(map((bytes) => {
  const size = imageSize(bytes);
  return {
    bytes,
    extension: EXTENSIONS.get(mimeType) ?? "bin",
    height: size.pipe(map(([, height]) => height)),
    mimeType,
    width: size.pipe(map(([width]) => width))
  };
}));

// src/commands/images.ts
var saveImage = (image, options) => gen2(function* persist() {
  const fs = yield* FileSystem;
  const path = yield* Path;
  const decoded = decodeImage(image.data, image.mimeType);
  if (isNone2(decoded)) {
    return yield* new RefusedError({
      fix: "Retry the screenshot.",
      message: "Wonder returned an image that is not valid base64."
    });
  }
  const now = yield* currentTimeMillis2;
  const target = getOrElse(options.out, () => path.join(".wonder", "shots", `${options.stem}-${now}.${decoded.value.extension}`));
  yield* fs.makeDirectory(path.dirname(target), { recursive: true });
  yield* fs.writeFile(target, decoded.value.bytes);
  const { height, width } = decoded.value;
  return {
    bytes: decoded.value.bytes.length,
    format: decoded.value.mimeType,
    path: target,
    size: isSome2(width) && isSome2(height) ? `${width.value}x${height.value}` : "unknown"
  };
}).pipe(catchTag2("PlatformError", (cause) => fail7(new UsageError({
  fix: "Pass --out with a writable path.",
  message: `Could not write the image: ${cause.message}`
}))));

// src/commands/call.ts
var STDIN = "-";
var usage = (message) => new UsageError({
  fix: "Run `wonder-axi guide <tool>` for the tool's parameters.",
  message
});
var readBase = (argsJson, argsFile) => gen2(function* base() {
  if (isSome2(argsJson)) {
    return argsJson.value;
  }
  if (isNone2(argsFile)) {
    return "{}";
  }
  if (argsFile.value === STDIN) {
    return yield* readInput(none2());
  }
  const fs = yield* FileSystem;
  return yield* fs.readFileString(argsFile.value).pipe(mapError3(() => usage(`Could not read ${argsFile.value}.`)));
});
var orUsage = (parsed) => match2(parsed, {
  onFailure: (message) => fail7(usage(message)),
  onSuccess: (value) => succeed7(value)
});
var call = make48("call", {
  arg: String11("arg").pipe(atLeast3(0), withDescription3("key=value, repeatable. Values are coerced from the tool's schema.")),
  argsFile: String11("args-file").pipe(optional5, withDescription3("Read the JSON arguments from a file, or `-` for stdin.")),
  argsJson: String11("args-json").pipe(optional5, withDescription3("Arguments as one JSON object.")),
  full: fullFlag,
  tool: String10("tool").pipe(withDescription2("Any Wonder tool name. `wonder-axi guide` lists them."))
}, fn2(function* runCall({ arg, argsFile, argsJson, full, tool }) {
  const wonder = yield* Wonder;
  const base = yield* orUsage(parseJsonObject(yield* readBase(argsJson, argsFile)));
  const tools = yield* wonder.listTools;
  const inputSchema = tools.find((candidate) => candidate.name === tool)?.inputSchema;
  const pairs = yield* orUsage(parseArgPairs(arg, inputSchema));
  const supplied = { ...base, ...pairs };
  const wantsPage = declaresParam(inputSchema, "pageId");
  const args = wantsPage && supplied.pageId === undefined ? { ...supplied, pageId: yield* resolvePage(none2()) } : supplied;
  const result = yield* wonder.callTool(tool, args);
  const { images, texts } = splitContent(result);
  for (const text of texts) {
    yield* printText(text, full);
  }
  for (const image of images) {
    const saved = yield* saveImage(image, { out: none2(), stem: tool });
    yield* log2(toon({ image: { ...saved } }));
  }
  if (result.structuredContent !== undefined) {
    yield* log2(toon({ structuredContent: result.structuredContent }));
  }
  if (texts.length === 0 && images.length === 0 && result.structuredContent === undefined) {
    yield* log2("ok: the tool returned no content");
  }
})).pipe(withDescription4("Call any Wonder tool with key=value or JSON arguments. New server tools work with no release."));

// src/commands/home.ts
var RECENT = 5;
var showHome = (json) => gen2(function* overview() {
  const wonder = yield* Wonder;
  const looked = yield* lookupAccount;
  const account = {
    email: looked.email,
    info: getOrUndefined(looked.info)
  };
  const listed = decodeText3(FilesSchema, yield* wonder.callTool("list_files", {}));
  const files = match(listed, {
    onNone: () => [],
    onSome: (decoded) => decoded.files
  });
  yield* emit({
    account: getOrElse(account.email, () => "unknown"),
    organization: account.info?.currentApplicationContext?.currentOrganizationName ?? files[0]?.organizationName ?? "unknown",
    here: describeHere(account.info, false).summary,
    files: files.slice(0, RECENT).map((file) => ({
      id: file.fileId,
      name: file.fileName,
      org: file.organizationName ?? ""
    })),
    totalFiles: files.length
  }, [
    files.length === 0 ? "No files yet. Create one in Wonder, then run `wonder-axi files`." : "Run `wonder-axi open <id>` to select a file.",
    "Run `wonder-axi here` to see the file, page and selection open in Wonder.",
    "Run `wonder-axi files --name <text>` to find a file by name.",
    "Run `wonder-axi guide` to list every Wonder tool."
  ], json);
});

// src/domain/guide.ts
var PropertySchema = Struct({
  description: optionalKey2(String5),
  enum: optionalKey2(ArraySchema(Unknown2)),
  items: optionalKey2(Struct({ type: optionalKey2(String5) })),
  type: optionalKey2(Union2([String5, ArraySchema(String5)]))
});
var InputParamsSchema = Struct({
  properties: optionalKey2(Record(String5, PropertySchema)),
  required: optionalKey2(ArraySchema(String5))
});
var typeLabel = (property) => {
  if (property.enum !== undefined) {
    return property.enum.map(String).join("|");
  }
  const base = [property.type ?? "any"].flat().join("|");
  return base === "array" && property.items?.type !== undefined ? `${property.items.type}[]` : base;
};
var summarizeTools = (tools) => toon({
  tools: tools.map((tool) => ({
    name: tool.name,
    readOnly: tool.annotations?.readOnlyHint === true,
    summary: oneLine(firstSentence(tool.description ?? ""), 110)
  }))
});
var describeTool = (tool) => {
  const declared = getOrUndefined3(decodeUnknownResult2(InputParamsSchema)(tool.inputSchema ?? {}));
  const required = new Set(declared?.required);
  const params = Object.entries(declared?.properties ?? {}).map(([name, property]) => ({
    description: oneLine(firstSentence(property.description ?? ""), 120),
    name,
    required: required.has(name),
    type: typeLabel(property)
  }));
  const readOnly = tool.annotations?.readOnlyHint === true ? " (read-only)" : "";
  return [
    `# ${tool.name}${readOnly}`,
    "",
    tool.description ?? "",
    "",
    toon({ params })
  ].join(`
`);
};

// src/commands/read.ts
var OpenSchema = Struct({
  branchName: optionalKey2(String5),
  fileId: String5,
  fileName: String5,
  organizationName: optionalKey2(String5),
  pageId: String5
});
var ArtboardsSchema2 = ArraySchema(Struct({
  dataNodeId: String5,
  dataNodeLabel: optionalKey2(String5),
  descendantsCount: optionalKey2(Finite),
  height: optionalKey2(Finite),
  width: optionalKey2(Finite)
}));
var TokensSchema = Struct({
  tokens: ArraySchema(Struct({
    aliasOf: optionalKey2(String5),
    cssVar: optionalKey2(String5),
    name: String5,
    type: String5,
    value: optionalKey2(Json2)
  }))
});
var IconsSchema = Struct({
  icons: ArraySchema(Struct({
    componentName: optionalKey2(String5),
    name: String5
  }))
});
var TreesSchema = Struct({
  results: ArraySchema(Struct({
    code: optionalKey2(String5),
    dataNodeId: String5,
    tree: optionalKey2(String5)
  })),
  warnings: optionalKey2(ArraySchema(String5))
});
var displayValue = (value) => value4(value).pipe(when3(_undefined, () => ""), when3(string3, (text) => text), orElse3((other) => JSON.stringify(other)));
var idsArgument = String10("id").pipe(atLeast2(1), withDescription2("One or more data-node-id values."));
var files = make48("files", {
  json: jsonFlag,
  limit: Int5("limit").pipe(withDefault5(30), withDescription3("Maximum rows to print.")),
  name: String11("name").pipe(optional5, withDescription3("Only files whose name contains this text."))
}, fn2(function* runFiles({ json, limit, name }) {
  const wonder = yield* Wonder;
  const result = yield* wonder.callTool("list_files", withOptional({}, "nameContains", name));
  const decoded = decodeText3(FilesSchema, result);
  if (isNone2(decoded)) {
    yield* printText(textOf(result), false);
    return;
  }
  const all = decoded.value.files;
  yield* emit({
    files: all.slice(0, limit).map((file) => ({
      id: file.fileId,
      name: file.fileName,
      org: file.organizationName ?? ""
    })),
    total: all.length
  }, all.length === 0 ? [
    "No files matched. Run `wonder-axi files` without --name to list everything."
  ] : ["Run `wonder-axi open <id>` to select a file and its landing page."], json);
})).pipe(withDescription4("List the Wonder files you can open."));
var open3 = make48("open", {
  fileId: String10("fileId").pipe(withDescription2("A fileId from `wonder-axi files`.")),
  json: jsonFlag
}, fn2(function* runOpen({ fileId, json }) {
  const wonder = yield* Wonder;
  const store = yield* Store;
  const result = yield* wonder.callTool("open_file", { fileId });
  const opened = decodeText3(OpenSchema, result);
  if (isNone2(opened)) {
    yield* printText(textOf(result), false);
    return;
  }
  const file = opened.value;
  yield* store.writeState({
    fileId: file.fileId,
    fileName: file.fileName,
    pageId: file.pageId
  });
  const info = decodeBasicInfo(textOf(yield* wonder.callTool("get_basic_info", {
    fileId: file.fileId,
    pageId: file.pageId
  })));
  const pages = match(info, {
    onNone: () => [],
    onSome: (basic) => basic.currentApplicationContext?.pages ?? []
  });
  yield* emit({
    file: `${file.fileName} (${file.fileId})`,
    org: file.organizationName ?? "",
    branch: file.branchName ?? "",
    currentPage: file.pageId,
    pages: pages.map((page) => ({ id: page.pageId, name: page.pageName }))
  }, [
    "Run `wonder-axi context` to read the file, its tokens and its artboards.",
    "Run `wonder-axi use <pageId>` to switch pages."
  ], json);
})).pipe(withDescription4("Select a file and set its landing page as current."));
var use = make48("use", {
  json: jsonFlag,
  pageId: String10("pageId").pipe(withDescription2("A pageId from `wonder-axi open` or `context`."))
}, fn2(function* runUse({ json, pageId }) {
  const store = yield* Store;
  const previous = yield* store.readState;
  yield* store.writeState({ ...getOrUndefined(previous), pageId });
  yield* emit({ page: pageId }, ["Run `wonder-axi context` to read this page."], json);
})).pipe(withDescription4("Set the current page."));
var here = make48("here", { full: fullFlag, json: jsonFlag }, fn2(function* runHere({ full, json }) {
  const store = yield* Store;
  const info = yield* callBasicInfo.pipe(map7((result) => decodeBasicInfo(textOf(result))), catchTag2("RefusedError", () => succeed7(none2())));
  const found = describeHere(getOrUndefined(info), full);
  if (found.state !== undefined) {
    yield* store.writeState(found.state);
  }
  yield* emit(found.summary, [
    found.state === undefined ? "Run `wonder-axi files` then `wonder-axi open <id>` to pick a file." : "Run `wonder-axi context` to read this page, or `wonder-axi tree <id>` for a selected element."
  ], json);
})).pipe(withDescription4("Show the file, page and selection open in the Wonder app, and make that page current."));
var context3 = make48("context", {
  full: fullFlag,
  guidelines: Literals3("guidelines", ["none", "fundamentals"]).pipe(withDefault5("fundamentals"), withDescription3("Include the design fundamentals text (default) or skip it.")),
  json: jsonFlag,
  page: pageFlag
}, fn2(function* runContext({ full, guidelines, json, page }) {
  const wonder = yield* Wonder;
  const pageId = yield* resolvePage(page);
  const text = textOf(yield* wonder.callTool("get_design_context", { guidelines, pageId }));
  if (json) {
    yield* log2(JSON.stringify({ text }, null, 2));
    return;
  }
  if (full) {
    yield* log2(text);
    return;
  }
  yield* log2(summarizeContext(text, 12, "Pass --full for the design fundamentals text."));
  yield* log2(toon({
    help: [
      "Run `wonder-axi skill <job>` before designing: new-design, edit-design, code-to-design, design-to-code, design-library, generate-assets",
      "Run `wonder-axi guide update_elements` before authoring JSX"
    ]
  }));
})).pipe(withDescription4("Read the file, page, selection, artboards, tokens and icons in one call."));
var skill = make48("skill", {
  name: String10("name").pipe(withDescription2("new-design, edit-design, code-to-design, design-to-code, design-library or generate-assets."))
}, fn2(function* runSkill({ name }) {
  const wonder = yield* Wonder;
  yield* log2(textOf(yield* wonder.callTool("get_skills", { skill: name })));
})).pipe(withDescription4("Print Wonder's own instructions for a design job, in full."));
var guide = make48("guide", {
  tool: String10("tool").pipe(optional4, withDescription2("A Wonder tool name. Omit to list every tool."))
}, fn2(function* runGuide({ tool }) {
  const wonder = yield* Wonder;
  const tools = yield* wonder.listTools;
  if (isNone2(tool)) {
    yield* log2(summarizeTools(tools));
    yield* log2(toon({
      help: [
        "Run `wonder-axi guide <tool>` for a tool's full description and parameters"
      ]
    }));
    return;
  }
  const found = tools.find((candidate) => candidate.name === tool.value);
  yield* found === undefined ? fail7(new UsageError({
    fix: `Run \`wonder-axi guide\` to list tools. Known: ${tools.map((candidate) => candidate.name).join(", ")}.`,
    message: `Unknown tool "${tool.value}".`
  })) : log2(describeTool(found));
})).pipe(withDescription4("List Wonder's tools, or print one tool's description and parameters."));
var artboards = make48("artboards", { json: jsonFlag, page: pageFlag }, fn2(function* runArtboards({ json, page }) {
  const wonder = yield* Wonder;
  const pageId = yield* resolvePage(page);
  const result = yield* wonder.callTool("get_artboards", { pageId });
  const decoded = decodeText3(ArtboardsSchema2, result);
  if (isNone2(decoded)) {
    yield* printText(textOf(result), false);
    return;
  }
  yield* emit({
    artboards: decoded.value.map((artboard) => ({
      id: artboard.dataNodeId,
      label: artboard.dataNodeLabel ?? "",
      nodes: artboard.descendantsCount ?? 0,
      size: `${artboard.width ?? "?"}x${artboard.height ?? "?"}`
    })),
    total: decoded.value.length
  }, ["Run `wonder-axi tree <id>` to read an artboard's structure."], json);
})).pipe(withDescription4("List the page's artboards."));
var treeCommand = (name, tool, description) => make48(name, {
  depth: Int5("depth").pipe(optional5, withDescription3("Levels below each id to include.")),
  full: fullFlag,
  ids: idsArgument,
  page: pageFlag
}, fn2(function* runTree({ depth, full, ids, page }) {
  const wonder = yield* Wonder;
  const pageId = yield* resolvePage(page);
  const result = yield* wonder.callTool(tool, withOptional({ dataNodeIds: ids, pageId }, "depth", depth));
  const decoded = decodeText3(TreesSchema, result);
  if (isNone2(decoded)) {
    yield* printText(textOf(result), full);
    return;
  }
  for (const entry of decoded.value.results) {
    yield* printText(`# ${entry.dataNodeId}
${entry.tree ?? entry.code ?? ""}`, full);
  }
  for (const warning of decoded.value.warnings ?? []) {
    yield* error(`warning: ${oneLine(warning, 300)}`);
  }
})).pipe(withDescription4(description));
var tree = treeCommand("tree", "get_element_tree", "Print the element tree under one or more ids.");
var code = treeCommand("code", "get_element_code", "Print the React+Tailwind JSX under one or more ids.");
var find = make48("find", {
  full: fullFlag,
  limit: Int5("limit").pipe(optional5, withDescription3("Maximum matches per query.")),
  page: pageFlag,
  queries: String10("query").pipe(atLeast2(1), withDescription2("One or more independent searches. Quote multi-word queries."))
}, fn2(function* runFind({ full, limit, page, queries }) {
  const wonder = yield* Wonder;
  const pageId = yield* resolvePage(page);
  const result = yield* wonder.callTool("find_elements", withOptional({ pageId, queries }, "limit", limit));
  yield* printText(textOf(result), full);
})).pipe(withDescription4("Find elements on the page by free-form queries."));
var tokens = make48("tokens", { json: jsonFlag, page: pageFlag }, fn2(function* runTokens({ json, page }) {
  const wonder = yield* Wonder;
  const pageId = yield* resolvePage(page);
  const result = yield* wonder.callTool("get_design_tokens", { pageId });
  const decoded = decodeText3(TokensSchema, result);
  if (isNone2(decoded)) {
    yield* printText(textOf(result), false);
    return;
  }
  yield* emit({
    tokens: decoded.value.tokens.map((token) => ({
      cssVar: token.cssVar ?? "",
      name: token.name,
      type: token.type,
      value: token.aliasOf === undefined ? displayValue(token.value) : `alias:${token.aliasOf}`
    })),
    total: decoded.value.tokens.length
  }, [
    "Bind colors in JSX with a token's cssVar. Create missing tokens with `wonder-axi call create_design_tokens`."
  ], json);
})).pipe(withDescription4("List the file's design tokens."));
var icons = make48("icons", { json: jsonFlag, page: pageFlag }, fn2(function* runIcons({ json, page }) {
  const wonder = yield* Wonder;
  const pageId = yield* resolvePage(page);
  const result = yield* wonder.callTool("get_icons", { pageId });
  const decoded = decodeText3(IconsSchema, result);
  if (isNone2(decoded)) {
    yield* printText(textOf(result), false);
    return;
  }
  yield* emit({
    icons: decoded.value.icons.map((icon) => ({
      component: icon.componentName ?? "",
      name: icon.name
    })),
    total: decoded.value.icons.length
  }, [
    "Run `wonder-axi call get_icons --arg names='[\"<name>\"]'` for an icon's svg markup."
  ], json);
})).pipe(withDescription4("List the file's icon library."));

// src/commands/write.ts
var createArtboard = make48("create-artboard", { file: fileFlag, full: fullFlag, page: pageFlag }, fn2(function* runCreateArtboard({ file, full, page }) {
  const wonder = yield* Wonder;
  const pageId = yield* resolvePage(page);
  const code = yield* readInput(file);
  const result = yield* wonder.callTool("create_artboard", { code, pageId });
  yield* printText(unwrapTrees(textOf(result)), full);
  yield* log2(format4({}, [
    "Run `wonder-axi screenshot <artboardId>` to check the render.",
    "Run `wonder-axi finish <artboardId>` when the design is done."
  ], false));
})).pipe(withDescription4("Create an artboard from JSX on stdin or --file. Run `guide create_artboard` first."));
var update = make48("update", { file: fileFlag, full: fullFlag, page: pageFlag }, fn2(function* runUpdate({ file, full, page }) {
  const wonder = yield* Wonder;
  const pageId = yield* resolvePage(page);
  const edit = yield* readInput(file);
  const result = yield* wonder.callTool("update_elements", { edit, pageId });
  yield* printText(unwrapTrees(textOf(result)), full);
})).pipe(withDescription4("Apply JSX directives from stdin or --file. Run `guide update_elements` first."));
var screenshot = make48("screenshot", {
  dataNodeId: String10("dataNodeId").pipe(withDescription2("The element to capture.")),
  json: jsonFlag,
  mode: Literals3("mode", ["thumbnail", "full"]).pipe(withDefault5("thumbnail"), withDescription3("Use full to judge fine detail.")),
  out: String11("out").pipe(optional5, withDescription3("Where to write the image. Default ./.wonder/shots/.")),
  page: pageFlag
}, fn2(function* runScreenshot({ dataNodeId, json, mode, out, page }) {
  const wonder = yield* Wonder;
  const pageId = yield* resolvePage(page);
  const result = yield* wonder.callTool("take_screenshot", {
    dataNodeId,
    mode,
    pageId
  });
  const [image] = splitContent(result).images;
  if (image === undefined) {
    return yield* new RefusedError({
      fix: "Check the element id with `wonder-axi tree <id>`.",
      message: textOf(result) || "Wonder returned no image."
    });
  }
  const saved = yield* saveImage(image, { out, stem: dataNodeId });
  yield* log2(format4({ ...saved }, [
    "Open the file to inspect the render, then run `wonder-axi finish <artboardId>`."
  ], json));
  return saved;
})).pipe(withDescription4("Render an element to an image file. Never prints image data."));
var finish = make48("finish", {
  artboardId: String10("artboardId").pipe(withDescription2("The artboard root's data-node-id.")),
  page: pageFlag
}, fn2(function* runFinish({ artboardId, page }) {
  const wonder = yield* Wonder;
  const pageId = yield* resolvePage(page);
  yield* log2(textOf(yield* wonder.callTool("finish_artboard", { artboardId, pageId })));
})).pipe(withDescription4("Mark an artboard finished and print the server's reply (a design link when the server has one)."));

// src/cli.ts
var VERSION = "0.1.0";
var root = make48("wonder-axi", { json: jsonFlag }, ({ json }) => showHome(json)).pipe(withDescription4("Drive Wonder (wonder.design) canvases from the shell. Run with no arguments for your account and files."), withSubcommands([
  files,
  open3,
  here,
  use,
  context3,
  skill,
  guide,
  artboards,
  tree,
  code,
  find,
  createArtboard,
  update,
  screenshot,
  finish,
  tokens,
  icons,
  call,
  auth
]));
var services = Wonder.layer.pipe(provideMerge(Auth.layer), provideMerge(mergeAll2(layer2, layerWebCrypto, layerWebCrypto2, CallbackServer.layer, Store.layer)), provideMerge(layer13));
var reportApp = (error) => log2(toon({ error: error.message, fix: error.fix, kind: error.kind })).pipe(andThen2(fail7(error)));
var isShowHelp = is2(ShowHelp);
var cliMessage = (error) => isShowHelp(error) ? error.errors.map((cause) => cause.message).join(" ") : error.message;
var reportCli = (error) => isShowHelp(error) && error.errors.length === 0 ? void_3 : log2(toon({
  error: cliMessage(error),
  fix: "Run `wonder-axi --help` or `<command> --help`.",
  kind: "usage"
})).pipe(andThen2(fail7(new UsageError({
  fix: "Run with --help.",
  message: cliMessage(error)
}))));
var program = run5(root, {
  renderErrors: false,
  version: VERSION
}).pipe(catchTags2({
  AuthError: reportApp,
  NetworkError: reportApp,
  RefusedError: reportApp,
  StorageError: reportApp,
  UsageError: reportApp
}), catchIf2(isCliError, reportCli), provide4(mergeAll2(services, layer15(Object.assign(defaultFormatter2(), {
  formatVersion: (_name, version) => version
})))));

// src/main.ts
runMain2(program, { disableErrorReporting: true });
