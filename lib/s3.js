var sys = require('sys');
var dns = require('dns');
var net = require('net');
var fs = require('fs');
var http = require('http');
var crypto = require('crypto');
var EventEmitter = require('events').EventEmitter;

var // npm packages
querystring = require('querystring');

var xml = require('xml');

var // exports
mime = require('mime');

var S3;

S3 = function(awsAccessKey, awsSecretKey, options) {
  if (typeof awsSecretKey !== 'string' || typeof awsAccessKey !== 'string') {
    throw 'Aws Secret-Key and Acess-Key are required';
  }
  this.properties = extend({}, S3.properties, {
    secretKey: awsSecretKey,
    accessKey: awsAccessKey
  }, options);
}

S3.properties = {
  acl: null,
  bucket: null,
  // We can calculate it as we stream the file to the server, and check it against the response ETag
  checkMD5: true,
  httpsOnly: true,
  storageClass: null,
  endpoint: 's3.amazonaws.com',
  httpPort: 80,
  httpsPort: 443
}

S3.prototype.constructor = S3;

// TODO: make another request if max is reached
S3.prototype.listObjects = function(prefix, o) {
  // the prefix is optional, and it can be inside the options object
  o = o || (typeof prefix === 'object'? prefix : {});
  if (typeof prefix === 'string') {
    o.prefix = prefix;
  }

  var p = extend({}, this.properties, o, {
      method: 'GET'
      , querystring: querystringify(o, ['prefix', 'delimiter'])
      , object: ''
    });

  var headers = this._createHeaders({'Content-Length': 0}, p);
  return this._sendEmptyRequest(p, headers, (response, ee) => {
    var parser = new xml.SaxParser(cb => {
      var object;
      var startContent = false;
      var startPrefix = false;
      var path;
      var currentChars = [];
      cb.onStartElementNS(elem => {
        if ( elem == 'Contents' ) {
          startContent = true;
          object = [{}];
          path = [];
        } else if (elem == 'CommonPrefixes') {
          startContent = false;
          startPrefix = true;
        } else if (startContent) {
          var ol = object.length;
          object[ol] = object[ol -1][elem] = {};
          path.push(elem);
        }
      });
      cb.onCharacters(chars => {
        if (startContent) {
          currentChars.push(chars);
        } else if (startPrefix) {
          ee.emit('prefix', chars);
        }
      });
      cb.onEndElementNS(elem => {
        if (startContent) {
          var ol = object.length;
          if (currentChars.length) {
            object[ol -2][path[ol -2]] = currentChars.join('');
            currentChars = [];
          }
          if (object.length == 1) {
            ee.emit('object', object[0]);
          }
          object.pop();
          path.pop();
        }
      });
    });
    response.on('data', chunk => {
      parser.parseString(chunk);
    });
    response.on('end', () => {
      ee.emit('complete', response);
    });
  });
}

S3.prototype.upload = function(path, destination, o) {
  var // Cleanup arguments
  args = parseLoadArgs.apply(this, arguments);

  var fileName = args.fileName;
  var p = extend({}, this.properties, {object: args.destination, method: 'PUT'}, args.o);
  var _readStream;
  var ee = new EventEmitter;
  var s3 = this;

  _readStream = fs.createReadStream(args.origin, { encoding: 'ascii' });
  _readStream.pause();

  fs.stat(path, (err, stats) => {
    if (err) {
      ee.emit('error', err);
    }
    var headers = s3._createHeaders({
        'Expect': '100-continue'
        , 'Content-Length': stats.size
        , 'Content-Type': p['Content-Type'] || mime.lookup(fileName)
      }, p, [
          'Cache-Control'
        , 'Content-Disposition'
        , 'Content-Encoding'
        , 'Expect'
        , 'Expires'
      ]);
    s3._connectToAws(p, headers, function() {
      var readStream = _readStream;
      var netStream = this;
      var progress = new Progress(stats.size, ee);
      netStream.on('data', data => { 
        // Convert the Buffer
          data = data.toString('utf8');
          // Continue
          if (data.indexOf('HTTP/1.1 100 Continue') == 0) {
            sys.pump(readStream, netStream, err => {
              if (err) {
                ee.emit('error', err);
              }
              readStream.destroy();
              netStream.end();
            });
            readStream.resume();
            
          // Complete
          } else if (data.indexOf('HTTP/1.1 200 OK') == 0) {
            // TODO: check MD5
            ee.emit('complete');
            
          // Error
          } else {
            ee.emit('error', data);
            //readStream.destroy();
            //netStream.end();
          }
      });
      readStream.on('data', data => {
        progress.add(data.length);
      });
    });
  });
  return ee;
}

S3.prototype.download = function(object, path, o) {
  var 
      args = parseLoadArgs.apply(this, arguments)
    ;
  object = args.origin;
  path = args.destination;
  o = args.o;
}

S3.prototype.head = function(object, o) {
  var p = extend({}, this.properties, o, {
      method: 'HEAD'
      , object
      , querystring: querystringify(o, ['versionId'])
    });

  var headers = this._createHeaders({'Content-Length': 0}, p);
  return this._sendEmptyRequest(p, headers, (response, ee) => {
    ee.emit('complete', response);
  });
}

S3.prototype.delete = function(object, o) {
  var p = extend({}, this.properties, o, {
      method: 'DELETE'
      , object
      , querystring: querystringify(o, ['versionId'])
    });

  var headers = this._createHeaders({'Content-Length': 0}, p);
  return this._sendEmptyRequest(p, headers, (response, ee) => {
    ee.emit('complete', response);
  });
}

// TODO: The statusCode can be 200 and the body can still be an error message
S3.prototype.copy = function(object, source, o) {
  var tmp = extend({}, this.properties, o);

  var p = extend(tmp, {
        method: 'PUT'
        , object
        , 'x-amz-copy-source': '%2F'+ encodeURIComponent(typeof source === 'string'? 
            tmp.bucket + '/' + source:
            source.bucket + '/' + source.object
          )
        , 'x-amz-metadata-directive': o? 'REPLACE' : 'COPY'
      });

  var headers = this._createHeaders({'Content-Length': 0}, p, ['x-amz-copy-source', 'x-amz-metadata-directive']);
  return this._sendEmptyRequest(p, headers, (response, ee) => {
    ee.emit('complete', response);
  });
}

S3.prototype.rename = function(source, newName, o) {
  var copy = this.copy(newName, source, o);
  var ee = new EventEmitter;
  var s3 = this;
  copy.on('complete', () => {
    var delet = s3.delete(source, o);
    delet.on('complete', response => {
      ee.emit('complete', response);
    });
    delet.on('error', renameError);
  });
  copy.on('error', renameError);
  return ee;

  function renameError(response, body) {
    ee.emit('error', response, body);
  }
}

S3.prototype._createHeaders = function(specific, p, filters) {
  filters = filters || [];
  if (!p.bucket) {
    p.bucket = p.object.split('/')[1];
    if(!p.bucket) {
      // TODO: AWS Access Keys are required as well
      throw 'Bucket name is missing'
    }
  }

  var headers = extend({
      Host: p.bucket + '.' + p.endpoint,
      Date: new Date().toUTCString()
    }, specific);

  var i = filters.length;

  var // Search for ACL headers
  prop;

  var acl = p.acl || p['x-amz-acl'];
  while (i--) {
    prop = filters[i];
    if (p[prop] != null) {
      headers[prop] = p[prop];  
    }
  }
  // Add acl headers if present
  if (acl) {
    headers['x-amz-acl'] = acl;
  }
  var canonicalizedAmzHeaders = this._canonicalizeAmzHeaders(headers);
  var stringToSign = this._createStringToSign(p, headers, canonicalizedAmzHeaders);
  var signature = this._signHeaders(p.secretKey, stringToSign);
  headers.Authorization = 'AWS ' + p.accessKey + ':' + signature;
  return headers;
}

S3.prototype._canonicalizeAmzHeaders = headers => {
  var canonicalizedHeaders = [];
  var key;
  var value;

  for (key in headers) {
		// Filter amazon headers
		if (key.toLowerCase().indexOf('x-amz-') == 0) {
			value = headers[key];
			if (value instanceof Array) {
				value = value.join(',');
			}
			canonicalizedHeaders.push(key.toString().toLowerCase() + ':' + value);
		}
	}
  canonicalizedHeaders.sort();
  return canonicalizedHeaders.length?
    canonicalizedHeaders.join('\n')+'\n' :
    '' ;
};

S3.prototype._createStringToSign = (p, headers, canonicalizedAmzHeaders) => {
  var // leave off the date in the string to sign if we have the amx date
  date = canonicalizedAmzHeaders.indexOf('x-amz-date') == -1?
        headers.Date :
    '';

  var contentType = headers['Content-Type'] || '';
  var md5 =  headers['Content-MD5'] || '';
  return stringToSign = 
		p.method + "\n" +
    md5 + '\n' +
    contentType + '\n' +
		date + '\n' +
		canonicalizedAmzHeaders +
		'/' + p.bucket + '/'+ (p.object || '') + (p.subresource || '');
};

S3.prototype._signHeaders = (secretKey, stringToSign) => {
  var hmac = crypto.createHmac('sha1', secretKey);
	hmac.update(stringToSign);
	return hmac.digest(encoding = 'base64');
}

S3.prototype._sendEmptyRequest = (p, headers, callback) => {
  var bucket = http.createClient(p.httpsOnly? p.httpsPort : p.httpPort, headers.Host, p.httpsOnly);
  var request = bucket.request(p.method, '/'+ (p.object || '') + (p.querystring || ''), headers);
  var ee = new EventEmitter;
  request.end();
  request.on('response', function(response) {
    if (response.statusCode == 200 || response.statusCode == 204) {
      callback.call(this, response, ee);
    } else {
      var body = '';
      response.on('data', chunk => {
        body += chunk;
      });
      response.on('end', () => {
        ee.emit('error', response, body);
      })
    }
  });
  return ee;
}

S3.prototype._connectToAws = (p, headers, callback) => {
  // DEBUG
  p.httpsOnly = false;
  // Resolve dns once for all
  dns.resolve4(headers.Host, (err, addresses) => {
    // TODO: should only emit an event here
    if (err) {
      throw err;
    }
    var netStream = new net.createConnection(p.httpsOnly? p.httpsPort : p.httpPort, addresses[0]);
    if (p.httpsOnly) {
      netStream.setSecure(true);
    }
    netStream.on('connect', () => {
      console.log('CONNECT')
      var headerString = [p.method + ' /' + p.object + " HTTP/1.1"];
      var key;
      var value;
      for(key in headers) {
        value = headers[key];
  			if (value != null && value != '') {
  				headerString.push(key + ': ' + value);
  			}
  		}
      netStream.write(headerString.join('\n')+'\n\n');
      callback.apply(netStream);
    });
  });
}

function extend(obj) {
  var target = arguments[0] || {};
  var length = arguments.length;
  var options;
  var name;
  var i = 0;

  while (++i < length) {
    if ( (options = arguments[ i ]) != null ) {
      for (name in options) {
        target[name] = options[name];
      }
    }
  }
  return target;
}

function querystringify(obj, filter) {
  if (!obj || !filter || !filter.length) {
    return '';
  }
  var i = filter.length;
  var key;
  var filteredObj = {};
  while ( i-- ) {
    key = filter[i];
    if (obj[key] != null && obj[key] != '') {
      filteredObj[key] = obj[key];
    }
  }
  return '?' + querystring.stringify(filteredObj);
}

function parseLoadArgs(origin, destination, o) {
  if (!o && typeof destination === 'object') {
    o = destination;
  }
  var fileName = origin.substr(origin.lastIndexOf('/') +1);
  if (!destination || typeof destination === 'object') {
    destination = fileName;
  // If the destination ends with a '/', append the original file name.
  } else if (destination[destination.length -1] == '/') {
     destination += fileName;
  }
  return {
      o
    , origin
    , destination
    , fileName
  };
}

Progress = function(totalLength, ee) {
  this.total = totalLength;
  this.ee = ee;
  this.sent = 0;
}

Progress.prototype.add = function(dataLength) {
  this.sent += dataLength;
  console.log('SENT: '+this.sent)
  this.ee.emit('progress', this.sent / this.total);
}

exports.S3 = S3;