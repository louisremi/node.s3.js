var
  sys = require('sys')
  , dns = require('dns')
  , net = require('net')
  , fs = require('fs')
  , http = require('http')
  , crypto = require('crypto')
  , EventEmitter = require('events').EventEmitter
  , querystring = require('querystring')
  // npm packages
  , xml = require('xml')
  , mime = require('mime')
  // exports
  , S3
  ;

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
  var
    p = extend({}, this.properties, o, {
        method: 'GET'
        , querystring: querystringify(o, ['prefix', 'delimiter'])
        , object: ''
      })
    , headers = this._createHeaders({'Content-Length': 0}, p)
    ;
  return this._sendEmptyRequest(p, headers, function(response, ee) {
    var parser = new xml.SaxParser(function(cb) {
      var 
        object
        , startContent = false
        , startPrefix = false
        , path
        , currentChars = []
        ;
      cb.onStartElementNS(function(elem) {
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
      cb.onCharacters(function(chars) {
        if (startContent) {
          currentChars.push(chars);
        } else if (startPrefix) {
          ee.emit('prefix', chars);
        }
      });
      cb.onEndElementNS(function(elem) {
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
    response.on('data', function(chunk) {
      parser.parseString(chunk);
    });
    response.on('end', function() {
      ee.emit('complete', response);
    });
  });
}

S3.prototype.upload = function(path, destination, o) {
  var 
      // Cleanup arguments
      args = parseLoadArgs.apply(this, arguments)
    , fileName = args.fileName
    , p = extend({}, this.properties, {object: args.destination, method: 'PUT'}, args.o)
    , _readStream
    , ee = new EventEmitter
    , s3 = this
    ;
  
  _readStream = fs.createReadStream(args.origin, { encoding: 'ascii' });
  _readStream.pause();
  
  fs.stat(path, function(err, stats) {
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
      var
        readStream = _readStream
        , netStream = this
        , progress = new Progress(stats.size, ee)
        ;
      this.on('data', function(data) {
        // Convert the Buffer
        data = data.toString('utf8');
        // Continue
        if (data.indexOf('HTTP/1.1 100 Continue') == 0) {
          readStream.resume();
          
        // Complete
        } else if (data.indexOf('HTTP/1.1 200 OK') == 0) {
          // TODO: check MD5
          ee.emit('complete');
          
        // Error
        } else {
          console.log('HEADERS ERROR')
          ee.emit('error', data);
          //readStream.destroy();
          //netStream.end();
        }
      });
      
      readStream.on('data', function(data) {
        console.log('WRITE')
        // if stream returns false, then we need to pause reading from file and wait for the content to 
	      // sent across to the remote server
	      if(!netStream.write(data)){
	        readStream.pause();
	      }
        // update progress
        progress.add(data.length);
      });
      
      readStream.on('end', function(){
        console.log('END NETSTREAM')
        // Apparently the readStream doesn't need to be destroyed/shouldn't be destroyed 
        //readStream.destroy();
        netStream.end();
      });
			
	    readStream.on('error', function(exception){
        console.log('READSTREAM ERROR')
        ee.emit('error', exception);
        readStream.destroy();
        netStream.end();
      });
      
      this.on('error', function(exception) { console.log('NETSTREAM ERROR'); ee.emit('error', exception); });
      this.on('drain', function(){ readStream.resume(); });
	    //this.on('close', function(hadError){ console.log('NETSTREAM CLOSE'); /*if (!hadError) { ee.emit('complete'); }*/ });
	    this.on('end', function(){ netStream.end(); });
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
  var
    p = extend({}, this.properties, o, {
        method: 'HEAD'
        , object: object
        , querystring: querystringify(o, ['versionId'])
      })
    , headers = this._createHeaders({'Content-Length': 0}, p)
    ;
  return this._sendEmptyRequest(p, headers, function(response, ee) {
    ee.emit('complete', response);
  });
}

S3.prototype.delete = function(object, o) {
  var
    p = extend({}, this.properties, o, {
        method: 'DELETE'
        , object: object
        , querystring: querystringify(o, ['versionId'])
      })
    , headers = this._createHeaders({'Content-Length': 0}, p)
    ;
  return this._sendEmptyRequest(p, headers, function(response, ee) {
    ee.emit('complete', response);
  });
}

// TODO: The statusCode can be 200 and the body can still be an error message
S3.prototype.copy = function(object, source, o) {
  var
    tmp = extend({}, this.properties, o)
    , p = extend(tmp, {
        method: 'PUT'
        , object: object
        , 'x-amz-copy-source': '%2F'+ encodeURIComponent(typeof source === 'string'? 
            tmp.bucket + '/' + source:
            source.bucket + '/' + source.object
          )
        , 'x-amz-metadata-directive': o? 'REPLACE' : 'COPY'
      })
    , headers = this._createHeaders({'Content-Length': 0}, p, ['x-amz-copy-source', 'x-amz-metadata-directive'])
    ;
  return this._sendEmptyRequest(p, headers, function(response, ee) {
    ee.emit('complete', response);
  });
}

S3.prototype.rename = function(source, newName, o) {
  var 
    copy = this.copy(newName, source, o)
    , ee = new EventEmitter
    , s3 = this
    ;
  copy.on('complete', function() {
    var delet = s3.delete(source, o);
    delet.on('complete', function(response) {
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
  var 
    headers = extend({
        Host: p.bucket + '.' + p.endpoint,
        Date: new Date().toUTCString()
      }, specific)
    , i = filters.length
    , prop
    // Search for ACL headers
    , acl = p.acl || p['x-amz-acl']
    ;
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
  var 
    canonicalizedAmzHeaders = this._canonicalizeAmzHeaders(headers)
    , stringToSign = this._createStringToSign(p, headers, canonicalizedAmzHeaders)
    , signature = this._signHeaders(p.secretKey, stringToSign)
    ;
  headers.Authorization = 'AWS ' + p.accessKey + ':' + signature;
  return headers;
}

S3.prototype._canonicalizeAmzHeaders = function(headers){
	var 
    canonicalizedHeaders = []
	  , key
    , value
    ; 
	
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

S3.prototype._createStringToSign = function(p, headers, canonicalizedAmzHeaders){
	var 
    // leave off the date in the string to sign if we have the amx date
    date = canonicalizedAmzHeaders.indexOf('x-amz-date') == -1?
		  headers.Date :
      ''
    , contentType = headers['Content-Type'] || ''
    , md5 =  headers['Content-MD5'] || ''
    ;
  return stringToSign = 
		p.method + "\n" +
    md5 + '\n' +
    contentType + '\n' +
		date + '\n' +
		canonicalizedAmzHeaders +
		'/' + p.bucket + '/'+ (p.object || '') + (p.subresource || '');
};

S3.prototype._signHeaders = function(secretKey, stringToSign) {
  var hmac = crypto.createHmac('sha1', secretKey);
	hmac.update(stringToSign);
	return hmac.digest(encoding = 'base64');
}

S3.prototype._sendEmptyRequest = function(p, headers, callback) {
  var 
    bucket = http.createClient(p.httpsOnly? p.httpsPort : p.httpPort, headers.Host, p.httpsOnly)
    , request = bucket.request(p.method, '/'+ (p.object || '') + (p.querystring || ''), headers)
    , ee = new EventEmitter
    ;
  request.end();
  request.on('response', function(response) {
    if (response.statusCode == 200 || response.statusCode == 204) {
      callback.call(this, response, ee);
    } else {
      var body = '';
      response.on('data', function(chunk) {
        body += chunk;
      });
      response.on('end', function() {
        ee.emit('error', response, body);
      })
    }
  });
  return ee;
}

S3.prototype._connectToAws = function(p, headers, callback) {
  // DEBUG
  p.httpsOnly = false;
  // Resolve dns once for all
  dns.resolve4(headers.Host, function(err, addresses) {
    // TODO: should only emit an event here
    if (err) {
      throw err;
    }
    var netStream = new net.createConnection(p.httpsOnly? p.httpsPort : p.httpPort, addresses[0]);
    if (p.httpsOnly) {
      netStream.setSecure(true);
    }
    netStream.on('connect', function() {
      var 
        headerString = [p.method + ' /' + p.object + " HTTP/1.1"]
        , key, value
        ;
      for(key in headers) {
        value = headers[key];
  			if (value != null && value != '') {
  				headerString.push(key + ': ' + value);
  			}
  		}
      headerString.push('');
      netStream.write(headerString.join('\n'));
      callback.apply(netStream);
    });
  });
}

function extend(obj) {
  var target = arguments[0] || {}, length = arguments.length, options, name, i = 0;
  
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
  var i = filter.length
    , key
    , filteredObj = {}
    ;
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
      o: o
    , origin: origin
    , destination: destination
    , fileName: fileName
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