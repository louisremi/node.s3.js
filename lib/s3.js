var
  sys = require('sys')
  , http = require('http')
  , crypto = require('crypto')
  , EventEmitter = require('events').EventEmitter
  , querystring = require('querystring')
  // npm packages
  , xml = require('xml')
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
  o = o || (typeof prefix === 'object'? prefix : {});
  if (typeof prefix === 'string') {
    o.prefix = prefix;
  }
  var
    // merge properties
    p = extend(this.properties, o, {
        method: 'GET'
        , querystring: querystringify(o, ['prefix', 'delimiter'])
        , object: ''
      })
    , headers = this._createHeaders({'Content-Length': 0}, p)
    , request = this._sendEmptyRequest(p, headers)
    , list = new EventEmitter
    , s3 = this
    ;
  request.on('response', function(response) {
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
          startPrefix = false;
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
          list.emit('prefix', chars);
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
            list.emit('object', object[0]);
          }
          object.pop();
          path.pop();
        }
      });
    });
    response.on('data', function(chunk) {
      parser.parseString(chunk);
    });
    response.on('end', response.statusCode == 200?
      function() {
        list.emit('complete');
      } : function() {
        console.log(response.statusCode)
        list.emit('error', response.headers);
      }
    );
    /*response.on('error', function(err) {
      list.emit('error', err);
    });*/
  });
  return list;
}

S3.prototype.head = function(object, options) {
  
}

S3.prototype._createHeaders = function(specific, p, filters) {
  filters = filters || [];
  if (!p.bucket) {
    p.bucket = p.object.split('/')[1];
    if(!p.bucket) {
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
    ;
  while (i--) {
    prop = filters[i];
    if (p[prop] != null) {
      headers[prop] = p[prop];  
    }
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

S3.prototype._sendEmptyRequest = function(p, headers) {
  var 
    bucket = http.createClient(p.httpsOnly? p.httpsPort : p.httpPort, headers.Host, p.httpsOnly)
    request = bucket.request(p.method, '/'+ (p.object || '') + (p.querystring || ''), headers)
    ;
  request.end();
  return request;
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
  if (!filter || !filter.length) {
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

exports.S3 = S3;