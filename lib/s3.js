var
  sys = require('sys')
  , http = require('http')
  , crypto = require('crypto')
  , EventEmitter = require('events').EventEmitter
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

S3.prototype = new EventEmitter;
S3.prototype.constructor = S3;

S3.prototype.listObjects = function(prefix, options) {
  var
    // merge properties
    p = extend({method: 'GET', objectName: ''}, this.properties, options || (typeof prefix === 'string'? {prefix: prefix} : prefix) )
    , headers = this._createHeaders({'Content-Length': 0}, p, ['prefix', 'delimiter'])
    , request = this._createRequest(p, headers)
    ;
  request.end();
  request.on('response', function(response) {
    sys.log(response.statusCode + '  ' + response.headers)
    var body = '';
    response.on('data', function(chunk) {
      body += chunk;
    });
    response.on('end', response.statusCode == 200?
      function() {
        sys.log(body)
        //s3.emit('complete');
      } : function() {
        sys.log(body)
        //s3.emit('error', response.statusCode + body);
      }
    );
  });
}

S3.prototype.head = function(object, options) {
  
}

S3.prototype._createHeaders = function(specific, p, filters) {
  // filters = filters || [];
  if (!p.bucket) {
    throw 'bucket name is missing';
  }
  var 
    headers = extend({
        Host: p.bucket + '.' + p.endpoint,
        Date: new Date().toUTCString()
      }, specific)
    , i = filters.length;
  while (i--) {
    if (p[i] != null) {
      headers[filters[i]] = p[i];  
    }
  }
  var 
    canonicalizedAmzHeaders = this._canonicalizeAmzHeaders(headers)
    , stringToSign = this._createStringToSign(p, headers, canonicalizedAmzHeaders)
    , signature = this._signHeaders(p.secretKey, stringToSign)
    ;
  console.log(p.accessKey)
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
		if (key.indexOf('x-amz-') == 0) {
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
    , md5 =  headers['Content-MD5'] || '';
    ;
	
	return stringToSign = 
		p.method + "\n" +
    md5 + '\n' +
    contentType + '\n' +
		date + '\n' +
		canonicalizedAmzHeaders +
		'/' + p.bucket + '/' + p.objectName;
};

S3.prototype._signHeaders = function(secretKey, stringToSign) {
  var hmac = crypto.createHmac('sha1', secretKey);
	hmac.update(stringToSign);
	return hmac.digest(encoding = 'base64');
}

S3.prototype._createRequest = function(p, headers) {
  var bucket = http.createClient(80, headers.Host);
  return bucket.request(p.method, '/' + p.objectName, headers);
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

exports.S3 = S3;