var http = require("http");
var https = require("https");
var url = require("url");
var Q = require("q");

function startServer(port, serverApi) {
    var deferred = Q.defer();
    var server = http.createServer(function (req, res) {
        var handler = url.parse(req.url).pathname;
        if (serverApi[handler]) {
            serverApi[handler].apply(null, [req, res]);
        } else if (serverApi.index) {
            if (typeof serverApi.index == "function") {
                serverApi.index.apply(null, [req, res]);
            } else {
                responseJson(res, JSON.stringify(serverApi.index));
            }
        } else {
            responseNoSuchApi(res);
        }
    });
    server.on("error", function(err) {
        deferred.reject(err);
    });
    server.listen(port, function() {
        deferred.resolve(server);
    });
    server.setTimeout(0);
    return deferred.promise;
}

function parseQuery(req) {
    return url.parse(req.url, true).query;
}

function readBody(req, raw) {
    var deferred = Q.defer();
    if (hasBody(req)) {
        var buffers = [];
        req.on("data", function (chunk){
            buffers.push(chunk);
        });
        req.on("end", function() {
            var body = Buffer.concat(buffers);
            if (raw) { } else body = body.toString();
            deferred.resolve(body);
        });
        req.on("error", function(err) {
            deferred.reject(err);
        });
    } else {
        deferred.resolve("");
    }
    return deferred.promise;

    function hasBody(req) {
        return "transfer-encoding" in req.headers || "content-length" in req.headers;
    }
}

function responseData(res, contentType, data) {
    res.writeHead(200, {"Content-Type": contentType, "Access-Control-Allow-Origin": "*"});
    res.end(data);
}

function responseJson(res, data) {
    res.writeHead(200, {"Content-Type": "text/json;charset=utf-8", "Access-Control-Allow-Origin": "*"});
    res.end(data);
    return true;
}

function responseHtml(res, data) {
    res.writeHead(200, {"Content-Type": "text/html;charset=utf-8", "Access-Control-Allow-Origin": "*"});
    res.end(data);
    return true;
}

function responseText(res, data) {
    res.writeHead(200, {"Content-Type": "text/plain;charset=utf-8", "Access-Control-Allow-Origin": "*"});
    res.end(data);
    return true;
}

function responseNoSuchApi(res) {
    res.writeHead(400, {"Content-Type": "text/plain;charset=utf-8", "Access-Control-Allow-Origin": "*"});
    res.end('{"err_no":-1,"stack":["no such Api"]}');
    return false;
}

function responseServerError(res) {
    res.writeHead(500, {"Access-Control-Allow-Origin": "*"});
    res.end('{"err_no":-1,"msg":"server error"}');
    return false;
}

function request(targetUrl, method, postBody, headers, needRawResult, needResponseHeader) {
    var deferred = Q.defer();

    var options = url.parse(targetUrl);
    options.headers = headers || {};
    method = (method || "GET").toUpperCase();
    if (method === "POST") {
        var postBuffer = new Buffer(postBody);
        options.method = "POST";
        options.headers["Content-Length"] = postBuffer.length;
        if (options.headers.hasOwnProperty("Content-type")) { } else {
            options.headers["Content-type"] = "application/x-www-form-urlencoded";
        }
    }
    var client = (options.protocol == "https:") ? https : http;
    var req = client.request(options, function(res) {
        var chunks = [];
        res.on("data", function (chunk) {
            chunks.push(chunk);
        });
        res.on("end", function() {
            var raw = Buffer.concat(chunks);
            var body = (needRawResult) ? raw : raw.toString();
            if (needResponseHeader) {
                return deferred.resolve({
                    headers: res.headers,
                    body: body
                });
            } else {
                return deferred.resolve(body);
            }
        });
        res.on("error", function(err) {
            deferred.reject(err);
        });
    });
    req.on("error", function(err) {
        deferred.reject(err);
    });
    if (method == "POST") {
        req.write(postBuffer);
    }
    req.end();
    return deferred.promise;
}

function createTextResponser(res) {
    return function(data) {
        responseText(res, data);
    }
}

function createHtmlResponser(res) {
    return function(html) {
        responseHtml(res, html);
    }
}

function createOKResponser(res) {
    return function() {
        responseJson(res, '{"err_no":0}');
    }
}

function createResultResponser(res) {
    return function(result) {
        responseJson(res, JSON.stringify({
            err_no: 0,
            result: result
        }));
    }
}

function createPageResultResponser(res) {
    return function(result) {
        responseJson(res, JSON.stringify({
            err_no: 0,
            total: result.total,
            offset: result.offset,
            count: result.count,
            result: result.docs
        }));
    }
}

function createErrorResponser(res) {
    return function(err) {
        responseJson(res, JSON.stringify({
            err_no: -1,
            stack: err.stack.split("\n")
        }));
    }
}


function HttpRpcClient(remotes) {

    if (!(remotes instanceof Array)) remotes = [remotes];
    validateRemotes(remotes);

    var mRemotes = remotes;
    var mNextStartIndex = 0;

    function validateRemotes(remotes) {
        if (remotes instanceof Array) { } else throw new Error("remotes is not array");
        remotes.forEach(function(remote) {
            if (typeof remote.host !== "string") throw new Error("host is missing or is not string");
            if (typeof remote.port !== "number") throw new Error("port is missing or is not number");
        });
    }

    this.call = this.callAny = function(method, params, raw) {
        var startIndex = mNextStartIndex;
        ++mNextStartIndex;
        if (mNextStartIndex === mRemotes.length) mNextStartIndex = 0;
        var data = "";
        if (params !== undefined) {
            data = (raw) ? params : JSON.stringify(params);
        }
        return loopRequest(method, data, startIndex, 0).then(function(data) {
            if (raw) return data;
            var res = JSON.parse(data);
            if (res.err_no !== 0) throw new Error(res.msg || res.stack.join("\n"));
            return res.result;
        })
    };

    this.callAll = function(method, params, raw) {
        var promises = mRemotes.map(function(remote) {
            var data = (raw) ? params : JSON.stringify(params);
            return request(remote, method, data).then(function(data) {
                if (raw) return data;
                var res = JSON.parse(data);
                if (res.err_no !== 0) throw new Error(res.msg || res.stack.join("\n"));
                return res.result;
            });
        });
        return Q.all(promises);
    };

    function loopRequest(path, data, remoteIndex, failCount) {
        return request(mRemotes[remoteIndex], path, data).catch(function(err) {
            ++failCount;
            if (failCount >= mRemotes.length) throw new Error("service is not available, err=" + err.stack);
            ++remoteIndex;
            if (remoteIndex === mRemotes.length) remoteIndex = 0;
            return loopRequest(path, data, remoteIndex, failCount);
        });
    }

    function request(remote, path, data) {
        var deferred = Q.defer();

        var postBuffer = new Buffer(data);
        var options = {
            protocol: "http:",
            host: remote.host,
            port: remote.port,
            path: path,
            method: "POST",
            headers: {
                "Content-Length": postBuffer.length
            }
        };
        var req = http.request(options, function (res) {
            var chunks = [];
            res.on("data", function (chunk) {
                chunks.push(chunk);
            });
            res.on("end", function () {
                var raw = Buffer.concat(chunks);
                var body = raw.toString();
                return deferred.resolve(body);
            });
            res.on("error", function (err) {
                deferred.reject(err);
            });
        });
        req.on("error", function (err) {
            deferred.reject(err);
        });
        req.write(postBuffer);
        req.end();

        return deferred.promise;
    }
}

exports.startServer = startServer;
exports.parseQuery = parseQuery;
exports.readBody = readBody;
exports.responseData = responseData;
exports.responseJson = responseJson;
exports.responseHtml = responseHtml;
exports.responseText = responseText;
exports.responseBadRequest = responseNoSuchApi;
exports.responseServerError = responseServerError;
exports.request = request;
exports.createTextResponser = createTextResponser;
exports.createHtmlResponser = createHtmlResponser;
exports.createOKResponser = createOKResponser;
exports.createResultResponser = createResultResponser;
exports.createPageResultResponser = createPageResultResponser;
exports.createErrorResponser = createErrorResponser;
exports.HttpRpcClient = HttpRpcClient;
