var Q = require("q");
var grpc = require("grpc");
var Log = require("./log");

function RpcServer(options) {
    if (typeof options.name !== "string") throw new Error("name is missing");
    if (typeof options.protoPath !== "string") throw new Error("protoPath is missing");
    if (typeof options.port !== "number") throw new Error("port is missing");

    var mOptions = options;

    this.start = function() {
        Log.init(mOptions.name);
        return Q.all([
            mOptions.mongodbOption ? require("./mongodb").connectMongoDB(mOptions.mongodbOption) : null,
            mOptions.redisOption ? require("./redis").connectRedis(mOptions.redisOption) : null
        ]).then(function () {
            handleExit();
            var proto = grpc.load(__dirname + "/" + mOptions.protoPath);
            var server = new grpc.Server();
            server.addService(proto[mOptions.name].service, createServiceImpl(mOptions.service));
            Log.i("RpcServer listening at port: " + mOptions.port);
            server.bind("0.0.0.0:" + mOptions.port, grpc.ServerCredentials.createInsecure());
            server.start();
        });
    };

    function handleExit() {
        process.on("uncaughtException", function (err) {
            Log.e("Got an uncaughtException.\nerr = " + err + "\nstack = " + err.stack);
            process.exit(-1);
        });

        var exitSignals = ["SIGINT", "SIGTERM"];
        exitSignals.forEach(function (signal) {
            process.on(signal, function () {
                Log.e("Got a " + signal + " signal.");
                process.exit(-1);
            });
        });
    }

    function createServiceImpl(service) {
        var wrappedService = {};
        for (var key in service) {
            if (typeof service[key] === "function") {
                wrappedService[key] = wrapService(service[key]);
            }
        }
        return wrappedService;
    }

    function wrapService(fun){
        return function(call, callback) {
            try {
                var ret = fun.call(null, call.request);
                if (Q.isPromise(ret)) {
                    ret.then(function(res) {
                        callback(null, res);
                    }).catch(function(err) {
                        callback(err);
                    });
                } else {
                    callback(null, ret);
                }
            } catch(err) {
                callback(err);
            }
        };
    }

}

function RpcClient(options) {
    if (typeof options.name !== "string") throw new Error("name is missing");
    if (typeof options.protoPath !== "string") throw new Error("protoPath is missing");
    if (options.remotes === undefined) throw new Error("remotes is missing");
    if (!(options.remotes instanceof Array)) options.remotes = [options.remotes];
    if (options.remotes.length === 0) throw new Error("remotes is empty");
    options.remotes.forEach(function(remote) {
        if (typeof remote.host !== "string") throw new Error("host is missing");
        if (typeof remote.port !== "number") throw new Error("port is missing");
    });

    var mProto = grpc.load(__dirname + "/" + options.protoPath);
    if (options.package !== undefined) {
        mProto = mProto[options.package];
    }
    var mClients = options.remotes.map(function(remote) {
        var client = new mProto[options.name](remote.host + ":" + remote.port, grpc.credentials.createInsecure());
        return client;
    });
    var mNextStartIndex = 0;
    var mSelf = this;

    var client = mClients[0];
    var reservedMethodNames = ["makeUnaryRequest", "makeClientStreamRequest", "makeServerStreamRequest", "makeBidiStreamRequest", "close", "getChannel", "waitForReady"];
    for (var key in client) {
        if (typeof client[key] === "function" && reservedMethodNames.indexOf(key) === -1) {
            createRemoteMethod(key);
        }
    }

    this.call = function(method, args) {
        var startIndex = mNextStartIndex;
        ++mNextStartIndex;
        if (mNextStartIndex === mClients.length) mNextStartIndex = 0;
        return loopInvoke(method, args, startIndex, 0);
    };

    this.callAll = function(method, args) {
        var promises = mClients.map(function(client) {
            return invoke(client, method, args);
        });
        return Q.all(promises);
    };

    function loopInvoke(method, args, remoteIndex, failCount) {
        return invoke(mClients[remoteIndex], method, args).catch(function(err) {
            ++failCount;
            if (failCount >= mClients.length) throw new Error(err.stack);
            ++remoteIndex;
            if (remoteIndex === mClients.length) remoteIndex = 0;
            return loopInvoke(method, args, remoteIndex, failCount);
        });
    }

    function invoke(client, method, args) {
        var deferred = Q.defer();
        client[method](args, function(err, res) {
            if (err) return deferred.reject(err);
            return deferred.resolve(res);
        });
        return deferred.promise;
    }

    function createRemoteMethod(method) {
        mSelf[method] = function(args) {
            return mSelf.call(method, args);
        }
    }
}

exports.RpcServer = RpcServer;
exports.RpcClient = RpcClient;
