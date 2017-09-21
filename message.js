var cluster = require("cluster");
var Q = require("q");
var Log = require("./log");
var Process = require("./process");
var HttpRpcClient = require("./http").HttpRpcClient;

function MessageClient(remotes) {

    var mHttpRpcClient = new HttpRpcClient(remotes);

    this.publish = function(topic, message) {
        return mHttpRpcClient.call("/publish?topic=" + encodeURIComponent(topic), message, true);
    };

    this.publishMore = function(topic, messages) {
        return mHttpRpcClient.call("/publishMore?topic=" + encodeURIComponent(topic), messages);
    };

    this.subscribe = function(topic) {
        return mHttpRpcClient.call("/subscribe?topic=" + encodeURIComponent(topic), {});
    };

    this.trySubscribe = function(topic) {
        return mHttpRpcClient.call("/subscribe/try?topic=" + encodeURIComponent(topic), {});
    };

    this.subscribeMore = function(topic, limit) {
        return mHttpRpcClient.call("/subscribeMore?topic=" + encodeURIComponent(topic) + "&limit=" + limit, {});
    };

    this.trySubscribeMore = function(topic, limit) {
        return mHttpRpcClient.call("/subscribeMore/try?topic=" + encodeURIComponent(topic) + "&limit=" + limit, {});
    };

    this.detail = function(topic, start, end) {
        var path = "/detail?topic=" + encodeURIComponent(topic);
        if (start) path +="&start=" + start;
        if (end) path += "&end=" + end;
        return mHttpRpcClient.call(path, {});
    };

    this.addHub = function(inputTopic, outputTopic) {
        var path = "/addHub?inputTopic=" + encodeURIComponent(inputTopic) + "&outputTopic=" + encodeURIComponent(outputTopic);
        return mHttpRpcClient.call(path, {});
    };

    this.delHub = function(inputTopic, outputTopic) {
        var path = "/delHub?inputTopic=" + encodeURIComponent(inputTopic) + "&outputTopic=" + encodeURIComponent(outputTopic);
        return mHttpRpcClient.call(path, {});
    };

    this.rename = function(oldTopic, newTopic) {
        var path = "/rename?oldTopic=" + encodeURIComponent(oldTopic) + "&newTopic=" + encodeURIComponent(newTopic);
        return mHttpRpcClient.call(path, {});
    }
}

var RESTART_DELAY = 5000;

function startDaemon(options) {
    return (cluster.isMaster) ? startMasterDaemon(options) : startWorkerDaemon(options);
}

function startMasterDaemon(options) {
    var name = Process.getProcessName();
    Log.init(name, options.debug);
    Process.handleExit(null, true);
    var deferred = Q.defer();
    setTimeout(createWorker, 0);
    return deferred.promise;

    function createWorker() {
        var worker = cluster.fork();
        Log.i("create worker process, pid = " + worker.process.pid);
        cluster.on("exit", function(worker, code, signal) {
            Log.i("worker(pid=" + worker.process.pid + " exit with code=" + code + ", signal=" + signal + ", will restart after 5 seconds");
            setTimeout(function() {
                cluster.fork();
            }, RESTART_DELAY);
        });
        process.on("exit", function() {
            for (var id in cluster.workers) {
                cluster.workers[id].process.kill();
            }
        });
        deferred.resolve(true);
    }
}

function startWorkerDaemon(options) {
    var name = Process.getProcessName();
    Log.init(name, options.debug);
    Log.i(name, "worker deamon started. options =\n" + JSON.stringify(options, true, 2));
    Process.handleExit(null, false);

    return Q.all([
        options.mysqlOption ? require("./mysql").connectMySql(options.mysqlOption) : null,
        options.redisOption ? require("./redis").connectRedis(options.redisOption) : null,
        options.mongodbOption ? require("./mongodb").connectMongoDB(options.mongodbOption) : null
    ]).then(function() {
        var client = new MessageClient(options.remotes);
        options.handlers.forEach(function(handler) {
            startPolling(client, handler.topic, handler.batch_limit, handler.run);
        });
        return false;
    }).catch(function(err) {
        Log.fatal(name, "worker start failed, err=\n" + err.stack);
    });
}

function startPolling(client, topic, limit, run) {
    var TIMEGAP = 1000;
    if (limit === 1) {
        pollOne();
    } else {
        pollMore();
    }

    function pollOne() {
        client.subscribe(topic).then(function(message) {
            if (message === null) {
                setTimeout(pollOne, TIMEGAP);
                return;
            }
            var ret = run([message]);
            if (Q.isPromise(ret)) {
                ret.then(pollOne).catch(function(err) {
                    Log.e("handle messages failed, err = \n" + err.stack);
                    pollOne();
                });
            } else {
                setTimeout(pollOne, 0);
            }
        }).catch(function(err) {
            Log.e("subscribe message failed, err = \n" + err.stack);
            setTimeout(pollOne, TIMEGAP);
        });
    }

    function pollMore() {
        client.subscribeMore(topic, limit).then(function(messages) {
            if (messages.length === 0) {
                setTimeout(pollMore, TIMEGAP);
                return;
            }
            var ret = run(messages);
            if (Q.isPromise(ret)) {
                ret.then(pollMore).catch(function(err) {
                    Log.e("handle messages failed, err = \n" + err.stack);
                    pollMore();
                });
            } else {
                setTimeout(pollMore, 0);
            }
        }).catch(function(err) {
            Log.e("subscribe message failed, err = \n" + err.stack);
            setTimeout(pollMore, TIMEGAP);
        });
    }
}
exports.MessageClient = MessageClient;
exports.startDaemon = startDaemon;