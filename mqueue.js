/**
 * Created by beiying on 17/4/6.
 */
var cluster = require("cluster");
var Q = require("q");
var Log = require("./log");
var Process = require("./process");
var Http = require("./http");

var RESTART_DELAY = 5000;

var mMqueueServer = "http://mqueue.api.jndroid.com";

function setMessageServer(url) {
    mMqueueServer = url;
}

function detail(topic, start, end) {
    var url = mMqueueServer + "/detail?topic=" + topic;
    if (start) url +="&start=" + start;
    if (end) url += "&end=" + end;
    return Http.request(url).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error(res.err_no);
        return res.result;
    });
}

function publish(topic, message) {
    return Http.request(mMqueueServer + "/publish?topic=" + topic, "POST", message).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error();
    });
}

function publishMore(topic, messages) {
    return Http.request(mMqueueServer + "/publishMore?topic=" + topic, "POST", JSON.stringify(messages)).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error();
    });
}

function subscribe(topic) {
    return Http.request(mMqueueServer + "/subscribe?topic=" + topic).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error();
        return res.result;
    });
}

function trySubscribe(topic) {
    return Http.request(mMqueueServer + "/subscribe/try?topic=" + topic).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error();
        return res.result;
    });
}

function subscribeMore(topic, limit) {
    return Http.request(mMqueueServer + "/subscribeMore?topic=" + topic + "&limit=" + limit).then(function(data) {
        var res = JSON.parse(data);
        if (res.err_no != 0) throw new Error();
        return res.result;
    });
}

function startMqueueDaemon(options) {
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
        options.handlers.forEach(function(handler) {
            startPolling(handler.topic, handler.batch_limit, handler.run);
        });
        return false;
    }).catch(function(err) {
        Log.fatal(name, "worker start failed, err=\n" + err.stack);
    });
}

function startPolling(topic, limit, run) {
    var TIMEGAP = 0;
    if (limit === 1) {
        pollOne();
    } else {
        pollMore();
    }

    function pollOne() {
        subscribe(topic).then(function(message) {
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
        subscribeMore(topic, limit).then(function(messages) {
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

exports.setMessageServer = setMessageServer;
exports.detail = detail;
exports.publish = publish;
exports.publishMore = publishMore;
exports.subscribe = subscribe;
exports.trySubscribe = trySubscribe;
exports.subscribeMore = subscribeMore;
exports.startMqueueDaemon = startMqueueDaemon;