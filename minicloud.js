var Q = require("q");
var Process = require("./process");
var Log = require("./log");
var Http = require("./http");

function startServer(options) {
    var name = Process.getProcessName();
    Log.init(name, true);
    var port = parseInt(process.argv[2]);
    return Http.startServer(port, options.serverApi).then(function(server) {
        Log.i("server start listening at port: " + port);
        Process.handleExit(server, false);
        return Q.all([
            options.mysqlOption ? require("./mysql").connectMySql(options.mysqlOption) : null,
            options.redisOption ? require("./redis").connectRedis(options.redisOption) : null,
            options.mongodbOption ? require("./mongodb").connectMongoDB(options.mongodbOption) : null
        ]);
    }).catch(function(err) {
        Log.fatal(name + " start failed, err=\n" + err.stack);
    });
}

function startDaemon(options) {
    var name = Process.getProcessName();
    Log.init(name, true);
    Process.handleExit(null, false);
    return Q.all([
        options.mysqlOption ? require("./mysql").connectMySql(options.mysqlOption) : null,
        options.redisOption ? require("./redis").connectRedis(options.redisOption) : null,
        options.mongodbOption ? require("./mongodb").connectMongoDB(options.mongodbOption) : null
    ]).then(function() {
        options.tasks.forEach(function(task) {
            if (typeof task.run == "function") {
                doDaemon(task.run, task.timegap || 0);
            }
        });
    }).catch(function(err) {
        Log.fatal(name, "daemon start failed, err=\n" + err.stack);
    });
}

function doDaemon(run, timegap) {
    loop();
    function loop() {
        try {
            var ret = run();
            if (Q.isPromise(ret)) {
                ret.delay(timegap).then(loop).catch(function(err) {
                    Log.e("daemon run failed, err = \n" + err.stack);
                    loop();
                });
            } else {
                Q.delay(null, timegap).then(loop);
            }
        } catch(err) {
            Log.e("daemon run failed, err = \n" + err.stack);
            setTimeout(loop, timegap);
        }
    }
}

exports.startServer = startServer;
exports.startDaemon = startDaemon;