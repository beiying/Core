var Q = require("q");

var Log = require("./log");
var Process = require("./process");

function startDaemon(options) {
    var name = Process.getProcessName();
    Log.init(name, options.debug);
    Log.i(name, "deamon started. options =\n" + JSON.stringify(options, true, 2));
    Process.handleExit(null, options.needPidFile);

    return Q.all([
        options.mysqlOption ? require("./mysql").connectMySql(options.mysqlOption) : null,
        options.redisOption ? require("./redis").connectRedis(options.redisOption) : null
    ]).then(function(results) {
        exports.mysqlClient = results[0];
        exports.redisClient = results[1];
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
    run().then(function() {
        Log.i("daemon run finish.");
        setTimeout(function() {
            doDaemon(run, timegap);
        }, timegap);
    }).catch(function(err) {
        Log.e("daemon run failed, err=\n" + err.stack);
        setTimeout(function() {
            doDaemon(run, timegap);
        }, timegap);
    });
}

exports.startDaemon = startDaemon;