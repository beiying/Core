var fs = require("fs");
var path = require("path");
var Log = require("./log.js");

function handleExit(server, needPidFile) {
    function exitProcess() {
        if (server) {
            server.close(function () {
                Log.fatal("server is closed.");
            });
            setTimeout(function () {
                Log.fatal("close server timeout, exit process anyway.");
            }, 5000);
        } else {
            Log.fatal("exit process.");
        }
    }

    process.on("uncaughtException", function (err) {
        Log.e("Got an uncaughtException.\nerr = " + err + "\nstack = " + err.stack);
        exitProcess();
    });

    var exitSignals = ["SIGINT", "SIGTERM"];
    exitSignals.forEach(function (signal) {
        process.on(signal, function () {
            Log.e("Got a " + signal + " signal.");
            exitProcess();
        });
    });

    if (needPidFile) {
        var pid_filename = path.parse(process.argv[1]).name + ".pid";
        fs.writeFileSync(pid_filename, process.pid);
        process.on("exit", function() {
            if (fs.existsSync(pid_filename)) fs.unlinkSync(pid_filename);
        });
    }
}

function getProcessName() {
    return path.parse(process.argv[1]).name;
}

exports.handleExit = handleExit;
exports.getProcessName = getProcessName;