var child_process = require("child_process");
var Q = require("q");

function spawn(command, args) {
    var deferred = Q.defer();
    var proc = child_process.spawn(command, args);
    var buffers = [];
    proc.stdout.on("data", function(data) {
        buffers.push(data);
    });
    proc.stderr.on("data", function(data) {
        buffers.push(data);
    });
    proc.on("close", function(code) {
        var output = Buffer.concat(buffers).toString();
        if (code != 0) return deferred.reject(new Error(output));
        return deferred.resolve(output);
    });
    return deferred.promise;
}

exports.spawn = spawn;