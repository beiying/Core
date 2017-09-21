var Q = require("q");
var fs = require("fs");
var path = require("path");

function writeFile(file, data, options) {
    var deferred = Q.defer();
    fs.writeFile(file, data, options, function(err) {
        if (err) return deferred.reject(err);
        deferred.resolve();
    });
    return deferred.promise;
}

function writeFileSync(file, data, options) {
    return fs.writeFileSync(file, data, options);
}

function appendFile(file, data, options) {
    var deferred = Q.defer();
    fs.appendFile(file, data, options, function(err) {
        if (err) return deferred.reject(err);
        deferred.resolve();
    });
    return deferred.promise;
}

function appendFileSync(file, data, options) {
    return fs.appendFileSync(file, data, options);
}

function readFile(file, options) {
    var deferred = Q.defer();
    fs.readFile(file, options, function(err, data) {
        if (err) return deferred.reject(err);
        deferred.resolve(data);
    });
    return deferred.promise;
}

function readFileSync(file, options) {
    return fs.readFileSync(file, options);
}

function createReadStream(file, options) {
    return fs.createReadStream(file, options);
}

function createWriteStream(path, options) {
    return fs.createWriteStream(path, options);
}

function unlink(path) {
    var deferred = Q.defer();
    fs.unlink(path, function(err) {
        if (err) return deferred.reject(err);
        deferred.resolve();
    });
    return deferred.promise;
}

function mkdir(path) {
    var deferred = Q.defer();
    fs.mkdir(path, function(err) {
        if (err) return deferred.reject(err);
        deferred.resolve();
    });
    return deferred.promise;
}

function rmdir(path) {
    var deferred = Q.defer();
    fs.rmdir(path, function(err) {
        if (err) return deferred.reject(err);
        deferred.resolve();
    });
    return deferred.promise;
}

function stat(path) {
    var deferred = Q.defer();
    fs.stat(path, function(err, stats) {
        if (err) return deferred.reject(err);
        deferred.resolve(stats);
    });
    return deferred.promise;
}

function mkdirs(dirpath) {
    var levels = [];
    while (dirpath !== "." && dirpath !== "/") {
        levels.unshift(dirpath);
        dirpath = path.dirname(dirpath);
    }
    var promise = Q();
    levels.forEach(function(level) {
        promise = promise.then(function() {
            return stat(level);
        }).then(function(stats) {
            return stats.isDirectory() ? null : mkdir(level);
        }, function(/*err*/) {
            return mkdir(level);
        });
    });
    return promise;
}

function readdir(path) {
    var deferred = Q.defer();
    fs.readdir(path, function(err, files) {
        if (err) return deferred.reject(err);
        deferred.resolve(files);
    });
    return deferred.promise;
}

function rmdirs(dirpath) {
    var childfiles = null;
    return readdir(dirpath)
    .then(function(files) {
        childfiles = files;
        var promises = [];
        files.forEach(function(file) {
            promises.push(stat(path.join(dirpath, file)));
        });
        return Q.all(promises);
    }).then(function(results) {
        var promises = [];
        results.forEach(function(stats, index) {
            var childpath = path.join(dirpath, childfiles[index]);
            if (stats.isFile()) {
                promises.push(unlink(childpath));
            } else if (stats.isDirectory()) {
                promises.push(rmdirs(childpath));
            }
        });
        return Q.all(promises);
    }).then(function() {
        return rmdir(dirpath);
    });
}

function rename(oldPath, newPath) {
    var deferred = Q.defer();
    fs.rename(oldPath, newPath, function(err) {
        if (err) return deferred.reject(err);
        deferred.resolve();
    });
    return deferred.promise;
}

exports.fs = fs;
exports.writeFile = writeFile;
exports.writeFileSync = writeFileSync;
exports.appendFile = appendFile;
exports.appendFileSync = appendFileSync;
exports.readFile = readFile;
exports.readFileSync = readFileSync;
exports.createReadStream = createReadStream;
exports.createWriteStream = createWriteStream;
exports.unlink = unlink;
exports.mkdir = mkdir;
exports.rmdir = rmdir;
exports.stat = stat;
exports.mkdirs = mkdirs;
exports.readdir = readdir;
exports.rmdirs = rmdirs;
exports.rename = rename;