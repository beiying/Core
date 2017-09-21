var process = require("process");
var redis = require("redis");
var Q = require("q");

var mRedisClient = null;

function connectRedis(options) {
    var deferred = Q.defer();
    mRedisClient = redis.createClient(options);
    mRedisClient.on("ready", function() {
        exports.client = mRedisClient;
        deferred.resolve(mRedisClient);
    });
    mRedisClient.on("error", function(err) {
        deferred.reject(err);
    });
    mRedisClient.on("end", function(err) {
        setTimeout(function() {
            process.exit(1);
        }, 5000);
    });
    return deferred.promise;
}

function end() {
    mRedisClient.removeAllListeners();
    return mRedisClient.end(true);
}

function duplicate() {
    var deferred = Q.defer();
    mRedisClient.duplicate(function(err, client) {
        if (err) return deferred.reject(err);
        deferred.resolve(client);
    });
    return deferred.promise;
}

function expire(key, seconds) {
    var deferred = Q.defer();
    mRedisClient.expire(key, seconds, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function multi() {
    return mRedisClient.multi();
}

function exec(multi) {
    var deferred = Q.defer();
    multi.exec(function (err, replies) {
        if (err) return deferred.reject(err);
        deferred.resolve(replies);
    });
    return deferred.promise;
}

function set(key, value) {
    var deferred = Q.defer();
    mRedisClient.set(key, value, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function setex(key, seconds, value) {
    var deferred = Q.defer();
    mRedisClient.setex(key, seconds, value, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function get(key) {
    var deferred = Q.defer();
    mRedisClient.get(key, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function del(key) {
    var deferred = Q.defer();
    mRedisClient.del(key, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function incr(key) {
    var deferred = Q.defer();
    mRedisClient.incr(key, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function incrby(key, increment) {
    var deferred = Q.defer();
    mRedisClient.incrby(key, increment, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function incrbyfloat(key, increment) {
    var deferred = Q.defer();
    mRedisClient.incrbyfloat(key, increment, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function rename(key, newkey) {
    var deferred = Q.defer();
    mRedisClient.rename(key, newkey, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function mget(keys) {
    var deferred = Q.defer();
    if (keys.length == 0) {
        deferred.resolve([]);
    } else {
        mRedisClient.mget(keys, function(err, reply) {
            if (err) return deferred.reject(err);
            if (reply) return deferred.resolve(reply);
            deferred.resolve([]);
        });
    }
    return deferred.promise;
}

function rpush(key, value) {
    var deferred = Q.defer();
    mRedisClient.rpush(key, value, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function lpush(key, value) {
    var deferred = Q.defer();
    mRedisClient.lpush(key, value, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function lpop(key) {
    var deferred = Q.defer();
    mRedisClient.lpop(key, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function rpop(key) {
    var deferred = Q.defer();
    mRedisClient.rpop(key, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function blpop(client, key, timeout) {
    var deferred = Q.defer();
    client.blpop(key, timeout, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function llen(key) {
    var deferred = Q.defer();
    mRedisClient.llen(key, function(err, reply) {
        if (err) return deferred.reject(err);
        return deferred.resolve(reply);
    });
    return deferred.promise;
}

function lrange(key, start, end) {
    var deferred = Q.defer();
    mRedisClient.lrange(key, start, end, function(err, reply) {
        if (err) return deferred.reject(err);
        if (reply) return deferred.resolve(reply);
        deferred.resolve([]);
    });
    return deferred.promise;
}

function ltrim(key, start, end) {
    var deferred = Q.defer();
    mRedisClient.ltrim(key, start, end, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function sadd(key, member) {
    var deferred = Q.defer();
    mRedisClient.sadd(key, member, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function smembers(key) {
    var deferred = Q.defer();
    mRedisClient.smembers(key, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function srandmember(key, count) {
    var deferred = Q.defer();
    mRedisClient.srandmember(key, count, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function sismember(key, member) {
    var deferred = Q.defer();
    mRedisClient.sismember(key, member, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function srem(key, member) {
    var deferred = Q.defer();
    mRedisClient.srem(key, member, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function hget(key, field) {
    var deferred = Q.defer();
    mRedisClient.hget(key, field, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function hset(key, field, value) {
    var deferred = Q.defer();
    mRedisClient.hset(key, field, value, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function hdel(key, field) {
    var deferred = Q.defer();
    mRedisClient.hdel(key, field, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function hincrby(key, field, increment) {
    var deferred = Q.defer();
    mRedisClient.hincrby(key, field, increment, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function hmget(key, field) {
    var deferred = Q.defer();
    mRedisClient.hmget(key, field, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function zcard(key) {
    var deferred = Q.defer();
    mRedisClient.zcard(key, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function zadd(key, score, member) {
    var deferred = Q.defer();
    mRedisClient.zadd(key, score, member, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function zrank(key, member) {
    var deferred = Q.defer();
    mRedisClient.zrank(key, member, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function zscore(key, member) {
    var deferred = Q.defer();
    mRedisClient.zscore(key, member, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function zrem(key, member) {
    var deferred = Q.defer();
    mRedisClient.zrem(key, member, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function zrange(key, start, stop, withscores) {
    var deferred = Q.defer();
    if (withscores) {
        mRedisClient.zrange(key, start, stop, "withscores", callback);
    } else {
        mRedisClient.zrange(key, start, stop, callback);
    }
    return deferred.promise;

    function callback(err, reply) {
        if (err) return deferred.reject(err);
        if (reply) return deferred.resolve(reply);
        deferred.resolve([]);
    }
}

function zrangebyscore(key, min, max) {
    var deferred = Q.defer();
    mRedisClient.zrangebyscore(key, min, max, function(err, reply) {
        if (err) return deferred.reject(err);
        if (reply) return deferred.resolve(reply);
        deferred.resolve([]);
    });
    return deferred.promise;
}

function zrevrange(key, start, stop, withscores) {
    var deferred = Q.defer();
    if (withscores) {
        mRedisClient.zrevrange(key, start, stop, "withscores", callback);
    } else {
        mRedisClient.zrevrange(key, start, stop, callback);
    }
    return deferred.promise;

    function callback(err, reply) {
        if (err) return deferred.reject(err);
        if (reply) return deferred.resolve(reply);
        deferred.resolve([]);
    }
}

function zremrangebyrank(key, start, stop) {
    var deferred = Q.defer();
    mRedisClient.zremrangebyrank(key, start, stop, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function keys(pattern) {
    var deferred = Q.defer();
    mRedisClient.keys(pattern, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function info(section) {
    var deferred = Q.defer();
    if (section) { } else section = "all";
    mRedisClient.info(section, function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

function bgsave() {
    var deferred = Q.defer();
    mRedisClient.bgsave(function(err, reply) {
        if (err) return deferred.reject(err);
        deferred.resolve(reply);
    });
    return deferred.promise;
}

exports.connectRedis = connectRedis;
exports.end = end;
exports.duplicate = duplicate;
exports.expire = expire;
exports.multi = multi;
exports.exec = exec;
exports.set = set;
exports.setex = setex;
exports.get = get;
exports.del = del;
exports.incr = incr;
exports.incrby = incrby;
exports.incrbyfloat = incrbyfloat;
exports.rename = rename;
exports.mget = mget;
exports.rpush = rpush;
exports.lpush = lpush;
exports.lpop = lpop;
exports.rpop = rpop;
exports.blpop = blpop;
exports.llen = llen;
exports.lrange = lrange;
exports.ltrim = ltrim;
exports.sadd = sadd;
exports.srem = srem;
exports.smembers = smembers;
exports.srandmember = srandmember;
exports.sismember = sismember;
exports.hget = hget;
exports.hset = hset;
exports.hdel = hdel;
exports.hincrby = hincrby;
exports.hmget = hmget;
exports.zcard = zcard;
exports.zadd = zadd;
exports.zrank = zrank;
exports.zscore = zscore;
exports.zrem = zrem;
exports.zrange = zrange;
exports.zrangebyscore = zrangebyscore;
exports.zrevrange = zrevrange;
exports.zremrangebyrank = zremrangebyrank;
exports.expire = expire;
exports.keys = keys;
exports.info = info;
exports.bgsave = bgsave;