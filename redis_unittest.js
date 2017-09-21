var assert = require('assert');
var Q = require("q");
var Redis = require("./redis");

var redis_options = {
    "host": "127.0.0.1",
    "port": 6379
};

describe("test connect redis", function() {

    it("connect ok", function(done) {
        Redis.connectRedis(redis_options).then(function(client) {
            assert.ok(client);
            done();
        });
    });

});

describe("test string value key", function() {

    it("set, get, del ok", function (done) {
        Redis.set("hello", "kitty").then(function (data) {
            assert.equal(data, "OK");
            return Redis.get("hello");
        }).then(function (data) {
            assert.equal(data, "kitty");
            return Redis.del("hello");
        }).then(function (data) {
            assert.equal(data, "1");
            done();
        });
    });

    it("setex ok", function(done) {
        Redis.setex("hello", 1, "kitty").then(function(data) {
            assert.equal(data, "OK");
            return Redis.get("hello");
        }).then(function(data) {
            assert.equal(data, "kitty");
            return Q.delay(null, 1500);
        }).then(function() {
            return Redis.get("hello");
        }).then(function(data) {
            assert.equal(data, null);
            done();
        });
    });

    it("incr, incrby, incrbyfloat ok", function (done) {
        Redis.incr("counter").then(function (reply) {
            assert.equal(reply, 1);
            return Redis.incrby("counter", 2);
        }).then(function (reply) {
            assert.equal(reply, 3);
            return Redis.incrbyfloat("counter", 3.3);
        }).then(function (reply) {
            assert.equal(reply, 6.3);
            return Redis.del("counter");
        }).then(function () {
            done();
        });
    });

    it("mget ok", function (done) {
        Q.all([Redis.set("a1", "x1"), Redis.set("a2", "x2"),Redis.set("a3", "x3")]).then(function() {
            return Redis.mget(["a1", "a2", "a3"]);
        }).then(function(reply) {
            assert.equal(reply[0], "x1");
            assert.equal(reply[1], "x2");
            assert.equal(reply[2], "x3");
            done();
        });
    });

});

describe("test list value key", function() {

    it("push, pop ok", function (done) {
        Redis.rpush("mylist", ["one", "two"])
            .then(function (data) {
                assert.equal(data, "2");
                return Redis.lpop("mylist");
            }).then(function (data) {
            assert.equal(data, "one");
            return Redis.lpop("mylist");
        }).then(function (data) {
            assert.equal(data, "two");
            done();
        });
    });

});

describe("test config ok", function() {

    it("info ok", function(done) {
       Redis.info("memory").then(function(data) {
           assert.ok(data.indexOf("memory:") >= 0);
           done();
       });
    });

});

describe("test disconnect redis", function() {

    it("close ok", function() {
        Redis.end();
    });

});