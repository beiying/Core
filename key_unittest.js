var assert = require("assert");
var Q = require("q");
var MongoDB = require("./mongodb");
var AutoIncrement = require("./key").AutoIncrement;

var mongodb_options = {
    host: "127.0.0.1",
    port: 27017,
    database : "key_unittest"
};

describe("test auto increment", function() {

    before("connect mongodb", function() {
        return MongoDB.connectMongoDB(mongodb_options).then(function() {
            return MongoDB.deleteMany("autoincrement", {});
        });
    });

    after("close mongodb", function() {
        return MongoDB.close(true);
    });

    it("set next id ok", function(done) {
        AutoIncrement.setNextId("test", 24).then(function(r) {
            console.log("setNextId");
            assert.equal(r.upsertedCount, 1);
            return AutoIncrement.setNextId("test", 30);
        }).then(function(r) {
            assert.equal(r.modifiedCount, 1);
            return AutoIncrement.getNextId("test");
        }).then(function(id) {
            console.log(id);
            assert.equal(id, 31);
            done();
        });
    });

    it("get next id ok", function(done) {
        AutoIncrement.getNextId("test1").then(function(id) {
            console.log("getNextId");
            assert.equal(id, 1);
            return AutoIncrement.getNextId("test1");
        }).then(function(id) {
            assert.equal(id, 2);
            done();
        });
    });

    it("get next id in current", function() {
        this.timeout(30000);
        var promises = [];
        for (var i = 0; i < 10000; ++i) {
            promises.push((function() {
                return AutoIncrement.getNextId("curr_test");
            })());
        }
        return Q.allSettled(promises).then(function(results) {
            results.sort(function(lhs, rhs) { return lhs.value - rhs.value; });
            results.forEach(function(r, i) {
                assert.equal(r.value, i + 1);
            });
        });
    });

    it("del next id ok", function(done) {
        AutoIncrement.delNextId("test").then(function (r) {
            console.log("delNextId");
            assert.equal(r.deletedCount, 1);
            return AutoIncrement.delNextId("test1");
        }).then(function(r) {
            assert.equal(r.deletedCount, 1);
            done();
        });
    });

});