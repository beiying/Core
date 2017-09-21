var assert = require("assert");
var Q = require("q");
var MongoDB = require("./mongodb");

describe("mongodb basic function", function() {

    it("connect ok", function() {
        return MongoDB.connectMongoDB({
            host: "127.0.0.1",
            port: 27017,
            database : "test"
        });
    });

    it("insert, find, update and delete one doc ok", function() {
        return MongoDB.insertOne("data", {id: 2, text: "hello"})
        .then(function(result) {
            assert.equal(result.insertedCount, 1);
            return MongoDB.updateOne("data", {id: 2}, {id: 42, text: "world"});
        }).then(function(result) {
            assert.equal(result.modifiedCount, 1);
            return MongoDB.findOne("data", {id: 42}, {text: 1});
        }).then(function(result) {
            assert.equal(result.text, "world");
            return MongoDB.deleteOne("data", {id:42})
        }).then(function(result) {
            assert.equal(result.deletedCount, 1);
        });
    });

    it("close ok", function() {
        return MongoDB.close(true);
    });
});