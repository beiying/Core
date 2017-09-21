var Q = require("q");
var uuid = null;
var MongoDB = null;

var AutoIncrement = {};
var UUID = {};

AutoIncrement.getNextId = function(schema, errorId) {
    if (MongoDB === null) MongoDB = require("./mongodb");
    return MongoDB.findOneAndUpdate("autoincrement", {schema: schema}, {$set: {schema: schema}, $inc: {id: 1}}, {projection: {_id: 0}, returnOriginal: false, upsert: true}).then(function(r) {
        if (r != null && r.id) {
            return r.id;
        } else {
            return errorId;
        }
    });
};

AutoIncrement.setNextId = function(schema, id) {
    if (MongoDB === null) MongoDB = require("./mongodb");
    return MongoDB.updateOne("autoincrement",{schema: schema}, {schema: schema, id: id}, {upsert: true});
};

AutoIncrement.delNextId = function(schema) {
    if (MongoDB === null) MongoDB = require("./mongodb");
    return MongoDB.deleteOne("autoincrement",{schema: schema});
};

UUID.getUuid = UUID.getUuidV1 = function (options) {
    if (uuid === null) uuid = require("uuid");
    return options ? uuid.v1(options) : uuid.v1();
};

UUID.getUuidV4 = function (options) {
    if (uuid === null) uuid = require("uuid");
    return options ? uuid.v4(options) : uuid.v4();
};

exports.AutoIncrement = AutoIncrement;
exports.UUID = UUID;