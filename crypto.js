var crypto = require("crypto");

function md5sum(data, outputEncoding) {
    outputEncoding = outputEncoding || "hex";
    return crypto.createHash("md5").update(data).digest(outputEncoding);
}

function sha1sum(data, outputEncoding) {
    outputEncoding = outputEncoding || "hex";
    return crypto.createHash("sha1").update(data).digest(outputEncoding);
}

exports.md5sum = md5sum;
exports.sha1sum = sha1sum;