function shuffle(array) {
    for (var i = 0; i < array.length; ++i) {
        var p = Math.floor(Math.random() * (array.length - i));
        var temp = array[i];
        array[i] = array[p];
        array[p] = temp;
    }
    return array;
}

function randomInt(start, end) {
    return Math.floor(start + Math.random() * (end - start));
}

function isChinese(str) {
    if(/^[\u4e00-\u9fa5]+$/i.test(str)) {
        return true;
    }
    return false;
}

exports.shuffle = shuffle;
exports.randomInt = randomInt;
exports.isChinese = isChinese;