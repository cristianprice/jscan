const isLogEnabled = true;
const isDebugEnabled = true;
const log = require('color-logs')(isLogEnabled, isDebugEnabled, __filename);

function info(msg) {
    log.info(msg);
}


function debug(msg) {
    log.debug(msg);
}


function warn(msg) {
    log.warning(msg);
}


function error(msg) {
    log.error(msg);
}


module.exports.info = info;
module.exports.debug = debug;
module.exports.error = error;
module.exports.warn = warn;
