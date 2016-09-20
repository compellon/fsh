'use strict';

const _ = require('lodash');

// Shamelessly borrowed from http://stackoverflow.com/questions/31089801/extending-error-in-javascript-with-es6-syntax
class NamedError extends Error {
    constructor(message, name) {
        super(message);
        this.name = name || this.constructor.name;
        this.message = message;
        this.stack = new Error(message).stack;
    }
}

class HDFSError extends NamedError {
    constructor(hdfsRemoteException = {}) {
        let message = _.get(hdfsRemoteException, 'RemoteException.message', 'There was an error when interacting with HDFS');
        let name = _.get(hdfsRemoteException, 'RemoteException.exception');
        super(message, name);
    }
}

class ValidationError extends NamedError {
    constructor(message) {
        super(message);
    }
}

class ResponseError extends NamedError {
    constructor(message) {
        super(message);
    }
}

module.exports = { HDFSError, ValidationError, ResponseError };
//# sourceMappingURL=errors.js.map