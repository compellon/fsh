const _ = require('lodash');

// Shamelessly borrowed from http://stackoverflow.com/questions/31089801/extending-error-in-javascript-with-es6-syntax
class NamedError extends Error {
    constructor( message, name ) {
        super( message );
        this.name = name || this.constructor.name;
        this.message = message;
        this.stack = ( new Error(message) ).stack;
    }
}

class HDFSError extends NamedError {
    constructor( { hdfsRemoteException = {} } ) {
        let message = _.get( hdfsRemoteException, 'message' );
        let name = _.get( hdfsRemoteException, 'exception', this.constructor.name );

        if ( !message ) {
            message = 'There was an error when interacting with HDFS';
            name = this.constructor.name;
        }

        super( message, name );
    }
}

class ValidationError extends NamedError {
    constructor( message ) {
        super( message );
    }
}

class ResponseError extends NamedError {
    constructor( message ) {
        super( message );
    }
}

exports.modules = {HDFSError, ValidationError};
