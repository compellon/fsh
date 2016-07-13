const _ = require( 'lodash' );
const Rx = require('rx');
const RxNode = require('rx-node');
const request = require('request');
const URI = require('urijs');
const fs = require('fs-extra');
const errors = require('./errors');
const HDFSError = errors.HDFSError;
const ValidationError = errors.ValidationError;
const ResponseError = errors.ResponseError;

class ezraFS {
    constructor( config ) {
        const { user = 'root', host = 'localhost', port = 50070, protocol = 'http', path = '/webhdfs/v1', useHDFS = false } = config;
        const connection = { user, hostname: host, port, protocol, path };
        this.config = { connection, useHDFS };
    }

    _constructURL(path, op, params = {}) {
        params['user.name'] = params['user.name'] || this.config.connection.user;
        const queryParams = _.extend({ op }, params);
        const uriParts = _.extend(_.clone(this.config.connection), { path: this.config.connection.path + path });
        return new URI( uriParts ).query( queryParams );
    }

    _sendRequest( method, op, path, params, cb ) {

        if ( !cb )
            throw new ValidationError( 'A callback must be specified.' );

        if ( !_.isString( path ) )
            return cb(new ValidationError( 'path must be a string' ));

        const url = this._constructURL( path, op, params).toString();

        return request[method](url, (err, res, body) => {
            if ( err )
                return cb( err );

            if ( _.has( body, 'RemoteException') )
                return cb( new HDFSError( body ) );
            console.log('got statusCode: ', res.statusCode);
            console.log('body: ', body);
            cb( null, res, body );
        });
    }

    hdfs() {
        this.config.useHDFS = true;
        return this;
    }

    fs() {
        this.config.useHDFS = false;
        return this;
    }

    mkdir( path, mode = 0o755, cb ) {
        if (!this.config.useHDFS) return fs.mkdir( path, mode, cb );

        return this._sendRequest( 'put', 'MKDIRS', path, { permissions: mode }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to create directory in ${path}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    chmod( path, mode = 0o755, cb ) {
        if (!this.config.useHDFS) return fs.chmod( path, mode, cb );

        return this._sendRequest( 'put', 'SETPERMISSION', path, { permissions: mode }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to modify permissions to ${path}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    chown( path, owner, group, cb) {
        if (!this.config.useHDFS) return fs.chown( path, owner, group, cb );

        return this._sendRequest( 'put', 'SETOWNER', path, { owner, group }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to modify owner for ${path}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    readdir( path, cb ) {
        if (!this.config.useHDFS) return fs.readdir( path, null, cb );

        return this._sendRequest( 'get', 'LISTSTATUS', path, {}, (err, res, body) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to list directory ${path}: ${res.statusCode}` ) );

            cb( null, body.FileStatuses.FileStatus );
        });
    }

    rename( path, destination, cb ) {
        if (!this.config.useHDFS) return fs.rename( path, destination, cb );

        return this._sendRequest( 'put', 'RENAME', path, { destination }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to rename ${path} to ${destination}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    unlink( path, recursive = null, cb) {
        if (!this.config.useHDFS) return fs.unlink( path, cb );

        return this._sendRequest( 'delete', 'DELETE', path, { recursive }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to delete ${path}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    stat( path, cb ) {
        if (!this.config.useHDFS) return fs.stat( path, cb );

        return this._sendRequest( 'get', 'GETFILESTATUS', path, {}, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to get status for ${path}: ${res.statusCode}` ) );

            cb( null, body.FileStatus );
        });
    }

    exists( path, cb ) {
        if (!this.config.useHDFS) return fs.exists( path, cb );

        return this.stat( path, ( err ) => err ? cb( false ) : cb( true ) );
    }

    writeFile( path, data, opts, cb ) {
        if (!this.config.useHDFS) return fs.writeFile( path, data, opts, cb );

        const createUrl = this._constructURL(path, 'CREATE', opts).toString();
        let writeUrl = null;
        return request.post( createUrl )
            .on( 'response', res => {
                console.log('res.headers: ', res.headers);
                writeUrl = res.headers.location;
            })
            .pipe( request.put( writeUrl ) )
            .on( 'complete', cb )
            .on( 'error', err => cb(err) );
    }

    appendFile( path, data, opts, cb ) {
        if (!this.config.useHDFS) return fs.appendFile( path, data, opts, cb );

        const createUrl = this._constructURL(path, 'APPEND', opts).toString();
        let writeUrl = null;
        return request.post( createUrl )
            .on( 'response', res => {
                console.log('res.headers: ', res.headers);
                writeUrl = res.headers.location;
            })
            .pipe( request.put( writeUrl ) )
            .on( 'complete', cb )
            .on( 'error', err => cb(err) );
    }

    readFile( path, opts, cb ) {
        if (!this.config.useHDFS) return fs.readFile( path, opts, cb );

        return this._sendRequest( 'get', 'OPEN', path, opts, (err, res, body) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to read ${path}: ${res.statusCode}` ) );

            cb( body );
        });
    }
}



module.exports = ezraFS;
