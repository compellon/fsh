const _ = require( 'lodash' );
const rest = require('restler');
const URI = require('urijs');
const fs = require('fs');
const errors = require('./errors');
const HDFSError = errors.HDFSError;
const ValidationError = errors.ValidationError;
const ResponseError = errors.ResponseError;

class FSH {
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
        rest[method](url, { followRedirects: false })
            .on( 'error', err => cb(err) )
            .on( 'fail', (data, res) => _.has( data, 'RemoteException') ?
                cb( new HDFSError( data ), res ) :
                cb( new ResponseError( `Got unexpected status code for ${url}: ${res.statusCode}` ), res)
            )
            .on( 'success', (data, res) => cb( null, res, data) );
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

        this._sendRequest( 'put', 'MKDIRS', path, { permissions: mode }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to create directory in ${path}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    chmod( path, mode = 0o755, cb ) {
        if (!this.config.useHDFS) return fs.chmod( path, mode, cb );

        this._sendRequest( 'put', 'SETPERMISSION', path, { permissions: mode }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to modify permissions to ${path}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    chown( path, owner, group, cb) {
        if (!this.config.useHDFS) return fs.chown( path, owner, group, cb );

        this._sendRequest( 'put', 'SETOWNER', path, { owner, group }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to modify owner for ${path}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    readdir( path, cb ) {
        if (!this.config.useHDFS) return fs.readdir( path, null, cb );

        this._sendRequest( 'get', 'LISTSTATUS', path, {}, (err, res, body) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to list directory ${path}: ${res.statusCode}` ) );

            cb( null, body.FileStatuses.FileStatus );
        });
    }

    rename( path, destination, cb ) {
        if (!this.config.useHDFS) return fs.rename( path, destination, cb );

        this._sendRequest( 'put', 'RENAME', path, { destination }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to rename ${path} to ${destination}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    unlink( path, recursive = null, cb) {
        if (!this.config.useHDFS) return fs.unlink( path, cb );

        this._sendRequest( 'del', 'DELETE', path, { recursive }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to delete ${path}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    stat( path, cb ) {
        if (!this.config.useHDFS) return fs.stat( path, cb );

        this._sendRequest( 'get', 'GETFILESTATUS', path, {}, ( err, res, data ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to get status for ${path}: ${res.statusCode}` ) );

            cb( null, data.FileStatus );
        });
    }

    exists( path, cb ) {
        if (!this.config.useHDFS) return fs.exists( path, cb );

        this.stat( path, err => err ? cb( null, false ) : cb( null, true ) );
    }

    writeFile( path, data, opts, cb ) {
        if (!this.config.useHDFS) return fs.writeFile( path, data, opts, cb );

        this._sendRequest( 'put', 'CREATE', path, opts, (err, res) => {
            if (err) return cb(err);
            const writeUrl = res.headers.location;
            rest.put( writeUrl, { data })
                .on( 'error', cb)
                .on( 'fail', (data, res) => _.has( data, 'RemoteException') ?
                    cb( new HDFSError( data ), res ) :
                    cb( new ResponseError( `Got unexpected status code for ${writeUrl}: ${res.statusCode}` ), res)
                )
                .on( 'success', () => cb( null ) );
        });
    }

    appendFile( path, data, opts, cb ) {
        if (!this.config.useHDFS) return fs.appendFile( path, data, opts, cb );

        this._sendRequest( 'post', 'APPEND', path, opts, (err, res) => {
            if (err) return cb(err);
            const writeUrl = res.headers.location;
            rest.post(writeUrl, { data })
                .on('error', cb)
                .on('fail', (data, res) => _.has(data, 'RemoteException') ?
                    cb(new HDFSError(data), res) :
                    cb(new ResponseError(`Got unexpected status code for ${writeUrl}: ${res.statusCode}`), res)
                )
                .on('success', () => cb(null));
        });
    }

    readFile( path, opts, cb ) {
        if (!this.config.useHDFS) return fs.readFile( path, opts, cb );

        this._sendRequest( 'get', 'OPEN', path, opts, (err, res) => {
            if ( err ) return cb( err );

            const readUrl = res.headers.location;

            rest.get(readUrl)
                .on('error', cb)
                .on('fail', (data, res) => _.has(data, 'RemoteException') ?
                    cb(new HDFSError(data), res) :
                    cb(new ResponseError(`Got unexpected status code for ${readUrl}: ${res.statusCode}`), res)
                )
                .on('success', data => cb(null, data));
        });
    }
}

module.exports = FSH;
