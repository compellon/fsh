const _ = require( 'lodash' );
const rest = require('restler');
const URI = require('urijs');
const fs = require('fs-extra');
const errors = require('./errors');
const HDFSError = errors.HDFSError;
const ValidationError = errors.ValidationError;
const ResponseError = errors.ResponseError;
const WebHDFS = require('webhdfs');
const os = require('os');

class FSH {
    constructor( config ) {
        const { user = 'root', host = 'localhost', port = 50070, protocol = 'http', path = '/webhdfs/v1', useHDFS = false } = config;
        const connection = { user, hostname: host, port, protocol, path };
        this.config = { connection, useHDFS };
        this.hdfs = WebHDFS.createClient( {user, host, port, path} );
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

    mkdir( path, mode, cb ) {
        if (_.isFunction(mode)) {
            cb = mode;
            mode = 0o755;
        }

        if (!this.config.useHDFS) return fs.ensureDir( path, mode, cb );

        this._sendRequest( 'put', 'MKDIRS', path, { permissions: mode }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to create directory in ${path}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    chmod( path, mode, cb ) {
        if (_.isFunction(mode)) {
            cb = mode;
            mode = 0o755;
        }

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

    copy( path, destination, cb) {
        if (!this.config.useHDFS) return fs.copy( path, destination, cb );

        const tmpDir = os.tmpdir();
        const timestamp = new Date().getTime();
        const tmpFile = `${tmpDir}/${timestamp}`;

        this.copyToLocal( path, tmpFile, err => {
            if (err) return cb(err);
            this.copyFromLocal( tmpFile, destination, cb );
        })
    }

    copyToLocal( path, destination, cb ) {
        if (!this.config.useHDFS) return cb( 'HDFS must be enabled to copy to local' );

        const remoteFileStream = this.hdfs.createReadStream( path );
        const localFileStream = fs.createWriteStream( destination );

        remoteFileStream.pipe(localFileStream);

        localFileStream.on( 'error', cb );
        localFileStream.on( 'finish', res => {
            if ( _.isError(res) ) {
                return cb(res);
            }
            cb();
        });
    }

    copyFromLocal( path, destination, cb ) {
        if (!this.config.useHDFS) return cb( 'HDFS must be enabled to copy from local' );

        const localFileStream = fs.createReadStream( path );
        const remoteFileStream = this.hdfs.createWriteStream( destination );

        localFileStream.pipe(remoteFileStream);

        remoteFileStream.on( 'error', cb );
        remoteFileStream.on( 'finish', res => {
            if ( _.isError(res) ) {
                return cb(res);
            }
            cb();
        });
    }

    rename( path, destination, cb ) {
        if (!this.config.useHDFS) return fs.move( path, destination, cb );

        this._sendRequest( 'put', 'RENAME', path, { destination }, ( err, res ) => {
            if ( err )
                return cb( err );

            if ( res.statusCode !== 200 )
                return cb( new ResponseError( `Received an unexpected status code when attempting to rename ${path} to ${destination}: ${res.statusCode}` ) );

            cb( null );
        });
    }

    unlink( path, recursive, cb) {
        if (_.isFunction(recursive)) {
            cb = recursive;
            recursive = null;
        }

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

    writeJson(path, json, opts, cb) {
        if (!this.config.useHDFS) return fs.writeJson( path, data, opts, cb );
        if (_.isFunction(opts)) {
            cb = opts;
            opts = {};
        }
        if (typeof json !== 'object')
            return cb('Input must be an object. Try using writeFile instead or convert to an object.');

        const jsonToWrite = JSON.stringify(json);

        this.writeFile(path, jsonToWrite, opts, cb);
    }

    writeFile( path, data, opts, cb ) {
        if (_.isFunction(opts)) {
            cb = opts;
            opts = {};
        }

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
        if (_.isFunction(opts)) {
            cb = opts;
            opts = {};
        }

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
        if (_.isFunction(opts)) {
            cb = opts;
            opts = {};
        }

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

    readJson( path, opts, cb ) {
        if (_.isFunction(opts)) {
            cb = opts;
            opts = {};
        }
        if (!this.config.useHDFS) return fs.readJson( path, opts, cb );

        this.readFile( path, opts, (err, data) => {
            if (err) return cb(err);
            try {
                const parsedJSON = JSON.parse(data);
                cb(null, parsedJSON);
            } catch (e) {
                cb(`Invalid JSON data in ${path}`);
            }
        });
    }
}

module.exports = FSH;
