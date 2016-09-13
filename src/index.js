const _ = require( 'lodash' );
const Promise = require('bluebird');
const rest = require('restler');
const URI = require('urijs');
const fs = Promise.promisifyAll(require('fs-extra'));
const errors = require('./errors');
const HDFSError = errors.HDFSError;
const ValidationError = errors.ValidationError;
const ResponseError = errors.ResponseError;
const WebHDFS = require('webhdfs');
const os = require('os');

const handleHDFSError = err => {
    if ( err.response ) {
        if ( _.has( err.response.data, 'RemoteException' ) ) 
            throw new HDFSError( data );
        else 
            throw new ResponseError( `Got unexpected status code for ${url}: ${res.statusCode}` );
    }
    throw err;
};

const validateUri = ( pathOrUri, validProtocols = [ 'hdfs', 'file', '' ] ) => Promise.try( () => {
    const uri = new URI( pathOrUri );
    const protocol = uri.protocol();

    if ( !_.includes( validProtocols, protocol ) )
        throw new ValidationError( `Unsupported protocol [${protocol}].` )

    return uri;
});

class FSH {
    constructor( conn ) {
        const { user = 'root', host = 'localhost', port = 50070, protocol = 'http', path = '/webhdfs/v1' } = conn;
        this.conn = conn
        this.baseURI = new URI( _.omit( conn, 'user' ) );
        this.client = axios.createClient();
        this.client.defaults.baseURL = this.baseURI.toString();
        this.client.defaults.maxRedirects = 0;
    }

    _constructURL( path, op, params = {} ) {
        params['user.name'] = params['user.name'] || this.conn.user;
        const queryParams = _.extend({ op }, params);
        const uriParts = _.extend( _.clone( this.conn ), { path: this.conn.path + path } );
        return new URI( uriParts ).query( queryParams );
    }

    _sendRequest( method, op, uri, params = {} ) {
        const url = this._constructURL( uri.path(), op, params).toString();
        const opts = { url, method };

        if ( uri.hostname() )
            opts.baseURL = new URI( this.baseURI ).hostname( uri.hostname() ).toString();

        return this.client.request( opts ).catch( handleHDFSError );
    }

    mkdir( path, mode = 0o755 ) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.ensureDirAsync( uri.path(), mode ) :
            this._sendRequest( 'put', 'MKDIRS', uri, { permissions: mode } ).then( res => res.data )
        );
    }

    chmod( path, mode = 0o755 ) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.chmodAsync( path, mode ) :
            this._sendRequest( 'put', 'SETPERMISSION', uri, { permissions: mode } ).then( res => res.data )
        );
    }

    chown( path, owner, group ) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.chownAsync( path, owner, group ) :
            this._sendRequest( 'put', 'SETOWNER', uri, { owner, group } ).then( res => res.data )
        );
    }

    readdir( path ) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.readdirAsync( path, null ) :
            this._sendRequest( 'get', 'LISTSTATUS', uri ).then( res => res.data.FileStatuses.FileStatus )
        );
    }

    copy( path, destination ) {
        return Promise.all([ validateUri( path ), validateUri( destination ) ])
            .spread( ( srcURI, destURI ) => {
                if ( srcURI.protocol() !== 'hdfs' && destURI.protocol() !== 'hdfs' )
                    return fs.copyAsync( path, destination );
                else if ( srcURI.protocol() === 'hdfs' && destURI.protocol() !== 'hdfs' )
                    return this.copyToLocal( path, destination );
                else if ( srcURI.protocol() !== 'hdfs' && destURI.protocol() === 'hdfs' )
                    return this.copyFromLocal( path, destination );
                else if ( srcURI.protocol() === 'hdfs' && destURI.protocol() === 'hdfs' ) {
                    const tmpDir = os.tmpdir();
                    const timestamp = new Date().getTime();
                    //TODO: replace with guids?
                    const tmpFile = `${tmpDir}/${timestamp}`;

                    return this.copyToLocal( path, tmpFile ).then( () => this.copyFromLocal( tmpFile, destination) );
                }
            });
    }

    copyToLocal( hdfsSrc, destination ) {
        return Promise.all([ validateUri( hdfsSrc, [ 'hdfs' ] ), validateUri( destination, [ 'file', '' ] ) ] )
            .spread( ( srcUri, destUri ) => {
                const conn = _.clone( this.conn );
                if ( srcUri.hostname() ) conn.hostname = srcUri.hostname(); 
                const hdfs = WebHDFS.createClient( conn );

                const remoteFileStream = hdfs.createReadStream( srcUri.path() );
                const localFileStream = fs.createWriteStream( destUri.path() );

                return new Promise( ( resolve, reject ) => {
                    remoteFileStream.pipe( localFileStream );

                    localFileStream.on( 'error', reject );

                    localFileStream.on( 'finish', res => {
                        if ( _.isError( res ) ) {
                            return reject(res);
                        }
                        resolve();
                    });
                });
            });
    }

    copyFromLocal( path, hdfsDestination ) {
        return Promise.all([ validateUri( path, ['file', ''] ), validateUri( hdfsDestination, [ 'hdfs' ] ) ])
            .spread( ( srcUri, destUri ) => {
                const hdfs = WebHDFS.createClient( this.conn );
            
                const localFileStream = fs.createReadStream( path );
                const remoteFileStream = this.hdfs.createWriteStream( destination );
                
                return new Promise( ( resolve, reject ) => {
                    localFileStream.pipe( remoteFileStream );

                    remoteFileStream.on( 'error', reject );

                    remoteFileStream.on( 'finish', res => {
                        if ( _.isError( res ) ) {
                            return reject( res );
                        }
                        resolve();
                    });
                });
            });
    }

    //TODO: implement like copy()
    rename( path, destination ) {
        return Promise.all([ validateUri( path ), validateUri( destination ) ])
            .spread( ( srcUri, destURI ) => 
                srcUri.protocol() !== 'hdfs' && destURI.protocol() !== 'hdfs' ?
                    fs.moveAsync( srcUri.path(), destURI.path(), { clobber: true } ) :
                    this._sendRequest( 'put', 'RENAME', uri, { destination: destURI.path() } ).then( res => res.data )
            );
    }

    unlink( path, recursive = null) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.unlinkAsync( path ) :
            this._sendRequest( 'delete', 'DELETE', uri, { recursive } ).then( res => res.data )
        );
    }

    remove( path ) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.removeAsync( path ) :
            this.unlink( path, true )
        );
    }

    stat( path ) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.statAsync( path ) :
            this._sendRequest( 'get', 'GETFILESTATUS', path ).then( res => res.data.FileStatus )
        );
    }

    writeJson( path, json, opts = {} ) {
        return validateUri( path ).then( uri => {
            const useHDFS = uri.protocol() === 'hdfs';

            if (typeof json !== 'object')
                throw new ValidationError('Input must be an object. Try using writeFile instead or convert to an object.');

            if ( !useHDFS ) return fs.writeJsonAsync( path, json, opts );
            
            return this.writeFile( path, json.stringify( json ), opts )
        });
    }

    writeFile( path, data, opts = {} ) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.writeFileAsync( path, data, opts ) :
            this._sendRequest( 'put', 'CREATE', path, opts )
                .then( res => res.headers.location )
                .then( url => axios.request( { url, method, data } ) )
                .then( res => res.data )
                .catch( err => handleHDFSError )
        );
    }

    appendFile( path, data, opts = {} ) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.appendFileAsync( path, data, opts ) :
            this._sendRequest( 'post', 'APPEND', path, opts )
                .then( res => res.headers.location )
                .then( url => axios.request( { url, method, data } ) )
                .then( res => res.data )
                .catch( err => handleHDFSError );
        );
    }

    readFile( path, opts = {} ) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.readFileAsync( path, opts ) :
            this._sendRequest( 'get', 'OPEN', path, opts )
                .then( res => res.headers.location )
                .then( url => axios.request( { url, method } ) )
                .then( res => res.data )
                .catch( err => handleHDFSError )
        );
    }

    readJson( path, opts = {} ) {
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.readJsonAsync( path, opts ) :
            this.readFile( path, opts).then( JSON.stringify )
        );
    }
}

module.exports = FSH;
