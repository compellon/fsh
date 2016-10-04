import _ from 'lodash';
import Promise from 'bluebird';
import axios from 'axios';
import URI  from 'urijs';
import { HDFSError, ValidationError, ResponseError } from './errors';
import WebHDFS from 'webhdfs';
import os from 'os';

const fs = Promise.promisifyAll( require('fs-extra') );

const handleHDFSError = err => {
    if ( err.response ) {
        if ( _.has( err.response, 'data.RemoteException' ) ) 
            throw new HDFSError( err.response.data );
        else 
            throw new ResponseError( `Got unexpected status code for ${url}: ${res.statusCode}` );
    }
    throw err;
};

const validateUri = ( pathOrUri, validProtocols = [ 'hdfs', 'file', '' ] ) => Promise.try( () => {

    let uri = new URI( uriString );
    if ( !uri.protocol() ) {
        uri = uri.protocol('file');
    }

    let finalURIString = uri.toString();    
    if ( !finalURIString.test(/.*\:\/\/.*/) ) {
        finalURIString = finalURIString.replace(':', '://');
    }
    
    uri = URI( finalURIString );

    if ( !_.includes( validProtocols, protocol ) )
        throw new ValidationError( `Unsupported protocol [${protocol}].` );

    return uri;
});

export default class FSH {
    constructor( { user = 'root', host = 'localhost', port = 50070, protocol = 'http', path = '/webhdfs/v1' } ) {
        this.conn = { user, host, port, protocol, path, hostname: host };
        const uriParts = _.omit( this.conn, [ 'user', 'host' ] );
        this.baseURI = new URI( uriParts );
        this.client = axios.create();
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
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.ensureDirAsync( uri.path(true), mode ) :
            self._sendRequest( 'put', 'MKDIRS', uri, { permissions: mode } ).then( res => res.data )
        );
    }

    chmod( path, mode = 0o755 ) {
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.chmodAsync( uri.path(true), mode ) :
            self._sendRequest( 'put', 'SETPERMISSION', uri, { permissions: mode } ).then( res => res.data )
        );
    }

    chown( path, owner, group ) {
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.chownAsync( uri.path(true), owner, group ) :
            self._sendRequest( 'put', 'SETOWNER', uri, { owner, group } ).then( res => res.data )
        );
    }

    readdir( path ) {
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.readdirAsync( uri.path(true), null ) :
            self._sendRequest( 'get', 'LISTSTATUS', uri ).then( res => res.data.FileStatuses.FileStatus )
        );
    }

    copy( path, destination ) {
        const self = this;
        return Promise.all([ validateUri( path ), validateUri( destination ) ])
            .spread( ( srcURI, destURI ) => {
                if ( srcURI.protocol() !== 'hdfs' && destURI.protocol() !== 'hdfs' )
                    return fs.copyAsync( srcURI.path(true), destURI.path(true) );
                else if ( srcURI.protocol() === 'hdfs' && destURI.protocol() !== 'hdfs' )
                    return self.copyToLocal( path, destination );
                else if ( srcURI.protocol() !== 'hdfs' && destURI.protocol() === 'hdfs' )
                    return self.copyFromLocal( path, destination );
                else if ( srcURI.protocol() === 'hdfs' && destURI.protocol() === 'hdfs' ) {
                    const tmpDir = os.tmpdir();
                    const timestamp = new Date().getTime();
                    // TODO: replace with guids?
                    const tmpFile = `${tmpDir}/${timestamp}`;

                    return self.copyToLocal( path, tmpFile ).then( () => self.copyFromLocal( tmpFile, destination) );
                }
            });
    }

    // TODO: implement without webhdfs lib
    copyToLocal( hdfsSrc, destination ) {
        return Promise.all([ validateUri( hdfsSrc, [ 'hdfs' ] ), validateUri( destination, [ 'file', '' ] ) ] )
            .spread( ( srcUri, destUri ) => {
                const conn = _.omit( this.conn, 'hostname' );
                if ( srcUri.hostname() ) conn.host = srcUri.hostname(); 
                const hdfs = WebHDFS.createClient( conn );

                const remoteFileStream = hdfs.createReadStream( srcUri.path(true) );
                const localFileStream = fs.createWriteStream( destUri.path(true) );

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

    // TODO: implement without webhdfs lib 
    copyFromLocal( path, hdfsDestination ) {
        const self = this;
        return Promise.all([ validateUri( path, ['file', ''] ), validateUri( hdfsDestination, [ 'hdfs' ] ) ])
            .spread( ( srcUri, destUri ) => {
                const conn = _.omit( this.conn, 'hostname' );
                if ( srcUri.hostname() ) conn.host = srcUri.hostname();
                const hdfs = WebHDFS.createClient( conn );
            
                const localFileStream = fs.createReadStream( srcUri.path(true) );
                const remoteFileStream = hdfs.createWriteStream( destUri.path(true) );
                
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

    rename( path, destination ) {
        const self = this;
        return Promise.all([ validateUri( path ), validateUri( destination ) ])
            .spread( ( srcUri, destURI ) => {
                if ( srcUri.protocol() !== 'hdfs' && destURI.protocol() !== 'hdfs' ) {
                    return fs.moveAsync( srcUri.path(true), destURI.path(true) );
                } else {
                    return self.copy( path, destination ).then( () => self.remove( path ) );
                }
            });
    }

    unlink( path, recursive = null) {
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.unlinkAsync( uri.path(true) ) :
            self._sendRequest( 'delete', 'DELETE', uri, { recursive } ).then( res => res.data )
        );
    }

    remove( path ) {
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.removeAsync( uri.path(true) ) :
            self.unlink( path, true )
        );
    }

    stat( path ) {
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.statAsync( uri.path(true) ) :
            self._sendRequest( 'get', 'GETFILESTATUS', uri ).then( res => res.data.FileStatus )
        );
    }

    writeJson( path, json, opts = {} ) {
        const self = this;
        return validateUri( path ).then( uri => {
            const useHDFS = uri.protocol() === 'hdfs';

            if (typeof json !== 'object')
                throw new ValidationError('Input must be an object. Try using writeFile instead or convert to an object.');

            if ( !useHDFS ) return fs.writeJsonAsync( uri.path(true), json, opts );
            
            return self.writeFile( path, JSON.stringify( json ), opts );
        });
    }

    writeFile( path, data, opts = {} ) {
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.writeFileAsync( uri.path(true), data, opts ) :
            self._sendRequest( 'put', 'CREATE', uri, opts )
                .then( res => res.headers.location )
                .then( url => axios.request( { url, method: 'put', data } ) )
                .then( res => res.data )
                .catch( err => handleHDFSError )
        );
    }

    appendFile( path, data, opts = {} ) {
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.appendFileAsync( uri.path(true), data, opts ) :
            self._sendRequest( 'post', 'APPEND', uri, opts )
                .then( res => res.headers.location )
                .then( url => axios.request( { url, method: 'post', data } ) )
                .then( res => res.data )
                .catch( err => handleHDFSError )
        );
    }

    readFile( path, opts = {} ) {
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.readFileAsync( uri.path(true), opts ) :
            self._sendRequest( 'get', 'OPEN', uri, opts )
                .then( res => res.headers.location )
                .then( url => axios.request( { url, method: 'get' } ) )
                .then( res => res.data )
                .catch( err => handleHDFSError )
        );
    }

    readJson( path, opts = {} ) {
        const self = this;
        return validateUri( path ).then( uri => uri.protocol() !== 'hdfs' ?
            fs.readJsonAsync( uri.path(true), opts ) :
            this.readFile( path, opts).then( JSON.stringify )
        );
    }
}
