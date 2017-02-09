import { expect } from 'chai';
import request from 'supertest';
import os from 'os';
import path from 'path';
import fsh from '../lib/fsh';
import uuid from 'node-uuid';
import URI from 'urijs';
import Promise from 'bluebird';
import colors from 'colors';
// Using the native fs module for testing fsh
import fs from 'fs';
import fse from 'fs-extra';

Promise.promisifyAll(fse);

// const fsClient = fs.create({ host: '127.0.0.1', port: 1234, protocol: 'http', path: '/webhdfs/v1' });
const webHDFSServer = request('http://127.0.0.1:50070/webhdfs/v1');

/**
 * - [copy](#copy)
 * - [copySync](#copy)
 * - [emptyDir](#emptydirdir-callback)
 * - [emptyDirSync](#emptydirdir-callback)
 * - [ensureFile](#ensurefilefile-callback)
 * - [ensureFileSync](#ensurefilefile-callback)
 * - [ensureDir](#ensuredirdir-callback)
 * - [ensureDirSync](#ensuredirdir-callback)
 * - [ensureLink](#ensurelinksrcpath-dstpath-callback)
 * - [ensureLinkSync](#ensurelinksrcpath-dstpath-callback)
 * - [ensureSymlink](#ensuresymlinksrcpath-dstpath-type-callback)
 * - [ensureSymlinkSync](#ensuresymlinksrcpath-dstpath-type-callback)
 * - [mkdirs](#mkdirsdir-callback)
 * - [mkdirsSync](#mkdirsdir-callback)
 * - [move](#movesrc-dest-options-callback)
 * - [outputFile](#outputfilefile-data-options-callback)
 * - [outputFileSync](#outputfilefile-data-options-callback)
 * - [outputJson](#outputjsonfile-data-options-callback)
 * - [outputJsonSync](#outputjsonfile-data-options-callback)
 * - [readJson](#readjsonfile-options-callback)
 * - [readJsonSync](#readjsonfile-options-callback)
 * - [remove](#removedir-callback)
 * - [removeSync](#removedir-callback)
 * - [walk](#walk)
 * - [writeJson](#writejsonfile-object-options-callback)
 * - [writeJsonSync](#writejsonfile-object-options-callback)
 */

const TEST_DIR = path.join( 'fsh', uuid.v4() );
const LOCAL_TEST_DIR = path.join( os.tmpdir(), TEST_DIR );
const LOCAL_TEST_DIR_URI = new URI( LOCAL_TEST_DIR ).scheme( 'file' ).toString();
const HDFS_TEST_DIR_URI = new URI( { protocol: 'hdfs', hostname: '127.0.0.1', port: 9000, path: `/tmp/${TEST_DIR}` } ).toString();
const WEBHDFS_TEST_DIR_URI = new URI( { protocol: 'webhdfs', hostname: '127.0.0.1', port: 1234, path: `/webhdfs/v1/tmp/${TEST_DIR}` } ).toString();

console.log( `Directory path to be used in local testing: ${LOCAL_TEST_DIR.cyan}`.yellow );
console.log( `Directory URI to be used in local testing: ${LOCAL_TEST_DIR_URI.cyan}`.yellow );
console.log( `Directory HDFS URI to be used in local testing: ${HDFS_TEST_DIR_URI.cyan}`.yellow );
console.log( `Directory WEBHDFS URI to be used in local testing: ${WEBHDFS_TEST_DIR_URI.cyan}`.yellow );


before( () => fse.ensureDirSync( LOCAL_TEST_DIR ) );

after( () => fse.removeAsync( path.join( os.tmpdir(), 'fsh' ) ) );

describe( 'mkdir', () => {
    const testDirPath = path.join(LOCAL_TEST_DIR, 'somedir');
    const testDirUri = new URI({ protocol: 'file', path: testDirPath });
    
    it( 'should create a local directory provided a path', () =>
        fsh.mkdir( testDirPath ).then( () => fs.accessSync( testDirPath ) )
    );

    it( 'should create a local directory with provided a file uri', () =>
        fsh.mkdir( testDirUri ).then( () => fs.accessSync( testDirUri.path() ) )
    );
});

describe( 'copy', () => {
    const srcFileName = uuid.v4();
    const srcFilePath = path.join(LOCAL_TEST_DIR, srcFileName);
    const srcFileUri = new URI({ protocol: 'file', path: srcFilePath });
    const destFileName = uuid.v4();
    const destFilePath = path.join(LOCAL_TEST_DIR, destFileName);
    const destFileUri = new URI({ protocol: 'file', path: destFilePath });

    beforeEach( () => fse.writeFile(srcFilePath, 'This is some really interesting text') );
    afterEach( () => fse.removeAsync(srcFilePath) );

    it( 'should copy a local file path to a destination file path', () =>
        fsh.copy( srcFilePath, destFilePath )
            .then( () => fs.accessSync(destFilePath) )
            .then( () => fs.unlink(destFilePath) )
    );

    it( 'should copy a local file path to a destination file uri', () =>
        fsh.copy( srcFilePath, destFileUri.toString() )
            .then( () => fs.accessSync(destFileUri.path()) )
            .then( () => fs.unlink(destFileUri.path()) )
    );

    it( 'should copy a file uri to a destination path', () =>
        fsh.copy(srcFileUri.toString(), destFilePath)
            .then( () => fs.accessSync(destFilePath) )
            .then( () => fs.unlink(destFilePath) )
    );

    it( 'should copy a file uri to a destination file uri', () =>
        fsh.copy( srcFileUri.toString(), destFileUri.toString() )
            .then( () => fs.accessSync(destFileUri.path()) )
            .then( () => fs.unlink(destFileUri.path()) )
    );
});
