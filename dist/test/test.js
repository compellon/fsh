'use strict';

var _chai = require('chai');

var _supertest = require('supertest');

var _supertest2 = _interopRequireDefault(_supertest);

var _os = require('os');

var _os2 = _interopRequireDefault(_os);

var _path = require('path');

var _path2 = _interopRequireDefault(_path);

var _fsh = require('../lib/fsh');

var _fsh2 = _interopRequireDefault(_fsh);

var _nodeUuid = require('node-uuid');

var _nodeUuid2 = _interopRequireDefault(_nodeUuid);

var _urijs = require('urijs');

var _urijs2 = _interopRequireDefault(_urijs);

var _bluebird = require('bluebird');

var _bluebird2 = _interopRequireDefault(_bluebird);

var _colors = require('colors');

var _colors2 = _interopRequireDefault(_colors);

var _fs = require('fs');

var _fs2 = _interopRequireDefault(_fs);

var _fsExtra = require('fs-extra');

var _fsExtra2 = _interopRequireDefault(_fsExtra);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

// Using the native fs module for testing fsh
_bluebird2.default.promisifyAll(_fsExtra2.default);

// const fsClient = fs.create({ host: '127.0.0.1', port: 1234, protocol: 'http', path: '/webhdfs/v1' });
const webHDFSServer = (0, _supertest2.default)('http://127.0.0.1:50070/webhdfs/v1');

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

const TEST_DIR = _path2.default.join('fsh', _nodeUuid2.default.v4());
const LOCAL_TEST_DIR = _path2.default.join(_os2.default.tmpdir(), TEST_DIR);
const LOCAL_TEST_DIR_URI = new _urijs2.default(LOCAL_TEST_DIR).scheme('file').toString();
const HDFS_TEST_DIR_URI = new _urijs2.default({ protocol: 'hdfs', hostname: '127.0.0.1', port: 9000, path: `/tmp/${TEST_DIR}` }).toString();
const WEBHDFS_TEST_DIR_URI = new _urijs2.default({ protocol: 'webhdfs', hostname: '127.0.0.1', port: 1234, path: `/webhdfs/v1/tmp/${TEST_DIR}` }).toString();

console.log(`Directory path to be used in local testing: ${LOCAL_TEST_DIR.cyan}`.yellow);
console.log(`Directory URI to be used in local testing: ${LOCAL_TEST_DIR_URI.cyan}`.yellow);
console.log(`Directory HDFS URI to be used in local testing: ${HDFS_TEST_DIR_URI.cyan}`.yellow);
console.log(`Directory WEBHDFS URI to be used in local testing: ${WEBHDFS_TEST_DIR_URI.cyan}`.yellow);

before(() => _fsExtra2.default.ensureDirSync(LOCAL_TEST_DIR));

after(() => _fsExtra2.default.removeAsync(_path2.default.join(_os2.default.tmpdir(), 'fsh')));

describe('mkdir', () => {
    const testDirPath = _path2.default.join(LOCAL_TEST_DIR, 'somedir');
    const testDirUri = new _urijs2.default({ protocol: 'file', path: testDirPath });

    it('should create a local directory provided a path', () => _fsh2.default.mkdir(testDirPath).then(() => _fs2.default.accessSync(testDirPath)));

    it('should create a local directory with provided a file uri', () => _fsh2.default.mkdir(testDirUri).then(() => _fs2.default.accessSync(testDirUri.path())));
});

describe('copy', () => {
    const srcFileName = _nodeUuid2.default.v4();
    const srcFilePath = _path2.default.join(LOCAL_TEST_DIR, srcFileName);
    const srcFileUri = new _urijs2.default({ protocol: 'file', path: srcFilePath });
    const destFileName = _nodeUuid2.default.v4();
    const destFilePath = _path2.default.join(LOCAL_TEST_DIR, destFileName);
    const destFileUri = new _urijs2.default({ protocol: 'file', path: destFilePath });

    beforeEach(() => _fsExtra2.default.writeFile(srcFilePath, 'This is some really interesting text'));
    afterEach(() => _fsExtra2.default.removeAsync(srcFilePath));

    it('should copy a local file path to a destination file path', () => _fsh2.default.copy(srcFilePath, destFilePath).then(() => _fs2.default.accessSync(destFilePath)).then(() => _fs2.default.unlink(destFilePath)));

    it('should copy a local file path to a destination file uri', () => _fsh2.default.copy(srcFilePath, destFileUri.toString()).then(() => _fs2.default.accessSync(destFileUri.path())).then(() => _fs2.default.unlink(destFileUri.path())));

    it('should copy a file uri to a destination path', () => _fsh2.default.copy(srcFileUri.toString(), destFilePath).then(() => _fs2.default.accessSync(destFilePath)).then(() => _fs2.default.unlink(destFilePath)));

    it('should copy a file uri to a destination file uri', () => _fsh2.default.copy(srcFileUri.toString(), destFileUri.toString()).then(() => _fs2.default.accessSync(destFileUri.path())).then(() => _fs2.default.unlink(destFileUri.path())));
});
//# sourceMappingURL=test.js.map