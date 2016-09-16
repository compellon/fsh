'use strict';

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

var _bluebird = require('bluebird');

var _bluebird2 = _interopRequireDefault(_bluebird);

var _axios = require('axios');

var _axios2 = _interopRequireDefault(_axios);

var _urijs = require('urijs');

var _urijs2 = _interopRequireDefault(_urijs);

var _errors = require('./errors');

var _webhdfs = require('webhdfs');

var _webhdfs2 = _interopRequireDefault(_webhdfs);

var _os = require('os');

var _os2 = _interopRequireDefault(_os);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

const fs = _bluebird2.default.promisifyAll(require('fs-extra'));

const handleHDFSError = err => {
    if (err.response) {
        if (_lodash2.default.has(err.response.data, 'RemoteException')) throw new _errors.HDFSError(data);else throw new _errors.ResponseError(`Got unexpected status code for ${ url }: ${ res.statusCode }`);
    }
    throw err;
};

const validateUri = (pathOrUri, validProtocols = ['hdfs', 'file', '']) => _bluebird2.default.try(() => {
    const uri = new _urijs2.default(pathOrUri);
    const protocol = uri.protocol();

    if (!_lodash2.default.includes(validProtocols, protocol)) throw new _errors.ValidationError(`Unsupported protocol [${ protocol }].`);

    return uri;
});

class FSH {
    constructor({ user = 'root', host = 'localhost', port = 50070, protocol = 'http', path = '/webhdfs/v1' }) {
        this.conn = { user, host, port, protocol, path, hostname: host };
        const uriParts = _lodash2.default.omit(this.conn, ['user', 'host']);
        this.baseURI = new _urijs2.default(uriParts);
        this.client = _axios2.default.create();
        this.client.defaults.baseURL = this.baseURI.toString();
        this.client.defaults.maxRedirects = 0;
    }

    _constructURL(path, op, params = {}) {
        params['user.name'] = params['user.name'] || this.conn.user;
        const queryParams = _lodash2.default.extend({ op }, params);
        const uriParts = _lodash2.default.extend(_lodash2.default.clone(this.conn), { path: this.conn.path + path });
        return new _urijs2.default(uriParts).query(queryParams);
    }

    _sendRequest(method, op, uri, params = {}) {
        const url = this._constructURL(uri.path(), op, params).toString();
        const opts = { url, method };

        if (uri.hostname()) opts.baseURL = new _urijs2.default(this.baseURI).hostname(uri.hostname()).toString();

        return this.client.request(opts).catch(handleHDFSError);
    }

    mkdir(path, mode = 0o755) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.ensureDirAsync(uri.path(), mode) : self._sendRequest('put', 'MKDIRS', uri, { permissions: mode }).then(res => res.data));
    }

    chmod(path, mode = 0o755) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.chmodAsync(path, mode) : self._sendRequest('put', 'SETPERMISSION', uri, { permissions: mode }).then(res => res.data));
    }

    chown(path, owner, group) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.chownAsync(path, owner, group) : self._sendRequest('put', 'SETOWNER', uri, { owner, group }).then(res => res.data));
    }

    readdir(path) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.readdirAsync(path, null) : self._sendRequest('get', 'LISTSTATUS', uri).then(res => res.data.FileStatuses.FileStatus));
    }

    copy(path, destination) {
        const self = this;
        return _bluebird2.default.all([validateUri(path), validateUri(destination)]).spread((srcURI, destURI) => {
            if (srcURI.protocol() !== 'hdfs' && destURI.protocol() !== 'hdfs') return fs.copyAsync(srcURI.path(), destURI.path());else if (srcURI.protocol() === 'hdfs' && destURI.protocol() !== 'hdfs') return self.copyToLocal(path, destination);else if (srcURI.protocol() !== 'hdfs' && destURI.protocol() === 'hdfs') return self.copyFromLocal(path, destination);else if (srcURI.protocol() === 'hdfs' && destURI.protocol() === 'hdfs') {
                const tmpDir = _os2.default.tmpdir();
                const timestamp = new Date().getTime();
                // TODO: replace with guids?
                const tmpFile = `${ tmpDir }/${ timestamp }`;

                return self.copyToLocal(path, tmpFile).then(() => self.copyFromLocal(tmpFile, destination));
            }
        });
    }

    // TODO: implement without webhdfs lib
    copyToLocal(hdfsSrc, destination) {
        return _bluebird2.default.all([validateUri(hdfsSrc, ['hdfs']), validateUri(destination, ['file', ''])]).spread((srcUri, destUri) => {
            const conn = _lodash2.default.omit(this.conn, 'hostname');
            if (srcUri.hostname()) conn.host = srcUri.hostname();
            const hdfs = _webhdfs2.default.createClient(conn);

            const remoteFileStream = hdfs.createReadStream(srcUri.path());
            const localFileStream = fs.createWriteStream(destUri.path());

            return new _bluebird2.default((resolve, reject) => {
                remoteFileStream.pipe(localFileStream);

                localFileStream.on('error', reject);

                localFileStream.on('finish', res => {
                    if (_lodash2.default.isError(res)) {
                        return reject(res);
                    }
                    resolve();
                });
            });
        });
    }

    // TODO: implement without webhdfs lib 
    copyFromLocal(path, hdfsDestination) {
        const self = this;
        return _bluebird2.default.all([validateUri(path, ['file', '']), validateUri(hdfsDestination, ['hdfs'])]).spread((srcUri, destUri) => {
            const conn = _lodash2.default.omit(this.conn, 'hostname');
            if (srcUri.hostname()) conn.host = srcUri.hostname();
            const hdfs = _webhdfs2.default.createClient(conn);

            const localFileStream = fs.createReadStream(srcUri.path());
            const remoteFileStream = hdfs.createWriteStream(destUri.path());

            return new _bluebird2.default((resolve, reject) => {
                localFileStream.pipe(remoteFileStream);

                remoteFileStream.on('error', reject);

                remoteFileStream.on('finish', res => {
                    if (_lodash2.default.isError(res)) {
                        return reject(res);
                    }
                    resolve();
                });
            });
        });
    }

    rename(path, destination) {
        const self = this;
        return _bluebird2.default.all([validateUri(path), validateUri(destination)]).spread((srcUri, destURI) => {
            if (srcUri.protocol() !== 'hdfs' && destURI.protocol() !== 'hdfs') {
                return fs.moveAsync(srcUri.path(), destURI.path());
            } else {
                return self.copy(path, destination).then(() => self.remove(path));
            }
        });
    }

    unlink(path, recursive = null) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.unlinkAsync(path) : self._sendRequest('delete', 'DELETE', uri, { recursive }).then(res => res.data));
    }

    remove(path) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.removeAsync(path) : self.unlink(path, true));
    }

    stat(path) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.statAsync(path) : self._sendRequest('get', 'GETFILESTATUS', uri).then(res => res.data.FileStatus));
    }

    writeJson(path, json, opts = {}) {
        const self = this;
        return validateUri(path).then(uri => {
            const useHDFS = uri.protocol() === 'hdfs';

            if (typeof json !== 'object') throw new _errors.ValidationError('Input must be an object. Try using writeFile instead or convert to an object.');

            if (!useHDFS) return fs.writeJsonAsync(path, json, opts);

            return self.writeFile(path, JSON.stringify(json), opts);
        });
    }

    writeFile(path, data, opts = {}) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.writeFileAsync(path, data, opts) : self._sendRequest('put', 'CREATE', uri, opts).then(res => res.headers.location).then(url => _axios2.default.request({ url, method: 'put', data })).then(res => res.data).catch(err => handleHDFSError));
    }

    appendFile(path, data, opts = {}) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.appendFileAsync(path, data, opts) : self._sendRequest('post', 'APPEND', uri, opts).then(res => res.headers.location).then(url => _axios2.default.request({ url, method: 'post', data })).then(res => res.data).catch(err => handleHDFSError));
    }

    readFile(path, opts = {}) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.readFileAsync(path, opts) : self._sendRequest('get', 'OPEN', uri, opts).then(res => res.headers.location).then(url => _axios2.default.request({ url, method: 'get' })).then(res => res.data).catch(err => handleHDFSError));
    }

    readJson(path, opts = {}) {
        const self = this;
        return validateUri(path).then(uri => uri.protocol() !== 'hdfs' ? fs.readJsonAsync(path, opts) : this.readFile(path, opts).then(JSON.stringify));
    }
}

module.exports = FSH;
//# sourceMappingURL=index.js.map