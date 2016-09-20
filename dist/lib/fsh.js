'use strict';

Object.defineProperty(exports, "__esModule", {
    value: true
});

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

var _FSH = require('./core/FSH');

var _FSH2 = _interopRequireDefault(_FSH);

var _fsExtra = require('fs-extra');

var _fsExtra2 = _interopRequireDefault(_fsExtra);

var _bluebird = require('bluebird');

var _bluebird2 = _interopRequireDefault(_bluebird);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

const createInstance = (defaultConfig = {}) => {
    const instance = new _FSH2.default(defaultConfig);
    // Add aliases
    instance.append = instance.appendFile;
    instance.move = instance.rename;
    return instance;
};

// Create the default instance to be exported
const fsh = createInstance();

// Factory for creating new instances
fsh.create = defaultConfig => createInstance(defaultConfig);

exports.default = fsh;
//# sourceMappingURL=fsh.js.map