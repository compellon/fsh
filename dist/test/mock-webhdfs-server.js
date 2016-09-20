'use strict';

Object.defineProperty(exports, "__esModule", {
    value: true
});

var _nock = require('nock');

var _nock2 = _interopRequireDefault(_nock);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

exports.default = url => {
    (0, _nock2.default)(url);
};
//# sourceMappingURL=mock-webhdfs-server.js.map