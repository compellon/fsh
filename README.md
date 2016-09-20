FSH
=================
`fs-h` is a promise based library that adds HDFS file system methods (by interacting with webhdfs). 
Methods support URI with the `file://`, `hdfs://` and `webhdfs://` schemes, as well as regular paths for local filesystem operations.

Inspirations
-----
- [`fs-extra`](https://github.com/jprichardson/node-fs-extra)
- lack of file URI support for file system operations
- the absence of a decently maintained hdfs/webhdfs library  

Installation
------------

```bash
npm install --save fsh
```

Usage
-----

`fsh` can be used as a drop in replacement for `fs-extra` or native `fs`.
Alternatively, there's a factory method that accepts hdfs connection options. 

As a singleton:

```js
const fs = require('fsh');
```

Using the factory method:

```js
const fs = require('fsh').create({ host: '127.0.0.1', port: 50070, path: '/webhdfs/v1' });
```