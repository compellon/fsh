const FSH = require('./src/index.js');

const fs = new FSH({ user: 'root', port: 50070, useHDFS: true });
const Promise = require('bluebird');
const stat = Promise.promisify(fs.stat, {context: fs});

stat('/webhdfs').then( (data) => console.log(data)).catch(err => console.log(err));

// fs.unlink( '/webhdfs', true, err => {
//     if (err) throw err;
//     console.log('Successfully deleted /webhdfs');
//
//     fs.unlink( '/ezra', true, err => {
//         if (err) throw err;
//         console.log('Successfully deleted /ezra');
//
//         fs.mkdir( '/ezra', 0o755, err => {
//             if (err) throw err;
//             console.log('Successfully created /ezra');
//
//             fs.writeFile ( '/ezra/sometext.txt', 'abcdefg', {}, err => {
//                 if (err) throw err;
//                 console.log('Successfully wrote \'abcdefg\' to  /ezra/sometext.txt');
//
//                 fs.readdir( '/ezra', (err, files) => {
//                     if (err) throw err;
//                     console.log('Files in /ezra: ', files);
//
//                     fs.readFile( '/ezra/sometext.txt', {}, (err, data) => {
//                         if (err) throw err;
//                         console.log('Content of /ezra/sometext.txt: ', data);
//
//                         fs.appendFile( '/ezra/sometext.txt', 'hijklmnop', {}, err => {
//                             if (err) throw err;
//                             console.log('Successfully appended \'hijklmnop\' to /ezra/sometext.txt');
//
//                             fs.readFile( '/ezra/sometext.txt', {}, (err, data) => {
//                                 if (err) throw err;
//                                 console.log('Content of /ezra/sometext.txt: ', data);
//
//                                 fs.rename( '/ezra/sometext.txt', '/ezra/othertext.txt', err => {
//                                     if (err) throw err;
//                                     console.log('Successfully renamed /ezra/sometext.txt to /ezra/othertext.txt');
//
//                                     fs.exists( '/ezra/sometext.txt', (err, exists) => {
//                                         if (err) throw err;
//                                         console.log('/ezra/sometext.txt exists: ', exists);
//
//                                         fs.exists( '/ezra/othertext.txt', (err, exists) => {
//                                             if (err) throw err;
//                                             console.log('/ezra/othertext.txt exists: ', exists);
//
//                                             fs.copy('/ezra/othertext.txt', '/ezra/othertext3.txt', err => {
//                                                 if (err) throw err;
//                                                 console.log('Copied /ezra/othertext.txt to /ezra/othertext2.txt');
//                                             });
//                                         });
//
//                                     });
//                                 });
//                             });
//                         });
//                     });
//                 });
//             });
//         });
//     });
// });

















