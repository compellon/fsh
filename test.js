const HDFS = require('./src/index.js');

const fs = new HDFS({ user: 'root', port: 50070, useHDFS: true });

fs.unlink( '/webhdfs', true, err => {
    if (err) throw err;
    console.log('Success')
});

fs.unlink( '/ezra', true, err => {
    if
})
    // .then( () => localhdfs.unlink( '/ezra', true ) )
    // .spread( (data, res) => {
    //     console.log(res.statusCode);
    //     console.log(data);
    // })
    // .then( () => localhdfs.mkdir( '/ezra' ) )
    // .spread( (data, res) => {
    //     console.log('statusCode: ', res.statusCode);
    //     console.log('data: ', data);
    // })
    // .then( () => localhdfs.writeFile( '/ezra/sometext.txt', 'abcdefg' ) )
    // .then( () => localhdfs.readdir( '/ezra' ) )
    // .then( (files) => console.log( files ) )
    // .then( () => localhdfs.readFile( '/ezra/sometext.txt' ) )
    // .spread( (data, res) => {
    //     console.log('statusCode: ', res.statusCode);
    //     console.log('data: ', data);
    // })
    // .then( () => localhdfs.appendFile( '/ezra/sometext.txt', 'abcdefg' ) )
    // .spread( (data, res) => {
    //     console.log('statusCode: ', res.statusCode);
    //     console.log('data: ', data);
    // })
    // .then( () => localhdfs.readFile( '/ezra/sometext.txt' ) )
    // .spread( (data, res) => {
    //     console.log('statusCode: ', res.statusCode);
    //     console.log('data: ', data);
    // })
    // .then( () => localhdfs.rename( '/ezra/sometext.txt', '/ezra/othertext.txt') )
    // .spread( (data, res) => {
    //     console.log('statusCode: ', res.statusCode);
    //     console.log('data: ', data);
    // })
    // .then( () => Promise.all([localhdfs.exists('/ezra/sometext.txt'), localhdfs.exists('/ezra/othertext.txt')]) )
    // .spread( ( first, second ) => console.log( `'/ezra/sometext.txt exists: ${first}, '/ezra/othertext.txt exists: ${second}` ) );
