const async = require('async');

var arr = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
async.eachLimit(arr, 5,
    function (file, complete) {
        console.log(file);
        complete();
    },
    function (err, result) { // 4
        //  next(null, err); // 5
    }
);

function sleep(ms) {
    return new Promise(resolve => {
        setTimeout(resolve, ms)
    })
}
sleep(1000);