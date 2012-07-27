var nodredismsg = require('./nodredismsg');

var c = new nodredismsg.connect();
c.request_operation("multiply", [15, 19], function (err, id) {
    if(err) {
        console.log(err);
        return;
    }
    c.operation_result(id, 10, function (err, ans) {
        console.log([err, ans]);
    });
});

