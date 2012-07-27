var nodredismsg = require('./nodredismsg');

var c = new nodredismsg.connect();
c.request_operation("log", ["log "+Math.random()], function (err, id) {
    if(err) {
        console.log(err);
        return;
    }
});

