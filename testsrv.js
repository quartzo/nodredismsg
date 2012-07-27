var nodredismsg = require('./nodredismsg');

var c = new nodredismsg.connect();

var s1 = c.listen_for_operations('multiply', 4);
s1.on('error', console.error);
s1.on('operation', function (oper, res) {
    res.answer(oper[0]*oper[1]);
});

var s2 = c.listen_for_operations('log', 1);
s2.on('error', console.error);
s2.on('operation', function (oper, res) {
    console.log(oper);
    res.done();
});
