nodredismsg
===========

Messaging framework using Redis

Examples
========

Server side
-----------
```javascript
var nodredismsg = require('nodredismsg');

var c = new nodredismsg.connect();

var s1 = c.listen_for_operations('multiply', 4);
s1.on('error', console.error);
s1.on('operation', function (oper, res) {
    console.log(res.id);
    res.answer(oper[0]*oper[1]);
});

var s2 = c.listen_for_operations({op:'log', qtt:1, timeout:5, retries:5});
s2.on('error', console.error);
s2.on('operation', function (oper, res) {
    console.log(oper);
    res.aborted();
});
```

Client side
-----------
```javascript
var nodredismsg = require('nodredismsg');

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
```

