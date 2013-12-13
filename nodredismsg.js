
var nodredis = require('nodredis');
var events = require('events');
var util = require('util');
var uuid = require('node-uuid');

function connect(host) {
    this.host = host || "localhost";
}

connect.prototype._rediscli = function() {
    return new nodredis.connect(this.host);
}

connect.prototype.request_operation = function(op, pars, cb) {
    var id = uuid.v4();
    var op = 'msg/'+op;
    var cl = this._rediscli();
    cl.cmd('lpush',op, JSON.stringify([id,pars]), function (err) {
        cl.end();
        if(cb) return cb(err, id);
    });
}

function get_operation(host, pars) {
    var self = this;
    this.connect = new connect(host);
    this.opers_active = {};
    this.requesting_oper = false;
    this.op = pars.op;
    this.qtt = pars.qtt || 10;
    this.processing_timeout = pars.timeout || 5*60;
    this.retries = pars.retries || 500;
    this.pending_superv = {}
    process.nextTick(function () {
        self._recover_pending();
        self._listen_for_operations();
    });
}

util.inherits(get_operation, events.EventEmitter);

get_operation.prototype._recover_pending = function() {
    var self = this;
    if(this.qtt <= 0) return;
    var opd = 'msg/pend/'+this.op;
    var cl = this.connect._rediscli();
    cl.cmd('lrange', opd, 0, -1, function (err, dad) {
        cl.end();
        var pending = {};
        for(var i in dad) {
            var el = dad[i];
            pending[el] = (self.pending_superv[el] || 0)+1;
            if(pending[el] >= 7) {
                var el2 = null;
                try {
                    var elp = JSON.parse(el);
                    var retry = (parseInt(elp[2]) || 0)+1;
                    elp[2] = retry;
                    if(retry < self.retries) el2 = JSON.stringify(elp);
                } catch(e) {
                };
                var cl2 = self.connect._rediscli();
                cl2.cmd('multi');
                cl2.cmd('lrem', opd, -1, el);
                if(el2) cl2.cmd('lpush', 'msg/'+self.op, el2);
                cl2.cmd('exec');
                cl2.end();
                delete pending[el];
            }
        }
        self.pending_superv = pending;
    });
    setTimeout(function () {
        self._recover_pending();
    }, this.processing_timeout*1000/5);
}

get_operation.prototype._get_one_operation = function(cb) {
    var self = this;
    var opf = 'msg/'+this.op;
    var opd = 'msg/pend/'+this.op;
    var cl = this.connect._rediscli();
    cl.cmd('brpoplpush',opf, opd, 60, function (err, resp) {
        cl.end();
        if(!err && !resp) return cb();
        if(err) return cb(err);
        var id, oper,err2;
        try {
            var dec = JSON.parse(resp);
            id = dec[0];
            oper = dec[1];
        } catch(err) {
            err2 = err;
        }
        if(err2) return cb(err2);
        var done = function () {
            var cl = self.connect._rediscli();
            cl.cmd('lrem', opd, -1, resp, function (err, rconc) {
                cl.end();
            });
        };
        cb(null, {id:id, oper:oper, done:done});
    });
}

get_operation.prototype._can_handle_more_operations = function() {
    var num_opers = 0;
    for (i in this.opers_active) num_opers++;
    return (num_opers < this.qtt);
}

function answer(get_operation, id) {
    this.get_operation = get_operation;
    this.id = id;
}

answer.prototype.partial = function(resp, cb) {
    var op = 'msg/din/'+this.id;
    var cl = this.get_operation.connect._rediscli();
    cl.cmd('lpush', op, JSON.stringify(resp), function (err1) {
        cl.cmd('expire', op, 3600, function (err2) {
            cl.end();
            if(cb) return cb(err1 || err2);
        });
    });
}

answer.prototype.done = function() {
    this.get_operation._operation_done(this.id, true);
}

answer.prototype.aborted = function() {
    this.get_operation._operation_done(this.id, false);
}

answer.prototype.answer = function(resp, cb) {
    var self = this;
    this.partial(resp, function (err) {
        if(err) return cb(err);
        self.done(true);
        if(cb) cb();
    });
}

get_operation.prototype._listen_for_operations = function() {
    var self = this;
    if(self.requesting_oper) return;
    if(!self._can_handle_more_operations()) return;
    self.requesting_oper = true;
    function next() {
        self._listen_for_operations();
    }
    self._get_one_operation(
        function (err, oper) {
            self.requesting_oper = false;
            if(err) {
                self.emit('error', err);
                setTimeout(next, 500);
                return;
            }
            if(!oper) return process.nextTick(next);
            var timeout = setTimeout(function () {
                delete self.opers_active[oper.id];
                process.nextTick(next);
            }, self.processing_timeout*1000);
            self.opers_active[oper.id] = [oper.done, timeout];
            self.emit('operation', oper.oper, new answer(self, oper.id));
            process.nextTick(next);
        }
    );
}

get_operation.prototype._operation_done = function(id, ok) {
    var self = this;
    var done = self.opers_active[id];
    if(!done) return;
    clearTimeout(done[1]);
    delete self.opers_active[id];
    process.nextTick(function () {
        self._listen_for_operations();
    });
    if(ok) done[0]();
}

get_operation.prototype.end = function() {
    this.qtt = -1;
}

connect.prototype.listen_for_operations = function(pars) {
    if(typeof pars != typeof {}) {
        pars = {
            op: arguments[0], qtt: arguments[1],
            timeout: arguments[2], retries: arguments[3]
        };
    }
    return new get_operation(this.host, pars);
}

connect.prototype.operation_result = function(id, timeout, cb) {
    var op = 'msg/din/'+id;
    var cl = this._rediscli();
    function cbr(err, resp) {
        cl.end();
        if(err) return cb(err);
        if(!resp) return cb(err, resp);
        var dec;
        try {
            dec = JSON.parse(resp[1]);
        } catch(err) {
            return cb(err);
        }
        cb(null, dec);
    }
    if(timeout) {
        cl.cmd('brpop', op, timeout, cbr);
    } else {
        cl.cmd('rpop', op, cbr);
    }
}

module.exports = {
    connect: connect
}
