"use strict";
var __spreadArrays = (this && this.__spreadArrays) || function () {
    for (var s = 0, i = 0, il = arguments.length; i < il; i++) s += arguments[i].length;
    for (var r = Array(s), k = 0, i = 0; i < il; i++)
        for (var a = arguments[i], j = 0, jl = a.length; j < jl; j++, k++)
            r[k] = a[j];
    return r;
};
Object.defineProperty(exports, "__esModule", { value: true });
var cfData = require("@crossfoam/data");
var utils_1 = require("@crossfoam/utils");
var queue = {};
var functions = {};
var removalList = [];
var functionDefinitions = {};
var wakeUpCalls = null;
/*

 If a func is part of a sequence of calls,
 the func should be passDown:true and when
 the func is done, it should simply add
 the following call to the queue.

 */
var init = function () {
    if (wakeUpCalls) {
        clearInterval(wakeUpCalls);
    }
    wakeUpCalls = setInterval(function () {
        wakeUp();
    }, 3000);
    return cfData.get("queue", {})
        .then(function (storedQueue) {
        Object.keys(storedQueue).forEach(function (func) {
            queue[func] = storedQueue[func];
        });
        return maintenance()
            .then(function () {
            // Reset all functions to be inactive
            Object.keys(functionDefinitions).forEach(function (func) {
                functionDefinitions[func].active = false;
            });
            Object.keys(queue).forEach(function (func) {
                if (queue[func].length > 0) {
                    execute(func);
                }
            });
            return cfData.get("queueRemovalList", [])
                .then(function (storedRemovalList) {
                removalList = storedRemovalList;
                return Promise.resolve();
            });
        });
    });
};
exports.init = init;
var maintenance = function () {
    return Promise.all(removalList.map(function (id) { return remove(id); }));
};
var execute = function (func) {
    if (func in queue && func in functionDefinitions && queue[func].length > 0 && !functionDefinitions[func].active) {
        functionDefinitions[func].active = true;
        // Make sure the function calls are still sorted by their original timestamp
        queue[func].sort(function (a, b) { return a[1] - b[1]; });
        // For skip functions, remove all calls with the same params from the queue
        if (functionDefinitions[func].skip && queue[func].length > 1) {
            var _loop_1 = function (i) {
                if (
                // same params
                (queue[func][0][0].length === queue[func][i][0].length)
                    // and same unique_id
                    && (queue[func][0][2] === queue[func][i][2])) {
                    var allMatch_1 = true;
                    queue[func][0][0].forEach(function (param, pi) {
                        if (param !== queue[func][i][0][pi]) {
                            allMatch_1 = false;
                        }
                    });
                    if (allMatch_1) {
                        queue[func].splice(i, 1);
                    }
                }
            };
            for (var i = queue[func].length - 1; i > 0; i--) {
                _loop_1(i);
            }
        }
        console.log(Date(), func, queue[func][0]);
        functions[func].apply(null, (functionDefinitions[func].passDown)
            ? __spreadArrays(queue[func][0][0], [queue[func][0][1], queue[func][0][2], { call: call, stillInQueue: stillInQueue }]) : queue[func][0][0])
            .then(function (done) {
            queue[func].splice(0, 1);
            return update();
        }).then(function (updatedQueue) {
            return Promise.resolve()
                // TODO: The duration of the call and function itself is not substracted from the timeout
                .then(function () { return utils_1.throttle(functionDefinitions[func].timeout); })
                .then(function () {
                functionDefinitions[func].active = false;
                execute(func);
            });
        }).catch(function (err) {
            throw err;
            // TDOO:   Timeout and try again...
            //         Depending on err, create notification
        });
    }
};
var update = function () {
    return cfData.set("queue", queue)
        .then(function () {
        return cfData.set("queueRemovalList", removalList);
    });
};
var call = function (func, params, initTime, uniqueID) {
    if (func in functions) {
        if (params.length <= functionDefinitions[func].paramCount.max
            && params.length >= functionDefinitions[func].paramCount.min) {
            queue[func].push([params, initTime, uniqueID]);
            execute(func);
        }
        else {
            throw new Error("number of parameter does not match function definition");
        }
    }
    else if (removalList.indexOf(uniqueID) === -1) {
        throw new Error("the process connected to this unique id was canceled: " + uniqueID);
    }
    else {
        throw new Error("function does not exist: " + func);
    }
};
exports.call = call;
var register = function (func, name, paramCount, skip, passDown, timeout) {
    functions[name] = func;
    functionDefinitions[name] = {
        active: false,
        paramCount: {
            max: paramCount[1],
            min: paramCount[0],
        },
        passDown: passDown,
        skip: skip,
        timeout: timeout,
    };
    if (!(name in queue)) {
        queue[name] = [];
    }
};
exports.register = register;
// Check if a certain scrape still has pending jobs for a function
var stillInQueue = function (uniqueID, func) {
    var inQueue = false;
    queue[func].forEach(function (callData) {
        if (callData[2] === uniqueID) {
            inQueue = true;
        }
    });
    return inQueue;
};
exports.stillInQueue = stillInQueue;
// check if an active queue is not being called (run every 3 minutes)
var wakeUp = function () {
    Object.keys(queue).forEach(function (func) {
        if (queue[func].length > 0 && functionDefinitions[func].active === false) {
            execute(func);
        }
    });
};
var remove = function (uniqueID) {
    Object.keys(queue).forEach(function (func) {
        for (var i = queue[func].length - 1; i >= 0; i -= 1) {
            if (queue[func][i][2] === uniqueID) {
                queue[func].splice(i, 1);
            }
        }
    });
    if (removalList.indexOf(uniqueID) === -1) {
        removalList.push(uniqueID);
    }
    return update();
};
exports.remove = remove;
//# sourceMappingURL=index.js.map