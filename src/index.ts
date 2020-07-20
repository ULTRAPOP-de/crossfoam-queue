import * as cfData from "@crossfoam/data";
import {throttle} from "@crossfoam/utils";

const queue = {};
const functions = {};
let removalList = [];
const functionDefinitions = {};
let wakeUpCalls = null;

/*

 If a func is part of a sequence of calls,
 the func should be passDown:true and when
 the func is done, it should simply add
 the following call to the queue.

 */

const init = (): Promise<any> => {
  if (wakeUpCalls) {
    clearInterval(wakeUpCalls);
  }

  wakeUpCalls = setInterval(() => {
    wakeUp();
  }, 3000);

  return cfData.get("queue", {})
    .then( (storedQueue: object) => {

      Object.keys(storedQueue).forEach((func) => {
        queue[func] = storedQueue[func];
      });

      return maintenance()
        .then(() => {

          // Reset all functions to be inactive
          Object.keys(functionDefinitions).forEach((func) => {
            functionDefinitions[func].active = false;
          });

          Object.keys(queue).forEach((func) => {
            if (queue[func].length > 0) {
              execute(func);
            }
          });

          return cfData.get("queueRemovalList", [])
            .then( (storedRemovalList: string[]) => {
              removalList = storedRemovalList;
              return Promise.resolve();
            });
        });
    });
};

const maintenance = (): Promise<any> => {
  return Promise.all(
    removalList.map((id) => remove(id)),
  );
};

const execute = (func: string): void => {
  if (func in queue && func in functionDefinitions && queue[func].length > 0 && !functionDefinitions[func].active) {
    functionDefinitions[func].active = true;

    // Make sure the function calls are still sorted by their original timestamp
    queue[func].sort((a, b) => a[1] - b[1]);

    // For skip functions, remove all calls with the same params from the queue
    if (functionDefinitions[func].skip && queue[func].length > 1) {
      for (let i = queue[func].length - 1; i > 0; i--) {
        if (
          // same params
          (queue[func][0][0].length === queue[func][i][0].length)
          // and same unique_id
          && (queue[func][0][2] === queue[func][i][2])
        ) {
          let allMatch = true;
          queue[func][0][0].forEach((param, pi) => {
            if (param !== queue[func][i][0][pi]) {
              allMatch = false;
            }
          });
          if (allMatch) {
            queue[func].splice(i, 1);
          }
        }
      }
    }

    // tslint:disable-next-line:no-console
    console.log(Date(), func, queue[func][0]);

    functions[func].apply(null, (functionDefinitions[func].passDown)
                          ? [...queue[func][0][0], queue[func][0][1], queue[func][0][2], {call, stillInQueue}]
                          : queue[func][0][0])
      .then((done) => {
        queue[func].splice(0, 1);
        return update();
      }).then((updatedQueue) => {
        return Promise.resolve()
          // TODO: The duration of the call and function itself is not substracted from the timeout
          .then( () => throttle(functionDefinitions[func].timeout))
          .then( () => {
            functionDefinitions[func].active = false;
            execute(func);
          });
      }).catch((err) => {
        // if network problem. try again
        setTimeout(() => {
          execute(func);
        }, 10000);

        // TODO:   catch other problems and handle
        //         Depending on err, create notification
      });
  }
};

const update = (): Promise<any> => {
  return cfData.set("queue", queue)
    .then(() => {
      return cfData.set("queueRemovalList", removalList);
    });
};

const call = (func: string, params: any[], initTime: number, uniqueID: string): any => {
  if (func in functions) {

    if (params.length <= functionDefinitions[func].paramCount.max
      && params.length >= functionDefinitions[func].paramCount.min) {

      queue[func].push([params, initTime, uniqueID]);
      execute(func);

    } else {
      throw new Error("number of parameter does not match function definition");
    }
  } else if (removalList.indexOf(uniqueID) === -1) {
    throw new Error(`the process connected to this unique id was canceled: ${uniqueID}`);
  } else {
    throw new Error(`function does not exist: ${func}`);
  }
};

const register = (func: () => Promise<any>, name: string, paramCount: number[],
                  skip: boolean, passDown: boolean, timeout: number): void => {

  functions[name] = func;
  functionDefinitions[name] = {
    active: false,
    paramCount: {
      max: paramCount[1],
      min: paramCount[0],
    },
    passDown, // should queue be passed down as last param?
    skip, // remove duplicate calls from queue on call
    timeout, // wait before next call
  };
  if (!(name in queue)) {
    queue[name] = [];
  }
};

// Check if a certain scrape still has pending jobs for a function
const stillInQueue = (uniqueID: string, func: string): boolean => {
  let inQueue = false;

  queue[func].forEach((callData) => {
    if (callData[2] === uniqueID) {
      inQueue = true;
    }
  });

  return inQueue;
};

// check if an active queue is not being called (run every 3 minutes)
const wakeUp = (): void  => {
  Object.keys(queue).forEach((func) => {
    if (queue[func].length > 0 && functionDefinitions[func].active === false) {
      execute(func);
    }
  });
};

const remove = (uniqueID: string): Promise<any> => {
  Object.keys(queue).forEach((func) => {
    for (let i = queue[func].length - 1; i >= 0; i -= 1) {
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

export {call, init, register, remove, stillInQueue};
