const { upperFirst } = require("lodash");
const { createClient } = require("redis");
// redis
const redisClient = createClient();
// console.log("redisClient", );

// total token
const Total = 10;

// one step Aman/Bman/Cman = 3/3/3
const weights = {
  Aman: 0.3,
  Bman: 0.3,
  Cman: 0.3,
};

const steps = [
  [{ dayIndex: 1 }],
  // ...
];

const logIds = [];

const printSteps = () => {
  const printHead = logIds.length === 0;
  // line title
  // console.log("id", "type", "start");
  for (const st of steps) {
    // const day = st[0].dayIndex + "";
    printHead &&
      console.log("[---------------" + st[0].dayIndex + "---------------]");
    for (let i = 1; i < st.length; i++) {
      const ele = st[i];
      (logIds.length === 0 || logIds.includes(ele.id)) &&
        console.log(
          ele.id,
          "\t",
          ele.source,
          "\t",
          ele.type.padEnd(7),
          "\t",
          ele.startDay,
          "\t",
          ele.history
        );
    }
    printHead && console.log("[-------------------------------]");
  }
};

const findNearestDayIndexForStartNewSchedule = () => {
  return steps[0][0].dayIndex;
};

// const checkValidDayIndex = () => {
// }

/**
 * calc next startDay
 */
const calcScheduleStartDay = (lastStartDay = 1, scheduleType) => {
  return lastStartDay + getStepNumByType(scheduleType);
};

/**
 * find pool, if pool exit's
 */
const findPoolByTagetSchedule = async (targetDay, source) => {
  // let targetPool = pools.find((p) => p.dayIndex === targetDay);
  const key = "p" + targetDay + "-" + source;
  const targetValue = await redisClient.exists(key);
  console.log("targetValue", key, targetValue);
  // not exist
  if (targetValue === 0) {
    // const newValue2 =
    await redisClient.set(key, Total * weights[upperFirst(source)]);
    // OK !!
    // console.log("newValue2", newValue2);
    // targetPool = {
    //   dayIndex: targetDay,
    //   amanNum: Total * weights.Aman,
    //   bmanNum: Total * weights.Bman,
    //   cmanNum: Total * weights.Cman,
    //   omanNum: Math.round(
    //     Total * (1 - weights.Aman - weights.Bman - weights.Cman)
    //   ),
    // };
    // pools.push(targetPool);
  }
  // find field
  // const field = source.toLocaleLowerCase() + "Num";
  // targetPool[field] = targetPool[field] - 1;
  const newValue3 = await redisClient.decr(key);
  console.log("newValue3", newValue3);
  //   console.log("field", field, targetPool[field]);
  if (newValue3 >= 0) {
    return { dayIndex: targetDay };
  }
  // invaild pool ?
  // find next schedule
  //   console.log("find next schedule");
  //   if (block > 5) throw new Error("block limit");
  //   block++;
  return findPoolByTagetSchedule(targetDay + 1, source);
};

const findDayIndexStep = (targetDayIndex) => {
  const max = steps.length - 1;
  if (steps[max][0].dayIndex === targetDayIndex) {
    // find current bucket
    return steps[max];
  }
  if (steps[max][0].dayIndex < targetDayIndex) {
    // new bucket
    const newStep = [{ dayIndex: targetDayIndex }];
    steps.push(newStep);
    return newStep;
  }
  if (steps[max][0].dayIndex > targetDayIndex) {
    // find before bucket
    return steps.find((st) => st[0].dayIndex === targetDayIndex);
  }
};

const getStepNumByType = (type) => {
  switch (type) {
    case "day2":
      return 2;
    case "weekly4":
      return 4;
  }
};

const getStartTagByType = (type) => {
  switch (type) {
    case "day2":
      return "d";
    case "weekly4":
      return "w";
  }
};

const scheduler = async (howDo /** add, remove, continue */, waitHandleIds) => {
  if (howDo === "continue") {
    // console.log("scheduler :", waitHandleIds);
    for (const data of waitHandleIds) {
      // 'start' 定位之前在那个时间的bucket
      // 'last' 上次安排工作的时间
      // 'next' 即将被安排工作的时间
      // calc next schedule startDay
      const newScheduleStartDay = calcScheduleStartDay(
        data.startDay,
        data.type
      );
      // find target pool in next schedule
      const targetPool = await findPoolByTagetSchedule(
        newScheduleStartDay,
        data.source
      );
      // record
      data.next = targetPool.dayIndex;
      data.startDay = newScheduleStartDay;
      // find target pool -> target step
      const targetStep = findDayIndexStep(data.next);
      targetStep.push(data);
    }
    return;
  }

  if (howDo === "add") {
    // 如果它是新增的数据？
    // V1. 对于新增的数据需要找它的schedule吗？或者它是的新的schedule
    // V1.1 先处理来自新的souce, 先按照权重分比例，再分别放进未来的桶
    const source = waitHandleIds[0].source;
    // const type = waitHandleIds[0].type;
    const startDay = findNearestDayIndexForStartNewSchedule(source);
    for (const item of waitHandleIds) {
      //   console.log("waitHandleIds.item", item, startDay);
      // const newScheduleStartDay = calcScheduleStartDay(data.last, data.type)
      //   block = 0;
      const targetPool = await findPoolByTagetSchedule(startDay, item.source);
      item.next = targetPool.dayIndex;
      item.startDay = startDay;
      item.history = "Init" + getStartTagByType(item.type) + item.next;
      console.log("item", item, targetPool, steps);
      const targetStep = findDayIndexStep(item.next);
      targetStep.push(item);
    }
    // const chunkSize = Math.ceil(waitHandleIds.length / getStepNumByType(type));
    // const groupIds = chunk(waitHandleIds, chunkSize);
    // for (let i = 0; i < groupIds.length; i++) {
    //   const arr = groupIds[i];
    //   // handle start tag
    //   const startHeadIndex = steps[startIndex][0].dayIndex;
    //   arr.forEach((aItem) => {
    //     aItem.history =
    //       "Init" + getStartTagByType(aItem.type) + (startHeadIndex + i);
    //     aItem.startDay = startHeadIndex;
    //   });
    //   findDayIndexStep(startHeadIndex + i).push(...arr);
    // }
    return;
  }
  if (howDo === "remove") {
    // TODO
  }
};

const randomNumByMaxAndMin = (min, max) =>
  min + Math.round(Math.random() * (max - min));

const eat = (ids, dayIndex) => {
  console.log("\n", ">>> day :", dayIndex, "eat after ALL", "\n");
  // console.log("eat :", ids);
  // how eat ??
  // mock network request
  // mock split 1,2,3,4
  const randomSplitArray = (arr) => {
    const max = arr.length;
    let splitNum = Math.round(Math.random() * (max - 3) + 2);
    splitNum = splitNum >= max || splitNum <= 0 ? max - 2 : splitNum;
    const splitArr = Array.from(
      { length: Math.round(arr.length / splitNum) + 1 },
      () => []
    );
    for (let i = 0; i < arr.length; i++) {
      const ele = arr[i];
      // console.log("Math.floor(i / splitNum)", Math.floor(i / splitNum));
      // console.log("splitArr", splitArr.length);
      splitArr[Math.floor(i / splitNum)].push(ele);
    }
    return splitArr;
  };
  const splitArr = randomSplitArray(ids);
  const ms = () => randomNumByMaxAndMin(400, 1000 * 1);
  for (const item of splitArr) {
    if (item.length >= 1) {
      setTimeout(() => {
        item.forEach((i) => {
          i.last = dayIndex;
          i.history += "/d" + dayIndex;
        });
        scheduler("continue", item);
      }, ms());
    }
  }
  printSteps(steps);
};

const run = (dayIndex) => {
  console.log("\n", "run before ALL", "\n");
  printSteps(steps);
  // console.log("into step");
  //   console.log("wait steps", steps);
  // mock 1 hours take 1 array data
  if (steps.length <= 0) return;
  const idsWithHeader = steps.shift();
  // eat ids
  const header = idsWithHeader.shift();
  header.dayIndex !== dayIndex &&
    console.log("!!!why dayIndex!==header", dayIndex, header);
  // console.log("ids.header", header);
  eat(idsWithHeader, dayIndex);
  // ...
  // retry scheduler
  // scheduler(ids);
  //   console.log("new steps", steps);
};

const work = () => {
  let dayIndex = 1;
  const runWork = () => {
    setTimeout(
      () => {
        // handle
        run(dayIndex++);
        // next run
        !stop && runWork();
        // how to do stop
        if (stop) {
          console.log("will stop ... ...");
          setTimeout(() => {
            printSteps(steps);
            process.exit();
          }, 1000 * 1 * 10);
        }
      },
      // 1s
      1000 * 1
    );
  };
  runWork();
};

async function init() {
  await redisClient.connect();

  // init data
  // mock add source or add ids
  (() => {
    const addCmanData = () => {
      const num = randomNumByMaxAndMin(5, 7);
      return [
        {
          id: 1 + num * 10,
          source: "Cman",
          type: "day2",
          next: null,
          last: null,
        },
        {
          id: 2 + num * 10,
          source: "Cman",
          type: "day2",
          next: null,
          last: null,
        },
        {
          id: 3 + num * 10,
          source: "Cman",
          type: "day2",
          next: null,
          last: null,
        },
        {
          id: 4 + num * 10,
          source: "Cman",
          type: "day2",
          next: null,
          last: null,
        },
        {
          id: 5 + num * 10,
          source: "Cman",
          type: "day2",
          next: null,
          last: null,
        },
      ];
    };
    setTimeout(() => {
      // random time add C man
      scheduler("add", addCmanData());
      printSteps();
    }, 1000);

    // Aman
    scheduler("add", [
      {
        id: 1,
        source: "Aman",
        type: "day2",
        next: null,
        last: null,
      },
      {
        id: 2,
        source: "Aman",
        type: "day2",
        next: null,
        last: null,
      },
      {
        id: 3,
        source: "Aman",
        type: "day2",
        next: null,
        last: null,
      },
      {
        id: 4,
        source: "Aman",
        type: "day2",
        next: null,
        last: null,
      },
      {
        id: 5,
        source: "Aman",
        type: "day2",
        next: null,
        last: null,
      },
    ]);
    // Bman
    scheduler("add", [
      {
        id: 11,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 12,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 13,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 14,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 15,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 16,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 21,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 22,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 23,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 24,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 25,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
      {
        id: 26,
        source: "Bman",
        type: "weekly4",
        next: null,
        last: null,
      },
    ]);
  })();

  // start ....
  work();
}

init();

// how stop and look all data
// which day , stop system
const stopDay = 10;
let stop = false;
setInterval(() => {
  steps[0][0].dayIndex >= stopDay && (stop = true);
}, 100);
