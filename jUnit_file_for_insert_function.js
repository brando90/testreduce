var ins = require("./insert_function.js");

console.log( ins.insert([], -1), " answer = ", [-1] );
console.log( ins.insert([0], -1), " answer = ", [-1, 0] );
console.log( ins.insert([0], 1), " answer = ", [0, 1] );
console.log( ins.insert([0, 1, 3], 2), "answer = ", [0, 1, 2, 3]);
console.log( ins.insert([0, 1, 1.5, 2, 3], 1.5), "answer = ", [0, 1, 1.5, 1.5, 2, 3]);