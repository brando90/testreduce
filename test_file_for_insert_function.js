var ins = require("./insert_function.js");

// attach the .compare method to Array's prototype to call it on any array
//from stackoverflow. Can't believe this is not a native function in javascript
//http://stackoverflow.com/questions/7837456/comparing-two-arrays-in-javascript
Array.prototype.compare = function (array) {
    // if the other array is a falsy value, return
    if (!array)
        return false;

    // compare lengths - can save a lot of time
    if (this.length != array.length)
        return false;

    for (var i = 0, l=this.length; i < l; i++) {
        // Check if we have nested arrays
        if (this[i] instanceof Array && array[i] instanceof Array) {
            // recurse into the nested arrays
            if (!this[i].compare(array[i]))
                return false;
        }
        else if (this[i] != array[i]) {
            // Warning - two different object instances will never be equal: {x:20} != {x:20}
            return false;
        }
    }
    return true;
}

console.log("All of the following lines should print true. If not, the insert function is not working correctly.");

console.log("first set of tests, should all return true:");
console.log( [-1].compare( ins.insert([], -1) ) );
console.log( [-1, 0].compare(ins.insert([0], -1) ) );
console.log( [0, 1].compare( ins.insert([0], 1) ) );
console.log( [0, 1, 2, 3].compare( ins.insert( [0, 1, 3], 2) ) );
console.log( [0, 1, 1.5, 1.5, 2, 3].compare( ins.insert( [0, 1, 1.5, 2, 3], 1.5) ) );

console.log("second set of tests:");
var sorted_list = [];
var ans = [-1];
var new_element = -1;
var i = ins.getIndexPositionToInsert(sorted_list, new_element);
var new_list =  ins.insertIntoPosition(sorted_list, new_element, i);
// console.log();
// console.log("i = ", i);
// console.log("got: ", new_list);
// console.log("expected", ans);
console.log( ans.compare(new_list) );

var sorted_list = [0];
var new_element = -1;
var ans =[-1, 0];
var i = ins.getIndexPositionToInsert(sorted_list, new_element);
var new_list =  ins.insertIntoPosition(sorted_list, new_element, i);
// console.log();
// console.log("i = ", i);
// console.log("got: ", new_list);
// console.log("expected", ans);
console.log( ans.compare(new_list) );

var sorted_list = [0];
var new_element = 1;
var ans = [0, 1];
var i = ins.getIndexPositionToInsert(sorted_list, new_element);
var new_list =  ins.insertIntoPosition(sorted_list, new_element, i);
// console.log();
// console.log("i = ", i);
// console.log("got: ", new_list);
// console.log("expected", ans);
console.log( ans.compare(new_list) );

var sorted_list = [0, 1, 3];
var new_element = 2;
var ans = [0, 1, 2, 3];
var i = ins.getIndexPositionToInsert(sorted_list, new_element);
var new_list =  ins.insertIntoPosition(sorted_list, new_element, i);
// console.log();
// console.log("i = ", i);
// console.log("got: ", new_list);
// console.log("expected", ans);
console.log( ans.compare(new_list) );

var sorted_list = [0, 1, 2, 3];
var new_element = 1.5;
var ans = [0, 1, 1.5, 2, 3];
var i = ins.getIndexPositionToInsert(sorted_list, new_element);
var new_list =  ins.insertIntoPosition(sorted_list, new_element, i);
// console.log();
// console.log("i = ", i);
// console.log("got: ", new_list);
// console.log("expected", ans);
console.log( ans.compare(new_list) );
