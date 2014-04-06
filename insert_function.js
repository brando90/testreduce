/**
* Inserts the given element to the sorted array.
**/
function insert(array, element){
  if(array.length == 0){
    return [element];
  }
  var index = getIndexPositionToInsert(array, element);
  return insertIntoPosition(array, element, index);
}

/**
* Inserts element into the position given by index.
* for examples look at the tests in the jUnit file.
**/
function insertIntoPosition(array, element, index){
  array.splice(index, 0, element)
  return array
}

/**
* returns the index where to insert the element to the sorted array.
**/
function getIndexPositionToInsert(array, element){
  return positionToInsertRec(array, element, 0, array.length - 1);
}

/**
* recursive function that finds the position to which to insert an element
**/
function positionToInsertRec(array, element, i, j){
  //j - i
  if( j <= i){
    if(element <= array[i]){
      return i;
    }else{
      return i+1;
    }
  }
  var index = Math.floor((j - i + 1)/2);
  var middle = array[index];
  if( element <= middle ){
    return positionToInsertRec(array, element, i, index - 1);
  }else{
    return positionToInsertRec(array, element, index + 1, j);
  }
}

// console.log( insert([], -1), " answer = ", [-1] );
// console.log( insert([0], -1), " answer = ", [-1, 0] );
// console.log( insert([0], 1), " answer = ", [0, 1] );
// console.log( insert([0, 1, 3], 2), "answer = ", [0, 1, 2, 3]);
// console.log( insert([0, 1, 1.5, 2, 3], 1.5), "answer = ", [0, 1, 1.5, 1.5, 2, 3]);

exports.insert = insert;
exports.insertIntoPosition = insertIntoPosition;
exports.getIndexPositionToInsert = getIndexPositionToInsert;
exports.positionToInsertRec = positionToInsertRec;