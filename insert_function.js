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
  if(array.length == 0){
    return [element];
  }
  array.splice(index, 0, element);
  return array;
}

/**
* returns the index where to insert the element to the sorted array.
**/
function getIndexPositionToInsert(array, element){
  return  positionToInsertIterative(array, element);
  //return positionToInsertRec(array, element, 0, array.length - 1);
}

/**
* recursive function that finds the position to which to insert an element.
* careful with this function. If to many of these are called for large data sets, stack size might need to be increased.
* use the iterative version instead.
**/
function positionToInsertRec(array, element, i, j){
  //if (j - i <= 1){
  if (j <= i){
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

/**
* recursive function that finds the position to which to insert an element.
**/
function positionToInsertIterative(array, element_to_insert){
  var n = array.length;
  var i;
  for (i = 0; i < n; i++){
    var current_element = array[i];
    if(element_to_insert <= current_element){
      return i;
    }
  }
  return array.length + 1;
}

exports.insert = insert;
exports.insertIntoPosition = insertIntoPosition;
exports.getIndexPositionToInsert = getIndexPositionToInsert;
exports.positionToInsertRec = positionToInsertRec;