/**
 * This class is an extension on the standard JavaScript Error, providing a better way to chain errors with a "caused by" relation and to combine errors contributing to the same failure on the same level.
 *
 * E.g. if you want a list of items processed, but you don't want failure of one item to stop the processing of the rest of the list, you encapsulate the processing of a single element in a try...catch... and capture the error and store it in an instance of StackableError. Processing the rest of the list can continue as normal. In the end you can decide to throw the error, indicating at least part of the list wasn't processed.
 *
 * E.g., if you divide tasks into smaller tasks by the use of functions and you wan't error handling on more top level functions, you might want to know which subfunction failed. This can be achieved by throwing a StackableError and by supplying the lower level error as "cause". When printing the StackableError, the lower level causes are properly printed with their stack trace.
 *
 * @class
 *
 */
export default class StackableError extends Error {
  /**
   * Constructs an instance of the StackableError class.
   *
   * @constructs StackableError
   * @param {Object} messageObject - Provide any object as the subject of this error. You will need a proper toString method on this object in order to display a nice error message.
   * @param {Object} cause - StackableError or other subclass of Error to indicate what other error caused this error. Optional.
   * @param {Error} nativeError - Provide, if possible, the native JavaScript Error object that is equivalent to this error. Use this when translating an Error into a StackableError. Optional.
   * @param {Boolean} logOnCreation - Print the error messages on the console when this error is created. Optional, defaults to true.
   * @returns {StackableError} Instance of the StackableError class
   */
  constructor(messageObject, cause, nativeError, logOnCreation = true) {
    if (cause) {
      super(messageObject, { cause: cause });
      if (cause.constructor.name == "StackableError")
        this.cause = cause;
      else
        this.cause = new StackableError(cause.message, false, cause);
    }
    else {
      if (nativeError)
        super(messageObject, { cause: nativeError });
      else
        super(messageObject);
    }
    this.messageObject = messageObject;
    this.cause         = cause;
    this.nativeError   = nativeError;
    this._errors       = [];

    if (logOnCreation && console && console.error) {
      console.error(this.toString());
    }
  }

  /**
   * Adds an error to this error level. This builds a collection of errors on the same level. E.g. when processing a list and some elements might fail, collect all errors with this method.
   *
   * @public
   * @function addError
   * @param {(Error|StackableError)} err - Provide error to collect on the same level.
   * @returns {void} Nothing
   */
  addError(err) {
    if (err.constructor.name == "StackableError") {
      this._errors.push(err);
    }
    else {
      this._errors.push(new StackableError(err.message, false, err));
    }
  }

  /**
   * Return a boolean to indicate if there are any collected errors on this level.
   *
   * @public
   * @function hasErrors
   * @returns {Boolean} True if at least one error has been collected so far.
   */
  get hasErrors() {
    return (this._errors.length > 0 ? true : false);
  }

  /**
   * Produces a readable error message, combining the causes, internal error and errors on lower levels recursively. It also prints the stack trace of where the error originated.
   *
   * @public
   * @function toString
   * @returns {String} Nice error message
   */
  toString() {
    let errorStrings = [];
    errorStrings.push(`ERROR: ${this.messageObject.toString()}`);
    if (this.nativeError) {
      errorStrings.push(`NATIVE ERROR: ${this.nativeError.toString()}\n${this.nativeError.stack}`);
    }
    if (this.cause) {
      errorStrings.push(`CAUSED BY: ${this.cause.toString()}\n${this.cause.stack}`);
    }
    for (let e of this._errors) {
      errorStrings.push(`WITH INTERNAL ERROR: ${e.toString()}`);
    }
    return errorStrings.join("\n");
  }
}

//  //Demo usage
//  
//  if (true) {
//    
//    let f = function () {
//      let err = new StackableError("Iterating failed", false, false, false);
//      for (let i = -5; i < 5; i++) {
//        try {
//          if (i === 0)
//            throw new Error("Division by zero not allowed");
//          else
//            console.log(10/i);
//        }
//        catch (e) {
//          let newerror = new StackableError(`Iteration on element ${i} failed.`, e);
//          err.addError(newerror);
//        }
//      }
//      for (let j = 0; j < 5; j++) {
//        err.addError(new Error(`Iteration on j with element ${j} failed.`));
//      }
//      if (err.hasErrors)
//        throw err;
//      return 5;
//    }
//  
//    try {
//      f();
//    }
//    catch (e) {
//      console.error(e.toString());
//    }
//  
//  }

