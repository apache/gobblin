package gobblin.http;


/**
 * Different types of response status
 */
public enum StatusType {
  // success
  OK,
  // error triggered by client
  CLIENT_ERROR,
  // error triggered by server
  SERVER_ERROR
}
