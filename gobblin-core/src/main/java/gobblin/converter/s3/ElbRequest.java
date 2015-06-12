/* (c) 2015 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.converter.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @author ahollenbach@nerdwallet.com
 */
public class ELBRequest extends Request {
  public ELBRequest() {}

  public ELBRequest(String requestString) {
    parseRequestString(requestString);
  }

  /**
   * Parses the request string according to this format:
   *
   * The request line from the client enclosed in double quotes and logged in the following format: HTTP Method + Protocol://Host header:port + Path + HTTP version.
   *    [TCP listener] The URL is three dashes, each separated by a space, and ending with a space ("- - - ").
   *
   * @param requestString - A string formatted in one of the two above ways
   */
  public void parseRequestString(String requestString) {
    if(requestString == null || requestString.isEmpty() || requestString.contains("-")) {
      // This is a TCP record, ignore
      return;
    }

    // Split record into the three parts: METHOD URL HTTP_VERSION
    String[] parts = requestString.split(" ");
    this.method = parts[0];

    String[] url = parts[1].split("://|:|/", 4);
    this.protocol = url[0];
    this.hostHeader = url[1];
    this.port = url[2];
    this.path = url[3];

    this.httpVersion = parts[2];
  }
}
