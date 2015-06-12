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

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link ELBRequest}
 *
 * @author ahollenbach@nerdwallet.com
 */
@Test(groups = {"gobblin.converter.s3"})
public class ELBRequestTest {

  /**
   * A simple use case test.
   */
  @Test
  public void testParseRequestStringSimple() {
    String requestString = "POST http://example.url.com:80/example/path HTTP/1.1";
    ELBRequest request = new ELBRequest(requestString);

    Assert.assertEquals(request.method, "POST");
    Assert.assertEquals(request.protocol, "http");
    Assert.assertEquals(request.hostHeader, "example.url.com");
    Assert.assertEquals(request.port, "80");
    Assert.assertEquals(request.path, "example/path");
    Assert.assertEquals(request.httpVersion, "HTTP/1.1");
  }

  /**
   * Tests if a path ending in a file ending (.html for example) works
   */
  @Test
  public void testParseRequestStringFile() {
    String requestString = "POST http://example.url.com:80/example/path.html HTTP/1.1";
    ELBRequest request = new ELBRequest(requestString);

    Assert.assertEquals(request.method, "POST");
    Assert.assertEquals(request.protocol, "http");
    Assert.assertEquals(request.hostHeader, "example.url.com");
    Assert.assertEquals(request.port, "80");
    Assert.assertEquals(request.path, "example/path.html");
    Assert.assertEquals(request.httpVersion, "HTTP/1.1");
  }

  /**
   * Tests if the TCP input for the request string ("- - -") fails
   */
  @Test
  public void testParseRequestStringTcpIn() {
    String requestString = "- - -";
    ELBRequest request = new ELBRequest(requestString);

    Assert.assertEquals(request.method, "");
    Assert.assertEquals(request.protocol, "");
    Assert.assertEquals(request.hostHeader, "");
    Assert.assertEquals(request.port, "");
    Assert.assertEquals(request.path, "");
    Assert.assertEquals(request.httpVersion, "");
  }

  /**
   * Tests if the empty string causes an error
   */
  @Test
  public void testParseRequestStringEmpty() {
    String requestString = "";
    ELBRequest request = new ELBRequest(requestString);

    Assert.assertEquals(request.method, "");
    Assert.assertEquals(request.protocol, "");
    Assert.assertEquals(request.hostHeader, "");
    Assert.assertEquals(request.port, "");
    Assert.assertEquals(request.path, "");
    Assert.assertEquals(request.httpVersion, "");
  }

  /**
   * Tests if a null value causes an error (bad split)
   */
  @Test
  public void testParseRequestStringNull() {
    String requestString = null;
    ELBRequest request = new ELBRequest(requestString);

    Assert.assertEquals(request.method, "");
    Assert.assertEquals(request.protocol, "");
    Assert.assertEquals(request.hostHeader, "");
    Assert.assertEquals(request.port, "");
    Assert.assertEquals(request.path, "");
    Assert.assertEquals(request.httpVersion, "");
  }
}
