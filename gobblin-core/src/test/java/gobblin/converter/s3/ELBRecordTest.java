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

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Joiner;
import gobblin.converter.DataConversionException;
import gobblin.converter.string.StringToCSVConverter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link ELBRecord}
 *
 * @author ahollenbach@nerdwallet.com
 */
@Test(groups = {"gobblin.converter.s3"})
public class ELBRecordTest {
  public static final char DELIMITER = '\u0020';
  /**
   * Generates a default ELB CSV string, replacing one input at the given position.
   * Useful for testing individual perversions of fields in the ELB CSV.
   *
   * @param input The string to test
   * @param pos The position to place the string
   * @param length The length of the CSV string (either 12 or 15)
   * @return An ELB CSV string containing all defaults except the specified value, which is placed at the specified pos
   */
  public static String generateDefaultELBCSV(String input, int pos, int length) {
    // Construct default
    List<String> values = Lists.newArrayList();
    values.add("2015-05-13T23:39:43.945958Z");
    values.add("my-loadbalancer");
    values.add("192.168.131.39:2817");
    values.add("10.0.0.1:80");
    values.add("0.000073");
    values.add("0.001048");
    values.add("0.000057");
    values.add("200");
    values.add("200");
    values.add("0");
    values.add("29");
    values.add("\"POST http://example.url.com:80/example/path HTTP/1.1\"");

    if(length == ELBRecord.RECORD_LENGTH_FULL) {
      values.add("\"curl/7.38.0\"");
      values.add("-");
      values.add("-");
    }

    // Replace our value
    values.set(pos, input);

    // Return joined by spaces
    return Joiner.on(DELIMITER).join(values);
  }

  /**
   * Generates an ELB record from a CSV string. Uses {@link StringToCSVConverter#splitString(String, char)} and
   * passes the result to make a new {@link ELBRecord}
   *
   * @param recordString A CSV string containing ELB log data
   * @return An {@link ELBRecord} that best represents the string passed in
   * @throws DataConversionException
   */
  public static ELBRecord generateELBRecord(String recordString)
      throws DataConversionException {
    try {
      return new ELBRecord(StringToCSVConverter.splitString(recordString, DELIMITER));
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Standard test to check if timestamp parsing is working
   * @throws DataConversionException
   */
  @Test
  public void testTimestamp()
      throws DataConversionException {
    String timeString = "2015-05-13T23:39:43.000000Z";
    String elbRecordString = generateDefaultELBCSV(timeString, 0, ELBRecord.RECORD_LENGTH_FULL);
    ELBRecord elbRecord = generateELBRecord(elbRecordString);

    DateTime d = new DateTime(2015,5,13,23,39,43,0);
    Assert.assertEquals(elbRecord.getTimestampInMillis(), d.getMillis());
  }

  /**
   * Check if bad timestamp parsing is working
   * @throws DataConversionException
   */
  @Test
  public void testTimestampMalformed()
      throws DataConversionException {
    String timeString = "2015-05-13T23:39";
    String elbRecordString = generateDefaultELBCSV(timeString, 0, ELBRecord.RECORD_LENGTH_FULL);

    try {
      ELBRecord elbRecord = generateELBRecord(elbRecordString);
      Assert.fail("ELB Record parsed malformed timestamp");
    } catch(DataConversionException ex) {

    }
  }

  /**
   * Standard test to check if client ip/port parsing is working
   * @throws DataConversionException
   */
  @Test
  public void testClientIpPort()
      throws DataConversionException {
    String clientIpPort = "192.168.131.39:2817";
    String elbRecordString = generateDefaultELBCSV(clientIpPort, 2, ELBRecord.RECORD_LENGTH_FULL);
    ELBRecord elbRecord = generateELBRecord(elbRecordString);

    Assert.assertEquals(elbRecord.getClientIp(), "192.168.131.39");
    Assert.assertEquals(elbRecord.getClientPort(), 2817);
  }

  /**
   * Standard test to check if backend ip/port parsing is working
   * @throws DataConversionException
   */
  @Test
  public void backendClientIpPort()
      throws DataConversionException {
    String backendIpPort = "10.0.0.1:80";
    String elbRecordString = generateDefaultELBCSV(backendIpPort, 3, ELBRecord.RECORD_LENGTH_FULL);
    ELBRecord elbRecord = generateELBRecord(elbRecordString);

    Assert.assertEquals(elbRecord.getBackendIp(), "10.0.0.1");
    Assert.assertEquals(elbRecord.getBackendPort(), 80);
  }

  /**
   * Check for performance in the absence of a valid backend port (as described by the ELB docs)
   * @throws DataConversionException
   */
  @Test
  public void backendClientIpPortMissing()
      throws DataConversionException {
    String backendIpPort = "-";
    String elbRecordString = generateDefaultELBCSV(backendIpPort, 3, ELBRecord.RECORD_LENGTH_FULL);
    ELBRecord elbRecord = generateELBRecord(elbRecordString);

    Assert.assertEquals(elbRecord.getBackendIp(), "");
    Assert.assertEquals(elbRecord.getBackendPort(), -1);
  }

  /**
   * A simple use case test.
   */
  @Test
  public void testParseRequestStringSimple()
      throws DataConversionException {
    String requestString = "\"POST http://example.url.com:80/example/path HTTP/1.1\"";
    String elbRecordString = generateDefaultELBCSV(requestString, 11, ELBRecord.RECORD_LENGTH_FULL);
    ELBRecord elbRecord = generateELBRecord(elbRecordString);

    Assert.assertEquals(elbRecord.getRequestMethod(), "POST");
    Assert.assertEquals(elbRecord.getRequestProtocol(), "http");
    Assert.assertEquals(elbRecord.getRequestHostHeader(), "example.url.com");
    Assert.assertEquals(elbRecord.getRequestPort(), 80);
    Assert.assertEquals(elbRecord.getRequestPath(), "example/path");
    Assert.assertEquals(elbRecord.getRequestHttpVersion(), "HTTP/1.1");
  }

  /**
   * Tests if a path ending in a file ending (.html for example) works
   */
  @Test
  public void testParseRequestStringFile()
      throws DataConversionException {
    String requestString = "\"POST http://example.url.com:80/example/path.html HTTP/1.1\"";
    String elbRecordString = generateDefaultELBCSV(requestString, 11, ELBRecord.RECORD_LENGTH_FULL);
    ELBRecord elbRecord = generateELBRecord(elbRecordString);

    Assert.assertEquals(elbRecord.getRequestMethod(), "POST");
    Assert.assertEquals(elbRecord.getRequestProtocol(), "http");
    Assert.assertEquals(elbRecord.getRequestHostHeader(), "example.url.com");
    Assert.assertEquals(elbRecord.getRequestPort(), 80);
    Assert.assertEquals(elbRecord.getRequestPath(), "example/path.html");
    Assert.assertEquals(elbRecord.getRequestHttpVersion(), "HTTP/1.1");
  }

  /**
   * Tests if the TCP input for the request string ("- - - ") fails
   */
  @Test
  public void testParseRequestStringTcpIn()
      throws DataConversionException {
    String requestString = "\"- - - \"";
    String elbRecordString = generateDefaultELBCSV(requestString, 11, ELBRecord.RECORD_LENGTH_FULL);
    ELBRecord elbRecord = generateELBRecord(elbRecordString);

    Assert.assertNull(elbRecord.getRequestMethod());
    Assert.assertNull(elbRecord.getRequestProtocol());
    Assert.assertNull(elbRecord.getRequestHostHeader());
    Assert.assertEquals(elbRecord.getRequestPort(), 0);
    Assert.assertNull(elbRecord.getRequestPath());
    Assert.assertNull(elbRecord.getRequestHttpVersion());
  }
}
