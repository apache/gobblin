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

import gobblin.converter.DataConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

/**
 * A Java representation for an ELB record using the following schema:
 * <p/>
 * <code>timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time
 * elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol</code>
 * <p/>
 * This is built from the ELB format as outlined here (API Version 2012-06-01). For additional
 * information, please visit:
 * http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format
 *
 * @author ahollenbach@nerdwallet.com
 */
public class ELBRecord {
  private static final Logger LOG = LoggerFactory.getLogger(ELBRecord.class);

  protected static final String ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.";
  protected static final String LOG_DATE_FORMAT = "yyyy-MM-dd";
  protected static final String LOG_TIME_FORMAT = "HH:mm:ss";

  private final Date timestamp;
  private final String elbName;
  private final String clientIp;
  private final int clientPort;
  private final String backendIp;
  private final int backendPort;
  private final double requestProcessingTime;
  private final double backendProcessingTime;
  private final double responseProcessingTime;
  private final int elbStatusCode;
  private final int backendStatusCode;
  private final int receivedBytes;
  private final int sentBytes;
  private final Request request;
  private final String userAgent;
  private final String sslCipher;
  private final String sslProtocol;

  /**
   * Creates an ELBRecord object.
   * This constructor in particular takes in an array of strings, presumably extracted directly
   * from the original record in the form of a space separated value.
   *
   * @param values An {@link ArrayList} of Strings in the following order:
   *               <code>timestamp elb client:port backend:port request_processing_time backend_processing_time
   *               response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request"
   *               "user_agent" ssl_cipher ssl_protocol</code>
   */
  public ELBRecord(ArrayList<String> values) throws DataConversionException {
    if (values.size() != 15) {
      throw new DataConversionException("Malformed log record");
    }

    // values[0]: timestamp
    this.timestamp = parseDate(values.get(0), new SimpleDateFormat(ISO8601_DATE_FORMAT));
    if (this.timestamp == null) {
      throw new DataConversionException("Failed to parse date. Use the format: " + ISO8601_DATE_FORMAT);
    }

    // values[1]: elb
    this.elbName = values.get(1);

    // values[2]: client:port
    String[] clientIpPort = values.get(2).split(":", 2);
    this.clientIp = clientIpPort[0];
    this.clientPort = Integer.parseInt(clientIpPort[1]);

    // values[3]: backend:port
    String[] backendIpPort = values.get(3).split(":", 2);
    if (backendIpPort.length == 2) {
      this.backendIp = backendIpPort[0];
      this.backendPort = Integer.parseInt(backendIpPort[1]);
    } else {
      // The request was unable to be processed by the ELB
      this.backendIp = "-";
      this.backendPort = -1;
    }

    // values[4]: request_processing_time
    this.requestProcessingTime = Double.parseDouble(values.get(4));

    // values[5]: backend_processing_time
    this.backendProcessingTime = Double.parseDouble(values.get(5));

    // values[6]: response_processing_time
    this.responseProcessingTime = Double.parseDouble(values.get(6));

    // values[7]: elb_status_code
    this.elbStatusCode = Integer.parseInt(values.get(7));

    // values[8]: backend_status_code
    this.backendStatusCode = Integer.parseInt(values.get(8));

    // values[9]: received_bytes
    this.receivedBytes = Integer.parseInt(values.get(9));

    // values[10]: sent_bytes
    this.sentBytes = Integer.parseInt(values.get(10));

    // values[11]: "request"
    this.request = new ELBRequest(values.get(11));

    // values[12]: "user_agent"
    this.userAgent = values.get(12);

    // values[13]: ssl_cipher
    this.sslCipher = values.get(13);

    // values[14]: ssl_protocol
    this.sslProtocol = values.get(14);
  }

  /**
   * A constructor that takes already parsed ELB record info and builds the ELB record object.
   *
   * @param timestamp
   * @param elbName
   * @param clientIp
   * @param clientPort
   * @param backendIp
   * @param backendPort
   * @param requestProcessingTime
   * @param backendProcessingTime
   * @param responseProcessingTime
   * @param elbStatusCode
   * @param backendStatusCode
   * @param receivedBytes
   * @param sentBytes
   * @param request
   * @param userAgent
   * @param sslCipher
   * @param sslProtocol
   */
  public ELBRecord(Date timestamp, String elbName, String clientIp, int clientPort,
                   String backendIp, int backendPort, double requestProcessingTime,
                   double backendProcessingTime, double responseProcessingTime,
                   int elbStatusCode, int backendStatusCode, int receivedBytes,
                   int sentBytes, Request request, String userAgent, String sslCipher,
                   String sslProtocol) {

    this.timestamp = timestamp;
    this.elbName = elbName;
    this.clientIp = clientIp;
    this.clientPort = clientPort;
    this.backendIp = backendIp;
    this.backendPort = backendPort;
    this.requestProcessingTime = requestProcessingTime;
    this.backendProcessingTime = backendProcessingTime;
    this.responseProcessingTime = responseProcessingTime;
    this.elbStatusCode = elbStatusCode;
    this.backendStatusCode = backendStatusCode;
    this.receivedBytes = receivedBytes;
    this.sentBytes = sentBytes;
    this.request = request;
    this.userAgent = userAgent;
    this.sslCipher = sslCipher;
    this.sslProtocol = sslProtocol;
  }


  /**
   * Parses a date with the given format. The motivation behind this function is to
   * catch multiple separate parse errors and write the errored date to the log for
   * more concise error reporting.
   *
   * @param input The input date as a string
   * @param dateFormat The format to convert to
   * @return the formatted date, or null if the date was unable to be formatted
   */
  public static Date parseDate(String input, SimpleDateFormat dateFormat) {
    // Clean the input
    input = input.trim();

    Date datetime;
    try {
      datetime = dateFormat.parse(input);
    } catch (ParseException e) {
      LOG.error("Failed to parse date:" + input);
      e.printStackTrace();
      return null;
    } catch (NumberFormatException e) {
      LOG.error("Failed to parse date:" + input);
      e.printStackTrace();
      return null;
    }

    return datetime;
  }


  // Custom getters

  /**
   * Gets the date in {@link ELBRecord#LOG_DATE_FORMAT}.
   * This is the date when the load balancer received the request from the client.
   *
   * @return The date formatted to the corresponding string
   */
  public String getDate() {
    return new SimpleDateFormat(LOG_DATE_FORMAT).format(timestamp);
  }

  /**
   * Gets the time in {@link ELBRecord#LOG_TIME_FORMAT}.
   * This is the time when the load balancer received the request from the client.
   *
   * @return The time formatted to the corresponding string
   */
  public String getTime() {
    return new SimpleDateFormat(LOG_TIME_FORMAT).format(timestamp);
  }

  /**
   * Gets the approximate time taken for the request.
   * This is just a sum of the the three ELBRecord processing times -> request, backend, and response.
   *
   * @return The time taken, in seconds for the request
   */
  public double getTimeTaken() {
    return requestProcessingTime + backendProcessingTime + responseProcessingTime;
  }

  /**
   * Gets the method of the request - i.e. GET, POST, etc.
   *
   * @return The request method
   */
  public String getRequestMethod() {
    return request.method;
  }

  /**
   * Gets the request URI. This will look something like
   * <code>www.example.com/resource/index.html</code>
   *
   * @return The URI of the request
   */
  public String getRequestUri() {
    return request.hostHeader + "/" + request.path;
  }

  // Auto-generated default getters

  /**
   * Do not use in favor of extracting the already formatted date and time separately.
   * <p/>
   * This is the date/time when the load balancer received the request from the client.
   *
   * @return The timestamp as a {@link Date} object
   */
  public Date getTimestamp() {
    return timestamp;
  }

  /**
   * Gets the name of the load balancer.
   *
   * @return The name of the load balancer
   */
  public String getElbName() {
    return elbName;
  }

  /**
   * Gets the IP of the requesting client.
   *
   * @return The client IP as a string
   */
  public String getClientIp() {
    return clientIp;
  }

  /**
   * Gets the port of the requesting client.
   *
   * @return The client port as an int.
   */
  public int getClientPort() {
    return clientPort;
  }

  /**
   * The IP address of the registered instance that processed this request.
   * <p/>
   * If the client didn't send a full request, the load balancer can't dispatch
   * the request to a registered instance, and this value is set to -.
   *
   * @return The backend IP as a string
   */
  public String getBackendIp() {
    return backendIp;
  }

  /**
   * The port of the registered instance that processed this request.
   * <p/>
   * If the client didn't send a full request, the load balancer can't dispatch
   * the request to a registered instance, and this value is set to -.
   *
   * @return The backend port
   */
  public int getBackendPort() {
    return backendPort;
  }

  /**
   * The total time elapsed, in seconds, from the time the load balancer received the request
   * and sent it to a registered instance.
   * <p/>
   * This value is set to -1 if the load balancer can't dispatch the request to a
   * registered instance. This can happen if the registered instance closes the connection
   * before the idle timeout or if the client sends a malformed request.
   *
   * @return The time elapsed in seconds
   */
  public double getRequestProcessingTime() {
    return requestProcessingTime;
  }

  /**
   * [HTTP listener] The total time elapsed, in seconds, from the time the load balancer
   * sent the request to a registered instance until the instance started to send the
   * response headers.
   * <p/>
   * [TCP listener] The total time elapsed, in seconds, from the time the load balancer
   * sent the first byte of the request to a registered instance until the instance sent
   * back the first byte.
   * <p/>
   * This value is set to -1 if the load balancer can't dispatch the request to a registered
   * instance. This can happen if the registered instance closes the connection before the
   * idle timeout or if the client sends a malformed request.
   *
   * @return The time elapsed in seconds
   */
  public double getBackendProcessingTime() {
    return backendProcessingTime;
  }

  /**
   * [HTTP listener] The total time elapsed (in seconds) from the time the load balancer
   * received the response header from the registered instance until it started to send
   * the response to the client. This includes both the queuing time at the load balancer
   * and the connection acquisition time from the load balancer to the back end.
   * <p/>
   * [TCP listener] The total time elapsed, in seconds, from the time the load balancer
   * received the first byte from the registered instance until it started to send the
   * response to the client.
   * <p/>
   * This value is set to -1 if the load balancer can't dispatch the request to a registered
   * instance. This can happen if the registered instance closes the connection before the
   * idle timeout or if the client sends a malformed request.
   *
   * @return The time elapsed in seconds
   */
  public double getResponseProcessingTime() {
    return responseProcessingTime;
  }

  /**
   * [HTTP listener] The status code of the response from the load balancer.
   *
   * @return the ELB status code
   */
  public int getElbStatusCode() {
    return elbStatusCode;
  }

  /**
   * [HTTP listener] The status code of the response from the registered instance.
   *
   * @return The backend status code
   */
  public int getBackendStatusCode() {
    return backendStatusCode;
  }

  /**
   * The size of the request, in bytes, received from the client (requester).
   * <p/>
   * [HTTP listener] The value includes the request body but not the headers.
   * <p/>
   * [TCP listener] The value includes the request body and the headers.
   *
   * @return The size of the client request in bytes
   */
  public int getReceivedBytes() {
    return receivedBytes;
  }

  /**
   * The size of the response, in bytes, sent to the client (requester).
   * <p/>
   * [HTTP listener] The value includes the response body but not the headers.
   * <p/>
   * [TCP listener] The value includes the request body and the headers.
   *
   * @return The size of the client request in bytes
   */
  public int getSentBytes() {
    return sentBytes;
  }

  /**
   * Gets the parsed request string.
   * <p/>
   * The request line from the client enclosed in double quotes and logged in the following
   * format: HTTP Method + Protocol://Host header:port + Path + HTTP version.
   * <p/>
   * [TCP listener] The URL is three dashes, each separated by a space, and ending
   * with a space ("- - - ").
   *
   * @return A {@link Request} object containing the parsed request string
   */
  public Request getRequest() {
    return request;
  }

  /**
   * [HTTP/HTTPS listener] A User-Agent string that identifies the client that originated the request.
   * The string consists of one or more product identifiers, product[/version]. If the string is
   * longer than 4 KB, it is truncated.
   *
   * @return The user agent string
   */
  public String getUserAgent() {
    return userAgent;
  }

  /**
   * [HTTP/SSL listener] The SSL cipher. This value is recorded only if the incoming SSL/TLS
   * connection was established after a successful negotiation. Otherwise, the value is set to -.
   *
   * @return The SSL cipher, if any.
   */
  public String getSslCipher() {
    return sslCipher;
  }

  /**
   * [HTTPS/SSL listener] The SSL protocol. This value is recorded only if the incoming
   * SSL/TLS connection was established after a successful negotiation. Otherwise, the
   * value is set to -.
   *
   * @return The SSL protocol, if any.
   */
  public String getSslProtocol() {
    return sslProtocol;
  }
}
