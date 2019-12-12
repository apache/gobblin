package org.apache.gobblin.service.loadbalancer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.Future;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.protocol.HTTP;
import com.google.common.hash.Hashing;


public class ForwardRequestServlet extends HttpServlet {

  public CloseableHttpAsyncClient client;

  private String statefulSetBaseURL = "http://gaas-";
  // TODO: split this up into configuration or something
  private String restOfTheURL = ".gaas.default.svc.cluster.local:6956";
  private int numSchedulers = 3;

  ForwardRequestServlet() {
    this.client = HttpAsyncClients.createDefault();
    client.start();
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
    System.out.println("GET Request");
    System.out.println(request.getRequestURI() + "?" + request.getQueryString());
    String forwardedURI = statefulSetBaseURL + "0";
    String requestURI = forwardedURI + request.getRequestURI();
    if (request.getQueryString() != null) {
      requestURI += "?" + request.getQueryString();
    }
    HttpGet forwardRequest = new HttpGet(requestURI);
    parseAndAddHeaders(forwardRequest, request);

    forwardRequestToServer(forwardRequest, response);
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException {
    System.out.println("POST Request");

    try {
      String body = IOUtils.toString(request.getReader());
      System.out.println(body);
      // get the flowName and flowGroup to hash
      JsonParser parser = new JsonParser();
      JsonObject jsonBody =  parser.parse(body).getAsJsonObject();
      System.out.println("parsed body");

      JsonObject flowIDMap = ((JsonObject) jsonBody.get("id"));

      System.out.println("got id map");

      if (flowIDMap.get("flowName") == null || flowIDMap.get("flowGroup") == null) {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }

      String flowID = flowIDMap.get("flowName").getAsString() + flowIDMap.get("flowGroup").getAsString();

      System.out.println(flowID);

      System.out.println(Hashing.sha256().hashString(flowID, StandardCharsets.UTF_8).asInt());

      int hashID = Hashing.sha256().hashString(flowID, StandardCharsets.UTF_8).asInt() % this.numSchedulers;
      if (hashID < 0) {
        hashID *= -1;
      }
      System.out.println(hashID);

      String forwardedURI = statefulSetBaseURL + hashID + this.restOfTheURL;
      System.out.println(forwardedURI);

      String requestURI = forwardedURI +  request.getRequestURI();
      HttpPost forwardRequest = new HttpPost(requestURI);
      parseAndAddHeaders(forwardRequest, request);

      StringEntity requestBody = new StringEntity(body);
      forwardRequest.setEntity(requestBody);

      if (request.getQueryString() != null) {
        requestURI += "?" + request.getQueryString();
      }
      System.out.println(requestURI);

      forwardRequestToServer(forwardRequest, response);
    } catch (IOException e) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    } catch (ClassCastException e) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      System.out.println(e.toString());
    }

  }

  private void parseAndAddHeaders(HttpRequestBase forwardRequest, HttpServletRequest request) {
    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String header = headerNames.nextElement();
      if (forwardRequest.containsHeader(header)) {
        forwardRequest.setHeader(header, request.getHeader(header));
      } else {
        forwardRequest.addHeader(header, request.getHeader(header));
      }
    }
    // remove this header as it is implicitly added when body is set
    forwardRequest.removeHeaders(HTTP.CONTENT_LEN);
  }

  private void forwardRequestToServer(HttpRequestBase req, HttpServletResponse response) throws ServletException {
    Future<org.apache.http.HttpResponse> future = client.execute(req, null);
    try {
      HttpResponse resp = future.get();
      response.setContentType("application/json");
      response.setStatus(resp.getStatusLine().getStatusCode());
      System.out.println("Finished forwarding request");
    } catch (Exception e) {
      // TODO: log exception properly
      System.out.println(e.toString());
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

}