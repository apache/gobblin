package org.apache.gobblin.service.loadbalancer;

import java.io.IOException;
import java.util.Enumeration;
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


public class ForwardRequestServlet extends HttpServlet {

  public CloseableHttpAsyncClient client;

  private String forwardedURI = "http://localhost:6956";

  ForwardRequestServlet() {
    this.client = HttpAsyncClients.createDefault();
    client.start();

  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
    System.out.println("GET Request");
    System.out.println(request.getRequestURI() + "?" + request.getQueryString());
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
    System.out.println(request.getRequestURI() + "?" + request.getQueryString());
    String requestURI = forwardedURI + request.getRequestURI();
    if (request.getQueryString() != null) {
      requestURI += "?" + request.getQueryString();
    }
    HttpPost forwardRequest = new HttpPost(requestURI);
    parseAndAddHeaders(forwardRequest, request);
    try {
      String body = IOUtils.toString(request.getReader());
      System.out.println(body);
      StringEntity requestBody = new StringEntity(body);
      forwardRequest.setEntity(requestBody);
    } catch (IOException e) {
      // TODO: do something useful with the error
    }

     forwardRequestToServer(forwardRequest, response);
  }

  private void parseAndAddHeaders(HttpRequestBase forwardRequest, HttpServletRequest request) {
    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String header = headerNames.nextElement();
      forwardRequest.addHeader(header, request.getHeader(header));
    }
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
    }
  }

}