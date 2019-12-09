package org.apache.gobblin.service.loadbalancer;

import java.util.concurrent.Future;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;


public class ForwardRequestServlet extends HttpServlet {

  public CloseableHttpAsyncClient client;

  ForwardRequestServlet() {
    this.client = HttpAsyncClients.createDefault();
    client.start();

  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
    System.out.println("GET Request");
    System.out.println(request.getQueryString());
    HttpGet req = new HttpGet("http://localhost:6956");

    forwardRequestToServer(req, response);
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException {
    System.out.println("POST Request");
    HttpPost req  = new HttpPost("http://localhost:6956");
    forwardRequestToServer(req, response);
  }

  protected void forwardRequestToServer(HttpRequestBase req, HttpServletResponse response) throws ServletException {
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