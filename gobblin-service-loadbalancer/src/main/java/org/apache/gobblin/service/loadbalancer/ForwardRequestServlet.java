package org.apache.gobblin.service.loadbalancer;

import com.google.api.client.http.HttpResponse;
import java.io.IOException;
import java.util.concurrent.Future;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import org.apache.commons.httpclient.HttpHost;
import org.apache.gobblin.http.ApacheHttpAsyncClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;


public class ForwardRequestServlet extends HttpServlet {

  CloseableHttpAsyncClient client;

  ForwardRequestServlet() {
    this.client = HttpAsyncClients.createDefault();
    client.start();

  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException {
    HttpGet req = new HttpGet("http://localhost:6956");

    forwardRequestToServer(req, response);
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException {
    HttpPost req  = new HttpPost("http://localhost:6956");
    forwardRequestToServer(req, response);
  }

  private void forwardRequestToServer(HttpRequestBase req, HttpServletResponse response) throws ServletException {
    Future<org.apache.http.HttpResponse> future = client.execute(req, null);

    HttpResponse resp = future.get();

    response.setContentType("application/json");
    response.setStatus(resp.getStatusCode());
    try {
      response.getWriter().print(resp.getContent());
    } catch (IOException e) {
      throw new ServletException("Failed to parse request");
    }
  }
}