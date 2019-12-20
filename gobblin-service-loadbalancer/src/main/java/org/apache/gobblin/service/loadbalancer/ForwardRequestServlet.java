package org.apache.gobblin.service.loadbalancer;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.concurrent.Future;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.util.HashingUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ForwardRequestServlet extends HttpServlet {

  public CloseableHttpAsyncClient client;

  private final String STATEFULSET_URL_PREFIX = "http://gaas-";
  // TODO: split this up into configuration or something
  private final String STATEFULSET_URL_SUFFIX = ".gaas.default.svc.cluster.local:6956";
  private final String LOADBALANCER_PREFIX = "gobblinServiceLoadbalancer.";
  private final String NUM_SCHEDULERS_KEY = "numSchedulers";
  private final String FLOW_NAME_KEY = "flowName";
  private final String FLOW_GROUP_KEY = "flowGroup";
  private int numSchedulers;
  protected final Logger _log;

  ForwardRequestServlet(Config config, Optional<Logger> log) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.numSchedulers = config.getInt(LOADBALANCER_PREFIX + NUM_SCHEDULERS_KEY);
    _log.info("Load balancer started with {} servers", this.numSchedulers);
    this.client = HttpAsyncClients.createDefault();
    this.client.start();
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) {
    try {
      String requestUri = this.determineServerUrl(request);
      HttpGet forwardRequest = new HttpGet(requestUri);
      this.forwardHeaders(forwardRequest, request);
      forwardRequestToServer(forwardRequest, response);
    } catch (ServletException e) {
      _log.error(e.toString());
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
  }

  protected void doPut(HttpServletRequest request, HttpServletResponse response) {
    try {
      String requestUri = this.determineServerUrl(request);
      HttpPut forwardRequest = new HttpPut(requestUri);
      this.forwardHeaders(forwardRequest, request);
      forwardRequestToServer(forwardRequest, response);
    } catch (ServletException e) {
      _log.error(e.toString());
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
  }

  protected void doDelete(HttpServletRequest request, HttpServletResponse response) {
    try {
      String requestUri = this.determineServerUrl(request);
      HttpDelete forwardRequest = new HttpDelete(requestUri);
      this.forwardHeaders(forwardRequest, request);
      forwardRequestToServer(forwardRequest, response);
    } catch (ServletException e) {
      _log.error(e.toString());
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
  }

  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    try {
      String body = IOUtils.toString(request.getReader());
      // Rebuild the request that was sent
      String requestUri = this.determineServerUrl(request);
      HttpPost forwardRequest = new HttpPost(requestUri);
      this.forwardHeaders(forwardRequest, request);
      // remove this header as it is implicitly added when body is set
      forwardRequest.removeHeaders(HTTP.CONTENT_LEN);
      StringEntity requestBody = new StringEntity(body);
      forwardRequest.setEntity(requestBody);
      forwardRequestToServer(forwardRequest, response);
    } catch (IOException | ClassCastException | ServletException e) {
      _log.error(e.toString());
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    }
  }

  private String determineServerUrl(HttpServletRequest request) throws ServletException {
    URI flowId = this.extractFlowIdFromHeaders(request);
    int hashID = HashingUtils.hashFlowSpec(flowId, this.numSchedulers);
    if (hashID < 0) {
      hashID *= -1;
    }
    String requestUri = this.STATEFULSET_URL_PREFIX + hashID + this.STATEFULSET_URL_SUFFIX;
    requestUri += request.getRequestURI();
    if (request.getQueryString() != null) {
      requestUri += "?" + request.getQueryString();
    }
    return requestUri;
  }

  private URI extractFlowIdFromHeaders(HttpServletRequest request) throws ServletException {
    String flowName = "";
    String flowGroup = "";
    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String header = headerNames.nextElement();
      if (header.equals(FLOW_NAME_KEY)) {
        flowName = request.getHeader(header);
      }
      if (header.equals(FLOW_GROUP_KEY)) {
        flowGroup = request.getHeader(header);
      }
    }
    if (flowName.equals("") || flowGroup.equals("")) {
      throw new ServletException("Header does not contain flowName or flowGroup");
    }
    try {
      return new URI(File.separatorChar + flowGroup + File.separatorChar + flowName);
    } catch (URISyntaxException e) {
      throw new ServletException("Error parsing flowName or flowGroup");
    }
  }

  private void forwardHeaders(HttpRequestBase forwardRequest, HttpServletRequest request) {
    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String header = headerNames.nextElement();
      if (forwardRequest.containsHeader(header)) {
        forwardRequest.setHeader(header, request.getHeader(header));
      } else {
        forwardRequest.addHeader(header, request.getHeader(header));
      }
    }
  }

  private void forwardRequestToServer(HttpRequestBase req, HttpServletResponse response) {
    Future<org.apache.http.HttpResponse> future = client.execute(req, null);
    try {
      HttpResponse resp = future.get();
      response.setContentType("application/json");
      response.setStatus(resp.getStatusLine().getStatusCode());
      _log.info("Finished forwarding request to: {}", req.getURI().toString());
    } catch (Exception e) {
      _log.error(e.toString());
      try {
        response.getWriter().println(e.toString());
      } catch (IOException e) {
        _log.error("Could not output error to user");
      }
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
  }

}