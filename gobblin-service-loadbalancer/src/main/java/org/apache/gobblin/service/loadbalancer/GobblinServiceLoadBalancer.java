package org.apache.gobblin.service.loadbalancer;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GobblinServiceLoadBalancer implements ApplicationLauncher {
  private Server server;
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinServiceLoadBalancer.class);
  private Config config;

  GobblinServiceLoadBalancer(Config config) {
    this.config = config;
  }

  @Override
  public void start() throws ApplicationException {
    this.server = new Server(8080);
    try {
      String sessionPath = "/";
      ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
      context.setContextPath(sessionPath);
      LOGGER.info("Set session path");
      server.setHandler(context);
      ForwardRequestServlet forwardRequestServlet = new ForwardRequestServlet(this.config, Optional.of(LOGGER));
      ServletHolder servletHolder = new ServletHolder("default", forwardRequestServlet);
      context.addServlet(servletHolder, "/");
      this.server.start();
      LOGGER.info("Started server");
    } catch (Exception e) {
      LOGGER.error(e.toString());
    }
  }

  @Override
  public void stop() throws ApplicationException {
  }

  @Override
  public void close() {
    //
  }

  public static void main(String[] args) throws Exception {
    Config config = ConfigFactory.load();
    GobblinServiceLoadBalancer loadBalancer = new GobblinServiceLoadBalancer(config);
    loadBalancer.start();
  }
}
