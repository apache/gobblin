package org.apache.gobblin.service.loadbalancer;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Connector;


public class GobblinServiceLoadBalancer implements ApplicationLauncher {
  private Server server;

  @Override
  public void start() throws ApplicationException {
    this.server = new Server(6956);
    try {
      ServletHandler servletHandler = new ServletHandler();
      this.server.setHandler(servletHandler);
      servletHandler.addServletWithMapping(ForwardRequestServlet.class, "/");
      this.server.start();
      this.server.join();

      System.out.println("Started server");



    } catch (Exception e) {
      // TODO: handle error and log it properly
      System.out.println(e.toString());
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
//    Options options = buildOptions();
//    try {
//      CommandLine cmd = new DefaultParser().parse(options, args);
//      if (!cmd.hasOption(SERVICE_NAME_OPTION_NAME)) {
//        printUsage(options);
//        System.exit(1);
//      }
//
//      if (!cmd.hasOption(SERVICE_ID_OPTION_NAME)) {
//        printUsage(options);
//        LOGGER.warn("Please assign globally unique ID for a GobblinServiceManager instance, or it will use default ID");
//      }
//
//      boolean isTestMode = false;
//      if (cmd.hasOption("test_mode")) {
//        isTestMode = Boolean.parseBoolean(cmd.getOptionValue("test_mode", "false"));
//      }
//
//      Config config = ConfigFactory.load();
//      try (GobblinServiceManager gobblinServiceManager = new GobblinServiceManager(
//          cmd.getOptionValue(SERVICE_NAME_OPTION_NAME), getServiceId(cmd),
//          config, Optional.<Path>absent())) {
//        gobblinServiceManager.getOrchestrator().setFlowStatusGenerator(gobblinServiceManager.buildFlowStatusGenerator(config));
//        gobblinServiceManager.start();
//
//        if (isTestMode) {
//          testGobblinService(gobblinServiceManager);
//        }
//      }
//
//    } catch (ParseException pe) {
//      printUsage(options);
//      System.exit(1);
//    }
    GobblinServiceLoadBalancer loadBalancer = new GobblinServiceLoadBalancer();
    loadBalancer.start();
  }
}
