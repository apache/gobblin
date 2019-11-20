package org.apache.gobblin.service.loadbalancer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;

public class GobblinServiceLoadBalancer implements ApplicationLauncher {

  @Override
  public void start() throws ApplicationException {
    Server server = new Server(8080);
    server.start();
    server.join();
  }

  @Override
  public void stop() throws ApplicationException {

  }
}
