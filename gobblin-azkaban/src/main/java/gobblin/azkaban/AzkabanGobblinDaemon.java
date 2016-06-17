package gobblin.azkaban;

import azkaban.jobExecutor.AbstractJob;
import gobblin.scheduler.SchedulerDaemon;
import java.util.Properties;
import org.apache.log4j.Logger;


public class AzkabanGobblinDaemon extends AbstractJob {

  private static final Logger LOG = Logger.getLogger(AzkabanGobblinDaemon.class);

  private SchedulerDaemon daemon;

  public AzkabanGobblinDaemon(String jobId, Properties props) throws Exception {
    super(jobId, LOG);
    this.daemon = new SchedulerDaemon(props);
  }

  @Override
  public void run()
      throws Exception {
    this.daemon.start();
  }

  @Override
  public void cancel()
      throws Exception {
    this.daemon.stop();
  }
}
