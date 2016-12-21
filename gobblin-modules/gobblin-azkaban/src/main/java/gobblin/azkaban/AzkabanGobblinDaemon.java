package gobblin.azkaban;

import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import gobblin.metrics.RootMetricContext;
import gobblin.metrics.Tag;
import gobblin.scheduler.SchedulerDaemon;
import azkaban.jobExecutor.AbstractJob;


/**
 * Wrapper of {@link SchedulerDaemon}, specially used by Azkaban to launch a job scheduler daemon.
 */

public class AzkabanGobblinDaemon extends AbstractJob {

  private static final Logger LOG = Logger.getLogger(AzkabanGobblinDaemon.class);

  private SchedulerDaemon daemon;

  public AzkabanGobblinDaemon(String jobId, Properties props) throws Exception {
    super(jobId, LOG);

    List<Tag<?>> tags = Lists.newArrayList();
    tags.addAll(Tag.fromMap(AzkabanTags.getAzkabanTags()));
    RootMetricContext.get(tags);

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
