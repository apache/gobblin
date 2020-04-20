package org.apache.gobblin.yarn;

import com.google.common.base.Throwables;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import java.io.IOException;
import org.apache.gobblin.yarn.event.DelegationTokenUpdatedEvent;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class YarnContainerSecurityManagerForApplicationMaster extends YarnContainerSecurityManager{

  private YarnService yarnService;
  public YarnContainerSecurityManagerForApplicationMaster(Config config, FileSystem fs, EventBus eventBus, YarnService yarnService) {
    super(config, fs, eventBus);
    this.yarnService = yarnService;
  }

  @Override
  public void handleTokenFileUpdatedEvent(DelegationTokenUpdatedEvent delegationTokenUpdatedEvent) {
    super.handleTokenFileUpdatedEvent(delegationTokenUpdatedEvent);
    try {
      yarnService.updateToken();
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }
}
