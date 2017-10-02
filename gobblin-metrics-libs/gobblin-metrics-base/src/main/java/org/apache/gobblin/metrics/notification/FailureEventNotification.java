package org.apache.gobblin.metrics.notification;

import org.apache.gobblin.metrics.GobblinTrackingEvent;


public class FailureEventNotification extends EventNotification {

  public FailureEventNotification(GobblinTrackingEvent event) {
    super(event);
  }
}
