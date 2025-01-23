package org.apache.gobblin.temporal.ddm.util;

import java.time.Duration;
import java.util.Properties;
import io.temporal.common.RetryOptions;
import lombok.experimental.UtilityClass;
import org.apache.gobblin.temporal.ddm.activity.ActivityType;


@UtilityClass
public class TemporalRetryUtils {

  private static final RetryOptions DEFAULT_RETRY_OPTIONS = RetryOptions.newBuilder()
      .setInitialInterval(Duration.ofSeconds(3))
      .setMaximumInterval(Duration.ofSeconds(100))
      .setBackoffCoefficient(2)
      .setMaximumAttempts(4)
      .build();

  public static RetryOptions getRetryOptions(ActivityType activityType, Properties props) {
    // Currently returning just the default retry options for each activity type
    return DEFAULT_RETRY_OPTIONS;
  }

}