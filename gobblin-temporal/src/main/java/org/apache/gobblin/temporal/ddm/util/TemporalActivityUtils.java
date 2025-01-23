package org.apache.gobblin.temporal.ddm.util;

import java.util.Properties;
import io.temporal.activity.ActivityOptions;
import lombok.experimental.UtilityClass;
import org.apache.gobblin.temporal.ddm.activity.ActivityType;


@UtilityClass
public class TemporalActivityUtils {

  public static ActivityOptions getActivityOptions(ActivityType activityType, Properties props) {
    return ActivityOptions.newBuilder()
        .setScheduleToCloseTimeout(TemporalTimeoutUtils.getTimeout(activityType, props))
        .setRetryOptions(TemporalRetryUtils.getRetryOptions(activityType, props))
        .build();
  }

}
