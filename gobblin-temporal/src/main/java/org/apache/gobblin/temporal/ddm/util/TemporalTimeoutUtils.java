package org.apache.gobblin.temporal.ddm.util;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lombok.experimental.UtilityClass;

import org.apache.gobblin.temporal.ddm.activity.ActivityTimeoutStrategy;
import org.apache.gobblin.temporal.ddm.activity.ActivityType;


@UtilityClass
public class TemporalTimeoutUtils {

  private static final Map<ActivityType, ActivityTimeoutStrategy> activityTimeoutStrategies = new HashMap<>();

  static {
    activityTimeoutStrategies.put(ActivityType.GENERATE_WORKUNITS, new ActivityTimeoutStrategy.GenerateWorkunitsActivityTimeoutStrategy());
    activityTimeoutStrategies.put(ActivityType.RECOMMEND_SCALING, new ActivityTimeoutStrategy.RecommendScalingActivityTimeoutStrategy());
    activityTimeoutStrategies.put(ActivityType.DELETE_WORK_DIRS, new ActivityTimeoutStrategy.DeleteWorkDirsActivityTimeoutStrategy());
    activityTimeoutStrategies.put(ActivityType.PROCESS_WORKUNIT, new ActivityTimeoutStrategy.ProcessWorkunitActivityTimeoutStrategy());
    activityTimeoutStrategies.put(ActivityType.COMMIT, new ActivityTimeoutStrategy.CommitActivityTimeoutStrategy());
  }

  public static Duration getTimeout(ActivityType activityType, Properties props) {
    ActivityTimeoutStrategy activityTimeoutStrategy = activityTimeoutStrategies.get(activityType);
    if (activityTimeoutStrategy == null) {
      return ActivityTimeoutStrategy.defaultActivityTimeout;
    }
    return activityTimeoutStrategy.getTimeout(props);
  }

}