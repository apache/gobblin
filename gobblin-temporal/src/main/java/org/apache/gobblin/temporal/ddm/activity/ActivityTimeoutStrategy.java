package org.apache.gobblin.temporal.ddm.activity;

import java.time.Duration;
import java.util.Properties;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.util.PropertiesUtils;


public interface ActivityTimeoutStrategy {
  Duration defaultActivityTimeout = Duration.ofMinutes(180);
  int DEFAULT_GENERATE_WORKUNITS_ACTIVITY_TIMEOUT_MINUTES = 120;
  int DEFAULT_RECOMMEND_SCALING_ACTIVITY_TIMEOUT_MINUTES = 5;
  int DEFAULT_DELETE_WORK_DIRS_ACTIVITY_TIMEOUT_MINUTES = 10;
  int DEFAULT_PROCESS_WORKUNIT_ACTIVITY_TIMEOUT_MINUTES = 180;
  int DEFAULT_COMMIT_ACTIVITY_TIMEOUT_MINUTES = 180;

  Duration getTimeout(Properties props);

  class GenerateWorkunitsActivityTimeoutStrategy implements ActivityTimeoutStrategy {
    @Override
    public Duration getTimeout(Properties props) {
      return Duration.ofMinutes(PropertiesUtils.getPropAsInt(
          props,
          GobblinTemporalConfigurationKeys.GENERATE_WORKUNITS_ACTIVITY_TIMEOUT_MINUTES,
          DEFAULT_GENERATE_WORKUNITS_ACTIVITY_TIMEOUT_MINUTES
      ));
    }
  }

  class RecommendScalingActivityTimeoutStrategy implements ActivityTimeoutStrategy {
    @Override
    public Duration getTimeout(Properties props) {
      return Duration.ofMinutes(PropertiesUtils.getPropAsInt(
          props,
          GobblinTemporalConfigurationKeys.RECOMMEND_SCALING_ACTIVITY_TIMEOUT_MINUTES,
          DEFAULT_RECOMMEND_SCALING_ACTIVITY_TIMEOUT_MINUTES
      ));
    }
  }

  class DeleteWorkDirsActivityTimeoutStrategy implements ActivityTimeoutStrategy {
    @Override
    public Duration getTimeout(Properties props) {
      return Duration.ofMinutes(PropertiesUtils.getPropAsInt(
          props,
          GobblinTemporalConfigurationKeys.DELETE_WORK_DIRS_ACTIVITY_TIMEOUT_MINUTES,
          DEFAULT_DELETE_WORK_DIRS_ACTIVITY_TIMEOUT_MINUTES
      ));
    }
  }

  class ProcessWorkunitActivityTimeoutStrategy implements ActivityTimeoutStrategy {
    @Override
    public Duration getTimeout(Properties props) {
      return Duration.ofMinutes(PropertiesUtils.getPropAsInt(
          props,
          GobblinTemporalConfigurationKeys.PROCESS_WORKUNIT_ACTIVITY_TIMEOUT_MINUTES,
          DEFAULT_PROCESS_WORKUNIT_ACTIVITY_TIMEOUT_MINUTES
      ));
    }
  }

  class CommitActivityTimeoutStrategy implements ActivityTimeoutStrategy {
    @Override
    public Duration getTimeout(Properties props) {
      return Duration.ofMinutes(PropertiesUtils.getPropAsInt(
          props,
          GobblinTemporalConfigurationKeys.COMMIT_ACTIVITY_TIMEOUT_MINUTES,
          DEFAULT_COMMIT_ACTIVITY_TIMEOUT_MINUTES
      ));
    }
  }
}
