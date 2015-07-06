package gobblin.data.management.retention.policy;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.hadoop.fs.FileStatus;
import org.joda.time.Duration;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import azkaban.utils.Props;

import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.TimestampedDatasetVersion;


/**
 * Retain dataset versions newer than now - {@link #retention}.
 */
public class TimeBasedRetentionPolicy implements RetentionPolicy<TimestampedDatasetVersion> {

  public static final String RETENTION_MINUTES_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX +
      "minutes.retained";
  public static final long RETENTION_MINUTES_DEFAULT = 24 * 60; // one day

  private final Duration retention;

  public TimeBasedRetentionPolicy(Props props) {
    this.retention = Duration.standardMinutes(props.getLong(RETENTION_MINUTES_KEY, RETENTION_MINUTES_DEFAULT));
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public List<TimestampedDatasetVersion> preserveDeletableVersions(List<TimestampedDatasetVersion> allVersions) {
    return Lists.newArrayList(Collections2.filter(allVersions, new Predicate<DatasetVersion>() {
      @Override
      public boolean apply(DatasetVersion version) {
        return ((TimestampedDatasetVersion) version).getDateTime().plus(retention).isBeforeNow();
      }
    }));
  }

}
