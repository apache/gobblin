package gobblin.data.management.trash;

import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.joda.time.DateTime;


/**
 * Policy that deletes snapshots if they are older than {@link #SNAPSHOT_RETENTION_POLICY_MINUTES_KEY} minutes.
 */
public class TimeBasedSnapshotCleanupPolicy implements SnapshotCleanupPolicy {

  public static final String SNAPSHOT_RETENTION_POLICY_MINUTES_KEY =
      "gobblin.trash.snapshot.retention.minutes";
  public static final int SNAPSHOT_RETENTION_POLICY_MINUTES_DEFAULT = 1440; // one day

  private final int retentionMinutes;

  public TimeBasedSnapshotCleanupPolicy(Properties props) {
    this.retentionMinutes = Integer.parseInt(props.getProperty(SNAPSHOT_RETENTION_POLICY_MINUTES_KEY,
        Integer.toString(SNAPSHOT_RETENTION_POLICY_MINUTES_DEFAULT)));
  }

  @Override
  public boolean shouldDeleteSnapshot(FileStatus snapshot, Trash trash) {
    DateTime snapshotTime = Trash.TRASH_SNAPSHOT_NAME_FORMATTER.parseDateTime(snapshot.getPath().getName());
    return snapshotTime.plusMinutes(retentionMinutes).isBeforeNow();
  }
}
