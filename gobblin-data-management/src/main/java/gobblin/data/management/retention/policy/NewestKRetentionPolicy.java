package gobblin.data.management.retention.policy;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.hadoop.fs.FileStatus;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import azkaban.utils.Props;

import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;


/**
 * Retains the newest k versions of the dataset.
 */
public class NewestKRetentionPolicy implements RetentionPolicy<DatasetVersion> {

  public static final String VERSIONS_RETAINED_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX +
      "versions.retained";
  public static final int VERSIONS_RETAINED_DEFAULT = 2;

  private final int versionsRetained;

  public NewestKRetentionPolicy(Props props) {
    this.versionsRetained = props.getInt(VERSIONS_RETAINED_KEY, VERSIONS_RETAINED_DEFAULT);
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return DatasetVersion.class;
  }

  @Override
  public List<DatasetVersion> preserveDeletableVersions(List<DatasetVersion> allVersions) {
    int newerVersions = 0;
    List<DatasetVersion> deletableVersions = Lists.newArrayList();
    for(DatasetVersion datasetVersion : allVersions) {
      if(newerVersions >= this.versionsRetained) {
        deletableVersions.add(datasetVersion);
      }
      newerVersions++;
    }
    return deletableVersions;
  }
}
