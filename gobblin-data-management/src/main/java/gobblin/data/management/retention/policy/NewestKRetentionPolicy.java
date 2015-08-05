package gobblin.data.management.retention.policy;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import gobblin.data.management.retention.DatasetCleaner;
import gobblin.data.management.retention.version.DatasetVersion;


/**
 * Retains the newest k versions of the dataset.
 */
public class NewestKRetentionPolicy implements RetentionPolicy<DatasetVersion> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NewestKRetentionPolicy.class);

  public static final String VERSIONS_RETAINED_KEY = DatasetCleaner.CONFIGURATION_KEY_PREFIX +
      "versions.retained";
  public static final String VERSIONS_RETAINED_DEFAULT = Integer.toString(2);

  private final int versionsRetained;

  public NewestKRetentionPolicy(Properties props) {
    this.versionsRetained = Integer.parseInt(props.getProperty(VERSIONS_RETAINED_KEY, VERSIONS_RETAINED_DEFAULT));
    LOGGER.info(String.format("%s will retain %d versions of each dataset.",
        NewestKRetentionPolicy.class.getCanonicalName(), this.versionsRetained));
  }

  @Override
  public Class<? extends DatasetVersion> versionClass() {
    return DatasetVersion.class;
  }

  @Override
  public Collection<DatasetVersion> listDeletableVersions(List<DatasetVersion> allVersions) {
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
