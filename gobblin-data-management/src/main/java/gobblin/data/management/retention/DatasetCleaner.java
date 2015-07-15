package gobblin.data.management.retention;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import azkaban.utils.Props;

import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.dataset.finder.DatasetFinder;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.finder.DatasetVersionFinder;
import gobblin.data.management.retention.version.finder.VersionFinder;
import gobblin.data.management.trash.Trash;


/**
 * Finds existing versions of datasets and cleans old or deprecated versions.
 */
public class DatasetCleaner {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetCleaner.class);

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.retention.";
  public static final String DATASET_PROFILE_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "dataset.profile.class";

  private final DatasetFinder datasetFinder;

  public DatasetCleaner(FileSystem fs, Props props) throws IOException {

    try{
      this.datasetFinder = (DatasetFinder) props.getClass(DATASET_PROFILE_CLASS_KEY).
          getConstructor(FileSystem.class, Props.class).newInstance(fs, props);
    } catch(NoSuchMethodException exception) {
      throw new IOException(exception);
    } catch(InstantiationException exception) {
      throw new IOException(exception);
    } catch(IllegalAccessException exception) {
      throw new IOException(exception);
    } catch(InvocationTargetException exception) {
      throw new IOException(exception);
    }
  }

  /**
   * Perform the cleanup of old / deprecated dataset versions.
   * @throws IOException
   */
  public void clean() throws IOException {
    List<Dataset> dataSets = this.datasetFinder.findDatasets();

    for(Dataset dataset: dataSets) {
      dataset.clean();
    }

  }

}
