package gobblin.data.management.retention;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import gobblin.data.management.retention.dataset.Dataset;
import gobblin.data.management.retention.dataset.finder.DatasetFinder;


/**
 * Finds existing versions of datasets and cleans old or deprecated versions.
 */
public class DatasetCleaner {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetCleaner.class);

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.retention.";
  public static final String DATASET_PROFILE_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "dataset.profile.class";

  private final DatasetFinder datasetFinder;

  public DatasetCleaner(FileSystem fs, Properties props) throws IOException {

    Preconditions.checkArgument(props.containsKey(DATASET_PROFILE_CLASS_KEY));

    try{
      Class<?> datasetFinderClass = Class.forName(props.getProperty(DATASET_PROFILE_CLASS_KEY));
      this.datasetFinder = (DatasetFinder) datasetFinderClass.
          getConstructor(FileSystem.class, Properties.class).newInstance(fs, props);
    } catch(ClassNotFoundException exception) {
      throw new IOException(exception);
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
