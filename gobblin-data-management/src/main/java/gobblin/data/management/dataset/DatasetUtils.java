package gobblin.data.management.dataset;

import gobblin.data.management.retention.dataset.finder.DatasetFinder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Preconditions;


/**
 * Utilities for datasets.
 */
public class DatasetUtils {

  public static final String CONFIGURATION_KEY_PREFIX = "gobblin.dataset.";
  public static final String DATASET_PROFILE_CLASS_KEY = CONFIGURATION_KEY_PREFIX + "profile.class";

  /**
   * Instantiate a {@link DatasetFinder}. The class of the {@link DatasetFinder} is read from property
   * {@link #DATASET_PROFILE_CLASS_KEY}.
   * @param props Properties used for building {@link DatasetFinder}.
   * @param fs {@link FileSystem} where datasets are located.
   * @return A new instance of {@link DatasetFinder}.
   * @throws IOException
   */
  public static DatasetFinder instantiateDatasetFinder(Properties props, FileSystem fs) throws IOException {
    Preconditions.checkArgument(props.containsKey(DATASET_PROFILE_CLASS_KEY));
    try {
      Class<?> datasetFinderClass = Class.forName(props.getProperty(DATASET_PROFILE_CLASS_KEY));
      return
          (DatasetFinder) datasetFinderClass.getConstructor(FileSystem.class, Properties.class).newInstance(fs, props);
    } catch (ClassNotFoundException exception) {
      throw new IOException(exception);
    } catch (NoSuchMethodException exception) {
      throw new IOException(exception);
    } catch (InstantiationException exception) {
      throw new IOException(exception);
    } catch (IllegalAccessException exception) {
      throw new IOException(exception);
    } catch (InvocationTargetException exception) {
      throw new IOException(exception);
    }

  }

}
