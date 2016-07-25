package gobblin.util.filesystem;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.hadoop.fs.Path;


/**
 * A listener that receives events of general file system modifications.
 * A generalized version Of FileAlterationListener interface using Path as the parameter for each method
 * @see FileAlterationListener
 */

public interface PathAlterationListener {
  void onStart(final PathAlterationObserver observer);

  void onFileCreate(final Path path);

  void onFileChange(final Path path);

  void onStop(final PathAlterationObserver observer);

  void onDirectoryCreate(final Path directory);

  void onDirectoryChange(final Path directory);

  void onDirectoryDelete(final Path directory);

  void onFileDelete(final Path path);
}
