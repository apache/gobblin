package gobblin.util.filesystem;

import org.apache.hadoop.fs.Path;


public class PathAlterationListenerAdaptor implements PathAlterationListener {
  public void onStart(final PathAlterationObserver observer) {
  }

  public void onFileCreate(final Path path) {
  }

  public void onFileChange(final Path path) {
  }

  public void onStop(final PathAlterationObserver observer) {
  }

  public void onDirectoryCreate(final Path directory) {
  }

  public void onDirectoryChange(final Path directory) {
  }

  public void onDirectoryDelete(final Path directory) {
  }

  public void onFileDelete(final Path path) {
  }
}
