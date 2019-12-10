package org.apache.gobblin.data.management.dataset;

import org.apache.hadoop.fs.Path;


public class EmptyFileSystemDataset extends SimpleFileSystemDataset {
  public EmptyFileSystemDataset(Path path) {
    super(path);
  }
}
