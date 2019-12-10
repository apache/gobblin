package org.apache.gobblin.data.management.dataset;

import org.apache.hadoop.fs.Path;

import org.apache.gobblin.dataset.FileSystemDataset;


public class SimpleFileSystemDataset implements FileSystemDataset {

  private final Path path;

  public SimpleFileSystemDataset(Path path) {
    this.path = path;
  }

  @Override
  public Path datasetRoot() {
    return path;
  }

  @Override
  public String datasetURN() {
    return path.toString();
  }
}