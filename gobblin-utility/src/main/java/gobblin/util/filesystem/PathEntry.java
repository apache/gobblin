package gobblin.util.filesystem;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class PathEntry implements Serializable {

  static final PathEntry[] EMPTY_ENTRIES = new PathEntry[0];

  private final PathEntry parent;
  private PathEntry[] children;
  private final Path path;
  private String name;
  private boolean exists;
  private boolean directory;
  private long lastModified;
  private long length;

  public PathEntry(final Path path) {
    this(null, path);
  }

  public PathEntry(final PathEntry parent, final Path path) {
    if (path == null) {
      throw new IllegalArgumentException("File is missing");
    }
    this.path = path;
    this.parent = parent;
    this.name = path.getName();
  }

  public boolean refresh(final Path path)
      throws IOException {

    // cache original values
    final boolean origExists = exists;
    final long origLastModified = lastModified;
    final boolean origDirectory = directory;
    final long origLength = length;

    Configuration conf = new Configuration();

    try (FileSystem fileSystem = path.getFileSystem(conf)) {

      // refresh the values
      name = path.getName();
      exists = fileSystem.exists(path);
      directory = exists && fileSystem.isDirectory(path);
      lastModified = exists ? fileSystem.getFileStatus(path).getModificationTime() : 0;
      length = exists && !directory ? fileSystem.getFileStatus(path).getLen() : 0;

      // Return if there are changes
      return exists != origExists ||
          lastModified != origLastModified ||
          directory != origDirectory ||
          length != origLength;
    }
  }

  /**
   * Create a new child instance.
   * <p>
   * Custom implementations should override this method to return
   * a new instance of the appropriate type.
   *
   * @param path The child file
   * @return a new child instance
   */
  public PathEntry newChildInstance(final Path path) {
    return new PathEntry(this, path);
  }

  /**
   * Return the parent entry.
   *
   * @return the parent entry
   */
  public PathEntry getParent() {
    return parent;
  }

  /**
   * Return the level
   *
   * @return the level
   */
  public int getLevel() {
    return parent == null ? 0 : parent.getLevel() + 1;
  }

  /**
   * Return the directory's files.
   *
   * @return This directory's files or an empty
   * array if the file is not a directory or the
   * directory is empty
   */
  public PathEntry[] getChildren() {
    return children != null ? children : EMPTY_ENTRIES;
  }

  /**
   * Set the directory's files.
   *
   * @param children This directory's files, may be null
   */
  public void setChildren(final PathEntry[] children) {
    this.children = children;
  }

  /**
   * Return the file being monitored.
   *
   * @return the file being monitored
   */
  public Path getPath() {
    return path;
  }

  /**
   * Return the file name.
   *
   * @return the file name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the file name.
   *
   * @param name the file name
   */
  public void setName(final String name) {
    this.name = name;
  }

  /**
   * Return the last modified time from the last time it
   * was checked.
   *
   * @return the last modified time
   */
  public long getLastModified() {
    return lastModified;
  }

  /**
   * Return the last modified time from the last time it
   * was checked.
   *
   * @param lastModified The last modified time
   */
  public void setLastModified(final long lastModified) {
    this.lastModified = lastModified;
  }

  /**
   * Return the length.
   *
   * @return the length
   */
  public long getLength() {
    return length;
  }

  /**
   * Set the length.
   *
   * @param length the length
   */
  public void setLength(final long length) {
    this.length = length;
  }

  /**
   * Indicate whether the file existed the last time it
   * was checked.
   *
   * @return whether the file existed
   */
  public boolean isExists() {
    return exists;
  }

  /**
   * Set whether the file existed the last time it
   * was checked.
   *
   * @param exists whether the file exists or not
   */
  public void setExists(final boolean exists) {
    this.exists = exists;
  }

  /**
   * Indicate whether the file is a directory or not.
   *
   * @return whether the file is a directory or not
   */
  public boolean isDirectory() {
    return directory;
  }

  /**
   * Set whether the file is a directory or not.
   *
   * @param directory whether the file is a directory or not
   */
  public void setDirectory(final boolean directory) {
    this.directory = directory;
  }
}
