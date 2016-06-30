package gobblin.util.filesystem;

import java.io.IOException;

import org.apache.commons.io.monitor.FileEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class FileStatusEntry extends FileStatus {

  static final FileStatusEntry[] EMPTY_ENTRIES = new FileStatusEntry[0];

  private final FileStatusEntry parent;
  private FileStatusEntry[] children;
  private final Path path;
  private String name;
  private boolean exists;
  private boolean directory;
  private long lastModified;
  private long length;

  public FileStatusEntry(final Path path) {
    this(null, path);
  }

  public FileStatusEntry(final FileStatusEntry parent, final Path path) {
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
  public FileStatusEntry newChildInstance(final Path path) {
    return new FileStatusEntry(this, path);
  }

  /**
   * @return path The path object for this FileStatusEntry.
   */
  @Override
  public Path getPath() {
    return this.path;
  }

  public boolean isDirectory() {
    return directory;
  }

  /**
   * Return the parent entry.
   *
   * @return the parent entry
   */
  public FileStatusEntry getParent() {
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
  public FileStatusEntry[] getChildren() {
    return children != null ? children : EMPTY_ENTRIES;
  }

  /**
   * Set the directory's files.
   *
   * @param children This directory's files, may be null
   */
  public void setChildren(final FileStatusEntry[] children) {
    this.children = children;
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
   * Set whether the file is a directory or not.
   *
   * @param directory whether the file is a directory or not
   */
  public void setDirectory(final boolean directory) {
    this.directory = directory;
  }


  /** Compare if this object is equal to another object
   * @param  o the object to be compared.
   * @return  true if two file status has the same path name; false if not.
   */
  @Override
  public boolean equals(Object o) {
    if ( o == null || this.getClass() != o.getClass()) {
      return false ;
    }
    FileStatus other = (FileStatus)o;
    return this.getPath().equals(other.getPath());
  }

  /**
   * Returns a hash code value for the object, which is defined as
   * the hash code of the path name.
   *
   * @return  a hash code value for the path name.
   */
  @Override
  public int hashCode() {
    return getPath().hashCode();
  }

}
