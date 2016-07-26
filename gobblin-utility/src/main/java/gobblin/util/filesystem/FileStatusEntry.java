package gobblin.util.filesystem;

import com.google.common.base.Optional;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class FileStatusEntry extends FileStatus {

  static final FileStatusEntry[] EMPTY_ENTRIES = new FileStatusEntry[0];

  private final FileStatusEntry parent;
  private FileStatusEntry[] children;

  private boolean exists;

  public Optional<FileStatus> _fileStatus;

  public FileStatusEntry(final Path path)
      throws IOException {
    this(null, path);
  }

  public FileStatusEntry(final FileStatusEntry parent, final Path path)
      throws IOException {
    if (path == null) {
      throw new IllegalArgumentException("Path is missing");
    }
    this.parent = parent;
    this._fileStatus = Optional.fromNullable(path.getFileSystem(new Configuration()).getFileStatus(path));
  }

  public boolean refresh(final Path path)
      throws IOException {
    try (FileSystem fs = path.getFileSystem(new Configuration())) {
      if (_fileStatus.isPresent()) {
        Optional<FileStatus> oldStatus = this._fileStatus;
        try {
          Optional<FileStatus> newStatus = Optional.of(fs.getFileStatus(path));
          this.exists = newStatus.isPresent();
          return (oldStatus.isPresent() != this._fileStatus.isPresent()
              || oldStatus.get().getModificationTime() != newStatus.get().getModificationTime()
              || oldStatus.get().isDirectory() != newStatus.get().isDirectory() || oldStatus.get().getLen() != newStatus
              .get()
              .getLen());
        } catch (FileNotFoundException e) {
          _fileStatus = Optional.absent();
          this.exists = false;
          return true;
        }
      } else {
        if (path.getFileSystem(new Configuration()).exists(path)) {
          _fileStatus = Optional.of(fs.getFileStatus(path));
          return true;
        } else {
          return false;
        }
      }
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
  public FileStatusEntry newChildInstance(final Path path)
      throws IOException {
    return new FileStatusEntry(this, path);
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
   * Indicate whether the file existed the last time it
   * was checked.
   *
   * @return whether the file existed
   */
  public boolean isExists() {
    return exists;
  }

  /**
   * Return the path from the instance FileStatus variable
   * @return
   */
  public Path getPath() {
    return _fileStatus.get().getPath();
  }

  /**
   * Return whether the path is a directory from the instance FileStatus variable.
   * @return
   */
  public boolean isDirectory() {
    return _fileStatus.get().isDirectory();
  }

  /** Compare if this object is equal to another object
   * @param  o the object to be compared.
   * @return true if two file status has the same path name; false if not.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    FileStatusEntry other = (FileStatusEntry) o;
    return this._fileStatus.get().equals(other._fileStatus.get());
  }

  /**
   * Returns a hash code value for the object, which is defined as
   * the hash code of the path name.
   *
   * @return a hash code value for the path name.
   */
  @Override
  public int hashCode() {
    return getPath().hashCode();
  }
}
