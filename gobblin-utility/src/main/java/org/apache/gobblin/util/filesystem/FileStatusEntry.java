/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.util.filesystem;

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
  private final FileSystem fs;

  public Optional<FileStatus> _fileStatus;

  public FileStatusEntry(final Path path)
      throws IOException {
    this(null, path, path.getFileSystem(new Configuration()));
  }

  private FileStatusEntry(final FileStatusEntry parent, final Path path, FileSystem fs)
      throws IOException {
    if (path == null) {
      throw new IllegalArgumentException("Path is missing");
    }
    this.parent = parent;
    this.fs = fs;
    this._fileStatus = Optional.fromNullable(this.fs.getFileStatus(path));
  }

  public boolean refresh(final Path path)
      throws IOException {
    if (_fileStatus.isPresent()) {
      Optional<FileStatus> oldStatus = this._fileStatus;
      try {
        this._fileStatus = Optional.of(this.fs.getFileStatus(path));
        this.exists = this._fileStatus.isPresent();

        return (oldStatus.isPresent() != this._fileStatus.isPresent()
            || oldStatus.get().getModificationTime() != this._fileStatus.get().getModificationTime()
            || oldStatus.get().isDirectory() != this._fileStatus.get().isDirectory()
            || oldStatus.get().getLen() != this._fileStatus.get().getLen());
      } catch (FileNotFoundException e) {
        _fileStatus = Optional.absent();
        this.exists = false;
        return true;
      }
    } else {
      if (path.getFileSystem(new Configuration()).exists(path)) {
        _fileStatus = Optional.of(this.fs.getFileStatus(path));
        return true;
      } else {
        return false;
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
    return new FileStatusEntry(this, path, this.fs);
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
