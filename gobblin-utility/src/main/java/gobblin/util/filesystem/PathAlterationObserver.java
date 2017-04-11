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

package gobblin.util.filesystem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.apache.commons.io.IOCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.google.common.collect.Maps;

import gobblin.util.DecoratorUtils;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PathAlterationObserver {

  private final Map<PathAlterationListener, PathAlterationListener> listeners = Maps.newConcurrentMap();
  private final FileStatusEntry rootEntry;
  private final PathFilter pathFilter;
  private final Comparator<Path> comparator;
  private final FileSystem fs;

  private final Path[] EMPTY_PATH_ARRAY = new Path[0];

  /**
   * Final processing.
   */
  public void destroy() {
  }

  /**
   * Construct an observer for the specified directory.
   *
   * @param directoryName the name of the directory to observe
   */
  public PathAlterationObserver(final String directoryName)
      throws IOException {
    this(new Path(directoryName));
  }

  /**
   * Construct an observer for the specified directory and file filter.
   *
   * @param directoryName the name of the directory to observe
   * @param pathFilter The file filter or null if none
   */
  public PathAlterationObserver(final String directoryName, final PathFilter pathFilter)
      throws IOException {
    this(new Path(directoryName), pathFilter);
  }

  /**
   * Construct an observer for the specified directory.
   *
   * @param directory the directory to observe
   */
  public PathAlterationObserver(final Path directory)
      throws IOException {
    this(directory, null);
  }

  /**
   * Construct an observer for the specified directory and file filter.
   *
   * @param directory the directory to observe
   * @param pathFilter The file filter or null if none
   */
  public PathAlterationObserver(final Path directory, final PathFilter pathFilter)
      throws IOException {
    this(new FileStatusEntry(directory), pathFilter);
  }

  /**
   * The comparison between path is always case-sensitive in this general file system context.
   */
  public PathAlterationObserver(final FileStatusEntry rootEntry, final PathFilter pathFilter)
      throws IOException {
    if (rootEntry == null) {
      throw new IllegalArgumentException("Root entry is missing");
    }
    if (rootEntry.getPath() == null) {
      throw new IllegalArgumentException("Root directory is missing");
    }
    this.rootEntry = rootEntry;
    this.pathFilter = pathFilter;

    this.fs = rootEntry.getPath().getFileSystem(new Configuration());

    // By default, the comparsion is case sensitive.
    this.comparator = new Comparator<Path>() {
      @Override
      public int compare(Path o1, Path o2) {
        return IOCase.SENSITIVE.checkCompareTo(o1.toUri().toString(), o2.toUri().toString());
      }
    };
  }

  /**
   * Add a file system listener.
   *
   * @param listener The file system listener
   */
  public void addListener(final PathAlterationListener listener) {
    if (listener != null) {
      this.listeners.put(listener, new ExceptionCatchingPathAlterationListenerDecorator(listener));
    }
  }

  /**
   * Remove a file system listener.
   *
   * @param listener The file system listener
   */
  public void removeListener(final PathAlterationListener listener) {
    if (listener != null) {
      this.listeners.remove(listener);
    }
  }

  /**
   * Returns the set of registered file system listeners.
   *
   * @return The file system listeners
   */
  public Iterable<PathAlterationListener> getListeners() {
    return listeners.keySet();
  }

  /**
   * Initialize the observer.
   * @throws IOException if an error occurs
   */
  public void initialize() throws IOException  {
    rootEntry.refresh(rootEntry.getPath());
    final FileStatusEntry[] children = doListPathsEntry(rootEntry.getPath(), rootEntry);
    rootEntry.setChildren(children);
  }

  /**
   * Check whether the file and its chlidren have been created, modified or deleted.
   */
  public void checkAndNotify()
      throws IOException {
    /* fire onStart() */
    for (final PathAlterationListener listener : listeners.values()) {
      listener.onStart(this);
    }

    /* fire directory/file events */
    final Path rootPath = rootEntry.getPath();

    if (fs.exists(rootPath)) {
      // Current existed.
      checkAndNotify(rootEntry, rootEntry.getChildren(), listPaths(rootPath));
    } else if (rootEntry.isExists()) {
      // Existed before and not existed now.
      checkAndNotify(rootEntry, rootEntry.getChildren(), EMPTY_PATH_ARRAY);
    } else {
      // Didn't exist and still doesn't
    }

    /* fire onStop() */
    for (final PathAlterationListener listener : listeners.values()) {
      listener.onStop(this);
    }
  }

  /**
   * Compare two file lists for files which have been created, modified or deleted.
   *
   * @param parent The parent entry
   * @param previous The original list of paths
   * @param currentPaths The current list of paths
   */
  private void checkAndNotify(final FileStatusEntry parent, final FileStatusEntry[] previous, final Path[] currentPaths)
      throws IOException {

    int c = 0;
    final FileStatusEntry[] current =
        currentPaths.length > 0 ? new FileStatusEntry[currentPaths.length] : FileStatusEntry.EMPTY_ENTRIES;
    for (final FileStatusEntry previousEntry : previous) {
      while (c < currentPaths.length && comparator.compare(previousEntry.getPath(), currentPaths[c]) > 0) {
        current[c] = createPathEntry(parent, currentPaths[c]);
        doCreate(current[c]);
        c++;
      }
      if (c < currentPaths.length && comparator.compare(previousEntry.getPath(), currentPaths[c]) == 0) {
        doMatch(previousEntry, currentPaths[c]);
        checkAndNotify(previousEntry, previousEntry.getChildren(), listPaths(currentPaths[c]));
        current[c] = previousEntry;
        c++;
      } else {
        checkAndNotify(previousEntry, previousEntry.getChildren(), EMPTY_PATH_ARRAY);
        doDelete(previousEntry);
      }
    }

    for (; c < currentPaths.length; c++) {
      current[c] = createPathEntry(parent, currentPaths[c]);
      doCreate(current[c]);
    }
    parent.setChildren(current);
  }

  /**
   * Create a new FileStatusEntry for the specified file.
   *
   * @param parent The parent file entry
   * @param childPath The file to create an entry for
   * @return A new file entry
   */
  private FileStatusEntry createPathEntry(final FileStatusEntry parent, final Path childPath)
      throws IOException {
    final FileStatusEntry entry = parent.newChildInstance(childPath);
    entry.refresh(childPath);
    final FileStatusEntry[] children = doListPathsEntry(childPath, entry);
    entry.setChildren(children);
    return entry;
  }

  /**
   * List the path in the format of FileStatusEntry array
   * @param path The path to list files for
   * @param entry the parent entry
   * @return The child files
   */
  private FileStatusEntry[] doListPathsEntry(Path path, FileStatusEntry entry)
      throws IOException {
    final Path[] paths = listPaths(path);

    final FileStatusEntry[] children =
        paths.length > 0 ? new FileStatusEntry[paths.length] : FileStatusEntry.EMPTY_ENTRIES;
    for (int i = 0; i < paths.length; i++) {
      children[i] = createPathEntry(entry, paths[i]);
    }
    return children;
  }

  /**
   * Fire directory/file created events to the registered listeners.
   *
   * @param entry The file entry
   */
  private void doCreate(final FileStatusEntry entry) {

    for (final PathAlterationListener listener : listeners.values()) {
      if (entry.isDirectory()) {
        listener.onDirectoryCreate(entry.getPath());
      } else {
        listener.onFileCreate(entry.getPath());
      }
    }
    final FileStatusEntry[] children = entry.getChildren();
    for (final FileStatusEntry aChildren : children) {
      doCreate(aChildren);
    }
  }

  /**
   * Fire directory/file change events to the registered listeners.
   *
   * @param entry The previous file system entry
   * @param path The current file
   */
  private void doMatch(final FileStatusEntry entry, final Path path)
      throws IOException {
    if (entry.refresh(path)) {
      for (final PathAlterationListener listener : listeners.values()) {
        if (entry.isDirectory()) {
          listener.onDirectoryChange(path);
        } else {
          listener.onFileChange(path);
        }
      }
    }
  }

  /**
   * Fire directory/file delete events to the registered listeners.
   *
   * @param entry The file entry
   */
  private void doDelete(final FileStatusEntry entry) {
    for (final PathAlterationListener listener : listeners.values()) {
      if (entry.isDirectory()) {
        listener.onDirectoryDelete(entry.getPath());
      } else {
        listener.onFileDelete(entry.getPath());
      }
    }
  }

  /**
   * List the contents of a directory denoted by Path
   *
   * @param path The path(File Object in general file system) to list the contents of
   * @return the directory contents or a zero length array if
   * the empty or the file is not a directory
   */
  private Path[] listPaths(final Path path)
      throws IOException {
    Path[] children = null;
    ArrayList<Path> tmpChildrenPath = new ArrayList<>();
    if (fs.isDirectory(path)) {
      // Get children's path list.
      FileStatus[] chiledrenFileStatus = pathFilter == null ? fs.listStatus(path) : fs.listStatus(path, pathFilter);
      for (FileStatus childFileStatus : chiledrenFileStatus) {
        tmpChildrenPath.add(childFileStatus.getPath());
      }
      children = tmpChildrenPath.toArray(new Path[tmpChildrenPath.size()]);
    }
    if (children == null) {
      children = EMPTY_PATH_ARRAY;
    }
    if (comparator != null && children.length > 1) {
      Arrays.sort(children, comparator);
    }
    return children;
  }
}
