package gobblin.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;


/**
 * Utility class for listing files on a {@link FileSystem}.
 *
 * @see {@link FileSystem}
 */
public class FileListUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FileListUtils.class);
  public static final PathFilter NO_OP_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return true;
    }
  };

  public static List<FileStatus> listFilesRecursively(FileSystem fs, Path path) throws IOException {
    return listFilesRecursively(fs, path, NO_OP_PATH_FILTER);
  }

  public static List<FileStatus> listFilesRecursively(FileSystem fs, Iterable<Path> paths) throws IOException {
    List<FileStatus> results = Lists.newArrayList();
    for (Path path : paths) {
      results.addAll(listFilesRecursively(fs, path));
    }
    return results;
  }

  /**
   * Helper method to list out all files under a specified path. The specified {@link PathFilter} is treated as a file
   * filter, that is it is only applied to file {@link Path}s.
   */
  public static List<FileStatus> listFilesRecursively(FileSystem fs, Path path, PathFilter fileFilter)
      throws IOException {
    return listFilesRecursivelyHelper(fs, Lists.<FileStatus> newArrayList(), fs.getFileStatus(path), fileFilter);
  }

  @SuppressWarnings("deprecation")
  private static List<FileStatus> listFilesRecursivelyHelper(FileSystem fs, List<FileStatus> files,
      FileStatus fileStatus, PathFilter fileFilter) throws FileNotFoundException, IOException {
    Path qualifiedPath = fs.makeQualified(fileStatus.getPath());
    if (fileStatus.isDir()) {
      for (FileStatus status : fs.listStatus(fileStatus.getPath())) {
        // Fix for hadoop issue: https://issues.apache.org/jira/browse/HADOOP-12169
        if (!qualifiedPath.equals(status.getPath())) {
          listFilesRecursivelyHelper(fs, files, status, fileFilter);
        }
      }
    } else if (fileFilter.accept(fileStatus.getPath())) {
      files.add(fileStatus);
    }
    return files;
  }

  /**
   * Method to list out all files, or directory if no file exists, under a specified path.
   */
  public static List<FileStatus> listMostNestedPathRecursively(FileSystem fs, Path path) throws IOException {
    return listMostNestedPathRecursively(fs, path, NO_OP_PATH_FILTER);
  }

  public static List<FileStatus> listMostNestedPathRecursively(FileSystem fs, Iterable<Path> paths) throws IOException {
    List<FileStatus> results = Lists.newArrayList();
    for (Path path : paths) {
      results.addAll(listMostNestedPathRecursively(fs, path));
    }
    return results;
  }

  /**
   * Method to list out all files, or directory if no file exists, under a specified path.
   * The specified {@link PathFilter} is treated as a file filter, that is it is only applied to file {@link Path}s.
   */
  public static List<FileStatus> listMostNestedPathRecursively(FileSystem fs, Path path, PathFilter fileFilter)
      throws IOException {
    return listMostNestedPathRecursivelyHelper(fs, Lists.<FileStatus> newArrayList(), fs.getFileStatus(path),
        fileFilter);
  }

  @SuppressWarnings("deprecation")
  private static List<FileStatus> listMostNestedPathRecursivelyHelper(FileSystem fs, List<FileStatus> files,
      FileStatus fileStatus, PathFilter fileFilter) throws IOException {
    if (fileStatus.isDir()) {
      Path qualifiedPath = fs.makeQualified(fileStatus.getPath());
      FileStatus[] curFileStatus = fs.listStatus(fileStatus.getPath());
      if (ArrayUtils.isEmpty(curFileStatus)) {
        files.add(fileStatus);
      } else {
        for (FileStatus status : curFileStatus) {
          // Fix for hadoop issue: https://issues.apache.org/jira/browse/HADOOP-12169
          if (!qualifiedPath.equals(status.getPath())) {
            listMostNestedPathRecursivelyHelper(fs, files, status, fileFilter);
          }
        }
      }
    } else if (fileFilter.accept(fileStatus.getPath())) {
      files.add(fileStatus);
    }
    return files;
  }

  /**
   * Helper method to list out all paths under a specified path. If the {@link org.apache.hadoop.fs.FileSystem} is
   * unable to list the contents of a relevant directory, will log an error and skip.
   */
  public static List<FileStatus> listPathsRecursively(FileSystem fs, Path path, PathFilter fileFilter)
      throws IOException {
    return listPathsRecursivelyHelper(fs, Lists.<FileStatus> newArrayList(), fs.getFileStatus(path), fileFilter);
  }

  @SuppressWarnings("deprecation")
  private static List<FileStatus> listPathsRecursivelyHelper(FileSystem fs, List<FileStatus> files,
      FileStatus fileStatus, PathFilter fileFilter) {
    if (fileFilter.accept(fileStatus.getPath())) {
      files.add(fileStatus);
    }
    if (fileStatus.isDir()) {
      try {
        Path qualifiedPath = fs.makeQualified(fileStatus.getPath());
        for (FileStatus status : fs.listStatus(fileStatus.getPath())) {
          // Fix for hadoop issue: https://issues.apache.org/jira/browse/HADOOP-12169
          if (!qualifiedPath.equals(status.getPath())) {
            listPathsRecursivelyHelper(fs, files, status, fileFilter);
          }
        }
      } catch (IOException ioe) {
        LOG.error("Could not list contents of path " + fileStatus.getPath());
      }
    }
    return files;
  }
}
