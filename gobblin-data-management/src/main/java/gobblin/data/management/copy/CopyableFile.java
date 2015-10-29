/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy;

import gobblin.data.management.partition.File;

import java.io.IOException;
import java.util.List;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


/**
 * Abstraction for a file to copy from {@link #origin} to {@link #destination}.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public class CopyableFile implements File {

  private static Gson gson = new Gson();
  /** {@link FileStatus} of the existing origin file. */
  private FileStatus origin;
  /** Complete destination {@link Path} of the file. Dataset's final publish directory + {@link #relativeDestination} */
  private Path destination;
  /** {@link Path} to the file relative to the dataset's final publish directory */
  private Path relativeDestination;
  /** Desired {@link OwnerAndPermission} of the destination path. */
  private OwnerAndPermission destinationOwnerAndPermission;
  /**
   * Desired {@link OwnerAndPermission} of the ancestor directories of the destination path. The list is ordered from
   * deepest to highest directory.
   *
   * <p>
   * For example, if {@link #destination} is /a/b/c/file, then the first element of this list is the desired owner and
   * permission for directory /a/b/c, the second is the desired owner and permission for directory /a/b, and so on.
   * </p>
   *
   * <p>
   * If there are fewer elements in the list than ancestor directories in {@link #destination}, it is understood that
   * extra directories are allowed to have any owner and permission.
   * </p>
   */
  private List<OwnerAndPermission> ancestorsOwnerAndPermission;
  /** Checksum of the origin file. */
  private byte[] checksum;

  @Override
  public FileStatus getFileStatus() {
    return this.origin;
  }

  /**
   * Serialize an instance of {@link CopyableFile} into a {@link String}.
   *
   * @param copyableFile to be serialized
   * @return serialized string
   */
  public static String serialize(CopyableFile copyableFile) throws IOException {
    return gson.toJson(copyableFile);
  }

  /**
   * Serialize a {@link List} of {@link CopyableFile}s into a {@link String}.
   *
   * @param copyableFile to be serialized
   * @return serialized string
   */
  public static String serializeList(List<CopyableFile> copyableFiles) throws IOException {
    return gson.toJson(copyableFiles, new TypeToken<List<CopyableFile>>(){}.getType());
  }

  /**
   * Deserializes the serialized {@link CopyableFile} string.
   *
   * @param serialized string
   * @return a new instance of {@link CopyableFile}
   */
  public static CopyableFile deserialize(String serialized) throws IOException {
    return gson.fromJson(serialized, CopyableFile.class);
  }

  /**
   * Deserializes the serialized {@link List} of {@link CopyableFile} string.
   * Used together with {@link #serializeList(List)}
   *
   * @param serialized string
   * @return a new {@link List} of {@link CopyableFile}s
   */
  public static List<CopyableFile> deserializeList(String serialized) throws IOException {
    return gson.fromJson(serialized, new TypeToken<List<CopyableFile>>(){}.getType());
  }
}
