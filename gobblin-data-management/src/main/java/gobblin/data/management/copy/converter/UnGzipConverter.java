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

package gobblin.data.management.copy.converter;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.util.PathUtils;
import gobblin.util.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;


/**
 * A {@link Converter} that converts an archived {@link InputStream} to a tar {@link InputStream}. Wraps the given
 * archived (.tar.gz or .tgz) {@link InputStream} with {@link GZIPInputStream} Use this converter if the
 * {@link InputStream} from source is compressed.
 * It also converts the destination file name by removing tar and gz extensions.
 */
public class UnGzipConverter extends Converter<String, String, FileAwareInputStream, FileAwareInputStream> {

  private static final String TAR_EXTENSION = ".tar";
  private static final String GZ_EXTENSION = ".gz";
  private static final String TGZ_EXTENSION = ".tgz";

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<FileAwareInputStream> convertRecord(String outputSchema, FileAwareInputStream fileAwareInputStream,
      WorkUnitState workUnit) throws DataConversionException {
    removeExtensionAtDestination(fileAwareInputStream.getFile());

    return convertInputStream(outputSchema, fileAwareInputStream, workUnit);
  }

  static Iterable<FileAwareInputStream> convertInputStream(String outputSchema,
      FileAwareInputStream fileAwareInputStream, WorkUnitState workUnit) throws DataConversionException {
    try {
      return new SingleRecordIterable<FileAwareInputStream>(new FileAwareInputStream(fileAwareInputStream.getFile(),
          StreamUtils.convertStream(new GZIPInputStream(fileAwareInputStream.getInputStream()))));

    } catch (IOException e) {
      throw new DataConversionException(String.format("Failed to convert %s ", fileAwareInputStream.getFile()
          .getOrigin().getPath()), e);
    }
  }

  /**
   * Remove {@value #TAR_EXTENSION} and {@value #GZ_EXTENSION} from {@link CopyableFile#getDestination()} and
   * {@link CopyableFile#getRelativeDestination()}
   */
  private void removeExtensionAtDestination(CopyableFile file) {
    file.setDestination(PathUtils.removeExtension(file.getDestination(), TAR_EXTENSION, GZ_EXTENSION, TGZ_EXTENSION));
    file.setRelativeDestination(PathUtils.removeExtension(file.getRelativeDestination(), TAR_EXTENSION, GZ_EXTENSION, TGZ_EXTENSION));
  }
}
