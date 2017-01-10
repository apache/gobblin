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

package gobblin.data.management.copy.converter;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;

import com.google.common.base.Function;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.util.PathUtils;


/**
 * Abstract class for distcp {@link Converter}. Simply transforms the {@link InputStream} in the
 * {@link FileAwareInputStream}, and possibly modifies extensions of the output file.
 */
public abstract class DistcpConverter extends Converter<String, String, FileAwareInputStream, FileAwareInputStream> {

  @Override
  public Converter<String, String, FileAwareInputStream, FileAwareInputStream> init(WorkUnitState workUnit) {
    return super.init(workUnit);
  }

  /**
   * @return A {@link Function} that transforms the {@link FSDataInputStream} in the {@link FileAwareInputStream}.
   */
  public abstract Function<FSDataInputStream, FSDataInputStream> inputStreamTransformation();

  /**
   * @return A list of extensions that should be removed from the output file name, which will be applied in order.
   *        For example, if this method returns ["gz", "tar", "tgz"] then "file.tar.gz" becomes "file".
   */
  public List<String> extensionsToRemove() {
    return new ArrayList<>();
  }

  /**
   * TODO: actually use this method and add the extensions.
   * @return A list of extensions that should be added to the output file name, to be applied in order.
   *        For example, if this method returns ["tar", "gz"] then "file" becomes "file.tar.gz".
   */
  public List<String> extensionsToAdd() {
    return new ArrayList<>();
  }

  /**
   * Identity schema converter.
   */
  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  /**
   * Applies the transformation in {@link #inputStreamTransformation} to the {@link FSDataInputStream} in the
   * {@link FileAwareInputStream}.
   */
  @Override
  public Iterable<FileAwareInputStream> convertRecord(String outputSchema, FileAwareInputStream fileAwareInputStream,
      WorkUnitState workUnit) throws DataConversionException {

    modifyExtensionAtDestination(fileAwareInputStream.getFile());
    try {
      FSDataInputStream newInputStream = inputStreamTransformation().apply(fileAwareInputStream.getInputStream());
      return new SingleRecordIterable<>(new FileAwareInputStream(fileAwareInputStream.getFile(), newInputStream));
    } catch (RuntimeException re) {
      throw new DataConversionException(re);
    }
  }

  private void modifyExtensionAtDestination(CopyableFile file) {
    if (extensionsToRemove().size() > 0) {
      file.setDestination(PathUtils.removeExtension(file.getDestination(), extensionsToRemove().toArray(new String[0])));
    }
  }
}
