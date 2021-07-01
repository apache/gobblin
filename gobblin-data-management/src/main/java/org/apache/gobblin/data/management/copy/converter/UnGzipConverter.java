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

package org.apache.gobblin.data.management.copy.converter;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.util.io.StreamUtils;


/**
 * A {@link Converter} that converts an archived {@link InputStream} to a tar {@link InputStream}. Wraps the given
 * archived (.tar.gz or .tgz) {@link InputStream} with {@link GZIPInputStream} Use this converter if the
 * {@link InputStream} from source is compressed.
 * It also converts the destination file name by removing tar and gz extensions.
 */
public class UnGzipConverter extends DistcpConverter {

  private static final String TAR_EXTENSION = ".tar";
  private static final String GZIP_EXTENSION = ".gzip";
  private static final String GZ_EXTENSION = ".gz";
  private static final String TGZ_EXTENSION = ".tgz";

  @Override public Function<InputStream, InputStream> inputStreamTransformation() {
    return new Function<InputStream, InputStream>() {
      @Nullable @Override public InputStream apply(InputStream input) {
        try {
          return StreamUtils.convertStream(new GZIPInputStream(input));
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    };
  }

  @Override public List<String> extensionsToRemove() {
    return Lists.newArrayList(TAR_EXTENSION, GZIP_EXTENSION, GZ_EXTENSION, TGZ_EXTENSION);
  }
}
