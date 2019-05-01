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

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;

import lombok.Data;

import org.apache.gobblin.annotation.Alias;


/**
 * An implementation of {@link DataFileVersionStrategy} that uses modtime as the file version.
 *
 * This is the default implementation and does data comparisons purely based on modification time.
 */
@Data
public class ModTimeDataFileVersionStrategy implements DataFileVersionStrategy<Long> {

  @Alias(value = "modtime")
	public static class Factory implements DataFileVersionStrategy.DataFileVersionFactory<Long> {
		@Override
		public DataFileVersionStrategy<Long> createDataFileVersionStrategy(FileSystem fs, Config config) {
			return new ModTimeDataFileVersionStrategy(fs);
		}
	}

  private final FileSystem fs;

  @Override
  public Long getVersion(Path path) throws IOException {
    return this.fs.getFileStatus(path).getModificationTime();
  }

  @Override
  public boolean setVersion(Path path, Long version) {
    return false;
  }

  @Override
  public boolean setDefaultVersion(Path path) {
    return false;
  }

  @Override
  public Set<Characteristic> applicableCharacteristics() {
    return ImmutableSet.of(Characteristic.COMPATIBLE_WITH_MODTIME);
  }
}
