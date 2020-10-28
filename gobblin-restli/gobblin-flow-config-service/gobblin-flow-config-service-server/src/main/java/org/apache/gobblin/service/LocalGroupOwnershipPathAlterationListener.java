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

package org.apache.gobblin.service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.util.filesystem.PathAlterationListenerAdaptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocalGroupOwnershipPathAlterationListener extends PathAlterationListenerAdaptor {
  private static final Logger LOG = LoggerFactory.getLogger(LocalGroupOwnershipPathAlterationListener.class);
  private JsonObject groupOwnerships;
  FileSystem fs;
  Path groupOwnershipFilePath;

  LocalGroupOwnershipPathAlterationListener(Path filePath) {
    this.groupOwnershipFilePath = filePath;
    try {
      this.fs = FileSystem.get(new Configuration());
      updateGroupOwnerships(filePath);
    } catch (IOException e) {
      throw new RuntimeException("Could not get local filesystem", e);
    }
  }

  public JsonObject getGroupOwnerships() {
    return groupOwnerships;
  }

  void updateGroupOwnerships(Path path) {
    // only update if the group ownership file is changed
    if (path.toUri().getPath().equals(this.groupOwnershipFilePath.toString())) {
      LOG.info("Detected change in group ownership file, updating groups");
      try (FSDataInputStream in = this.fs.open(path)) {
        String jsonString = IOUtils.toString(in, Charset.defaultCharset());
        JsonParser parser = new JsonParser();
        this.groupOwnerships = parser.parse(jsonString).getAsJsonObject();
      } catch (IOException e) {
        throw new RuntimeException("Could not open group ownership file at " + path.toString(), e);
      }
    }
  }

  @Override
  public void onFileCreate(Path path) {
    updateGroupOwnerships(path);
  }

  @Override
  public void onFileChange(Path path) {
    updateGroupOwnerships(path);
  }

  @Override
  public void onFileDelete(Path path) {
    // ignore if another file in same directory is deleted
    if (path.toUri().getPath().equals(this.groupOwnershipFilePath.toString())) {
      this.groupOwnerships = new JsonObject();
    }
  }
}
