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

package org.apache.gobblin.data.management.copy;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Copy Manifest schema and serDe for manifest based copy
 * https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Manifest+based+distcp+runbook
 */
public class CopyManifest {
  private static final String MISSING_FN_MESSAGE = "fileName cannot be null";
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final Type CopyableUnitListType = new TypeToken<ArrayList<CopyableUnit>>(){}.getType();

  public final ArrayList<CopyableUnit> _copyableUnits;

  public CopyManifest() {
    _copyableUnits = new ArrayList<>();
  }

  public CopyManifest(ArrayList<CopyableUnit> copyableUnits) {
    _copyableUnits = copyableUnits;
  }

  /**
   * Add a new copyable unit to a copy manifest. Used for building a manifest
   * @param copyableUnit
   */
  public void add(CopyManifest.CopyableUnit copyableUnit) {
    _copyableUnits.add(copyableUnit);
  }

  /**
   * One item in a copy manifest
   * Only filename is required
   */
  public static class CopyableUnit {
    public final String fileName;
    public final String fileGroup;
    public final Long fileSizeInBytes;
    public final Long fileModificationTime;

    public CopyableUnit(String fileName, String fileGroup, Long fileSizeInBytes, Long fileModificationTime) {
      this.fileName = fileName;
      this.fileGroup = fileGroup;
      this.fileSizeInBytes = fileSizeInBytes;
      this.fileModificationTime = fileModificationTime;
      if (this.fileName == null) {
        throw new IllegalArgumentException(MISSING_FN_MESSAGE);
      }
    }
  }

  /**
   * Note: naive read does not do validation of schema. For schema validation use CopyableUnitIterator
   * @param fs filsystem object used for accessing the filesystem
   * @param path path manifest file location
   * @return a copy manifest object from the json representation at path
   * @throws IOException
   */
  public static CopyManifest read(FileSystem fs, Path path) throws IOException {
    JsonReader jsonReader = new JsonReader(new InputStreamReader(fs.open(path), "UTF-8"));
    return new CopyManifest(GSON.fromJson(jsonReader, CopyableUnitListType));
  }

  /**
   *
   * @param fs filsystem object used for accessing the filesystem
   * @param path path manifest file location
   * @throws IOException
   */
  public void write(FileSystem fs, Path path) throws IOException {
    FSDataOutputStream out = null;
    try {
      String outputJson = GSON.toJson(this._copyableUnits, CopyableUnitListType);
      out = fs.create(path, true);
      out.write(outputJson.getBytes(StandardCharsets.UTF_8));
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  public static CopyableUnitIterator getReadIterator(FileSystem fs, Path path) throws IOException {
    return new CopyableUnitIterator(fs, path);
  }

  /**
   * An iterator for CopyManifest for more efficient reading
   */
  public static class CopyableUnitIterator implements Iterator {
    JsonReader reader;

    public CopyableUnitIterator(FileSystem fs, Path path) throws IOException {
      reader = new JsonReader(new InputStreamReader(fs.open(path), "UTF-8"));
      reader.beginArray();
    }

    @Override
    public boolean hasNext() {
      try {
        return reader.hasNext();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return false;
    }

    @Override
    public CopyManifest.CopyableUnit next() {
      CopyManifest.CopyableUnit copyableUnit = GSON.fromJson(reader, CopyManifest.CopyableUnit.class);
      if (copyableUnit.fileName == null) {
        throw new IllegalArgumentException(MISSING_FN_MESSAGE);
      }
      return copyableUnit;
    }

    public void close() throws IOException {
      if (reader != null) {
        reader.endArray();
        reader.close();
      }
    }
  }
}
