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

package gobblin.data.management;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.LongWatermark;


public class ConversionHiveTestUtils {
  public static Schema readSchemaFromJsonFile(String directory, String filename)
      throws IOException {

    return new Schema.Parser()
        .parse(ConversionHiveTestUtils.class.getClassLoader()
            .getResourceAsStream(StringUtils.removeEnd(directory, Path.SEPARATOR) + Path.SEPARATOR + filename));
  }

  public static String readQueryFromFile(String directory, String filename)
      throws IOException {
    InputStream is = ConversionHiveTestUtils.class.getClassLoader()
        .getResourceAsStream(StringUtils.removeEnd(directory, Path.SEPARATOR) + Path.SEPARATOR + filename);

    return IOUtils.toString(is, "UTF-8");
  }

  public static WorkUnitState createWus(String dbName, String tableName, long watermark) {
    WorkUnitState wus = new WorkUnitState();
    wus.setActualHighWatermark(new LongWatermark(watermark));
    wus.setProp(ConfigurationKeys.DATASET_URN_KEY, dbName + "@" + tableName);
    wus.setProp(ConfigurationKeys.JOB_ID_KEY, "jobId");
    return wus;
  }
}
