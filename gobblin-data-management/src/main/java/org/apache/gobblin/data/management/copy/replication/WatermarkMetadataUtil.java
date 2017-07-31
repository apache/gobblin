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

package org.apache.gobblin.data.management.copy.replication;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.gson.JsonParser;

import org.apache.gobblin.source.extractor.Watermark;
import org.apache.gobblin.source.extractor.WatermarkSerializerHelper;

/**
 * Utility class to serialize and deserialize the {@link Watermark}
 * @author mitu
 *
 */
public class WatermarkMetadataUtil {

  public static final String DELIMITER = "\n";

  public static String serialize(Watermark watermark) {
    return watermark.getClass().getCanonicalName() + DELIMITER + watermark.toJson().toString();
  }

  public static Watermark deserialize(String content) throws WatermarkMetadataMulFormatException {
    List<String> tmp = Splitter.on(DELIMITER).trimResults().omitEmptyStrings().splitToList(content);
    if (tmp.size() < 2) {
      throw new WatermarkMetadataMulFormatException("wrong format " + content);
    }

    String classname = tmp.get(0);
    String jsonStr = tmp.get(1);

    try {
      Class<? extends Watermark> watermarkClass = (Class<? extends Watermark>) Class.forName(classname);

      return WatermarkSerializerHelper.convertJsonToWatermark(new JsonParser().parse(jsonStr), watermarkClass);
    } catch (ClassNotFoundException e) {
      throw new WatermarkMetadataMulFormatException("wrong format " + e.getMessage());
    }
  }

  static class WatermarkMetadataMulFormatException extends Exception {
    private static final long serialVersionUID = 6748785718027224698L;

    public WatermarkMetadataMulFormatException(String s) {
      super(s);
    }

  }
}
