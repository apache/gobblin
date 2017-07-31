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
package org.apache.gobblin.compression;

import java.util.Map;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.codec.GzipCodec;
import org.apache.gobblin.codec.StreamCodec;


/**
 * This class has logic to create compression stream codecs based on configuration parameters.
 * Note: Interface will likely change to support dynamic registration of compression codecs
 */
@Alpha
public class CompressionFactory {
  public static StreamCodec buildStreamCompressor(Map<String, Object> properties) {
    String type = CompressionConfigParser.getCompressionType(properties);
    switch (type) {
      case GzipCodec.TAG:
        return new GzipCodec();
      default:
        throw new IllegalArgumentException("Can't build compressor of type " + type);
    }
  }

  private CompressionFactory() {
    // can't instantiate
  }
}
