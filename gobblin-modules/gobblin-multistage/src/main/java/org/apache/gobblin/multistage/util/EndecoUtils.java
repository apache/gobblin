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

package org.apache.gobblin.multistage.util;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;


/**
 * a collection of encoding and decoding functions
 */
@Slf4j
public class EndecoUtils {
  private EndecoUtils() {
    // hide constructor
  }

  /**
   * Decode an encoded URL string, complete or partial
   * @param encoded encoded URL string
   * @return decoded URL string
   */
  static public String decode(String encoded) {
    return decode(encoded, StandardCharsets.UTF_8.toString());
  }

  static public String decode(String encoded, String enc) {
    try {
      return URLDecoder.decode(encoded, enc);
    } catch (Exception e) {
      log.error("URL decoding error: " + e);
      return encoded;
    }
  }

  /**
   * Encode a URL string, complete or partial
   * @param plainUrl unencoded URL string
   * @return encoded URL string
   */
  static public String getEncodedUtf8(String plainUrl) {
    return getEncodedUtf8(plainUrl, StandardCharsets.UTF_8.toString());
  }

  static public String getEncodedUtf8(String plainUrl, String enc) {
    try {
      return URLEncoder.encode(plainUrl, enc);
    } catch (Exception e) {
      log.error("URL encoding error: " + e);
      return plainUrl;
    }
  }

  /**
   * Encode a Hadoop file name to encode path separator so that the file name has no '/'
   * @param fileName unencoded file name string
   * @return encoded path string
   */
  static public String getHadoopFsEncoded(String fileName) {
    return getHadoopFsEncoded(fileName, StandardCharsets.UTF_8.toString());
  }

  static public String getHadoopFsEncoded(String fileName, String enc) {
    try {
      String encodedSeparator = URLEncoder.encode(Path.SEPARATOR, enc);
      // we don't encode the whole string intentionally so that the state file name is more readable
      return fileName.replace(Path.SEPARATOR, encodedSeparator);
    } catch (Exception e) {
      log.error("Hadoop FS encoding error: " + e);
      return fileName;
    }
  }

  /**
   * Decode an encoded Hadoop file name to restore path separator
   * @param encodedFileName encoded file name string
   * @return encoded path string
   */
  static public String getHadoopFsDecoded(String encodedFileName) {
    return getHadoopFsDecoded(encodedFileName, StandardCharsets.UTF_8.toString());
  }

  static public String getHadoopFsDecoded(String encodedFileName, String enc) {
    try {
      String encodedSeparator = URLEncoder.encode(Path.SEPARATOR, enc);
      return encodedFileName.replace(encodedSeparator, Path.SEPARATOR);
    } catch (Exception e) {
      log.error("Hadoop FS decoding error: " + e);
      return encodedFileName;
    }
  }
}
