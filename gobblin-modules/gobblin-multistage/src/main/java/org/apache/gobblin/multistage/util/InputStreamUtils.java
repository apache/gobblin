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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;


public interface InputStreamUtils {
  /**
   * Convert a list of strings to an InputStream
   * @param stringList a list of strings
   * @return an InputStream made of the list
   */
  static InputStream convertListToInputStream(List<String> stringList) {
    return CollectionUtils.isEmpty(stringList) ? null
        : new ByteArrayInputStream(String.join("\n", stringList).getBytes());
  }

  /**
   * Extract the text from input stream using UTF-8 encoding
   * @param input the InputStream, which most likely is from an HttpResponse
   * @return the String extracted from InputStream, if the InputStream cannot be converted to a String
   * then an exception is thrown
   */
  static String extractText(InputStream input) throws IOException {
    return extractText(input, StandardCharsets.UTF_8.name());
  }

  /**
   * Extract the text from input stream using given character set encoding
   * @param input the InputStream, which most likely is from an HttpResponse
   * @param charSetName the character set name
   * @return the String extracted from InputStream, if the InputStream cannot be converted to a String
   * then an exception is thrown
   */
  static String extractText(InputStream input, String charSetName) throws IOException {
    return IOUtils.toString(input, charSetName);
  }
}
