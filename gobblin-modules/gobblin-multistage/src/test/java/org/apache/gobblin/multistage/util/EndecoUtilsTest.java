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

import java.io.UnsupportedEncodingException;
import org.apache.gobblin.multistage.util.EndecoUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class EndecoUtilsTest {
  @Test
  public void testDecode() throws UnsupportedEncodingException {
    String encoded = "test%25encoded%25string";
    Assert.assertEquals(EndecoUtils.decode(encoded), "test%encoded%string");
    Assert.assertEquals(EndecoUtils.decode(encoded, "wrong-enc"), encoded);
  }

  @Test
  public void testGetEncodedUtf8() throws Exception {
    String plainUrl = "plain%url";
    Assert.assertEquals(EndecoUtils.getEncodedUtf8(plainUrl), "plain%25url");
    Assert.assertEquals(EndecoUtils.getEncodedUtf8(plainUrl, "wrong-enc"), plainUrl);
  }

  @Test
  public void testGetHadoopFsEncoded() throws Exception {
    String fileName = "file/path";
    Assert.assertEquals(EndecoUtils.getHadoopFsEncoded(fileName), "file%2Fpath");
    Assert.assertEquals(EndecoUtils.getHadoopFsEncoded(fileName, "wrong-enc"), fileName);
  }

  @Test
  public void testGetHadoopFsDecoded() throws Exception {
    String encodedFileName = "dir%2FfileName";
    Assert.assertEquals(EndecoUtils.getHadoopFsDecoded(encodedFileName), "dir/fileName");
    Assert.assertEquals(EndecoUtils.getHadoopFsDecoded(encodedFileName, "wrong-enc"), encodedFileName);
  }
}
