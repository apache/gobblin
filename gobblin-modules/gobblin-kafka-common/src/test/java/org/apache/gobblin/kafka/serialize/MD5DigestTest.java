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

package org.apache.gobblin.kafka.serialize;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.digest.DigestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;



@Slf4j
public class MD5DigestTest {


  @Test
  public void testInvalidString() {
    String foobar = "clearly-bad-md5string";
    try {
      MD5Digest md5Digest = MD5Digest.fromString(foobar);
      Assert.fail("Should have thrown an exception");
    }
    catch (Exception e)
    {
      log.info("Found expected exception", e.getMessage());
    }

  }

  @Test
  public void testValidString()
      throws NoSuchAlgorithmException, UnsupportedEncodingException {
    String message = "3432rdaesdfdsf2443223 234 324324 23423 e23e 23d";
    byte[] md5digest = MessageDigest.getInstance("MD5").digest(message.getBytes("UTF-8"));
    String md5String = DigestUtils.md5Hex(message);

    Assert.assertNotNull(md5digest);
    MD5Digest md5 = MD5Digest.fromBytes(md5digest);
    Assert.assertEquals(md5.asString(), md5String);
    Assert.assertEquals(md5.asBytes(), md5digest);

    MD5Digest otherMd5 = MD5Digest.fromString(md5String);
    Assert.assertEquals(otherMd5.asBytes(), md5.asBytes());
  }

}
