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
package org.apache.gobblin.yarn;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.testng.annotations.Test;


public class YarnHelixUtilsTest {
  /**
   * Uses the token file created using {@link GobblinYarnTestUtils#createTokenFileForService(Path, String)} method and
   * added to the resources folder.
   * @throws IOException
   */
  @Test
  public void testUpdateToken()
      throws IOException {
    //Ensure the credentials is empty on start
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    Assert.assertNull(credentials.getToken(new Text("testService")));

    //Attempt reading a non-existent token file and ensure credentials object has no tokens
    YarnHelixUtils.updateToken(".token1");
    credentials = UserGroupInformation.getCurrentUser().getCredentials();
    Assert.assertNull(credentials.getToken(new Text("testService")));

    //Read a valid token file and ensure the credentials object has a valid token
    YarnHelixUtils.updateToken(GobblinYarnConfigurationKeys.TOKEN_FILE_NAME);
    credentials = UserGroupInformation.getCurrentUser().getCredentials();
    Token<?> readToken = credentials.getToken(new Text("testService"));
    Assert.assertNotNull(readToken);
  }
}