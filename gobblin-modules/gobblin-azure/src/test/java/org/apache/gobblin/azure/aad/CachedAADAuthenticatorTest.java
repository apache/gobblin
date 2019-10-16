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

package org.apache.gobblin.azure.aad;

import com.microsoft.aad.adal4j.AuthenticationResult;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class CachedAADAuthenticatorTest {
  private final static String url1 = "url1";
  private final static String targetResource1 = "tr1";
  private final static String targetResource2 = "tr2";
  private final static String sp1 = "sp1";
  private final static String sp2 = "sp2";
  private final static String sp1Secret = "sp1-secret";
  private final static String sp2Secret = "sp2-secret";
  public final static String url1s1sp1token2AccessToken = "accessToken2";
  //This token expires in 9999 seconds
  public final static AuthenticationResult url1s1sp1token2 = new AuthenticationResult("tokenType", url1s1sp1token2AccessToken, "refreshToken",
      9999, "id1Token", null, false);
  private final static AuthenticationResult url1s2sp1token = new AuthenticationResult("tokenType", "accessToken", "refreshToken",
      9999, "id1Token-r2", null, false);
  private final static AuthenticationResult url1s1sp2token = new AuthenticationResult("tokenType", "accessToken", "refreshToken",
      9999, "id2Token", null, false);

  @Test
  public void testGetTokenInCache() throws Exception {
    AADTokenRequesterMock mock = new AADTokenRequesterMock();
    CachedAADAuthenticator authenticator = new CachedAADAuthenticator(mock, url1);

    Assert.assertEquals(authenticator.getToken(targetResource1, sp1, sp1Secret), mock.sp1FirstToken);
    Assert.assertEquals(authenticator.getToken(targetResource1, sp1, sp1Secret), mock.sp1FirstToken); //Load again
    Assert.assertEquals(mock.sp1s1LoadTimes, 1); //Only load once within in 1 second
    Assert.assertEquals(authenticator.getToken(targetResource2, sp2, sp2Secret), null);
    Thread.sleep(1000);
    //Previous token for SP1 has expired, will fetch the new token for SP1
    Assert.assertEquals(authenticator.getToken(targetResource1, sp1, sp1Secret), url1s1sp1token2);
    Assert.assertEquals(authenticator.getToken(targetResource2, sp1, sp1Secret), url1s2sp1token);
    Assert.assertEquals(authenticator.getToken(targetResource2, sp1, sp1Secret), url1s2sp1token); //Load 2nd time
    Assert.assertEquals(authenticator.getToken(targetResource1, sp1, sp1Secret), url1s1sp1token2); //Load 2nd time
    Assert.assertEquals(authenticator.getToken(targetResource1, sp2, sp2Secret), url1s1sp2token);
    Assert.assertEquals(authenticator.getToken(targetResource1, sp2, sp2Secret), url1s1sp2token); //Load 2nd time
    Assert.assertEquals(authenticator.getToken(targetResource1, sp1, sp1Secret), url1s1sp1token2); //Load 3rd time
    Assert.assertEquals(mock.sp1s1LoadTimes, 2); //Only load twice
    Assert.assertEquals(mock.sp1s2LoadTimes, 1); //Only load once
    Assert.assertEquals(mock.sp2s1LoadTimes, 1); //Only load once
  }

  static class AADTokenRequesterMock implements AADTokenRequester {
    public int sp1s1LoadTimes = 0;
    public int sp1s2LoadTimes = 0;
    public int sp2s1LoadTimes = 0;
    public AuthenticationResult sp1FirstToken;

    @Override
    public AuthenticationResult getToken(CachedAADAuthenticator.CacheKey key) {
      if (key.equals(new CachedAADAuthenticator.CacheKey(url1, targetResource1, sp1, sp1Secret))) {
        ++sp1s1LoadTimes;
        if (sp1s1LoadTimes == 1) {
          //This token expires in 1 second
          sp1FirstToken = new AuthenticationResult("tokenType", "accessToken", "refreshToken",
              1, "id1Token", null, false);
          return sp1FirstToken; //return url1s1sp1token1 for the first time.
        }
        //return url1s1sp1token2 later
        return url1s1sp1token2;
      } else if (key.equals(new CachedAADAuthenticator.CacheKey(url1, targetResource2, sp1, sp1Secret))) {
        ++sp1s2LoadTimes;
        return url1s2sp1token;
      } else if (key.equals(new CachedAADAuthenticator.CacheKey(url1, targetResource1, sp2, sp2Secret))) {
        ++sp2s1LoadTimes;
        return url1s1sp2token;
      }
      return null;
    }
  }
}
