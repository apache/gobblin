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

package org.apache.gobblin.temporal.dynamic;

import java.util.Optional;
import java.util.function.Function;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.testng.annotations.Test;
import org.testng.Assert;


public class ProfileDerivationTest {

  @Test
  public void testFormulateConfigSuccess() throws ProfileDerivation.UnknownBasisException {
    String basisProfileName = "testProfile";
    ProfileOverlay overlay = new ProfileOverlay.Adding(Lists.newArrayList(new ProfileOverlay.KVPair("key1", "value1B")));
    ProfileDerivation profileDerivation = new ProfileDerivation(basisProfileName, overlay);

    Function<String, Optional<WorkerProfile>> basisResolver = name -> {
      if (basisProfileName.equals(name)) {
        Config config = ConfigFactory.parseString("key1=value1A, key2=value2");
        WorkerProfile profile = new WorkerProfile(basisProfileName, config);
        return Optional.of(profile);
      }
      return Optional.empty();
    };

    Config resultConfig = profileDerivation.formulateConfig(basisResolver);
    Assert.assertEquals(resultConfig.getString("key1"), "value1B");
    Assert.assertEquals(resultConfig.getString("key2"), "value2");
  }

  public void testFormulateConfigUnknownBasis() {
    String basisProfileName = "foo";
    try {
      ProfileDerivation derivation = new ProfileDerivation(basisProfileName, null);
      derivation.formulateConfig(ignore -> Optional.empty());
      Assert.fail("Expected UnknownBasisException");
    } catch (ProfileDerivation.UnknownBasisException ube) {
      Assert.assertEquals(ube.getName(), basisProfileName);
    }
  }

  @Test
  public void testRenderNameNonBaseline() {
    String name = "testProfile";
    ProfileDerivation profileDerivation = new ProfileDerivation(name, null);
    String renderedName = profileDerivation.renderName();
    Assert.assertEquals(renderedName, name);
  }

  @Test
  public void testRenderNameBaseline() {
    ProfileDerivation profileDerivation = new ProfileDerivation(WorkforceProfiles.BASELINE_NAME, null);
    String renderedName = profileDerivation.renderName();
    Assert.assertEquals(renderedName, WorkforceProfiles.BASELINE_NAME_RENDERING);
  }
}