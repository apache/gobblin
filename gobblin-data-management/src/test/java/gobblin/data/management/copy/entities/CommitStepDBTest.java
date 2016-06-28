/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.entities;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.commit.CommitStep;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;


public class CommitStepDBTest {

  @Test
  public void testBasic() throws Exception {

    String key = "testBasic";

    CommitStep commitStep = new TestStep("myData");
    CommitStepDB.put(key, commitStep);
    CommitStep recoveredStep = CommitStepDB.get(key);
    Assert.assertEquals(commitStep, recoveredStep);

  }

  @Test
  public void testReallyLongStep() throws Exception {

    String key = "testReallyLongStep";

    String reallyLongString = "long";
    for (int i = 0; i < 10; i++) {
      reallyLongString = reallyLongString + reallyLongString;
    }

    CommitStep commitStep = new TestStep(reallyLongString);

    CommitStepDB.put(key, commitStep);
    CommitStep recoveredStep = CommitStepDB.get(key);
    Assert.assertEquals(commitStep, recoveredStep);

  }

  @Test
  public void testRepeatedKeys() throws Exception {

    String key = "testRepeatedKeys";

    CommitStep commitStepa = new TestStep("aaa");
    CommitStepDB.put(key, commitStepa);

    // putting the same step will not throw exception
    CommitStepDB.put(key, commitStepa);

    // trying to store a different step under the same key will throw exception
    try {
      CommitStep commitStepb = new TestStep("bbb");
      CommitStepDB.put(key, commitStepb);
      Assert.fail();
    } catch (IllegalStateException exc) {
      // expected
    }

  }

  @AllArgsConstructor
  @EqualsAndHashCode
  public static class TestStep implements CommitStep {

    private final String data;

    @Override
    public boolean isCompleted()
        throws IOException {
      return false;
    }

    @Override
    public void execute()
        throws IOException {

    }
  }

}
