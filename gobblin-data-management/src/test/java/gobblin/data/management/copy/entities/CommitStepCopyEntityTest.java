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

import com.google.common.collect.Maps;

import gobblin.commit.CommitStep;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;


public class CommitStepCopyEntityTest {

  @Test
  public void test() throws Exception {
    TestStep step = new TestStep("myStep");
    CommitStepCopyEntity ce =
        new CommitStepCopyEntity("fileset", Maps.<String, Object>newHashMap(), step, 1, "step");

    Assert.assertEquals(step, ce.getStep());
  }

  @AllArgsConstructor
  @EqualsAndHashCode
  private class TestStep implements CommitStep {
    private final String name;

    @Override
    public boolean isCompleted()
        throws IOException {
      return true;
    }

    @Override
    public void execute()
        throws IOException {

    }
  }

}
