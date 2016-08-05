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

package gobblin.runtime.util;

import java.io.IOException;

import com.typesafe.config.Config;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * The testing routine is create a jobSpec( Simulated as the result of external JobSpecMonitor)
 * persist it, reload it from file system, and compare with the original JobSpec.
 *
 */

@Test(groups = {"gobblin.runtime"})
public class FSJobCatalogHelperTest {
  // Testing the materialized and de-materialized process here.
  private Config testConfig ;

  @BeforeClass
  public void setUp() throws IOException{

  }

  @Test
  public void testPersistingJobSpec() {

  }

  @Test
  public void testMarkMergedOption() {

  }

  @AfterClass
  public void tearDown() {

  }
}
