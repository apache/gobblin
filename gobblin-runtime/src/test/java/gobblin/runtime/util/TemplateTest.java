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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Testing the functions for reading template merging template with user-specified attributes.
 * 1. Reading the template configuration, testing size or something
 * 2. Testing the required attributes result.
 */
@Test(groups = {"gobblin.runtime"})
public class TemplateTest {

  // For template inside resource folder.
  private File sampleTemplate;
  private URL sampleTemplateName;

  @BeforeClass
  public void setUp()
      throws IOException,URISyntaxException {
    sampleTemplateName = this.getClass().getClassLoader().getResource("/templates/wikiSample.template");
    this.sampleTemplate = new File(sampleTemplateName.toURI());
  }
  // // TODO: 7/25/16  Finish this Test
}
