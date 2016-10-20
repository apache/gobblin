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

package gobblin.runtime.embedded;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;

import gobblin.annotation.Alias;
import gobblin.data.management.copy.RecursiveCopyableDataset;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.template.ResourceBasedJobTemplate;


/**
 * Embedded version of distcp.
 * Usage:
 * new EmbeddedGobblinDistcp(new Path("/source"), new Path("/dest")).run();
 */
@Alias(value = "distcp")
public class EmbeddedGobblinDistcp extends EmbeddedGobblin {

  public EmbeddedGobblinDistcp(Path from, Path to) throws JobTemplate.TemplateException, IOException {
    super("Distcp");
    try {
      setTemplate(ResourceBasedJobTemplate.forResourcePath("templates/distcp.template"));
    } catch (URISyntaxException | SpecNotFoundException exc) {
      throw new RuntimeException("Could not instantiate an " + EmbeddedGobblinDistcp.class.getName(), exc);
    }
    this.setConfiguration("from", from.toString());
    this.setConfiguration("to", to.toString());
  }

  /**
   * Specifies that files in the target should be updated if they have changed in the source. Equivalent to -update
   * option in Hadoop distcp.
   */
  public EmbeddedGobblinDistcp update() {
    this.setConfiguration(RecursiveCopyableDataset.UPDATE_KEY, Boolean.toString(true));
    return this;
  }

  /**
   * Specifies that files in the target that don't exist in the source should be deleted. Equivalent to -delete
   * option in Hadoop distcp.
   */
  public EmbeddedGobblinDistcp delete() {
    this.setConfiguration(RecursiveCopyableDataset.DELETE_KEY, Boolean.toString(true));
    return this;
  }

  /**
   * If {@link #delete()} is used, specifies that newly empty parent directories should also be deleted.
   */
  public EmbeddedGobblinDistcp deleteEmptyParentDirectories() {
    this.setConfiguration(RecursiveCopyableDataset.DELETE_EMPTY_DIRECTORIES_KEY, Boolean.toString(true));
    return this;
  }

}
