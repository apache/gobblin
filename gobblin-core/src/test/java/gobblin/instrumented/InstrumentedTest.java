/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.instrumented;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import junit.framework.Assert;

import gobblin.configuration.State;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.source.extractor.InstrumentedExtractor;


@Test(groups = {"gobblin.core"})
public class InstrumentedTest {

  @Test
  public void testInstrumented() {
    MetricContext parentContext = MetricContext.builder("parent.context").build();
    MetricContext.registerContext(parentContext);

    State state = new State();
    state.setProp(Instrumented.METRIC_CONTEXT_NAME_KEY, parentContext.getName());
    Instrumented instrumented = new Instrumented(state, InstrumentedExtractor.class);

    Assert.assertNotNull(instrumented.getContext());
    Assert.assertTrue(instrumented.getContext().getParent().isPresent());
    Assert.assertEquals(instrumented.getContext().getParent().get(), parentContext);

    List<Tag<?>> tags = instrumented.getContext().getTags();
    Map<String, String> expectedTags = new HashMap<String, String>();
    expectedTags.put("component", "extractor");
    expectedTags.put("class", InstrumentedExtractor.class.getCanonicalName());

    Assert.assertEquals(tags.size(), expectedTags.size());
    for(Tag<?> tag : tags) {
      Assert.assertTrue(expectedTags.containsKey(tag.getKey()));
      Assert.assertEquals(expectedTags.get(tag.getKey()), tag.getValue().toString());
    }
  }

}
