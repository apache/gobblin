/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter;

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link EmptyIterable}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.converter"})
public class EmptyIterableTest {

  @Test
  public void testIterator() {
    EmptyIterable<Object> emptyIterable = new EmptyIterable<Object>();
    Iterator<Object> iterator = emptyIterable.iterator();
    Assert.assertFalse(iterator.hasNext());
  }
}
