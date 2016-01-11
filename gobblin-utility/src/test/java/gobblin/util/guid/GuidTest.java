/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.guid;

import java.io.IOException;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;


public class GuidTest {

  @Test
  public void testLength() {
    Assert.assertEquals(new Guid(new byte[0]).sha.length, Guid.GUID_LENGTH);
  }

  // Obviously not an extensive test, but better than nothing.
  @Test
  public void testUniqueReplicable() {
    Random random = new Random();

    byte[] b = new byte[10];
    random.nextBytes(b);

    Assert.assertEquals(new Guid(b), new Guid(b));

    byte[] other = new byte[10];
    for (int i = 0; i < 1000; i++) {
      random.nextBytes(other);
      Assert.assertNotEquals(new Guid(b), new Guid(other));
    }
  }

  @Test
  public void testSerDe() throws Exception {
    Random random = new Random();

    byte[] b = new byte[10];
    random.nextBytes(b);

    Guid guid = new Guid(b);

    Assert.assertEquals(guid.toString().length(), 2 * Guid.GUID_LENGTH);

    Assert.assertEquals(guid, Guid.deserialize(guid.toString()));
  }

  @Test
  public void testFromHasGuid() throws IOException {
    Random random = new Random();

    byte[] b = new byte[10];
    random.nextBytes(b);

    Guid guid = new Guid(b);

    HasGuid hasGuid = new Guid.SimpleHasGuid(guid);

    Assert.assertEquals(hasGuid.guid(), guid);


  }

}
