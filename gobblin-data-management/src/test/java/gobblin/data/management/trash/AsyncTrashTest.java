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

package gobblin.data.management.trash;

import junit.framework.Assert;

import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class AsyncTrashTest {
  @Test public void testAsyncTrash() throws Exception {

    long delay = 500;

    Properties properties = TestTrash.propertiesForTestTrash();
    TestTrash.simulateDelay(properties, delay);

    FileSystem fs = mock(FileSystem.class);

    AsyncTrash trash = new AsyncTrash(fs, properties);

    long startTime = System.currentTimeMillis();

    for(int i = 0; i < 5; i++) {
      trash.moveToTrash(new Path("file" + i));
    }

    for(int i = 0; i < 5; i++) {
      trash.moveToTrashAsUser(new Path("file" + i), "user" + i);
    }

    Assert.assertTrue(System.currentTimeMillis() - startTime < delay);
    Assert.assertTrue(((TestTrash) trash.getDecoratedObject()).getDeleteOperations().isEmpty());

    Thread.sleep(2 * delay);

    Assert.assertEquals(((TestTrash) trash.getDecoratedObject()).getDeleteOperations().size(), 10);

  }
}
