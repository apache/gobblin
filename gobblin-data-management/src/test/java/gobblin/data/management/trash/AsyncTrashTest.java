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

    Properties properties = TestTrash.propertiesForTestTrash();
    TestTrash.simulateDelay(properties, 2);

    FileSystem fs = mock(FileSystem.class);

    AsyncTrash trash = new AsyncTrash(fs, properties);

    for(int i = 0; i < 5; i++) {
      trash.moveToTrash(new Path("file" + i));
      Thread.sleep(10);
    }

    for(int i = 0; i < 5; i++) {
      trash.moveToTrashAsUser(new Path("file" + i), "user" + i);
      Thread.sleep(10);
    }

    int maxWaits = 5;
    while(maxWaits > 0 && ((TestTrash) trash.getDecoratedObject()).getOperationsWaiting() < 10) {
      Thread.sleep(100);
      maxWaits--;
      //wait
    }

    Assert.assertTrue(((TestTrash) trash.getDecoratedObject()).getDeleteOperations().isEmpty());
    ((TestTrash) trash.getDecoratedObject()).tick();
    Assert.assertTrue(((TestTrash) trash.getDecoratedObject()).getDeleteOperations().isEmpty());

    ((TestTrash) trash.getDecoratedObject()).tick();
    // There is a race condition in the ScalingThreadPoolExecutor that can cause fewer threads than calls even before
    // reaching the max number of threads. This is somewhat rare, and if # calls > max threads, the effect is
    // essentially gone, so there is no issue for the application. However, the test would be sensitive to this issue,
    // so we check that at least 8 delete operations were scheduled, giving a tolerance of 2 unscheduled threads.
    // (These threads would be resolved after a few more ticks).
    Assert.assertTrue(((TestTrash) trash.getDecoratedObject()).getDeleteOperations().size() > 8);

    ((TestTrash) trash.getDecoratedObject()).tick();
    ((TestTrash) trash.getDecoratedObject()).tick();
    ((TestTrash) trash.getDecoratedObject()).tick();
    ((TestTrash) trash.getDecoratedObject()).tick();

    // Now we should see all 10 delete operations.
    Assert.assertEquals(((TestTrash) trash.getDecoratedObject()).getDeleteOperations().size(), 10);

  }
}
