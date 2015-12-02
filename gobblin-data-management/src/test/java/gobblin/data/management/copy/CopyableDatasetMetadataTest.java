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
package gobblin.data.management.copy;

import gobblin.configuration.WorkUnitState;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class CopyableDatasetMetadataTest {

  @Test
  public void testSerializeDeserialize() throws Exception {
    CopyableDataset copyableDataset = new TestCopyableDataset();
    Path target = new Path("/target");
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(copyableDataset, target);
    String serialized = metadata.serialize();

    CopyableDatasetMetadata deserialized = CopyableDatasetMetadata.deserialize(serialized);

    Assert.assertEquals(copyableDataset.datasetRoot(), deserialized.getDatasetRoot());
    Assert.assertEquals(target, deserialized.getDatasetTargetRoot());
  }

  @Test
  public void testHashCode() throws Exception {

    CopyableDataset copyableDataset = new TestCopyableDataset();
    Path target = new Path("/target");
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(copyableDataset, target);
    String serialized = metadata.serialize();

    CopyableDatasetMetadata deserialized = CopyableDatasetMetadata.deserialize(serialized);
    CopyableDatasetMetadata deserialized2 = CopyableDatasetMetadata.deserialize(serialized);

    Multimap<CopyableDatasetMetadata, WorkUnitState> datasetRoots = ArrayListMultimap.create();

    datasetRoots.put(deserialized, new WorkUnitState());
    datasetRoots.put(deserialized2, new WorkUnitState());

    Assert.assertEquals(datasetRoots.keySet().size(), 1);

  }

}
