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

package gobblin.source.extractor.hadoop.test;

import gobblin.data.management.dataset.Dataset;
import gobblin.data.management.retention.dataset.finder.DatasetFinder;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Lists;


public class TestCopyablePartitionableDatasedFinder implements DatasetFinder {

  public TestCopyablePartitionableDatasedFinder(FileSystem fs, Properties props) {
  }

  @Override public List<Dataset> findDatasets() throws IOException {
    return Lists.<Dataset>newArrayList(new TestCopyablePartitionableDataset());
  }

}
