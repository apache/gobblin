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

package gobblin.data.management.partition;

import gobblin.data.management.dataset.Dataset;

import java.util.Collection;


/**
 * A {@link Dataset} that allows partitioning of files in the dataset.
 */
public interface PartitionableDataset<T extends File> extends Dataset {

  public Collection<Partition<T>> partitionFiles(Collection<? extends T> files);

  public Class<?> fileClass();

}
