/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.data.management.dataset;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import lombok.AllArgsConstructor;
import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.util.function.CheckedExceptionPredicate;
import org.apache.hadoop.fs.FileSystem;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DatasetsFinderFilteringDecoratorTest {
  @Test
  public void testFindDatasets_emptyAllowed() throws IOException {
    DatasetsFinder<Dataset> mockFinder = Mockito.mock(DatasetsFinder.class);
    Dataset mockDataset = Mockito.mock(Dataset.class);
    Mockito.when(mockFinder.findDatasets()).thenReturn(Arrays.asList(mockDataset));

    DatasetsFinderFilteringDecorator<Dataset> d = new DatasetsFinderFilteringDecorator(
        mockFinder,Collections.emptyList(), Collections.emptyList());
    Assert.assertEquals(d.findDatasets(), Arrays.asList(mockDataset));
  }

  @Test
  public void testFindDatasets_allowed() throws IOException {
    DatasetsFinder<Dataset> mockFinder = Mockito.mock(DatasetsFinder.class);
    Dataset mockDataset = Mockito.mock(Dataset.class);
    Mockito.when(mockFinder.findDatasets()).thenReturn(Arrays.asList(mockDataset));

    DatasetsFinderFilteringDecorator<Dataset> d = new DatasetsFinderFilteringDecorator(
        mockFinder,
        Arrays.asList(new StubTrue(), new StubTrue()),
        Arrays.asList(new StubFalse(), new StubFalse()));
    Assert.assertEquals(d.findDatasets(), Arrays.asList(mockDataset));
  }

  @Test
  public void testFindDatasets_denied() throws IOException {
    DatasetsFinder<Dataset> mockFinder = Mockito.mock(DatasetsFinder.class);
    Dataset mockDataset = Mockito.mock(Dataset.class);
    Mockito.when(mockFinder.findDatasets()).thenReturn(Arrays.asList(mockDataset));

    DatasetsFinderFilteringDecorator<Dataset> d = new DatasetsFinderFilteringDecorator(mockFinder,
        Arrays.asList(new StubTrue(), new StubFalse()),
        Arrays.asList(new StubFalse()));
    Assert.assertEquals(d.findDatasets(), Collections.emptyList());

    d = new DatasetsFinderFilteringDecorator(mockFinder,
        Arrays.asList(new StubTrue()),
        Arrays.asList(new StubFalse(), new StubTrue()));
    Assert.assertEquals(d.findDatasets(), Collections.emptyList());
  }

  @Test
  public void testFindDatasets_throwsException() throws IOException {
    DatasetsFinder<Dataset> mockFinder = Mockito.mock(DatasetsFinder.class);
    Dataset mockDataset = Mockito.mock(Dataset.class);
    Mockito.when(mockFinder.findDatasets()).thenReturn(Arrays.asList(mockDataset));

    DatasetsFinderFilteringDecorator<Dataset> datasetFinder_1 = new DatasetsFinderFilteringDecorator(mockFinder,
        Arrays.asList(new StubTrue(), new ThrowsException()),
        Arrays.asList(new StubFalse()));
    Assert.assertThrows(IOException.class, datasetFinder_1::findDatasets);

    DatasetsFinderFilteringDecorator<Dataset> datasetFinder_2 = new DatasetsFinderFilteringDecorator(mockFinder,
        Arrays.asList(new StubTrue()),
        Arrays.asList(new StubFalse(), new ThrowsException()));
    Assert.assertThrows(IOException.class, datasetFinder_2::findDatasets);
  }

  @Test
  public void testInstantiationOfPredicatesAndDatasetFinder() throws IOException {
    DatasetsFinder<Dataset> mockFinder = Mockito.mock(DatasetsFinder.class);

    Properties props = new Properties();
    props.setProperty(DatasetsFinderFilteringDecorator.DATASET_CLASS, mockFinder.getClass().getName());
    props.setProperty(DatasetsFinderFilteringDecorator.ALLOWED, StubTrue.class.getName());
    props.setProperty(DatasetsFinderFilteringDecorator.DENIED, StubFalse.class.getName());
    DatasetsFinderFilteringDecorator<Dataset>
        testFilterDataFinder = new TestDatasetsFinderFilteringDecorator(Mockito.mock(FileSystem.class), props);

    Assert.assertEquals(testFilterDataFinder.datasetFinder.getClass(), mockFinder.getClass());

    Assert.assertEquals(testFilterDataFinder.allowDatasetPredicates.size(), 1);
    CheckedExceptionPredicate<Dataset, IOException> allowListPredicate = testFilterDataFinder.allowDatasetPredicates.get(0);
    Assert.assertEquals(allowListPredicate.getClass(), StubTrue.class);
    Assert.assertEquals(((StubTrue) allowListPredicate).props, props);

    Assert.assertEquals(testFilterDataFinder.denyDatasetPredicates.size(), 1);
    CheckedExceptionPredicate<Dataset, IOException> denyListPredicate = testFilterDataFinder.denyDatasetPredicates.get(0);
    Assert.assertEquals(denyListPredicate.getClass(), StubFalse.class);
    Assert.assertEquals(((StubFalse) denyListPredicate).props, props);
  }

  static class TestDatasetsFinderFilteringDecorator extends DatasetsFinderFilteringDecorator<Dataset> {
    public TestDatasetsFinderFilteringDecorator(FileSystem fs, Properties properties) throws IOException {
      super(fs, properties);
    }
  }

  @AllArgsConstructor
  static class StubTrue implements CheckedExceptionPredicate<Dataset, IOException> {
    Properties props;

    StubTrue() {
      this.props = null;
    }

    @Override
    public boolean test(Dataset arg) throws IOException {
      return true;
    }
  }

  @AllArgsConstructor
  static class StubFalse implements CheckedExceptionPredicate<Dataset, IOException> {
    Properties props;

    StubFalse() {
      this.props = null;
    }

    @Override
    public boolean test(Dataset arg) throws IOException {
      return false;
    }
  }

  static class ThrowsException implements CheckedExceptionPredicate<Dataset, IOException> {
    @Override
    public boolean test(Dataset arg) throws IOException {
      throw new IOException("Throwing a test exception");
    }
  }
}
