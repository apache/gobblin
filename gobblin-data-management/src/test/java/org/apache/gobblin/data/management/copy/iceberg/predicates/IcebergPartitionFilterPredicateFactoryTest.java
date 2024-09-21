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

package org.apache.gobblin.data.management.copy.iceberg.predicates;

import java.util.Properties;
import java.util.function.Predicate;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/** Tests for {@link org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergPartitionFilterPredicateFactory} */
public class IcebergPartitionFilterPredicateFactoryTest {
  private MockedStatic<IcebergPartitionFilterPredicateUtil> icebergPartitionFilterPredicateUtilMockedStatic;

  @BeforeMethod
  public void setup() {
    icebergPartitionFilterPredicateUtilMockedStatic = Mockito.mockStatic(IcebergPartitionFilterPredicateUtil.class);
    icebergPartitionFilterPredicateUtilMockedStatic.when(
        () -> IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex(Mockito.anyString(), Mockito.any(TableMetadata.class), Mockito.anyList()))
        .thenReturn(0);
  }

  @AfterMethod
  public void cleanup() {
    icebergPartitionFilterPredicateUtilMockedStatic.close();
  }

  @Test
  public void testGetFilterPredicateWithoutPartitionType() {
    String partitionColumnName = "random";
    TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
    Properties properties = new Properties();
    properties.setProperty("iceberg.dataset.source.partition.values", "dummy");

    Predicate<StructLike> predicate = IcebergPartitionFilterPredicateFactory.getFilterPredicate(partitionColumnName, tableMetadata, properties);

    Assert.assertTrue(predicate instanceof IcebergPartitionFilterPredicate);
  }

  @Test
  public void testGetFilterPredicateWithDateTimePartitionType() {
    String partitionColumnName = "datetime";
    TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
    Properties properties = new Properties();
    properties.setProperty("iceberg.dataset.source.partition.type", "datetime");
    properties.setProperty("iceberg.dataset.source.partition.datetime.pattern", "yyyy-MM-dd");
    properties.setProperty("iceberg.dataset.source.partition.datetime.startdate", "2024-09-20");
    properties.setProperty("iceberg.dataset.source.partition.datetime.enddate", "2024-09-24");

    Predicate<StructLike> predicate = IcebergPartitionFilterPredicateFactory.getFilterPredicate(partitionColumnName, tableMetadata, properties);

    Assert.assertTrue(predicate instanceof IcebergDateTimePartitionFilterPredicate);
  }

  @Test
  public void testGetFilterPredicateWithInvalidPartitionType() {
    String partitionColumnName = "random";
    TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
    Properties properties = new Properties();
    properties.setProperty("iceberg.dataset.source.partition.type", "invalid");
    properties.setProperty("iceberg.dataset.source.partition.values", "dummy");

    Predicate<StructLike> predicate = IcebergPartitionFilterPredicateFactory.getFilterPredicate(partitionColumnName, tableMetadata, properties);

    Assert.assertTrue(predicate instanceof IcebergPartitionFilterPredicate);
  }


}
