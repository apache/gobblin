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

package org.apache.gobblin.ingestion.google.webmaster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.*;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class GoogleWebmasterDataFetcherImplTest {

  private String _property = "https://www.myproperty.com/";

  @Test
  public void testGetAllPagesWhenRequestLessThan5000() throws Exception {
    GoogleWebmasterClient client = Mockito.mock(GoogleWebmasterClient.class);
    List<String> retVal = Arrays.asList("abc", "def");

    Mockito.when(client.getPages(eq(_property), any(String.class), any(String.class), eq("ALL"), any(Integer.class),
        any(List.class), any(List.class), eq(0))).thenReturn(retVal);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(GoogleWebMasterSource.KEY_PROPERTY, _property);

    GoogleWebmasterDataFetcher dataFetcher = new GoogleWebmasterDataFetcherImpl(_property, client, workUnitState);
    Collection<ProducerJob> allPages = dataFetcher.getAllPages(null, null, "ALL", 2);

    List<String> pageStrings = new ArrayList<>();
    for (ProducerJob page : allPages) {
      pageStrings.add(page.getPage());
    }

    Assert.assertTrue(CollectionUtils.isEqualCollection(retVal, pageStrings));
    Mockito.verify(client, Mockito.times(1))
        .getPages(eq(_property), any(String.class), any(String.class), eq("ALL"), any(Integer.class), any(List.class),
            any(List.class), eq(0));
  }

  @Test
  public void testGetAllPagesWhenDataSizeLessThan5000AndRequestAll() throws Exception {
    GoogleWebmasterClient client = Mockito.mock(GoogleWebmasterClient.class);
    List<String> allPages = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      allPages.add(Integer.toString(i));
    }
    Mockito.when(client.getPages(eq(_property), any(String.class), any(String.class), eq("ALL"), any(Integer.class),
        any(List.class), any(List.class), eq(0))).thenReturn(allPages);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(GoogleWebMasterSource.KEY_PROPERTY, _property);

    GoogleWebmasterDataFetcher dataFetcher = new GoogleWebmasterDataFetcherImpl(_property, client, workUnitState);
    Collection<ProducerJob> response = dataFetcher.getAllPages(null, null, "ALL", 5000);

    List<String> pageStrings = new ArrayList<>();
    for (ProducerJob page : response) {
      pageStrings.add(page.getPage());
    }

    Assert.assertTrue(CollectionUtils.isEqualCollection(pageStrings, allPages));
    Mockito.verify(client, Mockito.times(2))
        .getPages(eq(_property), any(String.class), any(String.class), eq("ALL"), any(Integer.class), any(List.class),
            any(List.class), eq(0));
  }

  @Test
  public void testGetPageSize1() throws Exception {
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(GoogleWebMasterSource.KEY_PROPERTY, _property);

    GoogleWebmasterClient client = Mockito.mock(GoogleWebmasterClient.class);
    List<String> list5000 = new ArrayList<>();
    for (int i = 0; i < 5000; ++i) {
      list5000.add(null);
    }

    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(0))).thenReturn(list5000);
    GoogleWebmasterDataFetcherImpl dataFetcher = new GoogleWebmasterDataFetcherImpl(_property, client, workUnitState);
    Assert.assertEquals(dataFetcher.getPagesSize("start_date", "end_date", "country", null, null), 5000);

    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(5000))).thenReturn(list5000);
    Assert.assertEquals(dataFetcher.getPagesSize("start_date", "end_date", "country", null, null), 10000);

    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(10000))).thenReturn(list5000);
    Assert.assertEquals(dataFetcher.getPagesSize("start_date", "end_date", "country", null, null), 15000);
  }

  @Test
  public void testGetPageSize2() throws Exception {
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(GoogleWebMasterSource.KEY_PROPERTY, _property);

    GoogleWebmasterClient client = Mockito.mock(GoogleWebmasterClient.class);
    List<String> list2 = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      list2.add(null);
    }

    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(0))).thenReturn(list2);
    GoogleWebmasterDataFetcherImpl dataFetcher = new GoogleWebmasterDataFetcherImpl(_property, client, workUnitState);
    int size = dataFetcher.getPagesSize("start_date", "end_date", "country", null, null);
    Assert.assertEquals(size, 2);
  }

  @Test
  public void testGetPageSize3() throws Exception {
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setProp(GoogleWebMasterSource.KEY_PROPERTY, _property);

    GoogleWebmasterClient client = Mockito.mock(GoogleWebmasterClient.class);
    List<String> list5000 = new ArrayList<>();
    for (int i = 0; i < 5000; ++i) {
      list5000.add(null);
    }

    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(0))).thenReturn(list5000);
    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(5000))).thenReturn(list5000);
    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(10000))).thenReturn(list5000);
    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(15000))).thenReturn(list5000);
    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(20000))).thenReturn(list5000);
    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(25000))).thenReturn(list5000);

    List<String> list2 = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      list2.add(null);
    }
    Mockito.when(client.getPages(any(String.class), any(String.class), any(String.class), any(String.class),
        eq(GoogleWebmasterClient.API_ROW_LIMIT), any(List.class), any(List.class), eq(30000))).thenReturn(list2);

    GoogleWebmasterDataFetcherImpl dataFetcher = new GoogleWebmasterDataFetcherImpl(_property, client, workUnitState);
    int size = dataFetcher.getPagesSize("start_date", "end_date", "country", null, null);
    Assert.assertEquals(size, 30002);
  }
}