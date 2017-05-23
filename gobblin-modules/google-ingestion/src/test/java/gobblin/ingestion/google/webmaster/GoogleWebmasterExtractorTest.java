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

package gobblin.ingestion.google.webmaster;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class GoogleWebmasterExtractorTest {
  /**
   * Test that positionMaps and iterators are constructed correctly in the constructor
   */
  @Test
  public void testConstructor()
      throws IOException, DataRecordException {
    WorkUnitState wuState = getWorkUnitState1();
    wuState.setProp(GoogleWebMasterSource.KEY_REQUEST_FILTERS, "Country.USA,Country.ALL");

    List<GoogleWebmasterFilter.Dimension> dimensions =
        Arrays.asList(GoogleWebmasterFilter.Dimension.PAGE, GoogleWebmasterFilter.Dimension.COUNTRY);
    List<GoogleWebmasterDataFetcher.Metric> metrics = Arrays.asList(GoogleWebmasterDataFetcher.Metric.CLICKS);
    Map<String, Integer> positionMap = new HashMap<>();
    positionMap.put(GoogleWebmasterDataFetcher.Metric.CLICKS.toString(), 0);
    positionMap.put(GoogleWebmasterFilter.Dimension.COUNTRY.toString(), 1);
    positionMap.put(GoogleWebmasterFilter.Dimension.PAGE.toString(), 2);

    GoogleWebmasterDataFetcher dataFetcher1 = Mockito.mock(GoogleWebmasterDataFetcher.class);
    GoogleWebmasterDataFetcher dataFetcher2 = Mockito.mock(GoogleWebmasterDataFetcher.class);

    GoogleWebmasterExtractor extractor =
        new GoogleWebmasterExtractor(wuState, wuState.getWorkunit().getLowWatermark(LongWatermark.class).getValue(),
            wuState.getWorkunit().getExpectedHighWatermark(LongWatermark.class).getValue(), positionMap, dimensions,
            metrics, null, Arrays.asList(dataFetcher1, dataFetcher2));

    List<GoogleWebmasterExtractorIterator> iterators = extractor.getIterators();
    Assert.assertEquals(iterators.size(), 4);
    Assert.assertEquals(iterators.get(0).getCountry(), "USA");
    Assert.assertEquals(iterators.get(1).getCountry(), "ALL");
    Assert.assertEquals(iterators.get(2).getCountry(), "USA");
    Assert.assertEquals(iterators.get(3).getCountry(), "ALL");

    List<int[]> responseToOutputSchema = extractor.getPositionMaps();
    Assert.assertEquals(responseToOutputSchema.size(), 4);
    Assert.assertEquals(new int[]{2, 1, 0}, responseToOutputSchema.get(0)); //country is Country.USA
    Assert.assertEquals(new int[]{2, 0},
        responseToOutputSchema.get(1)); //country is Country.ALL, so the country request will be removed.
    Assert.assertEquals(new int[]{2, 1, 0}, responseToOutputSchema.get(2));
    Assert.assertEquals(new int[]{2, 0}, responseToOutputSchema.get(3));
  }

  public static WorkUnitState getWorkUnitState1() {
    WorkUnit wu = new WorkUnit(new Extract(Extract.TableType.APPEND_ONLY, "namespace", "table"));
    wu.setWatermarkInterval(
        new WatermarkInterval(new LongWatermark(20160101235959L), new LongWatermark(20160102235959L)));
    State js = new State();
    return new WorkUnitState(wu, js);
  }
}
