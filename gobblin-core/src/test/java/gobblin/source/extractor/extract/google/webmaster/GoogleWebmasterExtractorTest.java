package gobblin.source.extractor.extract.google.webmaster;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class GoogleWebmasterExtractorTest {
  /**
   * Test that positionMaps and iterators are constructed correctly in the constructor
   */
  @Test
  public void testConstructor() throws IOException, DataRecordException {
    WorkUnitState wuState = getWorkUnitState1();
    wuState.setProp(GoogleWebMasterSource.KEY_REQUEST_FILTERS, "Country.USA,Country.ALL");

    List<GoogleWebmasterFilter.Dimension> dimensions =
        Arrays.asList(GoogleWebmasterFilter.Dimension.PAGE, GoogleWebmasterFilter.Dimension.COUNTRY);
    List<GoogleWebmasterDataFetcher.Metric> metrics = Arrays.asList(GoogleWebmasterDataFetcher.Metric.CLICKS);
    Map<String, Integer> positionMap = new HashMap<>();
    positionMap.put(GoogleWebmasterDataFetcher.Metric.CLICKS.toString(), 0);
    positionMap.put(GoogleWebmasterFilter.Dimension.COUNTRY.toString(), 1);
    positionMap.put(GoogleWebmasterFilter.Dimension.PAGE.toString(), 2);

    GoogleWebmasterDataFetcher dataFetcher = Mockito.mock(GoogleWebmasterDataFetcher.class);

    GoogleWebmasterExtractor extractor =
        new GoogleWebmasterExtractor(wuState, wuState.getWorkunit().getLowWatermark(LongWatermark.class).getValue(),
            wuState.getWorkunit().getExpectedHighWatermark(LongWatermark.class).getValue(), positionMap, dimensions,
            metrics, dataFetcher);

    Queue<GoogleWebmasterExtractorIterator> iterators = extractor.getIterators();
    GoogleWebmasterExtractorIterator iteratorUSA = iterators.poll();
    Assert.assertEquals("USA", iteratorUSA.getCountry());
    GoogleWebmasterExtractorIterator iteratorALL = iterators.poll();
    Assert.assertEquals("ALL", iteratorALL.getCountry());
    Assert.assertTrue(iterators.isEmpty());

    Queue<int[]> responseToOutputSchema = extractor.getPositionMaps();
    int[] positionMap1 = responseToOutputSchema.poll(); //country is Country.USA
    Assert.assertArrayEquals(new int[]{2, 1, 0}, positionMap1);
    int[] positionMap2 =
        responseToOutputSchema.poll(); //country is Country.ALL, so the country request will be removed.
    Assert.assertArrayEquals(new int[]{2, 0}, positionMap2);
    Assert.assertTrue(responseToOutputSchema.isEmpty());
  }

  public static WorkUnitState getWorkUnitState1() {
    WorkUnit wu = new WorkUnit(new Extract(Extract.TableType.APPEND_ONLY, "namespace", "table"));
    wu.setWatermarkInterval(
        new WatermarkInterval(new LongWatermark(20160101235959L), new LongWatermark(20160102235959L)));
    State js = new State();
    return new WorkUnitState(wu, js);
  }
}
