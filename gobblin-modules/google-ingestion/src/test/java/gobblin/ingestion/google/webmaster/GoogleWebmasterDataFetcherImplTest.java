package gobblin.ingestion.google.webmaster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class GoogleWebmasterDataFetcherImplTest {

  private String _property = "https://www.myproperty.com/";

  @Test
  public void testGetAllPagesWhenRequestLessThan5000()
      throws Exception {
    GoogleWebmasterClient client = Mockito.mock(GoogleWebmasterClient.class);
    List<String> retVal = Arrays.asList("abc", "def");

    Mockito.when(client
        .getPages(eq(_property), any(String.class), any(String.class), eq("ALL"), any(Integer.class), any(List.class),
            any(List.class), eq(0))).thenReturn(retVal);

    GoogleWebmasterDataFetcher dataFetcher =
        new GoogleWebmasterDataFetcherImpl(_property, client, new ArrayList<ProducerJob>(), null, null);
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
  public void testGetAllPagesWhenDataSizeLessThan5000AndRequestAll()
      throws Exception {
    GoogleWebmasterClient client = Mockito.mock(GoogleWebmasterClient.class);
    List<String> allPages = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      allPages.add(Integer.toString(i));
    }
    Mockito.when(client
        .getPages(eq(_property), any(String.class), any(String.class), eq("ALL"), any(Integer.class), any(List.class),
            any(List.class), eq(0))).thenReturn(allPages);

    GoogleWebmasterDataFetcher dataFetcher =
        new GoogleWebmasterDataFetcherImpl(_property, client, new ArrayList<ProducerJob>(), null, null);
    Collection<ProducerJob> response = dataFetcher.getAllPages(null, null, "ALL", 5000);

    List<String> pageStrings = new ArrayList<>();
    for (ProducerJob page : response) {
      pageStrings.add(page.getPage());
    }

    Assert.assertTrue(CollectionUtils.isEqualCollection(pageStrings, allPages));
    Mockito.verify(client, Mockito.times(1))
        .getPages(eq(_property), any(String.class), any(String.class), eq("ALL"), any(Integer.class), any(List.class),
            any(List.class), eq(0));
  }
}