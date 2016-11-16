package gobblin.source.extractor.extract.google.webmaster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Matchers.*;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class GoogleWebmasterDataFetcherImplTest {

  private String _property = "https://www.myproperty.com/";

  @Test
  public void testGetAllPagesWhenRequestLessThan5000() throws Exception {
    GoogleWebmasterClient client = Mockito.mock(GoogleWebmasterClient.class);
    List<String> retVal = Arrays.asList("abc", "def");

    Mockito.when(client.getPages(eq(_property), any(String.class), eq("ALL"), any(Integer.class), any(List.class),
        any(List.class), eq(0))).thenReturn(retVal);

    GoogleWebmasterDataFetcher dataFetcher = new GoogleWebmasterDataFetcherImpl(_property, true, client);
    Set<String> allPages = dataFetcher.getAllPages(null, "ALL", 2);

    Assert.assertTrue(CollectionUtils.isEqualCollection(retVal, allPages));
    Mockito.verify(client, Mockito.times(1))
        .getPages(eq(_property), any(String.class), eq("ALL"), any(Integer.class), any(List.class), any(List.class),
            eq(0));
  }

  @Test
  public void testGetAllPagesWhenDataSizeLessThan5000AndRequestAll() throws Exception {
    GoogleWebmasterClient client = Mockito.mock(GoogleWebmasterClient.class);
    List<String> allPages = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      allPages.add(Integer.toString(i));
    }
    Mockito.when(client.getPages(eq(_property), any(String.class), eq("ALL"), any(Integer.class), any(List.class),
        any(List.class), eq(0))).thenReturn(allPages);

    GoogleWebmasterDataFetcher dataFetcher = new GoogleWebmasterDataFetcherImpl(_property, true, client);
    Set<String> response = dataFetcher.getAllPages(null, "ALL", 5000);

    Assert.assertTrue(CollectionUtils.isEqualCollection(response, allPages));
    Mockito.verify(client, Mockito.times(1))
        .getPages(eq(_property), any(String.class), eq("ALL"), any(Integer.class), any(List.class), any(List.class),
            eq(0));
  }

  @Test
  public void testGetPagePrefixes() throws Exception {
    List<String> pagesRoots = new ArrayList<>();
    List<String> pagesNoRoots = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      pagesRoots.add(_property + "prefix_" + Integer.toString(i) + "/anythingHere1");
      pagesRoots.add(_property + "prefix_" + Integer.toString(i) + "/anythingHere2");
    }
    for (int i = 0; i < 3; ++i) {
      pagesNoRoots.add(_property + "content_" + Integer.toString(i));
    }
    List<String> allPages = new ArrayList<>();
    allPages.addAll(pagesNoRoots);
    allPages.addAll(pagesRoots);

    GoogleWebmasterClient client = Mockito.mock(GoogleWebmasterClient.class);
    Mockito.when(
        client.getPages(eq(_property), any(String.class), any(String.class), any(Integer.class), any(List.class),
            any(List.class), eq(0))).thenReturn(allPages);

    GoogleWebmasterDataFetcherImpl dataFetcher = new GoogleWebmasterDataFetcherImpl(_property, true, client);
    ImmutableTriple<Set<String>, Set<String>, Integer> results = dataFetcher.getPagePrefixes(null, null, null);
    Set<String> pagesNoRootsResponse = results.getLeft();
    Set<String> pagesRootsResponse = results.getMiddle();
    int expectedSize = results.getRight();

    Assert.assertTrue(CollectionUtils.isEqualCollection(pagesNoRootsResponse, pagesNoRoots));
    Assert.assertTrue(CollectionUtils.isEqualCollection(pagesRootsResponse,
        Arrays.asList(_property + "prefix_0/", _property + "prefix_1/")));
    Assert.assertEquals(pagesNoRoots.size() + pagesRoots.size(), expectedSize);
  }
}