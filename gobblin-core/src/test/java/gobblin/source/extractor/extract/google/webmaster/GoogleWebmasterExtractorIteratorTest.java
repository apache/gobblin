package gobblin.source.extractor.extract.google.webmaster;

import com.google.api.services.webmasters.model.ApiDimensionFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.mockito.Matchers.*;


class CollectionEquals extends ArgumentMatcher<Collection> {

  private final Collection _expected;

  public CollectionEquals(Collection expected) {
    _expected = expected;
  }

  @Override
  public boolean matches(Object actual) {
    return CollectionUtils.isEqualCollection((Collection) actual, _expected);
  }
}

@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class GoogleWebmasterExtractorIteratorTest {
  Logger logger = LoggerFactory.getLogger(GoogleWebmasterExtractorIteratorTest.class);
  String siteProperty = "https://www.abc.com/";

  /**
   * Test the GoogleWebmasterExtractorIterator to make sure that it first gets all pages based on the filters
   * and then for each page, it asks for the queries.
   * @throws IOException
   */
  @Test
  public void testIterator() throws IOException {
    GoogleWebmasterDataFetcher client = Mockito.mock(GoogleWebmasterDataFetcher.class);
    String country = "USA";
    String date = "2016-11-01";
    ArrayList<GoogleWebmasterFilter.Dimension> requestedDimensions = new ArrayList<>();
    ArrayList<GoogleWebmasterDataFetcher.Metric> requestedMetrics = new ArrayList<>();

    Set<String> allPages = new HashSet<>();
    String page1 = siteProperty + "a/1";
    String page2 = siteProperty + "b/1";
    allPages.add(page1);
    allPages.add(page2);
    Mockito.when(client.getAllPages(eq(date), eq(country), eq(5000))).thenReturn(allPages);

    //Set doQuery Mock1
    String[] a1 = {"r1-c1", "r1-c2"};
    List<String[]> results1 = new ArrayList<>();
    results1.add(a1);
    List<ApiDimensionFilter> filters1 = new ArrayList<>();
    filters1.add(GoogleWebmasterFilter.countryEqFilter(country));
    filters1.add(GoogleWebmasterFilter.pageFilter(GoogleWebmasterFilter.FilterOperator.EQUALS, page1));
    Mockito.when(client.doQuery(eq(date), eq(5000), eq(requestedDimensions), eq(requestedMetrics),
        argThat(new CollectionEquals(filters1)))).thenReturn(results1);

    //Set doQuery Mock2
    String[] a2 = {"r2-c1", "r2-c2"};
    List<String[]> results2 = new ArrayList<>();
    results2.add(a2);
    List<ApiDimensionFilter> filters2 = new ArrayList<>();
    filters2.add(GoogleWebmasterFilter.countryEqFilter(country));
    filters2.add(GoogleWebmasterFilter.pageFilter(GoogleWebmasterFilter.FilterOperator.EQUALS, page2));
    Mockito.when(client.doQuery(eq(date), eq(5000), eq(requestedDimensions), eq(requestedMetrics),
        argThat(new CollectionEquals(filters2)))).thenReturn(results2);

    Map<GoogleWebmasterFilter.Dimension, ApiDimensionFilter> map = new HashMap<>();
    map.put(GoogleWebmasterFilter.Dimension.COUNTRY, GoogleWebmasterFilter.countryEqFilter(country));
    GoogleWebmasterExtractorIterator iterator =
        new GoogleWebmasterExtractorIterator(client, date, requestedDimensions, requestedMetrics, map, 5000, 5000);

    List<String[]> response = new ArrayList<>();
    response.add(iterator.next());
    response.add(iterator.next());
    Assert.assertTrue(!iterator.hasNext());
    Assert.assertTrue(response.contains(a1));
    Assert.assertTrue(response.contains(a2));

    Mockito.verify(client, Mockito.times(1)).getAllPages(eq(date), eq(country), eq(5000));
    Mockito.verify(client, Mockito.times(1))
        .doQuery(eq(date), eq(5000), eq(requestedDimensions), eq(requestedMetrics),
            argThat(new CollectionEquals(filters1)));
    Mockito.verify(client, Mockito.times(1))
        .doQuery(eq(date), eq(5000), eq(requestedDimensions), eq(requestedMetrics),
            argThat(new CollectionEquals(filters2)));
  }
}
