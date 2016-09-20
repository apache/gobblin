package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link ConfigBasedDatasetsFinder}
 * @author mitu
 *
 */
public class ConfigBasedDatasetsFinderTest {
  
  @Test
  public void testGetLeafDatasetURIs() throws URISyntaxException, IOException {
    Collection<URI> allDatasetURIs = new ArrayList<URI>();
    // leaf URI
    allDatasetURIs.add(new URI("/data/derived/browsemaps/entities/anet"));
    allDatasetURIs.add(new URI("/data/derived/browsemaps/entities/comp"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation/daily"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation/daily_dedup"));
    
    // None leaf URI
    allDatasetURIs.add(new URI("/data/derived"));
    allDatasetURIs.add(new URI("/data/derived/browsemaps"));
    allDatasetURIs.add(new URI("/data/derived/browsemaps/entities/"));
    
    allDatasetURIs.add(new URI("/data/derived/gowl/"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation"));

    // wrong root
    allDatasetURIs.add(new URI("/data/derived2"));
    
    // disabled
    Collection<URI> disabled = new ArrayList<URI>();
    disabled.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation/daily"));
    
    Set<URI> validURIs = ConfigBasedDatasetsFinder.getValidDatasetURIs(allDatasetURIs, disabled, new Path("/data/derived"));
    
    Assert.assertTrue(validURIs.size() == 3);
    Assert.assertTrue(validURIs.contains(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation/daily_dedup")));
    Assert.assertTrue(validURIs.contains(new URI("/data/derived/browsemaps/entities/comp")));
    Assert.assertTrue(validURIs.contains(new URI("/data/derived/browsemaps/entities/anet")));
  }
}
