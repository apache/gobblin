package gobblin.data.management.copy.replication;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link ConfigBasedDatasetsFinder}
 * @author mitu
 *
 */
public class ConfigBasedDatasetsFinderTest {
  
  private ConfigBasedDatasetsFinder getDatasetsFinder() throws IOException{
    Properties p = new Properties();
    p.setProperty(ConfigBasedDatasetsFinder.CONFIG_STORE_ROOT, "root");
    p.setProperty(ConfigBasedDatasetsFinder.CONFIG_STORE_REPLICATION_ROOT, "/data/derived");
    p.setProperty(ConfigBasedDatasetsFinder.CONFIG_STORE_REPLICATION_TAG, "derivedTag");
   
    System.out.println("contains root " + p.containsKey(ConfigBasedDatasetsFinder.CONFIG_STORE_ROOT));
    System.out.println("contains root " + p.contains(ConfigBasedDatasetsFinder.CONFIG_STORE_ROOT));
    return new ConfigBasedDatasetsFinder(p);
  }
  
  @Test
  public void testGetLeafDatasetURIs() throws URISyntaxException, IOException {
    Collection<URI> allDatasetURIs = new ArrayList<URI>();
    // leaf URI
    allDatasetURIs.add(new URI("/data/derived/browsemaps/entities/anet"));
    allDatasetURIs.add(new URI("/data/derived/browsemaps/entities/comp"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation/daily"));
    
    // None leaf URI
    allDatasetURIs.add(new URI("/data/derived"));
    allDatasetURIs.add(new URI("/data/derived/browsemaps"));
    allDatasetURIs.add(new URI("/data/derived/browsemaps/entities/"));
    
    allDatasetURIs.add(new URI("/data/derived/gowl/"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/"));
    allDatasetURIs.add(new URI("/data/derived/gowl/pymk/invitationsCreationsSends/hourly_data/aggregation"));
    
    Set<URI> leafOnly = ConfigBasedDatasetsFinder.getValidDatasetURIs(allDatasetURIs, "/data/derived");
    
    for(URI u: leafOnly){
      System.out.println("leaf is " + u);
    }
    Assert.assertTrue(leafOnly.size() == 3);
  }
}
