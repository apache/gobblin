package gobblin.source.extractor.extract.google.webmaster;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.junit.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class GoogleWebmasterClientImplTest {

  @Test
  public void getPagePrefixes() throws Exception {
    String siteProperty = "https://www.linkedin.com/";
    List<String> predefinedFilters =
        Arrays.asList("psettings", "legal", "directory", "start", "help", "edu", "in", "pub", "learning", "uas",
            "badges", "company", "topic", "grp", "vsearch", "jobs", "pulse", "title", "groups", "redir", "profinder");

    Collection<String> fakePages =
        Arrays.asList(siteProperty + "zzz/hi", siteProperty + "zzz/hi2", siteProperty + "jobs/hi2");

    HashSet<String> prefixes = GoogleWebmasterClientImpl.getPagePrefixes(siteProperty, predefinedFilters, fakePages);

    Assert.assertEquals(prefixes.size(), predefinedFilters.size() + 1);

    for (String preDefined : predefinedFilters) {
      Assert.assertTrue(prefixes.remove(siteProperty + preDefined + "/"));
    }
    Assert.assertTrue(prefixes.remove(siteProperty + "zzz/"));
    Assert.assertTrue(prefixes.isEmpty());
  }
}