package gobblin.ingestion.google.adwords;

import java.util.Arrays;
import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.api.ads.adwords.lib.jaxb.v201609.ReportDefinitionReportType;


@Test(groups = {"gobblin.ingestion.google.adwords"})
public class GoogleAdWordsExtractorTest {
  @Test
  public void testGetDownloadFields1() {
    HashMap<String, String> fields = new HashMap<>();
    fields.put("c1", "String");
    fields.put("c2", "Integer");
    fields.put("c3", "Double");

    String schemaString = GoogleAdWordsExtractor
        .getDownloadFields(fields, ReportDefinitionReportType.CLICK_PERFORMANCE_REPORT, Arrays.asList("c1", "c2"));

    Assert.assertEquals(schemaString,
        "[{\"columnName\":\"c1\",\"isNullable\":false,\"dataType\":{\"type\":\"STRING\"}},{\"columnName\":\"c2\",\"isNullable\":false,\"dataType\":{\"type\":\"INT\"}}]");
  }

  @Test
  public void testGetDownloadFields2() {
    HashMap<String, String> fields = new HashMap<>();
    fields.put("c1", "String");

    String schemaString =
        GoogleAdWordsExtractor.getDownloadFields(fields, ReportDefinitionReportType.CLICK_PERFORMANCE_REPORT, null);

    Assert.assertEquals(schemaString,
        "[{\"columnName\":\"c1\",\"isNullable\":false,\"dataType\":{\"type\":\"STRING\"}}]");
  }

  @Test(expectedExceptions = {RuntimeException.class})
  public void testGetDownloadFieldsWhenColumnNotExist() {
    HashMap<String, String> fields = new HashMap<>();
    fields.put("c1", "String");
    GoogleAdWordsExtractor
        .getDownloadFields(fields, ReportDefinitionReportType.CLICK_PERFORMANCE_REPORT, Arrays.asList("c2"));
  }
}
