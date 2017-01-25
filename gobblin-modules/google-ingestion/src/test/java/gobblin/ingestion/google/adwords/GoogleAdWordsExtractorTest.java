package gobblin.ingestion.google.adwords;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.ingestion.google.adwords"})
public class GoogleAdWordsExtractorTest {
  @Test
  public void testGetDownloadFields1()
      throws IOException {
    HashMap<String, String> fields = new HashMap<>();
    fields.put("c1", "String");
    fields.put("c2", "Integer");
    fields.put("c3", "Double");

    String schemaString = GoogleAdWordsExtractor.createSchema(fields, Arrays.asList("c1", "c2"));

    Assert.assertEquals(schemaString,
        "[{\"columnName\":\"c1\",\"isNullable\":true,\"dataType\":{\"type\":\"STRING\"}},{\"columnName\":\"c2\",\"isNullable\":true,\"dataType\":{\"type\":\"INT\"}}]");
  }

  @Test
  public void testGetDownloadFields2()
      throws IOException {
    HashMap<String, String> fields = new HashMap<>();
    fields.put("c1", "String");

    String schemaString = GoogleAdWordsExtractor.createSchema(fields, null);

    Assert
        .assertEquals(schemaString, "[{\"columnName\":\"c1\",\"isNullable\":true,\"dataType\":{\"type\":\"STRING\"}}]");
  }

  @Test(expectedExceptions = {IOException.class})
  public void testGetDownloadFieldsWhenColumnNotExist()
      throws IOException {
    HashMap<String, String> fields = new HashMap<>();
    fields.put("c1", "String");
    GoogleAdWordsExtractor.createSchema(fields, Arrays.asList("c2"));
  }
}
