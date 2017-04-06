package gobblin.ingestion.google.adwords;

import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;


@Test(groups = {"gobblin.ingestion.google.adwords"})
public class GoogleAdWordsReportDownloaderTest {
  private LinkedBlockingDeque<String[]> queue = new LinkedBlockingDeque<>();

  @Test
  public void testSchemaToColumnNames()
      throws Exception {
    String schema =
        "[{\"columnName\":\"c1\",\"isNullable\":true,\"dataType\":{\"type\":\"STRING\"}},{\"columnName\":\"c2\",\"isNullable\":true,\"dataType\":{\"type\":\"INT\"}}]";
    JsonArray schemaArray = new JsonParser().parse(schema).getAsJsonArray();
    List<String> columns = GoogleAdWordsReportDownloader.schemaToColumnNames(schemaArray);
    Assert.assertEquals(columns.size(), 2);
    Assert.assertEquals(columns.get(0), "c1");
    Assert.assertEquals(columns.get(1), "c2");
  }

  @Test
  public void testAddToQueueEmptyAndEmpty()
      throws Exception {
    String remaining = GoogleAdWordsReportDownloader.addToQueue(queue, "", "", "8888");
    Assert.assertEquals(remaining, "");
    Assert.assertEquals(queue.size(), 0);
  }

  /**
   * test the case where current string doesn't complete a line
   */
  @Test
  public void testAddToQueueEmptyAndStringNotComplete()
      throws Exception {
    String remaining = GoogleAdWordsReportDownloader.addToQueue(queue, "", "c", "8888");
    Assert.assertEquals(remaining, "c");
    Assert.assertEquals(queue.size(), 0);
  }

  /**
   * test the case where current string complete a line
   */
  @Test
  public void testAddToQueueEmptyAndStringComplete1()
      throws Exception {
    String remaining = GoogleAdWordsReportDownloader.addToQueue(queue, "", "a,b\nc1,c2", "8888");
    Assert.assertEquals(remaining, "c1,c2");
    Assert.assertEquals(queue.size(), 1);
    String[] line = queue.poll();
    Assert.assertEquals(line, new String[]{"a", "b", "8888"});
  }

  @Test
  public void testAddToQueueEmptyAndStringComplete2()
      throws Exception {
    String remaining = GoogleAdWordsReportDownloader.addToQueue(queue, "", "a,b\nc1,c2\nc3,c4", "8888");
    Assert.assertEquals(remaining, "c3,c4");
    Assert.assertEquals(queue.size(), 2);
    String[] line1 = queue.poll();
    Assert.assertEquals(line1, new String[]{"a", "b", "8888"});
    String[] line2 = queue.poll();
    Assert.assertEquals(line2, new String[]{"c1", "c2", "8888"});
  }

  @Test
  public void testAddToQueueEmptyAndStringComplete3()
      throws Exception {
    String remaining = GoogleAdWordsReportDownloader.addToQueue(queue, "", "a,b\nc1,c2\nc3,c4\n", "8888");
    Assert.assertEquals(remaining, "");
    Assert.assertEquals(queue.size(), 3);
    String[] line1 = queue.poll();
    Assert.assertEquals(line1, new String[]{"a", "b", "8888"});
    String[] line2 = queue.poll();
    Assert.assertEquals(line2, new String[]{"c1", "c2", "8888"});
    String[] line3 = queue.poll();
    Assert.assertEquals(line3, new String[]{"c3", "c4", "8888"});
  }

  @Test
  public void testAddToQueueStringAndEmpty()
      throws Exception {
    String remaining = GoogleAdWordsReportDownloader.addToQueue(queue, "a,b", "", "8888");
    Assert.assertEquals(remaining, "a,b");
    Assert.assertEquals(queue.size(), 0);
  }

  /**
   * test the case where current string doesn't complete a line
   */
  @Test
  public void testAddToQueueStringAndStringNotComplete()
      throws Exception {
    String remaining = GoogleAdWordsReportDownloader.addToQueue(queue, "a,b", "c", "8888");
    Assert.assertEquals(remaining, "a,bc");
    Assert.assertEquals(queue.size(), 0);
  }

  /**
   * test the case where current string complete a line
   */
  @Test
  public void testAddToQueueStringAndStringComplete1()
      throws Exception {
    String remaining = GoogleAdWordsReportDownloader.addToQueue(queue, "a,b", "c, \"com,ma\"\nc1,c2", "8888");
    Assert.assertEquals(remaining, "c1,c2");
    Assert.assertEquals(queue.size(), 1);
    String[] line = queue.poll();
    //Should ignore commas in quotes
    Assert.assertEquals(line, new String[]{"a", "bc", "com,ma", "8888"});
  }

  @Test
  public void testAddToQueueStringAndStringComplete2()
      throws Exception {
    String remaining = GoogleAdWordsReportDownloader.addToQueue(queue, "a, b ,--, --,-- ", "\n", "8888");
    Assert.assertEquals(remaining, "");
    Assert.assertEquals(queue.size(), 1);
    String[] line = queue.poll();
    //Should remove leading and ending space around "b"
    //Should convert "--" to nulls.
    Assert.assertEquals(line, new String[]{"a", "b", null, null, null, "8888"});
  }
}
