package gobblin.converter.csv;

import java.io.IOException;
import java.io.InputStreamReader;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.converter.DataConversionException;


@Test(groups = {"gobblin.converter"})
public class PercentageDoubleStringConverterTest {
  public void convertOutput()
      throws IOException, DataConversionException {
    JsonParser parser = new JsonParser();
    JsonElement jsonElement = parser
        .parse(new InputStreamReader(getClass().getResourceAsStream("/converter/csv/schema_with_10_fields.json")));

    JsonObject inputRecord = parser
        .parse(new InputStreamReader(getClass().getResourceAsStream("/converter/csv/10_fields_with_percentage.json")))
        .getAsJsonObject();

    PercentageDoubleStringConverter converter = new PercentageDoubleStringConverter();
    Iterable<JsonObject> iterable = converter.convertRecord(jsonElement.getAsJsonArray(), inputRecord, null);
    converter.close();

    JsonObject converted = iterable.iterator().next();
    Assert.assertEquals(converted.toString(),
        "{\"Date\":\"20160924\",\"DeviceCategory\":\"desktop\",\"Sessions\":\"4\",\"BounceRate\":\"0.044\",\"AvgSessionDuration\":\"0.001\",\"Pageviews\":\"3\",\"PageviewsPerSession\":\"8.1\",\"UniquePageviews\":\"2\",\"AvgTimeOnPage\":\"1.0\",\"User_count\":\"17\"}");
  }
}
