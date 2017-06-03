package gobblin.converter.string;

import java.io.IOException;
import java.io.InputStreamReader;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gobblin.converter.DataConversionException;
import gobblin.converter.string.PercentageDoubleStringConverter;


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
    Assert.assertEquals(converted.toString(), getExpectedJson().toString());
  }

  private JsonObject getExpectedJson() {
    JsonObject json = new JsonObject();
    json.addProperty("Date", "20160924");
    json.addProperty("DeviceCategory", "desktop");
    json.addProperty("Sessions", "4");
    json.addProperty("BounceRate", "0.044");
    json.addProperty("AvgSessionDuration", "0.001");
    json.addProperty("Pageviews", "3");
    json.addProperty("PageviewsPerSession", "8.1");
    json.addProperty("UniquePageviews", "2");
    json.addProperty("AvgTimeOnPage", "1.0");
    json.addProperty("User_count", "17");
    return json;
  }
}
