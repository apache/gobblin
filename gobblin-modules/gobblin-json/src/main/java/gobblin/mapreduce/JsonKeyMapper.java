package gobblin.mapreduce;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper for json logs.
 * If you specify {@link CONF_KEY_TIMESTAMP_FIELD} then it will be added as prefix for the mapper key. In this way
 * file can be sorted.
 * Created by tamasnemeth on 12/11/16.
 */
public class JsonKeyMapper extends Mapper<LongWritable, Text, Text, Text> {

  private static final String CONF_KEY_TIMESTAMP_FIELD = "compaction.timestamp.field";

  private String timestampField;

  public enum EVENT_COUNTER {
    RECORD_COUNT
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    timestampField = context.getConfiguration().get(CONF_KEY_TIMESTAMP_FIELD);

  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Text outKey = value;

    JsonParser jsonParser = new JsonParser();
    JsonObject inputRecord = (JsonObject) jsonParser.parse(value.toString());

    if (StringUtils.isNotEmpty(this.timestampField)) {
      if (inputRecord.has(timestampField)){
        Long timestamp = inputRecord.get(timestampField).getAsLong();
        outKey = new Text(timestamp.toString()+"_"+value.toString());
      }
    }

    context.write(outKey, value);
    context.getCounter(JsonKeyMapper.EVENT_COUNTER.RECORD_COUNT).increment(1);
  }

}
