package gobblin.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Reducer job for json files
 * Created by tamasnemeth on 12/11/16.
 */
class JsonKeyDedupReducer extends Reducer<Text, Text, Text, NullWritable>{
  private static final Logger LOG = LoggerFactory.getLogger(JsonKeyDedupReducer.class);

  public enum EVENT_COUNTER {
    MORE_THAN_1,
    DEDUPED,
    RECORD_COUNT
  }

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int numVals = 0;

    Text valueToRetain = null;

    for (Text value : values) {
      if (valueToRetain == null) {
        valueToRetain = value;
      }

      numVals++;
    }

    if (numVals > 1) {
      LOG.warn("Duplicate value found: ["+valueToRetain+"] key:["+key+"]");
      context.getCounter(JsonKeyDedupReducer.EVENT_COUNTER.MORE_THAN_1).increment(1);
      context.getCounter(JsonKeyDedupReducer.EVENT_COUNTER.DEDUPED).increment(numVals - 1);
    }

    context.getCounter(JsonKeyDedupReducer.EVENT_COUNTER.RECORD_COUNT).increment(1);

    context.write(valueToRetain, NullWritable.get());
  }

}
