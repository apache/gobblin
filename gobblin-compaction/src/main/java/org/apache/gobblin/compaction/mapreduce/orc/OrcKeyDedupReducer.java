package org.apache.gobblin.compaction.mapreduce.orc;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcValue;


// TODO: Implement the real deduplication here.
public class OrcKeyDedupReducer extends Reducer<OrcKey, OrcValue, NullWritable, OrcValue> {

  // Reusable output record object.
  private OrcValue outValue = new OrcValue();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
  }

  @Override
  protected void reduce(OrcKey key, Iterable<OrcValue> values, Context context)
      throws IOException, InterruptedException {
    int numVal = 0;
    OrcValue valueToRetain = null;

    for (OrcValue orcRecord : values) {
      valueToRetain = orcRecord;
    }

    outValue = valueToRetain;
    context.write(NullWritable.get(), outValue);
  }
}
