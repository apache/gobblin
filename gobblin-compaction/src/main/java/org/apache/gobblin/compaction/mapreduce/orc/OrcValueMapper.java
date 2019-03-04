package org.apache.gobblin.compaction.mapreduce.orc;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcMapreduceRecordReader;


/**
 * To keep consistent with {@link OrcMapreduceRecordReader}'s decision on implementing
 * {@link RecordReader} with {@link NullWritable} as the key and generic type of value, the ORC Mapper will
 * read in the record as the input value,
 */
public class OrcValueMapper extends Mapper<NullWritable, OrcStruct, OrcKey, Object> {

  private OrcKey outKey;
  private OrcValue outValue;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    this.outKey = new OrcKey();
    this.outValue = new OrcValue();
  }

  @Override
  protected void map(NullWritable key, OrcStruct orcStruct, Context context) throws IOException, InterruptedException {
    if (context.getNumReduceTasks() == 0) {
      this.outKey.key = orcStruct;
      context.write(this.outKey, NullWritable.get());
    } else {
      // TODO: Start with the whole record for dedup now.
      this.outValue.value = orcStruct;
      context.write(getDedupKey(orcStruct), this.outValue);
    }
  }

  // TODO: Extend this method.
  private OrcKey getDedupKey(OrcStruct originalRecord) {
    return convertOrcStructToOrcKey(originalRecord);
  }

  /**
   * The output key of mapper needs to be comparable. In the scenarios that we need the orc record itself
   * to be the output key, this conversion will be necessary.
   */
  private OrcKey convertOrcStructToOrcKey(OrcStruct struct) {
    OrcKey orcKey = new OrcKey();
    orcKey.key = struct;
    return orcKey;
  }
}
