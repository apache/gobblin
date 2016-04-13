package gobblin.compaction.mapreduce.avro;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import gobblin.configuration.ConfigurationKeys;


/**
 * A subclass of {@link org.apache.hadoop.mapreduce.lib.input.CombineFileSplit}. The purpose is to add the input file's
 * avro schema to a split.
 *
 *
 * @author mwol
 */
public class AvroCombineFileSplit extends CombineFileSplit {

  private Schema schema;

  public AvroCombineFileSplit() {
  }

  public AvroCombineFileSplit(Path[] paths, long[] startOffsets, long[] lengths, String[] locations) {
    super(paths, startOffsets, lengths, locations);
  }

  public AvroCombineFileSplit(CombineFileSplit old) throws IOException {
    super(old);
  }

  public AvroCombineFileSplit(Path[] paths, long[] startOffsets, long[] lengths, String[] locations, Schema schema) {
    super(paths, startOffsets, lengths, locations);
    this.schema = schema;
  }

  public AvroCombineFileSplit(CombineFileSplit old, Schema schema) throws IOException {
    this(old);
    this.schema = schema;
  }

  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.schema = new Schema.Parser().parse(fromBase64(Text.readString(in)));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, toBase64(this.schema.toString()));
  }

  @Override
  public String toString() {
    return super.toString() + " Schema: " + this.schema.toString();
  }

  private static String toBase64(String rawString) {
    Base64 base64decoder = new Base64();
    return new String(base64decoder.encode(rawString.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)),
        ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
  }

  private static String fromBase64(String base64String) {
    Base64 base64decoder = new Base64();
    return new String(base64decoder.decode(base64String.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)),
        ConfigurationKeys.DEFAULT_CHARSET_ENCODING);
  }
}
