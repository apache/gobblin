package gobblin.data.management.copy;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Writable;


/**
 * Wrapper for owner, group, and permission of a path.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OwnerAndPermission implements Writable {

  private String owner;
  private String group;
  private FsPermission fsPermission;

  @Override public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeChars(this.owner);
    dataOutput.writeChars(this.group);
    fsPermission.write(dataOutput);
  }

  @Override public void readFields(DataInput dataInput) throws IOException {
    this.owner = dataInput.readUTF();
    this.group = dataInput.readUTF();
    this.fsPermission = FsPermission.read(dataInput);
  }

  /**
   * Read a {@link gobblin.data.management.copy.OwnerAndPermission} from a {@link java.io.DataInput}.
   * @throws IOException
   */
  public static OwnerAndPermission read(DataInput input) throws IOException {
    OwnerAndPermission oap = new OwnerAndPermission();
    oap.readFields(input);
    return oap;
  }
}
