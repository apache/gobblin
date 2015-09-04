package gobblin.data.management.copy;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;


/**
 * Abstraction for a file to copy from {@link #origin} to {@link #destination}.
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CopyableFile implements Writable {

  /** {@link FileStatus} of the existing origin file. */
  private FileStatus origin;
  /** Destination {@link Path} of the file. */
  private Path destination;
  /** Desired {@link OwnerAndPermission} of the destination path. */
  private OwnerAndPermission destinationOwnerAndPermission;
  /**
   * Desired {@link OwnerAndPermission} of the ancestor directories of the destination path.
   * The list is ordered from deepest to highest directory.
   *
   * <p>
   *   For example, if {@link #destination} is /a/b/c/file, then the first element of this list is the desired
   *   owner and permission for directory /a/b/c, the second is the desired owner and permission for directory
   *   /a/b, and so on.
   * </p>
   *
   * <p>
   *   If there are fewer elements in the list than ancestor directories in {@link #destination}, it is understood
   *   that extra directories are allowed to have any owner and permission.
   * </p>
   */
  private List<OwnerAndPermission> ancestorsOwnerAndPermission;
  /** Checksum of the origin file. */
  private byte[] checksum;

  @Override public void write(DataOutput dataOutput) throws IOException {
    this.origin.write(dataOutput);
    dataOutput.writeUTF(this.destination.toString());
    this.destinationOwnerAndPermission.write(dataOutput);
    dataOutput.writeInt(this.ancestorsOwnerAndPermission.size());
    for(OwnerAndPermission oap : this.ancestorsOwnerAndPermission) {
      oap.write(dataOutput);
    }
    dataOutput.writeInt(this.checksum.length);
    dataOutput.write(this.checksum);
  }

  @Override public void readFields(DataInput dataInput) throws IOException {
    this.origin = new FileStatus();
    this.origin.readFields(dataInput);
    this.destination = new Path(dataInput.readUTF());
    this.destinationOwnerAndPermission = OwnerAndPermission.read(dataInput);
    int ancestors = dataInput.readInt();
    this.ancestorsOwnerAndPermission = Lists.newArrayList();
    for(int i = 0; i < ancestors; i ++) {
      this.ancestorsOwnerAndPermission.add(OwnerAndPermission.read(dataInput));
    }
    int checksumSize = dataInput.readInt();
    this.checksum = new byte[checksumSize];
    dataInput.readFully(this.checksum);
  }

  /**
   * Read a {@link gobblin.data.management.copy.CopyableFile} from a {@link java.io.DataInput}.
   * @throws IOException
   */
  public static CopyableFile read(DataInput dataInput) throws IOException {
    CopyableFile copyableFile = new CopyableFile();
    copyableFile.readFields(dataInput);
    return copyableFile;
  }
}
