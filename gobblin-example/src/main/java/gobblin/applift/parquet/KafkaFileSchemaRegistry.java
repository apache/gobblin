package gobblin.applift.parquet;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Properties;
import org.apache.avro.Schema;

import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;


/**
 * Thread safe.
 * 
 *  
 */
public class KafkaFileSchemaRegistry extends KafkaSchemaRegistry<String, String> {
	public static final String  KAFKA_SCHEMA_REGISTRY_ROOT_PATH = "kafka.schema.registry.filepath";
	private final File root;
	
	protected KafkaFileSchemaRegistry(Properties props) {
		super(props);
		String rootPath = props.getProperty(KAFKA_SCHEMA_REGISTRY_ROOT_PATH);
		this.root = new File(rootPath);
		
	}
	
	@Override
	public String register(String schema) throws SchemaRegistryException {
		Schema avroSchema = new Schema.Parser().parse(schema);
		String topic = avroSchema.getName();
		return register(schema,topic);
	}

  @Override
  public String register(String schema, String topic) throws SchemaRegistryException  {
    FileOutputStream out = null;

    File topicDir = getTopicPath(topic);

    if (!topicDir.exists()) {
      topicDir.mkdirs();
    }

    try {
      byte[] bytes = toBytes(schema);
      String id = SHAsum(bytes);
      File file = getSchemaPath(topic, id, true);
      out = new FileOutputStream(file);
      out.write(bytes);

      // move any old "latest" files to be regular schema files
      for (File fileToRename : topicDir.listFiles()) {
        if (!fileToRename.equals(file) && fileToRename.getName().endsWith(".latest")) {
          String oldName = fileToRename.getName();
          // 7 = len(.latest)
          File renameTo = new File(fileToRename.getParentFile(), oldName.substring(0, oldName.length() - 7));
          fileToRename.renameTo(renameTo);
        }
      }
      StringBuilder sb = new StringBuilder(topic);
      sb.append("-").append(id);
      return sb.toString();
    } catch (Exception e) {
      throw new SchemaRegistryException(e);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          throw new SchemaRegistryException(e);
        }
      }
    }
  }

  @Override
  public String fetchSchemaByKey(String id) throws SchemaRegistryException {
  	String topic = id.split("-")[0];
  	File file = getSchemaPath(topic, id, false);
    if (!file.exists()) {
      file = getSchemaPath(topic, id, true);
    }
    if (!file.exists()) {
      throw new SchemaRegistryException("No matching schema found for topic " + topic + " and id " + id + ".");
    }
    return fromBytes(readBytes(file));
  }

  @Override
  public String getLatestSchemaByTopic(String topicName) throws SchemaRegistryException {
    File topicDir = getTopicPath(topicName);
    if (topicDir.exists()) {
      for (File file : topicDir.listFiles()) {
        if (file.getName().endsWith(".latest")) {
          return fromBytes(readBytes(file));
        }
      }
    }
    throw new SchemaRegistryException("Unable to find a latest schema for topic " + topicName + ".");
  }

  private byte[] readBytes(File file) throws SchemaRegistryException {
    DataInputStream dis = null;
    byte[] bytes = new byte[(int) file.length()];

    try {
      dis = new DataInputStream(new FileInputStream(file));
      dis.readFully(bytes);
    } catch (Exception e) {
      throw new SchemaRegistryException(e);
    } finally {
      if (dis != null) {
        try {
          dis.close();
        } catch (Exception e) {
          throw new SchemaRegistryException(e);
        }
      }
    }
    return bytes;
  }

  private File getTopicPath(String topic) {
    return new File(root, topic);
  }

  private File getSchemaPath(String topic, String id, boolean latest) {
    return new File(getTopicPath(topic), id + ".schema" + ((latest) ? ".latest" : ""));
  }

  public static String SHAsum(byte[] convertme) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-1");
    return byteArray2Hex(md.digest(convertme));
  }

  private static String byteArray2Hex(final byte[] hash) {
    Formatter formatter = new Formatter();
    for (byte b : hash) {
      formatter.format("%02x", b);
    }
    String hex = formatter.toString();
    formatter.close();
    return hex;
  }
  
  public byte[] toBytes(String obj) {
    try {
      return obj.getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public String fromBytes(byte[] bytes) {
    try {
      return new String(bytes, "utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
