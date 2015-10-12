package gobblin.data.management.trash;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;


/**
 * Job to run {@link Trash} cleanup in Azkaban or Hadoop.
 */
public class TrashCollectorJob extends AbstractJob implements Tool {

  private Configuration conf;
  private Trash trash;

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new TrashCollectorJob(TrashCollectorJob.class.getName()), args);
  }

  public TrashCollectorJob(String id) throws IOException {
    super(id, Logger.getLogger(TrashCollectorJob.class));
  }

  public TrashCollectorJob(String id, Props props) throws IOException {
    super(id, Logger.getLogger(TrashCollectorJob.class));
    this.conf = new Configuration();
    this.trash = new Trash(FileSystem.get(getConf()), props);
  }

  @Override
  public int run(String[] args)
      throws Exception {
    if (args.length < 1) {
      System.out.println("Must provide properties file as first argument.");
      return 1;
    }
    Props props = new Props(null, args[0]);
    new TrashCollectorJob(TrashCollectorJob.class.getName(), props).run();
    return 0;
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void run()
      throws Exception {
    if(this.trash != null) {
      this.trash.createTrashSnapshot();
      this.trash.purgeTrashSnapshots();
    }

  }
}
