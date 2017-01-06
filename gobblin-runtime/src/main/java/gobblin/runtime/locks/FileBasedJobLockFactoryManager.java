/**
 *
 */
package gobblin.runtime.locks;

import java.io.IOException;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.annotation.Alias;
import gobblin.runtime.instance.hadoop.HadoopConfigLoader;

/**
 * Manages instances of {@link FileBasedJobLockFactory}.
 */
@Alias(value="file")
public class FileBasedJobLockFactoryManager
       extends AbstractJobLockFactoryManager<FileBasedJobLock, FileBasedJobLockFactory> {
  public static final String CONFIG_PREFIX = "job.lock.file";

  @Override
  protected Config getFactoryConfig(Config sysConfig) {
    return sysConfig.hasPath(CONFIG_PREFIX) ?
        sysConfig.getConfig(CONFIG_PREFIX) :
        ConfigFactory.empty();
  }

  @Override
  protected FileBasedJobLockFactory createFactoryInstance(final Optional<Logger> log,
                                                          final Config sysConfig,
                                                          final Config factoryConfig) {
    HadoopConfigLoader hadoopConfLoader = new HadoopConfigLoader(sysConfig);
    try {
      return FileBasedJobLockFactory.create(factoryConfig, hadoopConfLoader.getConf(), log);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create job lock factory: " +e, e);
    }
  }

}
