package gobblin.test.setup.config;

/**
 * An interface for parsing the config data for test harness. 
 * This interface is allows to parse and validate the config setting
 * 
 * @author sveerama
 *
 * @param <I> Input folder where the config (JSON/YAML) is stored
 * @param <C> The reader of the input for the config
 */

public interface ConfigReader<I, C>
{
  
  /**
   *  This method will parse the config based on the input provided
   *
   * @param inputFolder
   * @return config
   *
   */
  
  public C parseConfigEntry(I testConfigFolder); 

  /**
   *  This method will validate the config implementer, this allows to customize the config validation
   *   for each type of config 
   *
   * @param <C> config
   * @return Boolean
   *
   */

  public Boolean validateConfigEntry(C config); 
}

