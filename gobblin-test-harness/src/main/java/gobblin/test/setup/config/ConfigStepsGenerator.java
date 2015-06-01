package gobblin.test.setup.config;


import java.util.Collection; 


/**
 * An interface for generating the steps associated to the config. The steps may include
 * copy of data or validating of the test. 
 * First the input to this interface is the config{@link ConfigReader#parseConfigEntry(I)} entry and the 
 * 
 * @author sveerama
 *
 * @param <C> Config for the test
 * @param <S> The steps for the test execution
 */


public interface ConfigStepsGenerator<C>
{
  /**
   *  This method will generate the list of execution steps associated to the config,
   * @param {@link ConfigReader}
   * @return List of config steps in {@link Step}
   */
  public Collection<Step> generateExecutionSteps(C config);
}
