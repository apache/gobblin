package gobblin.test.execution;

import gobblin.test.setup.config.Step;

import java.util.Collection; 

/**
 * This interface will execute the entire Test which will execute the list of steps
 * 
 * @author sveerama
 *
 */

public interface TestHarnessExecutor
{
  public void executeTest(Collection<Step> steps);
}
