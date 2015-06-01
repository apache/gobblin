package gobblin.test.setup.config;

import java.util.Collection; 

import gobblin.test.execution.operator.Operator;

/**
 * This interface is to define individual steps associated to a config entry, this implements the Operator interface.
 * Essentially one step can have many operators and each operator has an execution
 * 
 * @author sveerama
 *
 */

public interface Step extends Operator
{
  
  /**
   * This method is used to generate step level operator(s).
   * The Operator is a sub-level task associated to the step
   * 
   *
   */
  public Collection<Operator> getOperators();
  
  /**
   * This method will execute the current step which in turn will execute list of operators
   *
   */
  public Boolean execute();
}
