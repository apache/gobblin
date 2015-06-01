package gobblin.test.execution.operator;


/**
 * An interface for defining the operator , the operator could be a copy of file or converting a file from one format to another
 * 
 * @author sveerama
 */

public interface Operator
{
  /**
   * This method is invoked to execute an operator. The operator will have an associated execution process.
   * @return the success of execution for the operator
   */
  public Boolean executeOperator();

}
