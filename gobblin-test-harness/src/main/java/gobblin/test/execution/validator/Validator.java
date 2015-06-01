package gobblin.test.execution.validator;

import java.util.Comparator;

/**
 *  An interface is for defining the validator for the test. The input can be the set of input files and the output will be be output of 
 * 
 * @author sveerama
 *
 * @param <I>
 * @param <O>
 */

public interface Validator<I, O> extends Comparator<O>
{
  /**
   * This method will implement the underlying validation 
   * 
   * @param input
   * @return will be the output of the execution
   */
  public O executeValidator(I input);
  /**
   * 
   * @return will be a boolean based on the validation process
   */
  public Boolean isValid();
}
