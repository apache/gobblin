package gobblin.util.test;

import lombok.EqualsAndHashCode;

import java.util.Random;


/**
 * Used for {@link gobblin.util.io.GsonInterfaceAdapterTest}.
 */
@EqualsAndHashCode(callSuper = true)
public class ExtendedClass extends BaseClass {

  private final int otherField = new Random().nextInt();

}
