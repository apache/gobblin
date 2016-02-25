package gobblin.util.test;

import lombok.EqualsAndHashCode;

import java.util.Random;


/**
 * Used for {@link gobblin.util.io.GsonInterfaceAdapterTest}.
 */
@EqualsAndHashCode
public class BaseClass {

  public BaseClass() {
    this.field = Integer.toString(new Random().nextInt());
  }

  private String field;

}
