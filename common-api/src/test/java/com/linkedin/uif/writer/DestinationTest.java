package com.linkedin.uif.writer;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.State;


/**
 * Unit tests for {@link Destination}.
 *
 * @author ynli
 */
@Test(groups = {"com.linkedin.uif.writer"})
public class DestinationTest {

  @Test
  public void testMethods() {
    State state = new State();
    state.setProp("foo", "bar");
    Destination destination = Destination.of(Destination.DestinationType.HDFS, state);
    Assert.assertEquals(destination.getType(), Destination.DestinationType.HDFS);
    Assert.assertEquals(destination.getProperties().getPropertyNames().size(), 1);
    Assert.assertEquals(destination.getProperties().getProp("foo"), "bar");
  }
}
