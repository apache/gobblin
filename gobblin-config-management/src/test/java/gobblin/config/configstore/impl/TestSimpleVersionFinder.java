package gobblin.config.configstore.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.testng.annotations.BeforeClass;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.config.configstore.impl.SimpleVersionFinder;


public class TestSimpleVersionFinder {

  private SimpleVersionFinder comp;

  @BeforeClass
  public void setUpClass() throws Exception {
    comp = new SimpleVersionFinder();
  }

  @Test
  public void testValid() throws Exception {
    List<String> input = new ArrayList<String>();
    input.add("V1");
    input.add("v2");

    input.add("V1.0");
    input.add("v1.1");

    input.add("v2.0.4");
    input.add("V2.0.2");

    Collection<String> valid = comp.getValidVersions(input);
    Assert.assertEquals(input.size(), valid.size());
    Iterator<String> validIt = valid.iterator();
    Iterator<String> inputIt = input.iterator();
    while (validIt.hasNext()) {
      Assert.assertTrue(inputIt.next().equals(validIt.next()));
    }

    String latest = comp.getCurrentVersion(input);
    Assert.assertTrue(latest.equals("v2.0.4"));
  }

  @Test
  public void testInvalid() throws Exception {
    List<String> input = new ArrayList<String>();
    input.add("a1");

    input.add("V1.x");
    input.add("V1.");

    input.add("v2.0.4.9");
    input.add("V1.1.2x");

    Collection<String> result = comp.getValidVersions(input);
    Assert.assertTrue(result.size() == 0);
  }
}
