package gobblin.dataset.config;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestVersionPattern {
  @Test public void testValid() throws Exception {
    List<String> input = new ArrayList<String>();
    input.add("V1");
    input.add("v2");
    
    input.add("V1.0");
    input.add("v1.1");
    
    input.add("v2.0.4");
    input.add("V2.0.2");
    
    List<String> valid = VersionPattern.getValidVersions(input);
    Assert.assertEquals(input.size(), valid.size());
    for(int i=0; i<input.size(); i++){
      Assert.assertTrue(input.get(i).equals(valid.get(i)));
    }
    
    String latest = VersionPattern.getLatestVersion(input);
    Assert.assertTrue(latest.equals("v2.0.4"));
  }
  
  @Test public void testInvalid() throws Exception {
    List<String> input = new ArrayList<String>();
    input.add("a1");
    
    input.add("V1.x");
    
    input.add("v2.0.4.9");
    input.add("V1.1.2x");
    
    List<String> result = VersionPattern.getValidVersions(input);
    Assert.assertTrue(result.size()==0);
  }
}
