package org.apache.gobblin.compaction.mapreduce.orc;

import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.testng.annotations.Test;


public class OrcValueMapperTest {
  @Test
  public void testIsEvolutionValid() {
    TypeDescription schema_1 = TypeDescription.fromString("struct<i:int,j:int,k:int>");
    TypeDescription schema_2 = TypeDescription.fromString("struct<i:int,j:int,k:bigint>");
    TypeDescription schema_3 = TypeDescription.fromString("struct<i:int,j:int,k:tinyint>");
    TypeDescription schema_4 = TypeDescription.fromString("struct<i:int,j:int>");
    Assert.assertTrue(OrcValueMapper.isEvolutionValid(schema_1, schema_2));
    Assert.assertTrue(OrcValueMapper.isEvolutionValid(schema_1, schema_3));
    Assert.assertTrue(OrcValueMapper.isEvolutionValid(schema_1, schema_4));
    Assert.assertTrue(OrcValueMapper.isEvolutionValid(schema_4, schema_1));
  }
}