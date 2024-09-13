package org.apache.gobblin.temporal.ddm.activity.impl;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.temporal.api.common.v1.Payload;
import io.temporal.common.converter.JacksonJsonPayloadConverter;
import io.temporal.common.converter.PayloadConverter;

import org.apache.gobblin.temporal.ddm.activity.DeleteWorkDirsActivity;
import org.apache.gobblin.temporal.ddm.work.DirDeletionResult;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;


public class DeleteWorkDirsActivityImplTest {

  @Test
  public void testEmptyDeleteSupportsSerde() {
    DeleteWorkDirsActivity deleteWorkDirsActivity = new DeleteWorkDirsActivityImpl();
    WUProcessingSpec workSpec = new WUProcessingSpec();
    Set<String> workDirPaths = new HashSet<>();
    DirDeletionResult result = deleteWorkDirsActivity.delete(workSpec, null, workDirPaths);
    PayloadConverter converter = new JacksonJsonPayloadConverter();
    Optional<Payload> payload = converter.toData(result);
    DirDeletionResult result2 = converter.fromData(payload.get(), DirDeletionResult.class, DirDeletionResult.class);
    Assert.assertEquals(result, result2);
  }
}
