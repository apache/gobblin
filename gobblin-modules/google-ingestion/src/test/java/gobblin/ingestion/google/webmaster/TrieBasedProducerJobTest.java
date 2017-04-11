/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.ingestion.google.webmaster;

import java.util.List;

import org.apache.commons.lang3.tuple.Triple;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class TrieBasedProducerJobTest {
  private String _property = "www.linkedin.com/";

  @Test
  public void testPartitionJobs() throws Exception {
    UrlTrie trie = UrlTriePostOrderIteratorTest.getUrlTrie2(_property);
    UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, 4);
    String startDate = "2016-11-29";
    String endDate = "2016-11-30";

    Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode> node0 = grouper.next();
    TrieBasedProducerJob job0 = new TrieBasedProducerJob(startDate, endDate, node0, 100);
    Assert.assertEquals(job0.getPage(), _property + "0");
    Assert.assertEquals(job0.getOperator(), GoogleWebmasterFilter.FilterOperator.CONTAINS);
    Assert.assertEquals(job0.getStartDate(), startDate);
    Assert.assertEquals(job0.getEndDate(), endDate);
    Assert.assertEquals(job0.getPagesSize(), 3);

    List<? extends ProducerJob> granularJobs = job0.partitionJobs();
    Assert.assertEquals(granularJobs.size(), 3);
    ProducerJob job03 = granularJobs.get(0);
    Assert.assertEquals(job03.getPage(), _property + "03");
    Assert.assertEquals(job03.getOperator(), GoogleWebmasterFilter.FilterOperator.CONTAINS);
    Assert.assertEquals(((TrieBasedProducerJob) job03).getGroupSize(), 2);
    List<? extends ProducerJob> job03Dates = job03.partitionJobs();
    Assert.assertEquals(job03Dates.size(), 2);
    Assert.assertEquals(job03Dates.get(0), new SimpleProducerJob(_property + "03", startDate, startDate));
    Assert.assertEquals(job03Dates.get(1), new SimpleProducerJob(_property + "03", endDate, endDate));

    ProducerJob job04 = granularJobs.get(1);
    Assert.assertEquals(job04.getPage(), _property + "04");
    Assert.assertEquals(job04.getOperator(), GoogleWebmasterFilter.FilterOperator.CONTAINS);
    Assert.assertEquals(((TrieBasedProducerJob) job04).getGroupSize(), 2);
    List<? extends ProducerJob> job04Dates = job04.partitionJobs();
    Assert.assertEquals(job04Dates.size(), 2);
    Assert.assertEquals(job04Dates.get(0), new SimpleProducerJob(_property + "04", startDate, startDate));
    Assert.assertEquals(job04Dates.get(1), new SimpleProducerJob(_property + "04", endDate, endDate));

    ProducerJob job0Only = granularJobs.get(2);
    Assert.assertEquals(job0Only.getPage(), _property + "0");
    Assert.assertEquals(job0Only.getOperator(), GoogleWebmasterFilter.FilterOperator.EQUALS);
    Assert.assertEquals(((TrieBasedProducerJob) job0Only).getGroupSize(), 2);
    List<? extends ProducerJob> job0OnlyDates = job0Only.partitionJobs();
    Assert.assertEquals(job0OnlyDates.size(), 2);
    Assert.assertEquals(job0OnlyDates.get(0), new SimpleProducerJob(_property + "0", startDate, startDate));
    Assert.assertEquals(job0OnlyDates.get(1), new SimpleProducerJob(_property + "0", endDate, endDate));

    Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode> node1 = grouper.next();
    TrieBasedProducerJob job1 = new TrieBasedProducerJob(startDate, endDate, node1, grouper.getGroupSize());
    Assert.assertEquals(job1.getPage(), _property + "1");
    Assert.assertEquals(job1.getOperator(), GoogleWebmasterFilter.FilterOperator.CONTAINS);
    Assert.assertEquals(job1.getStartDate(), startDate);
    Assert.assertEquals(job1.getEndDate(), endDate);
    Assert.assertEquals(job1.getPagesSize(), 1);
    Assert.assertEquals(job1.partitionJobs().size(), 2);

    Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode> node2 = grouper.next();
    TrieBasedProducerJob job2 = new TrieBasedProducerJob(startDate, endDate, node2, grouper.getGroupSize());
    Assert.assertEquals(job2.getPage(), _property + "2");
    Assert.assertEquals(job2.getOperator(), GoogleWebmasterFilter.FilterOperator.CONTAINS);
    Assert.assertEquals(job2.getStartDate(), startDate);
    Assert.assertEquals(job2.getEndDate(), endDate);
    Assert.assertEquals(job2.getPagesSize(), 4);

    List<? extends ProducerJob> job2Partitions = job2.partitionJobs();
    Assert.assertEquals(job2Partitions.size(), 3);
    ProducerJob job5 = job2Partitions.get(0);
    Assert.assertEquals(job5.getPage(), _property + "25");
    Assert.assertEquals(job5.getOperator(), GoogleWebmasterFilter.FilterOperator.CONTAINS);
    Assert.assertEquals(job5.getStartDate(), startDate);
    Assert.assertEquals(job5.getEndDate(), endDate);
    Assert.assertEquals(job5.getPagesSize(), 2);
    Assert.assertEquals(job5.partitionJobs().size(), 2);

    ProducerJob job6 = job2Partitions.get(1);
    Assert.assertEquals(job6.getPage(), _property + "26");
    Assert.assertEquals(job6.getOperator(), GoogleWebmasterFilter.FilterOperator.CONTAINS);
    Assert.assertEquals(job6.getStartDate(), startDate);
    Assert.assertEquals(job6.getEndDate(), endDate);
    Assert.assertEquals(job6.getPagesSize(), 1);

    ProducerJob job2Only = job2Partitions.get(2);
    Assert.assertEquals(job2Only.getPage(), _property + "2");
    Assert.assertEquals(job2Only.getOperator(), GoogleWebmasterFilter.FilterOperator.EQUALS);
    Assert.assertEquals(job2Only.getStartDate(), startDate);
    Assert.assertEquals(job2Only.getEndDate(), endDate);
    Assert.assertEquals(job2Only.getPagesSize(), 1);

    Assert.assertFalse(grouper.hasNext());
  }
}