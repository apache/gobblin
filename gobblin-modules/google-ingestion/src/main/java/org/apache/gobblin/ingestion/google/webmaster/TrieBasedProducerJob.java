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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Triple;


public class TrieBasedProducerJob extends ProducerJob {
  private final String _startDate;
  private final String _endDate;
  private final int _groupSize;
  private final Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode> _jobNode;

  TrieBasedProducerJob(String startDate, String endDate,
      Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode> jobNode, int groupSize) {
    _startDate = startDate;
    _endDate = endDate;
    _jobNode = jobNode;
    _groupSize = groupSize;
  }

  @Override
  public String getPage() {
    return _jobNode.getLeft();
  }

  @Override
  public String getStartDate() {
    return _startDate;
  }

  @Override
  public String getEndDate() {
    return _endDate;
  }

  @Override
  public GoogleWebmasterFilter.FilterOperator getOperator() {
    return _jobNode.getMiddle();
  }

  @Override
  public int getPagesSize() {
    if (isOperatorEquals()) {
      return 1;
    } else {
      return _jobNode.getRight().getSize();
    }
  }

  /**
   * The implementation here will first partition the job by pages, and then by dates.
   * @return
   */
  @Override
  public List<? extends ProducerJob> partitionJobs() {
    UrlTrieNode root = _jobNode.getRight();
    if (isOperatorEquals() || root.getSize() == 1) {
      //Either at an Equals-Node or a Leaf-Node, both of which actually has actual size 1.
      return super.partitionJobs();
    } else {
      if (_groupSize <= 1) {
        throw new RuntimeException("This is impossible. When group size is 1, the operator must be equals");
      }

      UrlTrie trie = new UrlTrie(getPage(), root);
      int gs = Math.min(root.getSize(), _groupSize);

      UrlTriePrefixGrouper grouper = new UrlTriePrefixGrouper(trie, (int) Math.ceil(gs / 2.0));

      List<TrieBasedProducerJob> jobs = new ArrayList<>();
      while (grouper.hasNext()) {
        jobs.add(new TrieBasedProducerJob(_startDate, _endDate, grouper.next(), grouper.getGroupSize()));
      }
      return jobs;
    }
  }

  private boolean isOperatorEquals() {
    return getOperator().equals(GoogleWebmasterFilter.FilterOperator.EQUALS);
  }

  @Override
  public String toString() {
    return String.format(
        "TrieBasedProducerJob{_page='%s', _startDate='%s', _endDate='%s', _operator='%s', _groupSize='%s', _nodeSize='%s'}",
        getPage(), _startDate, _endDate, getOperator(), _groupSize, _jobNode.getRight().getSize());
  }

  public int getGroupSize() {
    return _groupSize;
  }
}
