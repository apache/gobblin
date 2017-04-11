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

import java.util.Iterator;

import org.apache.commons.lang3.tuple.Triple;


/**
 * Package the URL pages/nodes into groups given the group size while traversing the UrlTrie by utilizing a TrieIterator. If the current node is not a "leaf" node defined by the TrieIterator, then a "fake" group of size 1 will be created by only including this node.
 *
 * Iterating the groups with a Triple type return value:
 *
 * Triple.1 is this group's root URL. The full URL to the root node of the group.
 * Triple.2 is the FilterOperator type for this group.
 *      case 1. If the descendants of this group is <= groupSize, the operator is FilterOperator.CONTAINS. This is a real group.
 *      case 2. Otherwise, the node will only be returned if it exists, with the operator FilterOperator.EQUALS. This group is actually a single value.
 * Triple.3 is the root node of this group.
 */
public interface UrlGrouper extends Iterator<Triple<String, GoogleWebmasterFilter.FilterOperator, UrlTrieNode>> {
  int getGroupSize();
}
