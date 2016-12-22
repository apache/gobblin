/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.compliance;

/**
 * Each Hive Dataset using gobblin-compliance for their compliance needs, must contain a dataset.descriptor property
 * in the tblproperties of a Hive Dataset.
 *
 * A dataset.descriptor is a description of a Hive dataset in the Json format.
 *
 * A compliance id is a column name in a Hive dataset to decide which records should be purged.
 *
 * A dataset.descriptor must contain an identifier whose value corresponds the column name containing compliance id.
 *
 * Path to the identifier must be specified in the job properties file
 * via property dataset.descriptor.identifier.
 *
 * Example : dataset.descriptor = {Database : Repos, Owner : GitHub, ComplianceInfo : {IdentifierType : GitHubId}}
 * If IdentifierType corresponds to the identifier and GithubId is the compliance id column name, then
 * dataset.descriptor.identifier = ComplianceInfo.IdentifierType
 *
 * @author adsharma
 */
public interface DatasetDescriptor {
  public String getComplianceId();
}
