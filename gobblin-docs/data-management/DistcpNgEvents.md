# Table of contents
[TOC]

# Copy publisher monitoring events

The following metadata attributes are shared across all events:

- Standard execution metadata (TODO add link)
- `namespace=org.apache.gobblin.copy.CopyDataPublisher`
- `metadata["class"]=org.apache.gobblin.data.management.copy.publisher.CopyDataPublisher`

Events by `name`:

- `DatasetPublished` - a datasets gets successfully copied and published
    - `datasetUrn` - the URN of the dataset (dataset-specific) that was copied
    - `partition` - the partition (dataset-specific) that was copied
    - `originTimestamp` - the timestamp of the dataset partition as generated in the origin (e.g. the database)
    - `upstreamTimestamp` - the timestamp of the dataset partition as made available in the upstream system; this will be equal to `originTimestamp` if the data is read directly from the origin.

- `DatasetPublishFailed` - a dataset failed to publish 
    - `datasetUrn` - the URN of the dataset (dataset-specific) whose publish failed


- `FilePublished` - sent when an individual file gets published
    - `datasetUrn` - the URN of the dataset (dataset-specific) of which the part is part
    - `partition` - the partition (dataset-specific) of which this file is part
    - `SourcePath` - the full source path for the file 
    - `TargetPath` - the full destination path for the file 
    - `originTimestamp` - similar to `originTimestamp` for `DatasetPublished` events
    - `upstreamTimestamp` - similar to `upstreamTimestamp` for `DatasetPublished` events
    - `SizeInBytes` - the size in bytes of the file
