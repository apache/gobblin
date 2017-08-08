Table of Contents
-------------------

[TOC]

Overview
--------------------
The job configuration template is implemented for saving efforts of Gobblin users. For a specific type of job, e.g. Gobblin-Kafka data pulling, there exists quite amount of repetitive options to fill in. We are aiming at moving those repetitive options into a template for specific type of job, only exposing some essential configurable options for user to specify. This does not sacrifice flexibility, users can still specify options that already exist in the template to override the default value.

Here is the `.pull` file for wikipedia example with template support:

```
job.template=templates/wikiSample.template
source.page.titles=NASA,LinkedIn,Parris_Cues,Barbara_Corcoran

```  

How to Use Templates
--------------------
Users need only submit the `.pull` file above to the specified directory as described in wikipedia example. Although there are far fewer options there are still some mandatory options to specify in `.pull` file. 

In general, to use a template:
- Specify which template to use in the key `job.template`.
- All the keys specified in `gobblin.template.required_attributes` must be provided.
- As mentioned before, user can also specify existing options in template to override the default value. 


Available Templates 
--------------------
- wikiSample.template 
- gobblin-kafka.template 


Templates above are available on Github repo. 


How to Create Your Own Template 
--------------------
To create a template, simply create a file with all the common configurations for that template (recommended to use `.template` extension). Place this file into Gobblin's classpath, and set `job.template` to the path to that file in the classpath.

For reference, this is how the Wikipedia template looks:

```
job.name=PullFromWikipedia
job.group=Wikipedia
job.description=A getting started example for Gobblin

source.class=org.apache.gobblin.example.wikipedia.WikipediaSource
source.revisions.cnt=5

wikipedia.api.rooturl=https://en.wikipedia.org/w/api.php?format=json&action=query&prop=revisions&rvprop=content|timestamp|user|userid|size
wikipedia.avro.schema={"namespace": "example.wikipedia.avro","type": "record","name": "WikipediaArticle","fields": [{"name": "pageid", "type": ["double", "null"]},{"name": "title", "type": ["string", "null"]},{"name": "user", "type": ["string", "null"]},{"name": "anon", "type": ["string", "null"]},{"name": "userid",  "type": ["double", "null"]},{"name": "timestamp", "type": ["string", "null"]},{"name": "size",  "type": ["double", "null"]},{"name": "contentformat",  "type": ["string", "null"]},{"name": "contentmodel",  "type": ["string", "null"]},{"name": "content", "type": ["string", "null"]}]}

converter.classes=org.apache.gobblin.example.wikipedia.WikipediaConverter

extract.namespace=org.apache.gobblin.example.wikipedia

writer.destination.type=HDFS
writer.output.format=AVRO
writer.partitioner.class=org.apache.gobblin.example.wikipedia.WikipediaPartitioner

data.publisher.type=org.apache.gobblin.publisher.BaseDataPublisher

gobblin.template.required_attributes=source.page.titles

```

How does Template Work in Gobblin
--------------------

Currently Gobblin stores and loads existing templates as resources in the classpath. Gobblin will then resolve this template with the user-specified `.pull` file. Note that there is an option in template named  `gobblin.template.required_attributes` which lists all options that are required for users to fill in. If any of options in the required list is absent, the configuration will be detected as invalid by Gobblin throw an runtime excpetion accordingly.

Gobblin provides methods to retrieve all options inside `.template` file and resolved configuration option list. These interactive funtions will be integrated soon.