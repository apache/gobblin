<p align="center">
  <img src=img/Gobblin-Logo.png alt="Gobblin Logo" height="200" width="400">
</p>

Over the years, LinkedIn's data infrastructure team built custom solutions for ingesting diverse data entities into our Hadoop eco-system. At one point, we were running 15 types of ingestion pipelines which created significant data quality, metadata management, development, and operation challenges.
 
Our experiences and challenges motivated us to build _Gobblin_. Gobblin is a universal data ingestion framework for extracting, transforming, and loading large volume of data from a variety of data sources, e.g., databases, rest APIs, FTP/SFTP servers, filers, etc., onto Hadoop. Gobblin handles the common routine tasks required for all data ingestion ETLs, including job/task scheduling, task partitioning, error handling, state management, data quality checking, data publishing, etc. Gobblin ingests data from different data sources in the same execution framework, and manages metadata of different sources all in one place. This, combined with other features such as auto scalability, fault tolerance, data quality assurance, extensibility, and the ability of handling data model evolution, makes Gobblin an easy-to-use, self-serving, and efficient data ingestion framework.

You can find a lot of useful resources in our wiki pages, including [how to get started with Gobblin](Getting-Started), an [architecture overview of Gobblin](Gobblin-Architecture), and
the [Gobblin user guide](user-guide/Gobblin-Deployment). We also provide a discussion group: [Google Gobblin-Users Group](https://groups.google.com/forum/#!forum/gobblin-users). Please feel free to post any questions or comments.

For a detailed overview, please take a look at the [VLDB 2015 paper](http://www.vldb.org/pvldb/vol8/p1764-qiao.pdf) and the [LinkedIn's Gobblin blog post](https://engineering.linkedin.com/data-ingestion/gobblin-big-data-ease).
