Table of Contents
-----------------

[TOC]

# Contributing to Gobblin

You can contribute to Gobblin in multiple ways. For resources and guides, please refer [here](/contributor/).

## Code Contributions

We observe standard Apache practices for code contributions. For code changes, we recommend forking the repository and making your local changes on a feature branch, then updating the Jira, and opening a pull request (PR). A committer will review the changes and merge it in once it is approved. For first time contributors to Gobblin, we do request that you fill out a [one-time survey](https://docs.google.com/a/linkedin.com/forms/d/e/1FAIpQLSeH-8so0m68et6kPvxEiCNqezL7k6cyOlz9W-6eXnk7LEkwiA/viewform), so that we can identify and credit you properly in the future.  

## Documentation Contributions

To make changes to the documentation modify the files under `gobblin-docs` as you would any other version controlled file. All documentation is checked into GitHub, so the process for making documentation changes is similar to how code changes are made (creating Pull Requests). If one wants to see what the rendered documentation looks like they simply need to take the following steps:

1. Install MkDocs locally, this page has directions on how to do so: http://www.mkdocs.org/#installation
2. Make sure you are in the top level directory for the Gobblin repo and execute `mkdocs serve`

These steps will start a local server to server the documentation, simply go to the URL show by the output of `mkdocs serve` and you should be able to see the documentation.

One the changes have been made and tested, create a PR and a committer will review and merge the documentation changes. Updates to the documentation page happen automatically everytime a commit is merged into the master branch; however, there may be a 10 to 15 minute delay before the changes actually show up.
