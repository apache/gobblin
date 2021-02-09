Table of Contents
-----------------

[TOC]

# Documentation Overview

The documentation for Gobblin is based on [ReadTheDocs](https://readthedocs.org/) and [MkDocs](http://www.mkdocs.org/). MkDocs is used to convert MarkDown files to HTML, and ReadTheDocs is used to host the documentation.

# GitHub Wiki Limitations

Historically, documentation was hosted using the GitHub wiki. The problem is that only Gobblin committers can modify the wiki; any external contributors who want to update or add documentation cannot do so. Editing the Gobblin Wiki is also not PR based, so any committer can make changes without going through a review process.

# MkDocs

MkDocs is an open source tool that converts Python Flavored Markdown to HTML files. MkDocs has a number of pre-defined themes that can be used to display the MarkDown files. New themes can be added, or custom CSS and JavaScript can be added to modify existing themes. MkDocs is configured using `mkdocs.yml` file. This file also specifies a master Table of Contents for the entire website.

# ReadTheDocs

ReadTheDocs is an open source, free tool that can build documentation in a GitHub repository and host it for public use. ReadTheDocs links to a specified GitHub project, and on every push to the repository the documentation is updated. ReadTheDocs essentially clones the repo and builds the documentation using either Sphinx or MkDocs (for Gobblin we only use MkDocs). It then hosts the documentation on internal servers so any end user can view the documentation. ReadTheDocs has other very nice features such as versioning of documentation, exporting documentation to PDFs, and search. ReadTheDocs is configured via its UI, the home page for Gobblin on ReadTheDocs is: https://readthedocs.org/projects/gobblin/. The full documentation for ReadTheDocs can be found here: http://docs.readthedocs.org/

# Additional Information

For more information on how this architecture was decided and the different tradeoffs between other documentation services, check out the original PR: https://github.com/apache/gobblin/pull/788
