Overview
--------

The code formatting standard in this project is based on the [Oracle/Sun Code Convention](http://www.oracle.com/technetwork/java/codeconventions-150003.pdf) and [Google Java Style](http://google-styleguide.googlecode.com/svn/trunk/javaguide.html).  

Guideline
-------

The coding style is consistent with most of the open source projects with the following callout:

1. Naming Conventions
    * Variables are camel case beginning with a lowercase letter, e.g. `fooBar`
    * Constant variables are declared as static final and should be all uppercase ASCII letters delimited by underscore ("_"), e.g. `FOO_BAR`

1. Import statement
    * Do not use 'star' imports, e.g. `import java.io.*`;
    * Import order: `java`, `org`, `com`, `gobblin`.

1. Indentation
    * Two spaces should be used as the unit of indentation;
    * Tabs must expand to spaces and the tab width should be set to two;
    * Line length: lines should not exceed 120 characters;

1. White space
    * Blank lines should be provided to improve readability:
        * Between the local variables in a method and its first statement
        * Between methods
    * Blank spaces should be used in the following circumstances:
        * A keyword followed by a parenthesis should be separated by a space (e.g. `while (true) {`)
        * A binary operators except . should be separated from their operands by spaces (e.g. `a + b`);

1. Comments:
    * Implementation comments: Block comments (`/* ... */`), end-of-line comments (`//...`) can be used to illustrate a particular implementation;
    * Documentation comments (`/** ... */`) should be used to describe Java classes, interfaces, methods;

1. Compound statements are lists of statements enclosed in curly braces and should be formatted according to the following conventions:
    * The enclosed statements should be indented one more level than the enclosing statement
    * The opening brace should be on the same line as the enclosing statement (e.g. the 'if' clause)
    * The closing brace should be on a line by itself indented to match the enclosing statement
    * Braces are used around all statements, even single statements, when they are part of a control structure, such as if-else or for statements. This makes it easier to add statements without accidentally introducing bugs due to forgetting to add braces.

Code Style Template File
-------------------------
* Eclipse
    * Download the [codetyle-eclipse.xml](files/codestyle-eclipse.xml), Import the file through Preferences > Java > Code Style > Formatter
    * Download the [prefs-eclipse.epf](files/prefs-eclipse.epf), Import the file File > Import > General > Preferences
* IntelliJ
    * Download the [codestyle-intellij-gobblin.xml](files/codestyle-intellij-gobblin.xml), Copy the file to the appropriate codestyles directory for your installation. This is typically `~/.INTELLIJ_VERSION/config/codestyles` on Linux (or `$HOME/Library/Preferences/INTELLIJ_VERSION/codestyles` on Mac). The specific INTELLIJ_VERSION identifier will depend on your version; examples are IntelliJIdeal3, IdeaC15 etc.
    * Restart the IDE
    * Go to File > Settings > Code Style > General > Scheme to select the new style (LinkedIn Gobblin Style)
