(function () {
  $(document).ready(function () {
    fixSearch();
  });

  /*
   * RTD messes up MkDocs' search feature by tinkering with the search box defined in the theme, see
   * https://github.com/rtfd/readthedocs.org/issues/1088. This function sets up a DOM4 MutationObserver
   * to react to changes to the search form (triggered by RTD on doc ready). It then reverts everything
   * the RTD JS code modified.
   *
   * This method was copied from the following commit on the nodemcu/nodemcu-firmware project:
   * https://github.com/nodemcu/nodemcu-firmware/commit/7dd89dd15ef993c9740ba4d4361e6be8edd1284b
   * special thanks to @marcelstoer for implementing this workaround
   */
  function fixSearch() {
    var target = document.getElementById('rtd-search-form');
    var config = {attributes: true, childList: true};

    var observer = new MutationObserver(function(mutations) {
      // if it isn't disconnected it'll loop infinitely because the observed element is modified
      observer.disconnect();
      var form = $('#rtd-search-form');
      form.empty();
      form.attr('action', 'https://' + window.location.hostname + '/en/' + determineSelectedBranch() + '/search.html');
      $('<input>').attr({
        type: "text",
        name: "q",
        placeholder: "Search docs"
      }).appendTo(form);
    });

    if (window.location.origin.indexOf('readthedocs') > -1) {
      observer.observe(target, config);
    }
  }

  /**
   * Analyzes the URL of the current page to find out what the selected GitHub branch is. It's usually
   * part of the location path. The code needs to distinguish between running MkDocs standalone
   * and docs served from RTD. If no valid branch could be determined 'dev' returned.
   *
   * This method was copied from the following commit on the nodemcu/nodemcu-firmware project:
   * https://github.com/nodemcu/nodemcu-firmware/commit/7dd89dd15ef993c9740ba4d4361e6be8edd1284b
   * special thanks to @marcelstoer for implementing this workaround
   *
   * @returns GitHub branch name
   */
  function determineSelectedBranch() {
    var branch = 'dev', path = window.location.pathname;
    if (window.location.origin.indexOf('readthedocs') > -1) {
      // path is like /en/<branch>/<lang>/build/ -> extract 'lang'
      // split[0] is an '' because the path starts with the separator
      branch = path.split('/')[2];
    }
    return branch;
  }
}());
