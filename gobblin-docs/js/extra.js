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
