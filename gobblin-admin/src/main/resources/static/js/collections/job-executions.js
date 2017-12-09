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

/* global Backbone, Gobblin */
var app = app || {}

;(function () {
  var urlRoot = Gobblin.settings.restServerUrl + '/jobExecutions/';
  if (urlRoot.indexOf('http') !== 0) {
    urlRoot = 'http://' + urlRoot;
  }
  var JobExecutions = Backbone.Collection.extend({
    urlRoot: urlRoot,
    model: app.JobExecution,

    fetchCurrent: function (idType, id, params) {
      // Fetches using the custom rest.li url scheme
      params = params || ''
      var idString = idType === 'LIST_TYPE' ? 'org~2Eapache~2Egobblin~2Erest~2EQueryListType' : 'string'

      var generatedUrl = this.urlRoot + 'idType=' + idType
      generatedUrl += '&id.' + idString + '=' + id
      generatedUrl += this.jsonToParamString(params)
      console.log('Querying the the following URL: ' + generatedUrl)

      var options = {
        url: generatedUrl,
        reset: true,
        timeout: 10000
      }
      return Backbone.Collection.prototype.fetch.call(this, options)
    },
    parse: function (response) {
      return response.jobExecutions
    },
    jsonToParamString: function (params) {
      var paramString = ''
      for (var key in params) {
        paramString += '&' + key + '=' + params[key]
      }
      return paramString
    },
    hasExecuted: function() {
      var filtered = this.filter(function (e) {
        return e.get("launchedTasks") > 0;
      });
      return new JobExecutions(filtered)
    }
  })

  app.jobExecutions = new JobExecutions()
})()
