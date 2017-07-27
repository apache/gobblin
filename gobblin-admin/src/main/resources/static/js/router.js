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

/* global Backbone */
/*eslint-disable no-new */
var app = app || {}

;(function () {
  var GobblinRouter = Backbone.Router.extend({
    routes: {
      '': 'index',
      'job/:name': 'job',
      'job-details/:id': 'jobDetails'
    },

    index: function () {
      Gobblin.ViewManager.showView(new app.OverView())
    },
    job: function (name) {
      Gobblin.ViewManager.showView(new app.JobView(name))
    },
    jobDetails: function (id) {
      Gobblin.ViewManager.showView(new app.JobExecutionView(id))
    }
  })

  app.gobblinRouter = new GobblinRouter()
  Backbone.history.start()
})()
