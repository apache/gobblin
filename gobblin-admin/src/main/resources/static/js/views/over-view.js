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

/* global Backbone, _, jQuery */
var app = app || {}

;(function ($) {
  app.OverView = Backbone.View.extend({
    mainTemplate: _.template($('#main-template').html()),
    headerTemplate: _.template($('#header-template').html()),
    contentTemplate: _.template($('#list-all-template').html()),

    events: {
      'click #query-btn': '_fetchData',
      'enter #results-limit': '_fetchData'
    },

    initialize: function () {
      var self = this
      self.setElement($('#main-content'))
      self.collection = app.jobExecutions
      if (Gobblin.settings.refreshInterval > 0) {
        self.timer = setInterval(function () {
          if (self.initialized) {
            self._fetchData()
          }
        }, Gobblin.settings.refreshInterval);
      }
    },

    onBeforeClose: function() {
      var self = this
      if (self.timer) {
        clearInterval(self.timer);
      }
      if (self.table) {
        if (self.table.onBeforeClose) {
          self.table.onBeforeClose()
        }
        self.table.remove()
      }
    },

    render: function () {
      var self = this

      self.$el.html(self.mainTemplate)
      self.headerEl = self.$el.find('#header-container')
      self.headerEl.html(self.headerTemplate({
        header: {
          title: 'Gobblin Jobs'
        }
      }))
      self.contentEl = self.$el.find('#content-container')
      self.contentEl.html(self.contentTemplate({}))

      self.table = new app.TableView({
        el: '#list-all-table-container',
        collection: self.collection,
        columnSchema: 'listJobs',
        includeJobToggle: true,
        includeJobsWithTasksToggle: true
      })
      self.table.render()
      self.initialized = true

      return self._fetchData()
    },

    _fetchData: function () {
      var self = this

      var includeJobsWithoutTasks = $('#list-jobs-with-tasks-toggle .active input').val() === "ALL"

      var opts = {
        limit: self.table.getLimit(),
        includeTaskExecutions: false,
        includeJobMetrics: false,
        includeTaskMetrics: false,
        includeJobsWithoutTasks: includeJobsWithoutTasks,
        jobProperties: 'job.description,job.runonce,job.schedule',
        taskProperties: ''
      }
      var id = $('#list-jobs-toggle .active input').val()
      self.collection.fetchCurrent('LIST_TYPE', id, opts)
    }
  })
})(jQuery)
