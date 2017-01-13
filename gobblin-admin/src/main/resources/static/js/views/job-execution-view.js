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
  app.JobExecutionView = Backbone.View.extend({
    el: '#main-content',

    headerTemplate: _.template($('#header-template').html()),
    contentTemplate: _.template($('#job-execution-template').html()),
    keyValueTemplate: _.template($('#key-value-template').html()),

    events: {
      'click #query-btn': '_fetchData'
    },

    initialize: function (jobId) {
      this.jobId = jobId
      this.collection = app.jobExecutions
      this.model = {}

      this.headerEl = this.$el.find('#header-container')
      this.contentEl = this.$el.find('#content-container')

      this.render()
    },

    render: function () {
      this.renderHeader()
      this.contentEl.html(this.contentTemplate({}))

      this._fetchData()
    },

    _fetchData: function () {
      var self = this

      var opts = {
        limit: 1,
        taskProperties: "",
        includeTaskMetrics: false
      }
      self.collection.fetchCurrent('JOB_ID', self.jobId, opts).done(function () {
        self.model = self.collection.get(self.jobId)
        self.renderHeader(self.model.getJobStateMapped())
        self.renderSummary()

        self.table = new app.TableView({
          el: '#task-table-container',
          collection: self.model.getTaskExecutions(),
          columnSchema: 'listTasksByJobId',
          includeJobToggle: false
        })
        self.table.renderData()
      })
    },

    renderHeader: function (status) {
      var header = {
        title: 'Job Execution Details',
        subtitle: this.jobId
      }
      if (typeof status !== 'undefined') {
        header.highlightClass = status
      }
      this.headerEl.html(this.headerTemplate({ header: header }))
    },

    renderSummary: function () {
      this.generateKeyValue('About', this.getSummary(), '#important-key-value', false)
      this.generateKeyValue('Job Properties', this.getProperties(), '#job-properties-key-value .well', true)
      this.generateKeyValue('Metrics', this.getJobMetrics(), '#job-metrics-key-value .well', true)
    },
    generateKeyValue: function (title, keyValuePairs, elemId, center) {
      this.$el.find(elemId).html(this.keyValueTemplate({
        title: title,
        pairs: keyValuePairs,
        center: center
      }))
    },
    getSummary: function () {
      return {
        'Job Name': this.model.getJobNameLink(),
        'Job Id': this.model.getJobIdLink(),
        'State': this.model.getJobStateElem(),
        'Completed/Launched Tasks': this.model.getTaskRatio(),
        'Start Time': this.model.getJobStartTime(),
        'End Time': this.model.getJobEndTime(),
        'Duration (seconds)': this.model.getDurationInSeconds(),
        'Launcher Type': this.model.getLauncherType()
      }
    },
    getProperties: function () {
      if (this.model.hasProperties()) {
        return this.model.attributes.jobProperties
      }
      return {}
    },
    getJobMetrics: function () {
      if (this.model.attributes.metrics) {
        var jobMetrics = this.model.attributes.metrics.filter(function (metric) {
          return metric.group === 'JOB'
        })

        var metrics = {}
        for (var i in jobMetrics) {
          metrics[jobMetrics[i].name] = jobMetrics[i].value
        }
        return metrics
      }
      return {}
    }
  })
})(jQuery)
