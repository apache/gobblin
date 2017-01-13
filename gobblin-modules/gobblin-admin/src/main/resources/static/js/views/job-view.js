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

/* global Backbone, _, jQuery, Gobblin, Chart */
var app = app || {}

;(function ($) {
  app.JobView = Backbone.View.extend({
    el: '#main-content',

    headerTemplate: _.template($('#header-template').html()),
    contentTemplate: _.template($('#job-template').html()),
    chartTemplate: _.template($('#chart-template').html()),
    keyValueTemplate: _.template($('#key-value-template').html()),

    events: {
      'click #query-btn': '_fetchData'
    },

    initialize: function (jobName) {
      this.jobName = jobName
      this.collection = app.jobExecutions

      this.headerEl = this.$el.find('#header-container')
      this.contentEl = this.$el.find('#content-container')

      this.render()
    },

    render: function () {
      var self = this

      self.renderHeader()
      self.contentEl.html(self.contentTemplate({}))

      self.table = new app.TableView({
        el: '#job-table-container',
        collection: self.collection,
        columnSchema: 'listByJobName',
        includeJobToggle: false
      })

      self._fetchData()
    },

    _fetchData: function () {
      var self = this

      var opts = {
        limit: self.table.getLimit(),
        includeTaskExecutions: false,
        includeTaskMetrics: false,
        jobProperties: 'job.description,job.runonce,job.schedule',
        taskProperties: ''
      }
      self.collection.fetchCurrent('JOB_NAME', self.jobName, opts).done(function () {
        self.renderHeader(self.collection.first().getJobStateMapped())
        self.renderSummary()
        self.table.renderData()
      })
    },

    renderHeader: function (status) {
      var header = {
        title: 'Job Information',
        subtitle: this.jobName
      }
      if (typeof status !== 'undefined') {
        header.highlightClass = status
      }
      this.headerEl.html(this.headerTemplate({ header: header }))
    },

    renderSummary: function () {
      var jobData = this.getDurationAndRecordsRead()
      this.generateNewLineChart('Job Duration', jobData.labels, jobData.durations, '#duration-chart')
      this.generateNewLineChart('Records Read', jobData.labels, jobData.recordsRead, '#records-chart')
      this.generateKeyValue('Status', this.getStatusReport(), '#status-key-value')
    },
    getDurationAndRecordsRead: function (maxExecutions) {
      maxExecutions = maxExecutions || 10

      var values = {
        labels: [],
        durations: [],
        recordsRead: []
      }
      var max = this.collection.size() < maxExecutions ? this.collection.size() : maxExecutions
      for (var i = max - 1; i >= 0; i--) {
        var execution = this.collection.at(i)
        values.labels.push(execution.getJobStartTime())
        var time = execution.getDurationInSeconds() === '-' ? 0 : execution.getDurationInSeconds()
        values.durations.push(time)
        values.recordsRead.push(execution.getRecordsRead())
      }
      return values
    },
    getStatusReport: function () {
      var statuses = {}
      for (var i = 0; i < this.collection.size(); i++) {
        var execution = this.collection.at(i)
        statuses[execution.getJobState()] = (statuses[execution.getJobState()] || 0) + 1
      }
      return statuses
    },
    generateNewLineChart: function (title, labels, data, elemId) {
      var self = this

      var lineFormat = {
        fillColor: 'rgba(220,220,220,0.5)',
        strokeColor: 'rgba(220,220,220,1)',
        pointColor: 'rgba(220,220,220,1)',
        pointStrokeColor: '#fff',
        pointHighlightFill: '#fff',
        pointHighlightStroke: 'rgba(220,220,220,1)'
      }

      var chartData = {
        labels: labels,
        datasets: [
          $.extend({
            label: title,
            data: data
          }, lineFormat)
        ]
      }
      var chartElem = self.$el.find(elemId)
      chartElem.html(this.chartTemplate({
        title: title,
        height: 450,
        width: 600
      }))
      var ctx = chartElem.find('.chart-canvas')[0].getContext('2d')
      return new Chart(ctx).Line(chartData, { responsive: true })
    },
    generateKeyValue: function (title, keyValuePairs, elemId) {
      var self = this

      keyValuePairs = self.pseudoSortStates(keyValuePairs)
      self.$el.find(elemId).html(self.keyValueTemplate({ title: title, pairs: keyValuePairs, center: true }))
    },
    pseudoSortStates: function (data) {
      var newData = {}
      for (var key in Gobblin.stateMap) {
        if (data[key]) {
          newData[key] = data[key]
        }
      }
      return newData
    }
  })
})(jQuery)
