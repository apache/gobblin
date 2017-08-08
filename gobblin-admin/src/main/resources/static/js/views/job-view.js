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
    mainTemplate: _.template($('#main-template').html()),
    headerTemplate: _.template($('#header-template').html()),
    contentTemplate: _.template($('#job-template').html()),
    chartTemplate: _.template($('#chart-template').html()),
    summaryTemplate: _.template($('#summary-template').html()),

    events: {
      'click #query-btn': '_fetchData',
      'enter #results-limit': '_fetchData'
    },

    initialize: function (jobName) {
      var self = this
      self.setElement($('#main-content'))
      self.jobName = jobName
      self.collection = app.jobExecutions
      if (Gobblin.settings.refreshInterval > 0) {
        self.timer = setInterval(function() {
          if (self.initialized) {
            self._fetchData()
          }
        }, Gobblin.settings.refreshInterval)
      }
      self.listenTo(self.collection, 'reset', self.refreshData)
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
      self.contentEl = self.$el.find('#content-container')
      self.contentEl.html(self.contentTemplate({}))
      self.renderHeader()
      self.renderSummary()

      self.table = new app.TableView({
        el: '#job-table-container',
        collection: self.collection,
        columnSchema: 'listByJobName',
        includeJobToggle: false,
        includeJobsWithTasksToggle: true,
      })
      self.table.render()
      self.initialized = true

      self._fetchData()
    },

    _fetchData: function () {
      var self = this

      var includeJobsWithoutTasks = $('#list-jobs-with-tasks-toggle .active input').val() === "ALL"

      var opts = {
        limit: self.table.getLimit(),
        includeTaskExecutions: false,
        includeTaskMetrics: false,
        includeJobsWithoutTasks: includeJobsWithoutTasks,
        jobProperties: 'job.description,job.runonce,job.schedule',
        taskProperties: ''
      }
      self.collection.fetchCurrent('JOB_NAME', self.jobName, opts)
    },

    renderHeader: function (status) {
      var self = this
      var header = {
        title: 'Job Information',
        subtitle: self.jobName
      }
      if (typeof status !== 'undefined') {
        header.highlightClass = status
      }
      self.headerEl.html(self.headerTemplate({ header: header }))
    },
    refreshData: function() {
      var self = this
      if (self.initialized) {
        self.renderHeader(self.collection.last().getJobStateMapped())
        if (self.durationChart !== undefined || self.recordsChart !== undefined) {
          var jobData = self.getDurationAndRecordsRead()
          if (self.durationChart !== undefined) {
            self.durationChart.data.labels = jobData.labels
            self.durationChart.data.datasets[0].data = jobData.durations
            self.durationChart.update();
          }
          if (self.recordsChart !== undefined) {
            self.recordsChart.data.labels = jobData.labels
            self.recordsChart.data.datasets[0].data = jobData.recordsRead
            self.recordsChart.update();
          }
        }
        self.generateSummary('Status', self.getStatusReport(), '#status-key-value')
      }
    },
    renderSummary: function () {
      var self = this
      var jobData = self.getDurationAndRecordsRead()
      self.durationChart = self.generateNewLineChart('Job Duration', jobData.labels, jobData.durations, '#duration-chart')
      self.recordsChart = self.generateNewLineChart('Records Read', jobData.labels, jobData.recordsRead, '#records-chart')
      self.generateSummary('Status', self.getStatusReport(), '#status-key-value')
    },
    getDurationAndRecordsRead: function (maxExecutions) {
      var self = this
      maxExecutions = maxExecutions || 10

      var values = {
        labels: [],
        durations: [],
        recordsRead: []
      }
      var executedCollection = self.collection.hasExecuted();
      var min = executedCollection.size() < maxExecutions ? 0 : executedCollection.size() - maxExecutions;
      for (var i = min; i < executedCollection.size(); i++) {
        var execution = executedCollection.at(i)
        values.labels.push(execution.getJobStartTime())
        var time = execution.getDurationInSeconds() === '-' ? 0 : execution.getDurationInSeconds()
        values.durations.push(time)
        values.recordsRead.push(execution.getRecordsRead())
      }
      return values
    },
    getStatusReport: function () {
      var self = this
      var statuses = {}
      for (var i = 0; i < self.collection.size(); i++) {
        var execution = self.collection.at(i)
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
        type: 'line',
        data: {
          labels: labels,
          datasets: [
            $.extend({
              label: title,
              data: data
            }, lineFormat)
          ]
        },
        options: {
          legend: {
              display: false
          },
          scales: {
            yAxes: [{
              ticks: {
                beginAtZero: true,
                userCallback: function(label, index, labels) {
                  if (Math.floor(label) === label) {
                    return label;
                  }
                }
              }
            }]
          }
        }
      }
      var chartElem = self.$el.find(elemId)
      chartElem.html(self.chartTemplate({
        title: title,
        height: 450,
        width: 600
      }))
      var ctx = chartElem.find('.chart-canvas')[0].getContext('2d')
      return new Chart(ctx, chartData)
    },
    generateSummary: function (title, keyValuePairs, elemId) {
      var self = this

      keyValuePairs = self.pseudoSortStates(keyValuePairs)
      self.$el.find(elemId).html(self.summaryTemplate({ title: title, pairs: keyValuePairs, center: true }))
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
