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
    mainTemplate: _.template($('#main-template').html()),
    headerTemplate: _.template($('#header-template').html()),
    contentTemplate: _.template($('#job-execution-template').html()),
    summaryTemplate: _.template($('#summary-template').html()),

    events: {
      'click #query-btn': '_fetchData'
    },

    initialize: function (jobId) {
      var self = this
      self.setElement($('#main-content'))
      self.jobId = jobId
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
      if (self.propertiesTable) {
        if (self.propertiesTable.onBeforeClose) {
          self.propertiesTable.onBeforeClose()
        }
        self.propertiesTable.remove()
      }
      if (self.metricsTable) {
        if (self.metricsTable.onBeforeClose) {
          self.metricsTable.onBeforeClose()
        }
        self.metricsTable.remove()
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
        el: '#task-table-container',
        collection: self.collection,
        collectionResolver: function(c) {
          if (c) {
            return c.get(self.jobId).getTaskExecutions()
          }
          return {}
        },
        columnSchema: 'listTasksByJobId'
      })
      self.table.render()
      self.initialized = true

      return self._fetchData()
    },

    _fetchData: function () {
      var self = this

      var opts = {
        limit: 1,
        taskProperties: "",
        includeTaskMetrics: false
      }
      self.collection.fetchCurrent('JOB_ID', self.jobId, opts)
    },

    refreshData: function() {
      var self = this
      if (self.initialized) {
        self.model = self.collection.get(self.jobId)
        self.renderHeader(self.model.getJobStateMapped())
        self.refreshSummary()
      }
    },

    renderHeader: function (status) {
      var self = this
      var header = {
        title: 'Job Execution Details',
        subtitle: self.jobId
      }
      if (typeof status !== 'undefined') {
        header.highlightClass = status
      }
        self.headerEl.html(self.headerTemplate({ header: header }))
    },

    renderSummary: function () {
      var self = this
      self.generateSummary('About', self.getSummary(), '#important-key-value', false)
      self.propertiesTable = self.generateKeyValue('Job Properties', function(c) { return self.getProperties(c) }, '#job-properties-key-value', true)
      self.metricsTable = self.generateKeyValue('Metrics', function(c) { return self.getJobMetrics(c) }, '#job-metrics-key-value', true)
    },
    refreshSummary: function () {
      var self = this
      self.generateSummary('About', self.getSummary(), '#important-key-value', false)
    },
    generateSummary: function (title, keyValuePairs, elemId, center) {
      var self = this
      self.$el.find(elemId).html(self.summaryTemplate({
        title: title,
        pairs: keyValuePairs,
        center: center
      }))
    },
    generateKeyValue: function (title, keyValuePairResolver, elemId, center) {
      var self = this
      var propertiesTable = new app.KeyValueTableView({
        el: elemId,
        title: title,
        center: center,
        collection: self.collection,
        collectionResolver: keyValuePairResolver
      })
      propertiesTable.render()
      return propertiesTable
    },
    getSummary: function () {
      var self = this
      return {
        'Job Name': self.model ? self.model.getJobNameLink() : '',
        'Job Id': self.model ? self.model.getJobIdLink() : '',
        'State': self.model ? self.model.getJobStateElem() : '',
        'Completed/Launched Tasks': self.model ? self.model.getTaskRatio() : '',
        'Start Time': self.model ? self.model.getJobStartTime() : '',
        'End Time': self.model ? self.model.getJobEndTime() : '',
        'Duration (seconds)': self.model ? self.model.getDurationInSeconds() : '',
        'Launcher Type': self.model ? self.model.getLauncherType() : ''
      }
    },
    getProperties: function (collection) {
      var self = this
      var model = collection.get(self.jobId)
      if (model && model.hasProperties()) {
        return _.object(_.map(_.sortBy(_.keys(model.attributes.jobProperties)), function(key) {
          return [key, model.attributes.jobProperties[key]]
        }))
      }
      return {}
    },
    getJobMetrics: function (collection) {
      var self = this
      var model = collection.get(self.jobId)
      if (model && model.attributes.metrics) {
        var jobMetrics = model.attributes.metrics.filter(function (metric) {
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
