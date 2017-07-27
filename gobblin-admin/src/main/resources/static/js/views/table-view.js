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

/* global Backbone, _, jQuery, Gobblin */
var app = app || {}

;(function ($) {
  app.TableView = Backbone.View.extend({
    tableControlTemplate: _.template($('#table-control-template').html()),
    tableTemplate: _.template($('#table-template').html()),
    tableBodyTemplate: _.template($('#table-body-template').html()),

    initialize: function (options) {
      var self = this
      self.setElement(options.el)
      self.collectionResolver = options.collectionResolver || function(c) { return c }
      self.collection = options.collection
      self.columnSchema = options.columnSchema
      self.includeJobToggle = options.includeJobToggle || false
      self.includeJobsWithTasksToggle = options.includeJobsWithTasksToggle || false
      self.hideJobsWithoutTasksByDefault = options.hideJobsWithoutTasksByDefault || Gobblin.settings.hideJobsWithoutTasksByDefault || false
      self.resultsLimit = options.resultsLimit || 100
      self.listenTo(self.collection, 'reset', self.refreshData);
    },

    render: function () {
      var self = this

      self.$el.find('#table-control-container').html(self.tableControlTemplate({
        includeJobToggle: self.includeJobToggle,
        includeJobsWithTasksToggle: self.includeJobsWithTasksToggle,
        hideJobsWithoutTasksByDefault: self.hideJobsWithoutTasksByDefault,
        resultsLimit: self.resultsLimit
      }))

      var columnHeaders = Gobblin.columnSchemas[self.columnSchema]

      self.$el.find('#table-container').html(self.tableTemplate({
        includeJobToggle: self.includeJobToggle,
        includeJobsWithTasksToggle: self.includeJobsWithTasksToggle,
        hideJobsWithoutTasksByDefault: self.hideJobsWithoutTasksByDefault,
        columnHeaders: columnHeaders
      }))

      self.$el.find('#table-container table tbody').html(self.tableBodyTemplate({
        data: []
      }))

      var sortList = []
      for (var i in columnHeaders) {
        if ('sortInitialOrder' in columnHeaders[i]) {
          if (columnHeaders[i].sortInitialOrder === 'asc') {
            sortList.push([parseInt(i), 0])
          } else if (columnHeaders[i].sortInitialOrder === 'desc') {
            sortList.push([parseInt(i), 1])
          }
        }
      }
      if (sortList.length == 0) {
        sortList.push([0,0])
      }

      // TODO attach elsewhere?
      self.$el.find('#jobs-table').tablesorter({
        theme: 'bootstrap',
        headerTemplate: '{content} {icon}',
        widthFixed: true,
        widgets: [ 'uitheme', 'filter' ],
        sortList: sortList
      })
      .tablesorterPager({
        container: self.$el.find('#jobs-table-pager'),
        output: '{startRow} - {endRow} / {filteredRows} ({totalRows})',
        fixedHeight: false,
        removeRows: true
      });
      self.initialized = true
    },

    refreshData: function() {
      var self = this
      if (self.initialized) {
        self.tableCollection = self.collectionResolver(self.collection)
        var columnHeaders = Gobblin.columnSchemas[self.columnSchema]
        var table = self.$el.find('#table-container table')
        var page = table[0].config.pager.page + 1
        var data = self.tableCollection.map(function (execution) {
          var row = []
          for (var i in columnHeaders) {
            row.push(execution[columnHeaders[i].fn]())
          }
          return row
        })
        var tableBody = table.find('tbody')
        tableBody.empty()
        tableBody.html(self.tableBodyTemplate({ data: data }))
        table.trigger('update', [true])
        table.trigger('pagerUpdate', page)
      }
    },

    getLimit: function () {
      var self = this
      var limitElem = self.$el.find('#results-limit')
      if (limitElem.val() === undefined || limitElem.val().length === 0) {
        return self.resultsLimit
      }
      return limitElem.val()
    }
  })
})(jQuery)
