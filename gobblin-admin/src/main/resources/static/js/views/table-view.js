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

    initialize: function (options) {
      this.setElement(options.el)
      this.collection = options.collection
      this.columnSchema = options.columnSchema
      this.includeJobToggle = options.includeJobToggle || false
      this.resultsLimit = options.resultsLimit || 100

      this.render()
    },

    render: function () {
      var self = this

      self.$el.find('#table-control-container').html(self.tableControlTemplate({
        includeJobToggle: self.includeJobToggle,
        resultsLimit: self.resultsLimit
      }))
    },

    renderData: function () {
      // Data should be fetched by parent view before calling this function
      var self = this
      var columnHeaders = Gobblin.columnSchemas[self.columnSchema]

      self.$el.find('#table-container').html(self.tableTemplate({
        includeJobToggle: self.includeJobToggle,
        columnHeaders: columnHeaders,
        data: self.collection.map(function (execution) {
          var row = []

          for (var i in columnHeaders) {
            row.push(execution[columnHeaders[i].fn]())
          }
          return row
        })
      }))

      // TODO attach elsewhere?
      $('table').tablesorter({
        theme: 'bootstrap',
        headerTemplate: '{content} {icon}',
        widgets: [ 'uitheme' ]
      })
    },

    getLimit: function () {
      var limitElem = this.$el.find('#results-limit')
      if (limitElem.val().length === 0) {
        return limitElem.attr('placeholder')
      }
      return limitElem.val()
    }
  })
})(jQuery)
