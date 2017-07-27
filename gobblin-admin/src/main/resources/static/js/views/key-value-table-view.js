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
  app.KeyValueTableView = Backbone.View.extend({
    tableTemplate: _.template($('#key-value-table-template').html()),
    tableBodyTemplate: _.template($('#key-value-table-body-template').html()),

    initialize: function (options) {
      var self = this
      self.setElement(options.el)
      self.title = options.title
      self.center = options.center
      self.collection = options.collection
      self.collectionResolver = options.collectionResolver || function(c) { return c }

      self.listenTo(self.collection, 'reset', self.refreshData);
    },

    render: function () {
      // Data should be fetched by parent view before calling this function
      var self = this
      self.$el.find('#key-value-table-container').html(self.tableTemplate({
        title: self.title,
        center: self.center,
        data: []
      }))

      // TODO attach elsewhere?
      self.$el.find('#key-value-table').tablesorter({
        theme: 'bootstrap',
        headerTemplate: '{content} {icon}',
        widthFixed: true,
        widgets: [ 'uitheme', 'filter' ]
      })
      .tablesorterPager({
        container: self.$el.find('#key-value-table-pager'),
        output: '{startRow} - {endRow} / {filteredRows} ({totalRows})',
        fixedHeight: false,
        removeRows: true
      });
      self.initialized = true
    },

    refreshData: function() {
      var self = this
      if (self.initialized) {
        var table = self.$el.find('#key-value-table-container table')
        var page = table[0].config.pager.page + 1
        var data = self.collectionResolver(self.collection)
        var tableBody = table.find('tbody')
        tableBody.empty()
        tableBody.html(self.tableBodyTemplate({ data: data }))
        table.trigger('update', [true])
        table.trigger('pagerUpdate', page)
      }
    }
  })
})(jQuery)
