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
