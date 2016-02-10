/* global Backbone, _, jQuery */
var app = app || {}

;(function ($) {
  app.OverView = Backbone.View.extend({
    el: '#main-content',

    headerTemplate: _.template($('#header-template').html()),
    contentTemplate: _.template($('#list-all-template').html()),

    events: {
      'click #query-btn': '_fetchData'
    },

    initialize: function () {
      this.collection = app.jobExecutions

      this.headerEl = this.$el.find('#header-container')
      this.contentEl = this.$el.find('#content-container')

      this.render()
    },

    render: function () {
      var self = this

      self.headerEl.html(self.headerTemplate({
        header: {
          title: 'Gobblin Jobs'
        }
      }))
      self.contentEl.html(self.contentTemplate({}))

      self.table = new app.TableView({
        el: '#list-all-table-container',
        collection: self.collection,
        columnSchema: 'listJobs',
        includeJobToggle: true
      })

      self._fetchData()
    },

    _fetchData: function () {
      var self = this

      var opts = {
        limit: self.table.getLimit(),
        includeTaskExecutions: false,
        includeJobMetrics: false,
        includeTaskMetrics: false,
        jobProperties: 'job.description,job.runonce,job.schedule',
        taskProperties: ''
      }
      var id = $('#list-jobs-toggle .active input').val()
      self.collection.fetchCurrent('LIST_TYPE', id, opts).done(function () {
        self.table.renderData()
      })
    }
  })
})(jQuery)
