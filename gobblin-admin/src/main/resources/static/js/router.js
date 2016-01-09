/* global Backbone */
/*eslint-disable no-new */
var app = app || {}

;(function () {
  var GobblinRouter = Backbone.Router.extend({
    routes: {
      '': 'index',
      'job/:name': 'job',
      'job-details/:id': 'jobDetails'
    },

    index: function () {
      new app.OverView()
    },
    job: function (name) {
      new app.JobView(name)
    },
    jobDetails: function (id) {
      new app.JobExecutionView(id)
    }
  })

  app.gobblinRouter = new GobblinRouter()
  Backbone.history.start()
})()
