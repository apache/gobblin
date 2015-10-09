/* global Backbone, Gobblin */
var app = app || {}

;(function () {
  var JobExecutions = Backbone.Collection.extend({
    urlRoot: 'http://' + Gobblin.settings.restServerUrl + '/jobExecutions/',
    model: app.JobExecution,

    fetchCurrent: function (idType, id, params) {
      // Fetches using the custom rest.li url scheme
      params = params || ''
      var idString = idType === 'LIST_TYPE' ? 'gobblin~2Erest~2EQueryListType' : 'string'

      var generatedUrl = this.urlRoot + 'idType=' + idType
      generatedUrl += '&id.' + idString + '=' + id
      generatedUrl += this.jsonToParamString(params)
      console.log('Querying the the following URL: ' + generatedUrl)

      var options = {
        url: generatedUrl,
        reset: true,
        timeout: 10000
      }
      return Backbone.Collection.prototype.fetch.call(this, options)
    },
    parse: function (response) {
      return response.jobExecutions
    },
    jsonToParamString: function (params) {
      var paramString = ''
      for (var key in params) {
        paramString += '&' + key + '=' + params[key]
      }
      return paramString
    }
  })

  app.jobExecutions = new JobExecutions()
})()
