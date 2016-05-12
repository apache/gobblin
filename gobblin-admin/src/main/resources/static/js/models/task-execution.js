/* global Backbone, jQuery, Gobblin */
var app = app || {}

;(function ($) {
  app.TaskExecution = Backbone.Model.extend({
    idAttribute: 'taskId',

    hasMetrics: function () {
      return this.attributes.metrics && this.attributes.metrics.length > 0
    },
    hasProperties: function () {
      return this.attributes.taskProperties && !$.isEmptyObject(this.attributes.taskProperties)
    },

    getTaskId: function () {
      return this.id
    },
    getTaskStateMapped: function () {
      return Gobblin.stateMap[this.attributes.state].class
    },
    getTaskStateElem: function () {
      return app.TaskExecution.getTaskStateElemByState(this.attributes.state)
    },
    getTaskState: function () {
      return this.attributes.state
    },
    getTaskStartTime: function () {
      if (this.attributes.startTime) {
        return this._formatTime(this.attributes.startTime)
      }
      return '-'
    },
    getTaskEndTime: function () {
      if (this.attributes.endTime) {
        return this._formatTime(this.attributes.endTime)
      }
      return '-'
    },
    getTaskDurationInSeconds: function () {
      if (this.attributes.state === 'COMMITTED') {
        return (this.attributes.endTime - this.attributes.startTime) / 1000
      }
      return '-'
    },

    _formatTime: function (timeAsLong) {
      var timeAsDate = new Date(timeAsLong)
      return timeAsDate.toLocaleString()
    }
  }, {
    // Static methods
    getTaskStateElemByState: function (state) {
      return "<span class='highlight text-highlight highlight-" + Gobblin.stateMap[state].class + "'>" + state + '</span>'
    }
  })
})(jQuery)
