var theClass = "gobblin.azkaban.AzkabanJobLauncher";
var theProperties = {};

function update(queryClass, queryProps) {
  $.ajax({
    type: "POST",
    url: "/options/",
    data: "{theClass:\"" + queryClass + "\", properties:" + JSON.stringify(queryProps) + "}",
    success: function (data) {
      console.log(data);

      $(".options-for-class").attr("data-retain", "false");

      //var parsed = JSON.parse(data);
      for (var key in data) {
        var checkedClass = data[key];
        var thisClass = checkedClass["theClass"];

        var list = $("#" + thisClass.replace(/\./g,"\\."));
        var existingOptions = [];

        var legend = thisClass;
        if (checkedClass["shortName"] && checkedClass["shortName"] !== "") {
          legend = checkedClass["shortName"];
        }

        if (list.size() == 0) {
          var list = $("<fieldset class='form-group options-for-class' id='" + thisClass + "'></fieldset>");
          list.append("<legend>" + legend + "</legend>");
          list.append("<div class='basic-options'></div>");
          $("#content").append(list);
        } else {
          list.attr("data-retain", "true");
          list.find(".useroption").each(function(idx, el) { existingOptions.push($(el).attr("data-option")); });
        }

        var basicOptions = list.children(".basic-options");
        var advancedOptions = list.find(".avanced-options");

        checkedClass.userOptions.forEach(function (option) {

          var currentIdx = existingOptions.indexOf(option["key"]);
          if (currentIdx >= 0) {
            existingOptions.splice(currentIdx, 1);
            return;
          }

          var optionClass = "useroption";
          if (option.requiresRecompute) {
            optionClass += " recompute";
          }

          var label = option["key"];
          if (option["shortName"] && option["shortName"] != "") {
            label = option["shortName"];
          }

          var putOptionAt = basicOptions;
          if (option.advanced) {
            if (advancedOptions.size() == 0) {
              list.append("<div class='advanced-container'>"
                  + "<button type='button' class='btn btn-info' data-toggle='collapse' data-target='#advanced-for-" + thisClass.replace(/\./g,"\\.") + "'>Advanced</button>"
                  + "<div id='advanced-for-" + thisClass + "' class='advanced-options collapse'></div></div>")
              advancedOptions = list.find(".advanced-options");
            }
            putOptionAt = advancedOptions;
          }

          putOptionAt.append("<label for='" + option["key"] + "'>" + label + "</label>");
          if (option.valueStrings && option.valueStrings.length > 0) {
            var select = $("<select class='form-control " + optionClass + "' data-option='" + option["key"] + "'></select>");
            select.append("<option value=''></option>");
            option.valueStrings.forEach(function (value) {
              select.append("<option value='" + value + "'>" + value + "</option>");
            });
            putOptionAt.append(select);
          } else {
            putOptionAt.append("<input type='text' class='form-control " + optionClass + "' data-option='" + option["key"] + "'>");
          }
          $(".useroption[data-option='" + option["key"] + "']").val(theProperties[option["key"]]);
        });

        list.find(".useroption").each(function(idx, el) {
          if (existingOptions.indexOf($(el).attr("data-option")) > -1) {
            $(el).remove();
          }
        });

        $(".useroption").unbind().change(function(e) {
          var option = $(e.target).attr("data-option");
          var value = $(e.target).val();
          theProperties[option] = value;
          $(".useroption[data-option='" + option + "']").val(value);
        });
        $(".recompute").change(reRender);
      }

      $(".options-for-class").each(function(idx, el) {
        if ($(el).attr("data-retain") === "false") {
          $(el).remove();
        }
      });
    },
    failure: function (data) {
      console.log(data);
    }
  });
}

function reRender() {
  update(theClass, theProperties);
}

reRender();
