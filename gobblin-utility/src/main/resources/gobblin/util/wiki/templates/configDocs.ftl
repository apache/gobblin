<html>
   <head>
      <title>${title}</title>
      <style>
         table {
         table-layout: fixed;
         font-family: arial, sans-serif;
         border-collapse: collapse;
         width: 100%;
         }
         td, th {
         table-layout: fixed;
         border: 1px solid #dddddd;
         text-align: left;
         padding: 8px;
         }
         tr:nth-child(even) {
         background-color: #dddddd;
         }
      </style>
   </head>
   <body>
      <h1>${title}</h1>
      <#list groupConfigKeys?keys as group>
      <h2>${group}</h2>
      <#assign configKeys = groupConfigKeys[group]>
      <table border=1>
         <tr>
            <th>Key</th>
            <th>Doc</th>
            <th>Default</th>
            <th>Is Required</th>
         </tr>
         <#list configKeys as configKey>
         <tr>
            <td>${configKey.name}
            <td>${configKey.doc}
            <td>${configKey.def}
            <td>${configKey.required}
         </tr>
         </#list>
      </table>
      </#list>
   </body>
</html>
