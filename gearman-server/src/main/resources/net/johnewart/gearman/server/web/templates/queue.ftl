<#-- @ftlvariable name="" type="net.johnewart.gearman.server.web.JobQueueStatusView" -->

<#include "layout.ftl">
<@layout>

<h1>${jobQueueName}</h1>

<h2>${numberOfConnectedWorkers} worker(s)</h2>
<table class="grid">
<tr>
    <th>Active</th>
    <th>Enqueued</th>
    <th>Completed</th>
    <th>Failed</th>
    <th>Exceptions</th>
    <th>High Queue</th>
    <th>Mid Queue</th>
    <th>Low Queue</th>
</tr>
<tr>
    <td>${activeJobCount}</td>
    <td>${enqueuedJobCount}</td>
    <td>${completedJobCount}</td>
    <td>${failedJobCount}</td>
    <td>${exceptionCount}</td>
    <td>${highPriorityJobsCount}</td>
    <td>${midPriorityJobsCount}</td>
    <td>${lowPriorityJobsCount}</td>
</tr>
</table>

<div class="queue" style="width:960px; margin-top: 40px">
    <div class="tinygraph" id="graph_${jobQueueName}">
        <div class="overlay-number"></div>
    </div>
    <script type="text/javascript">
        var results = $.parseJSON(
            $.ajax({
                dataType: "json",
                url: "/gearman/?jobQueue=${jobQueueName}&history=true" ,
                data: { },
                success: function() { },
                type: "GET",
                async: false
            }).responseText
        );

        drawGraph(results, "graph_${jobQueueName}", { width: 960, height: 120, title: false});
    </script>
</div>
</@layout>