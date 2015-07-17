<#-- @ftlvariable name="" type="net.johnewart.gearman.server.web.SystemStatusView" -->

<#include "layout.ftl">
<@layout>

<h3>${jobQueueCount} Queues</h3>
<div id="jobqueues">
    <#list jobQueues as jobQueue>
    <div class="jobqueue">
        <a href="/status/?jobQueue=${jobQueue}"><h2>${jobQueue!"N/A"}</h2></a>
        <div class="tinygraph" id="graph_${jobQueue}">
            <div class="overlay-number"></div>
        </div>
        <script type="text/javascript">
            var results = $.parseJSON(
                $.ajax({
                    dataType: "json",
                    url: "/gearman/?jobQueue=${jobQueue}&history=true" ,
                    data: { },
                    success: function() { },
                    type: "GET",
                    async: false
                }).responseText
            );

            drawGraph(results, "graph_${jobQueue}");
        </script>
    </div>
    </#list>
</div>

</@layout>