<#-- @ftlvariable name="" type="net.johnewart.gearman.server.web.SystemStatusView" -->

<#include "layout.ftl">
<@layout>

<table class="grid">
    <tr>
        <th>Job Queue</th>
        <th>Workers</th>
        <th>Size</th>
        <th>Action</th>
    </tr>
    <#list jobQueues as jobQueue>
        <tr>
            <td>${jobQueue.name!"N/A"}</td>
            <td>${jobQueue.numberOfConnectedWorkers}</td>
            <td>${jobQueue.size()}</td>
            <td><a href="/status/?jobQueue=${jobQueue.name}">Show</a></td>
        </tr>
    </#list>
</table>
</@layout>