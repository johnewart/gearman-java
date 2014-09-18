<#-- @ftlvariable name="" type="net.johnewart.gearman.server.web.ClusterView" -->

<#include "layout.ftl">
<@layout>
    <h2>Cluster Members</h2>
    <table class="grid cluster">
        <tr>
            <th>ID</th>
            <th>Hostname</th>
            <th>Address</th>
        </tr>
        <#list clusterMembers as member>
        <tr class="${member.localMember()?string("local", "remote")}">
            <td>${member.uuid}</td>
            <td>${member.getInetSocketAddress().hostName}</td>
            <td>${member.getInetSocketAddress().address.hostAddress}:${member.getInetSocketAddress().port?c}</td>
        </tr>
        </#list>
    </table>

</@layout>