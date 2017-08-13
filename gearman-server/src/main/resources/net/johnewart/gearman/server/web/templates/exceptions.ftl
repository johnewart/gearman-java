<#-- @ftlvariable name="" type="net.johnewart.gearman.server.web.ExceptionsView" -->

<#include "layout.ftl">
<@layout>
    <h2>Exceptions</h2>
    Showing ${start}-${end} of ${exceptionCount} exceptions (page ${pageNum} of ${pageCount})<br/>

    <p class="pager">
    <#if (pageCount > 1) >
    	<a href="/exceptions/?pageSize=${pageSize}&pageNum=1">1..</a>
    </#if>
    <#if hasPreviousPage()>
        <a href="/exceptions/?pageSize=${pageSize}&pageNum=${previousPageNumber}">Previous Page</a>
    </#if>
    <#if (pageCount > 1) >
	    <#list paginationStart..paginationEnd as x>
	    <#if x == pageNum >
        <b>${x}</b>
        <#else>
        <a href="/exceptions/?pageSize=${pageSize}&pageNum=${x}">${x}</a>
        </#if>
		</#list>
    </#if>
    <#if hasNextPage()>
        <a href="/exceptions/?pageSize=${pageSize}&pageNum=${nextPageNumber}">Next Page</a>
    </#if>
    </p>

    <table class="grid cluster exceptions">
        <tr>
            <th>Job Handle</th>
            <th>Unique ID</th>
            <th>Job data</th>
            <th>Exception data</th>
            <th>Date/Time</th>
        </tr>
        <#list exceptions as exception>
            <tr>
                <td>${getJobHandle(exception)}</td>
                <td>${getUniqueId(exception)}</td>
                <td>${getJobDataString(exception)}</td>
                <td>${getExceptionDataString(exception)}</td>
                <td>${getDateTime(exception)}</td>
            </tr>
        </#list>
    </table>



</@layout>