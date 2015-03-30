<#-- @ftlvariable name="" type="net.johnewart.gearman.server.web.JobQueueStatusView" -->

<#include "layout.ftl">
<@layout>

<h1>${jobQueueName}</h1>

<h2>${numberOfConnectedWorkers} worker(s)</h2>
Active jobs: ${activeJobCount}<br/>
Enqueued jobs: ${enqueuedJobCount}<br/>
Completed jobs: ${completedJobCount}<br/>
Failed jobs: ${failedJobCount}<br/>
Exceptions: ${exceptionCount}<br/>
High queue: ${highPriorityJobsCount}<br/>
Mid queue: ${midPriorityJobsCount}<br/>
Low queue: ${lowPriorityJobsCount}<br/>
</@layout>