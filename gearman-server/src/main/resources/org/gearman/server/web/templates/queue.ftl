<#-- @ftlvariable name="" type="org.gearman.server.web.JobQueueStatusView" -->

<#include "layout.ftl">
<@layout>

<h1>${jobQueueName}</h1>

<div class="info">
<div class="box">
    <span class="number">${latestJobQueueSnapshot.immediate}</span>
    <span class="text">Immediate jobs</span>
</div>

<div class="box">
    <span class="number">${latestJobQueueSnapshot.future}</span>
    <span class="text">Future jobs</span>
</div>

<div class="box">
    <span class="number">${numberOfConnectedWorkers}</span>
    <span class="text">Connected Workers</span>
</div>
<div class="clear"></div>
</div>


<div id="snapshots" class="chart"></div>
<!DOCTYPE html>
<meta charset="utf-8">
<style>

    .axis path,
    .axis line {
        fill: none;
        stroke: #000;
        shape-rendering: crispEdges;
    }

    .x.axis path {
        display: none;
    }

    .line {
        fill: none;
        stroke: steelblue;
        stroke-width: 1.5px;
    }

</style>
<script>
    var margin = {top: 20, right: 80, bottom: 30, left: 50},
            width = 640 - margin.left - margin.right,
            height = 400 - margin.top - margin.bottom;

    var x = d3.time.scale()
            .range([0, width]);

    var y = d3.scale.linear()
            .range([height, 0]);

    var color = d3.scale.category10();

    var xAxis = d3.svg.axis()
            .scale(x)
            .orient("bottom");

    var yAxis = d3.svg.axis()
            .scale(y)
            .orient("left");

    var line = d3.svg.line()
            .interpolate("basis")
            .x(function(d) { return x(d.date); })
            .y(function(d) { return y(d.value); });

    var svg = d3.select("#snapshots").append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    d3.json("/gearman/?jobQueue=${jobQueueName}&history=true", function(error, data) {

        labelKeys = [];

        data = data.snapshots;
        data.forEach(function(d) {
            d.date = new Date(d.timestamp);
        });

        var lines = [
            {
                name: "Current Jobs",
                values: data.map(function(d){
                    return { date: d.date, value: d.currentJobs }
                })
            },
            {
                name: "Future Jobs (< 1h)",
                values: data.map(function(d){
                    if(d.futureJobs && d.futureJobs['0']) {
                        return { date: d.date, value: d.futureJobs['0'] }
                    } else {
                        return { date: d.date, value: 0 }
                    }
                })
            },
            {
                name: "Future Jobs (> 1h)",
                values: data.map(function(d){
                    if(d.futureJobs && d.futureJobs['1']) {
                        return { date: d.date, value: d.futureJobs['1'] }
                    } else {
                        return { date: d.date, value: 0 }
                    }
                })
            }, {
                name: "Future Jobs (> 2h)",
                values: data.map(function(d){
                    if(d.futureJobs && d.futureJobs['2']) {
                        return { date: d.date, value: d.futureJobs['2'] }
                    } else {
                        return { date: d.date, value: 0 }
                    }
                })
            }

        ];

        x.domain(d3.extent(data, function(d) { return d.date; }));

        y.domain([
            0,
            d3.max(lines, function(l) { return d3.max(l.values, function(v) { return v.value; }); })
        ]);

        svg.append("g")
                .attr("class", "x axis")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis);

        svg.append("g")
                .attr("class", "y axis")
                .call(yAxis)
                .append("text")
                .attr("transform", "rotate(-90)")
                .attr("y", 6)
                .attr("dy", ".71em")
                .style("text-anchor", "end")
                .text("Jobs");

        var jobs = svg.selectAll(".jobs")
                .data(lines)
                .enter().append("g")
                .attr("class", "jobs");

        jobs.append("path")
                .attr("class", "line")
                .attr("d", function(d) { return line(d.values); })
                .style("stroke", function(d) { return color(d.name); });

        jobs.append("text")
                .datum(function(d) { return {name: d.name, value: d.values[d.values.length - 1]}; })
                .attr("transform", function(d) { return "translate(" + x(d.value.date) + "," + y(d.value.value) + ")"; })
                .attr("x", 3)
                .attr("dy", ".35em")
                .text(function(d) { return d.name; });
    });
</script>


</@layout>