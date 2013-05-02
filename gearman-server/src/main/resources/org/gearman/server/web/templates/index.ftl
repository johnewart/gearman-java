<#-- @ftlvariable name="" type="org.gearman.server.web.StatusView" -->

<#include "layout.ftl">
<@layout>
    <h1>${hostname!"Unknown Host"}</h1>



    <div class="info">
        <div class="box">
            <span class="number">${numberFormatter.format(totalJobsPending)}</span>
            <span class="text">Pending Jobs</span>
        </div>

        <div class="box">
            <span class="number">${numberFormatter.format(totalJobsProcessed)}</span>
            <span class="text">Processed since startup</span>
        </div>

        <div class="box">
            <span class="number">${numberFormatter.format(totalJobsQueued)}</span>
            <span class="text">Queued since startup</span>
        </div>

        <div class="box">
            <span class="number">${numberFormatter.format(workerCount)}</span>
            <span class="text">Active Workers</span>
        </div>


        <div class="box">
            <span class="number">${uptime}</span>
            <span class="text">Uptime</span>
        </div>

        <div class="box">
            <span class="number">${memoryUsage}%</span>
            <span class="text">Used memory<br/>(${usedMemory}MB / ${maxMemory}MB)</span>
        </div>

        <div class="clear"></div>
    </div>

    <div id="snapshots"></div>
    <div id="memory"></div>

    <!DOCTYPE html>
    <meta charset="utf-8">
    <style>


        .axis path,
        .axis line {
            fill: none;
            stroke: #000;
            stroke-width: .5px;
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

        .grid .tick {
            stroke: lightgrey;
            opacity: 0.7;
        }
        .grid path {
            stroke-width: 0;
        }

    </style>
    <script>
                var margin = {top: 20, right: 80, bottom: 30, left: 50};
                var jobsWidth = 960 - margin.left - margin.right;
                var jobsHeight = 400 - margin.top - margin.bottom;
                var memoryWidth = jobsWidth;
                var memoryHeight = 150 - margin.top - margin.bottom;

                var x = d3.time.scale()
                        .range([0, jobsWidth]);

                var jobsY = d3.scale.linear()
                        .range([jobsHeight, 0]);
                
                var memoryY = d3.scale.linear()
                        .range([memoryHeight, 0]);

                var color = d3.scale.category10();

                var xAxis = d3.svg.axis()
                        .scale(x)
                        .orient("bottom");

                var jobsYAxis = d3.svg.axis()
                        .scale(jobsY)
                        .orient("left");

                var memoryYAxis = d3.svg.axis()
                        .scale(memoryY)
                        .orient("left")
                        .ticks(4);

                var jobsLine = d3.svg.line()
                        .interpolate("basis")
                        .x(function(d) { return x(d.date); })
                        .y(function(d) { return jobsY(d.value); });

                var memoryLine = d3.svg.line()
                        .interpolate("basis")
                        .x(function(d) { return x(d.date); })
                        .y(function(d) { return memoryY(d.value); });

                var jobsSVG = d3.select("#snapshots").append("svg")
                        .attr("width", jobsWidth + margin.left + margin.right)
                        .attr("height", jobsHeight + margin.top + margin.bottom)
                        .append("g")
                        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

                var memorySVG = d3.select("#memory").append("svg")
                        .attr("width", memoryWidth + margin.left + margin.right)
                        .attr("height", memoryHeight + margin.top + margin.bottom)
                        .append("g")
                        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

                d3.json("/gearman/?system=true", function(error, data) {

                    labelMap = {
                        //"totalQueued" : "Total Queued",
                        //"totalProcessed": "Total Processed",
                        "diffQueued" : "Queued",
                        "diffProcessed" : "Processed"
                    }

                    labelKeys = [];
                    for(key in labelMap)
                    {
                        labelKeys.push(key);
                    }

                    color.domain(labelKeys);

                    data.forEach(function(d) {
                        d.date = new Date(d.timestamp);
                    });

                    var jobsMetrics = color.domain().map(function(key) {
                        return {
                            name: labelMap[key],
                            values: data.map(function(d) {
                                return {date: d.date, value: d[key]};
                            })
                        };
                    });
                    
                    var memoryMetrics = [
                        {
                            name: "Heap Used (MB)",
                            values: data.map(function(d) {
                                return { date: d.date, value: d['heapUsed'] / (1024 * 1024)}
                            })
                        }
                    ];

                    x.domain(d3.extent(data, function(d) { return d.date; }));

                    jobsY.domain([
                        d3.min(jobsMetrics, function(c) { return d3.min(c.values, function(v) { return v.value; }); }),
                        d3.max(jobsMetrics, function(c) { return d3.max(c.values, function(v) { return v.value; }); })
                    ]);
                    
                    memoryY.domain([0, ${maxMemory?string.computer}]);

                    var legend = jobsSVG.selectAll('g')
                            .data(jobsMetrics)
                            .enter()
                            .append('g')
                            .attr('class', 'legend');

                    legend.append('rect')
                            .attr('x', jobsWidth - 20)
                            .attr('y', function(d, i){ return i *  20;})
                            .attr('width', 10)
                            .attr('height', 10)
                            .style('fill', function(d) {
                                return color(d.name);
                            });

                    legend.append('text')
                            .attr('x', jobsWidth - 8)
                            .attr('y', function(d, i){ return (i *  20) + 9;})
                            .text(function(d){ return d.name; });


                    jobsSVG.append("g")
                            .attr("class", "x axis")
                            .attr("transform", "translate(0," + jobsHeight + ")")
                            .call(xAxis);

                    memorySVG.append("g")
                            .attr("class", "x axis")
                            .attr("transform", "translate(0," + memoryHeight + ")")
                            .call(xAxis);

                    jobsSVG.append("g")
                            .attr("class", "y axis")
                            .call(jobsYAxis)
                            .append("text")
                            .attr("transform", "rotate(-90)")
                            .attr("y", 6)
                            .attr("dy", ".71em")
                            .style("text-anchor", "end")
                            .text("Jobs");

                    memorySVG.append("g")
                            .attr("class", "y axis")
                            .call(memoryYAxis)
                            .append("text")
                            .attr("transform", "rotate(-90)")
                            .attr("y", 6)
                            .attr("dy", ".71em")
                            .style("text-anchor", "end")
                            .text("MB");

                    var memoryMetric = memorySVG.selectAll(".memoryMetric")
                            .data(memoryMetrics)
                            .enter().append("g")
                            .attr("class", "memoryMetric");

                    memoryMetric.append("path")
                            .attr("class", "line")
                            .attr("d", function(d) { return memoryLine(d.values); })
                            .style("stroke", function(d) { return color(d.name); });

                    memoryMetric.append("text")
                            .datum(function(d) { return {name: d.name, value: d.values[d.values.length - 1]}; })
                            .attr("transform", function(d) { return "translate(" + x(d.value.date) + "," + memoryY(d.value.value) + ")"; })
                            .attr("x", 3)
                            .attr("dy", ".35em")
                            .text(function(d) { return d.name; });

                    var jobsMetric = jobsSVG.selectAll(".jobsMetric")
                            .data(jobsMetrics)
                            .enter().append("g")
                            .attr("class", "jobsMetric");

                    jobsMetric.append("path")
                            .attr("class", "line")
                            .attr("d", function(d) { return jobsLine(d.values); })
                            .style("stroke", function(d) { return color(d.name); });

                    /*jobsMetric.append("text")
                            .datum(function(d) { return {name: d.name, value: d.values[d.values.length - 1]}; })
                            .attr("transform", function(d) { return "translate(" + x(d.value.date) + "," + jobsY(d.value.value) + ")"; })
                            .attr("x", 3)
                            .attr("dy", ".35em")
                            .text(function(d) { return d.name; });*/


                });
        </script>
    </div>

</@layout>