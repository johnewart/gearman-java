<#-- @ftlvariable name="" type="net.johnewart.gearman.server.web.SystemStatusView" -->

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
            <span class="text">Used memory<br/>(${heapUsed}MB / ${maxHeapSize}MB)</span>
        </div>

        <div class="clear"></div>
    </div>



    <script type="text/javascript">

    function readableNumber(n) {
        if (n == 0) {
            return 0;
        } else {
            var s = ['', 'K', 'M', 'G', 'T', 'P'];
            var e = Math.floor(Math.log(n) / Math.log(1000));
            return (n / Math.pow(1000, e)).toFixed(2) + " " + s[e];
        }
    }


    function numberWithCommas(x) {
        return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    }

    function round(x) {
        return (Math.round(x) * 100 ) / 100;
    }

    function displayNumber(x) {
        return numberWithCommas(round(x));
    }

    function drawGraph(data, selector) {

        var color = d3.scale.category20b();
        var snapshots = data.snapshots;
        var latest = data.latest;

        // Set the dimensions of the canvas / graph
        var	margin = {top: 10, right: 40, bottom: 30, left: 40},
            width = 960 - margin.left - margin.right,
            height = 150 - margin.top - margin.bottom;

        // Parse the date / time
        var	parseDate = d3.time.format("%d-%b-%y").parse;

        // Set the ranges
        var	x = d3.time.scale().range([0, width]);
        var	y = d3.scale.linear().range([height, 0]);

        // Define the axes
        var	xAxis = d3.svg.axis().scale(x)
            .orient("bottom").ticks(5);

        var formatter = d3.format(".3s");
        var	yAxis = d3.svg.axis().scale(y)
            .orient("left")
            .tickFormat(formatter);

        // Define the line
        var	valueline = d3.svg.line()
            .x(function(d) { return x(d.date); })
            .y(function(d) { return y(d.total); });

        var area = d3.svg.area()
                .x(function(d) { return x(d.date); })
                .y0(height)
                .y1(function(d) { return y(d.totalPending); });

        // Adds the svg canvas
        var graph = d3.select("#" + selector);
        var	svg = graph
                    .append("svg")
                        .attr("width", width + margin.left + margin.right)
                        .attr("height", height + margin.top + margin.bottom)
                    .append("g")
                        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");




        snapshots.forEach(function(d) {
            d.date = new Date(d.timestamp);
        });

        if(snapshots.length > 0) {
            snapshots[snapshots.length-1].totalPending = latest.totalPending;
        }

        // Scale the range of the data
        x.domain(d3.extent(snapshots, function(d) { return d.timestamp; }));


        // Draw!


        var yMax = d3.max(snapshots, function(d) { return d.totalPending; });
        var yMin = d3.min(snapshots, function(d) { return d.totalPending; });

        y.domain([0, yMax]);

        var i = 0;

        // Draw shaded area
        svg.append("path")
                .attr("d", area(snapshots))
                .style({
                    "fill": "#8AB8E6",
                    "opacity": 0.3
                });

        // Add the valueline path.
        svg.append("path")
            .style({'stroke': "#8AB8E6", 'fill': 'none', 'stroke-width': '.5px'})
            .attr("d", valueline(snapshots));

        svg.selectAll("queued")
                .data(snapshots)
            .enter().append("rect")
                .style("fill", "steelblue")
                .attr("x", function(d) { return x(d.timestamp); - 1 })
                .attr("width", 2)
                .attr("y", function(d) { return y(d.diffQueued); })
                .attr("height", function(d) { return height - y(d.diffQueued); });

        svg.selectAll("processed")
                .data(snapshots)
            .enter().append("rect")
                .style("fill", "red")
                .attr("x", function(d) { return x(d.timestamp) + 1; })
                .attr("width", 2)
                .attr("y", function(d) { return y(d.diffProcessed); })
                .attr("height", function(d) { return height - y(d.diffProcessed); });




        // Add the X Axis
        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis);

        // Add the Y Axis
        yAxis.tickValues([yMin, yMax]);
        svg.append("g")
            .attr("class", "y axis")
            .call(yAxis);

        var totalPending = snapshots[snapshots.length-1].totalPending;
        var totalProcessed = snapshots[snapshots.length-1].totalProcessed;

        svg.append("text")
             .attr("x", width - 5)
             .attr("y", height - 17)
             .attr("dy", ".71em")
             .attr("style","font-size:20px; font-weight: 400;")
             .style("text-anchor", "end")
             .text(totalPending);
    }

    </script>
    <div id="metrics">
            <div class="tinygraph" id="queuemetrics">
            </div>
            <script type="text/javascript">
            function updateMetrics() {
                var snapshots = $.parseJSON(
                    $.ajax({
                        dataType: "json",
                        url: "/gearman/?system=true" ,
                        data: { },
                        success: function() { },
                        type: "GET",
                        async: false
                    }).responseText
                );

                drawGraph(snapshots, "queuemetrics");
            }

            updateMetrics();
            //setInterval(updateMetrics, 10000);
            </script>
        </div>
    </div>
</@layout>