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

    <div id="charts">
        <form class="chart">
            <h3>Current Jobs</h3>
            <div id="jobslegend" class="legend"></div>
            <div class="chart_container">
                <div id="jobschart"></div>
                <div id="jobstimeline"></div>
                <div id="jobsslider"></div>
            </div>
        </form>

        <form class="chart">
            <h3>Future Jobs</h3>
            <div id="futurelegend" class="legend"></div>
            <div class="chart_container">
                <div id="futurechart"></div>
            </div>
        </form>
    </div>
    <script>

    var palette = new Rickshaw.Color.Palette( { scheme: 'spectrum14' } );
    var graph = null;
    var drawn = false;
    var futuresDrawn = false;

    var jobsGraph = new Rickshaw.Graph.Ajax( {
        element: document.getElementById("jobschart"),
        width: 700,
        height: 160,
        renderer: 'line',
        stroke: true,
        preserve: false,
        dataURL: "/gearman/?jobQueue=${jobQueueName}&history=true",
        onData: function(data) {
            var graphData = [
                {
                    "name": "Immediate",
                    "data": []
                }
            ];

            data.snapshots.forEach(function(d) {
                var timestamp = parseInt(d.timestamp / 1000);
                graphData[0].data.push( { 'x': timestamp, 'y': d.currentJobs });
            });

            return graphData;
        },
        onComplete: function(transport) {
            var graph = transport.graph;

            if(!drawn)
            {
                var slider = new Rickshaw.Graph.RangeSlider( {
                    graph: graph,
                    element: $('#jobsslider')
                } );

                var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                    graph: graph
                } );

                var annotator = new Rickshaw.Graph.Annotate( {
                    graph: graph,
                    element: document.getElementById('jobstimeline')
                } );

                var legend = new Rickshaw.Graph.Legend( {
                    graph: graph,
                    element: document.getElementById('jobslegend')
                } );

                var shelving = new Rickshaw.Graph.Behavior.Series.Toggle( {
                    graph: graph,
                    legend: legend
                } );

                var order = new Rickshaw.Graph.Behavior.Series.Order( {
                    graph: graph,
                    legend: legend
                } );

                var highlighter = new Rickshaw.Graph.Behavior.Series.Highlight( {
                    graph: graph,
                    legend: legend
                } );

                var ticksTreatment = 'glow';

                var xAxis = new Rickshaw.Graph.Axis.Time( {
                    graph: graph,
                    ticksTreatment: ticksTreatment
                } );

                xAxis.render();

                var yAxis = new Rickshaw.Graph.Axis.Y( {
                    graph: graph,
                    tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
                    ticksTreatment: ticksTreatment
                } );

                yAxis.render();

                drawn = true;

            }
        },
        series: [
            {
                name: "Immediate",
                color: palette.color()
            }
        ],
        min: -.001,
        padding: {
            top: 0.05,
            bottom: 0.05,
            left: 0.02,
            right: 0.02
        }
    } );

    var futuresGraph = new Rickshaw.Graph.Ajax( {
        element: document.getElementById("futurechart"),
        width: 700,
        height: 160,
        renderer: 'bar',
        dataURL: "/gearman/?jobQueue=${jobQueueName}&history=true",
        onData: function(data) {
            var graphData = [
                {
                    "name": "Future Jobs",
                    "data": []
                }
            ];

            var points = data.snapshots[data.snapshots.length - 1];

            if(points.futureJobs)
            {
                for (var key in points.futureJobs)
                {
                    var column = parseInt(key);
                    var now = new Date()
                    var queueTime = new Date(now);
                    queueTime.setHours( now.getHours() + column);
                    var timestamp = parseInt(queueTime.getTime() / 1000)
                    graphData[0].data.push( { 'x': timestamp, 'y': points.futureJobs[key] });
                }
            }


            return graphData;
        },
        onComplete: function(transport) {
            var graph = transport.graph;

            if(!futuresDrawn)
            {
                var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                    graph: graph
                } );

                var legend = new Rickshaw.Graph.Legend( {
                    graph: graph,
                    element: document.getElementById('futurelegend')
                } );

                var shelving = new Rickshaw.Graph.Behavior.Series.Toggle( {
                    graph: graph,
                    legend: legend
                } );

                var order = new Rickshaw.Graph.Behavior.Series.Order( {
                    graph: graph,
                    legend: legend
                } );

                var highlighter = new Rickshaw.Graph.Behavior.Series.Highlight( {
                    graph: graph,
                    legend: legend
                } );

                var xAxis = new Rickshaw.Graph.Axis.Time ( {
                    graph: graph
                } );

                xAxis.render();

                var yAxis = new Rickshaw.Graph.Axis.Y( {
                    graph: graph,
                    tickFormat: Rickshaw.Fixtures.Number.formatKMBT
                } );

                yAxis.render();

                futuresDrawn = true;

            }
        },
        series: [
            {
                name: "Future Jobs",
                color: palette.color()
            }
        ]
    } );

    setInterval( function() {
        jobsGraph.request();
        futuresGraph.request();
    }, 30000 );

    </script>

</@layout>