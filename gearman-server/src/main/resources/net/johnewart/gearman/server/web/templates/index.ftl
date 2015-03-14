<#-- @ftlvariable name="" type="net.johnewart.gearman.server.web.StatusView" -->

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

    <div id="charts">
        <form class="chart">
            <h3>Activity</h3>
            <div id="jobslegend" class="legend"></div>
            <div class="chart_container">
                <div id="jobschart"></div>
                <div id="jobstimeline"></div>
                <div id="jobsslider"></div>
            </div>

        </form>

        <form class="chart">
            <h3>Memory Usage (MB)</h3>
            <div id="memorylegend" class="legend"></div>
            <div class="chart_container">
                <div id="memorychart"></div>
                <div id="memorytimeline"></div>
                <div id="memoryslider"></div>
            </div>

        </form>
    </div>
<script>

var palette = new Rickshaw.Color.Palette( { scheme: 'spectrum14' } );
var graph = null;
var drawn = false;
var memchartDrawn = false;

var jobsGraph = new Rickshaw.Graph.Ajax( {
    element: document.getElementById("jobschart"),
    width: 700,
    height: 220,
    renderer: 'line',
    interpolation: 'basis',
    stroke: true,
    preserve: false,
    dataURL: "/gearman/?system=true",
    onData: function(data) {
        var graphData = [
            {
                "name": "Queued",
                "data": []
            },
            {
                "name": "Processed",
                "data": []
            }
        ];

        data.forEach(function(d) {
            var timestamp = parseInt(d.timestamp / 1000);
            graphData[0].data.push( { 'x': timestamp, 'y': d.diffQueued });
            graphData[1].data.push( { 'x': timestamp, 'y': d.diffProcessed });
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
                graph: graph
                //ticksTreatment: ticksTreatment
            } );

            xAxis.render();

            var yAxis = new Rickshaw.Graph.Axis.Y( {
                graph: graph,
                tickFormat: Rickshaw.Fixtures.Number.formatKMBT
                //ticksTreatment: ticksTreatment
            } );

            yAxis.render();
            drawn = true;
        }
    },
    series: [
        {
            name: "Queued",
            color: "#6060c0",
        },
        {
            name: "Processed",
            color: "#30c020",
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

var memoryGraph = new Rickshaw.Graph.Ajax( {
    element: document.getElementById("memorychart"),
    width: 700,
    height: 160,
    renderer: 'area',
    interpolation: 'basis',
    stroke: true,
    preserve: false,
    dataURL: "/gearman/?system=true",
    onData: function(data) {
        var graphData = [
            {
                "name": "Heap Used",
                "data": []
            },
            {
                "name": "Heap Size",
                "data": []
            }
        ];

        data.forEach(function(d) {
            var timestamp = parseInt(d.timestamp / 1000);
            graphData[0].data.push( { 'x': timestamp, 'y': d.heapUsed / (1024 * 1024) });
            graphData[1].data.push( { 'x': timestamp, 'y': d.heapSize / (1024 * 1024) });
        });

        return graphData;
    },
    onComplete: function(transport) {
        graph = transport.graph;

        if(!memchartDrawn)
        {
            var slider = new Rickshaw.Graph.RangeSlider( {
                graph: graph,
                element: $('#memoryslider')
            } );

            var hoverDetail = new Rickshaw.Graph.HoverDetail( {
                graph: graph
            } );

            var annotator = new Rickshaw.Graph.Annotate( {
                graph: graph,
                element: document.getElementById('memorytimeline')
            } );

            var legend = new Rickshaw.Graph.Legend( {
                graph: graph,
                element: document.getElementById('memorylegend')
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
            memchartDrawn = true;
        }

    },
    series: [
        {
            name: "Heap Used",
            color: "#ffc020",
        },
        {
            name: "Heap Size",
            color: palette.color()
        }
    ],
    padding: {
        top: 0.05,
        bottom: 0.05,
        left: 0.02,
        right: 0.02
    },
    max: ${maxHeapSize?string.computer}
} );

setInterval( function() {
    jobsGraph.request();
    memoryGraph.request();
}, 30000 );



</script>

</@layout>