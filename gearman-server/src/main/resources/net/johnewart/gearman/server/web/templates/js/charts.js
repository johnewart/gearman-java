
function readableNumber(n) {
    if (n == 0) {
        return 0;
    } else {
        var s = ['', 'K', 'M', 'G', 'T', 'P'];
        var e = Math.floor(Math.log(n) / Math.log(1000));
        var value = (n / Math.pow(1000, e)).toFixed(2);
        if (e < 1) {
            return round(value);
        } else {
            return (value + " " + s[e]);
        }
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


function drawGraph(snapshots, selector, properties) {

    if (properties == undefined) {
        properties = {}
    }

    var width = properties["width"] || 400
    var height = properties["height"] || 80
    var title = properties["title"] || true

    var color = d3.scale.category20b();
    var stack = d3.layout.stack()
        .values(function(d) { return d.values; });

    var timestamps = snapshots.times;
    var high = snapshots.high;
    var mid = snapshots.mid;
    var low = snapshots.low;
    var dates = [];
    var datamap = {
        "high": {name: 'High', values: []},
        "mid": {name: 'Normal', values: []},
        "low": {name: 'Low', values: []}
    };

    $.map(timestamps, function(timestamp, idx) {
        dates.push(new Date(timestamp));
        datamap["high"].values.push( { date: new Date(timestamp), y: high[idx] } );
        datamap["mid"].values.push( { date: new Date(timestamp), y: mid[idx] } );
        datamap["low"].values.push( { date: new Date(timestamp), y: low[idx] } );
    });


    var queues = stack([datamap.low, datamap.mid, datamap.high]);

    // Set the dimensions of the canvas / graph
    var	margin = {top: 10, right: 40, bottom: 30, left: 40},
        width = width - margin.left - margin.right,
        height = height - margin.top - margin.bottom;

    // Set the ranges
    var	x = d3.time.scale().range([0, width]);
    // Scale the range of the data
    x.domain(d3.extent(dates));

    var	y = d3.scale.linear().range([height, 0]);
    var yMax =  d3.max(mid) + d3.max(high) + d3.max(low);
    var yMin = 0;
    y.domain([0, yMax]);

    // Define the axes
    var	xAxis = d3.svg.axis().scale(x)
        .orient("bottom").ticks(5);

    var formatter = d3.format(".3s");
    var	yAxis = d3.svg.axis().scale(y)
        .orient("left")
        .tickFormat(formatter);

    // Define the line
   /* var	valueline = d3.svg.line()
        .x(function(d) { return x(d.timestamp); })
        .y(function(d) { return y(d.y); });*/

    var color = d3.scale.category20();

    var area = d3.svg.area()
        .x(function(d) { return x(d.date); })
        .y0(function(d) { return y(d.y0); })
        .y1(function(d) { return y(d.y0 + d.y); });

    var stacked_line = d3.svg.line()
            .x(function(d) { return x(d.date); })
            .y(function(d) { return y(d.y + d.y0);
        });

    // Adds the svg canvas
    var graph = d3.select("#" + selector);
    var	svg = graph
                .append("svg")
                    .attr("width", width + margin.left + margin.right)
                    .attr("height", height + margin.top + margin.bottom)
                .append("g")
                    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");


    var queue = svg.selectAll(".queue")
            .data(queues)
        .enter().append("g")
            .attr("class", "queue");

    queue.append("path")
      .attr("class", "area")
      .attr("d", function(d) { return area(d.values); })
      .style("fill", function(d) { return color(d.name); });

    queue.append("path")
        .style({'stroke': "#8AB8E6", 'fill': 'none', 'stroke-width': '.5px'})
        .attr("d", function(d) { return stacked_line(d.values); });

    var lastMid = mid[timestamps.length-1];
    var lastHigh = high[timestamps.length-1];
    var lastLow = low[timestamps.length-1];
    var lastTotal = lastLow + lastMid + lastHigh;

    svg.append("text")
             .attr("x", width - 5)
             .attr("y", height - 17)
             .attr("dy", ".71em")
             .attr("style","font-size:20px; font-weight: 400;")
             .style("text-anchor", "end")
             .text(readableNumber(lastTotal));







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

    var legendHTML = "";

}
