<#macro layout>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
    <head>
        <link type="text/css" rel="stylesheet" href="/static/css/jquery-ui.css">
        <link type="text/css" rel="stylesheet" href="/static/css/rickshaw.min.css">
        <link type="text/css" rel="stylesheet" href="/static/css/extensions.css">

        <script src="/static/d3.v3.min.js"></script>
        <script src="/static/jquery.min.js"></script>
        <script src="/static/jquery-ui.min.js"></script>
        <script src="/static/rickshaw.js"></script>
        <script src="/static/rickshaw.extensions.js"></script>

        <meta http-equiv="Content-type" content="text/html; charset=utf-8" />
        <link href='http://fonts.googleapis.com/css?family=Open+Sans' rel='stylesheet' type='text/css'>
        <link href='http://fonts.googleapis.com/css?family=PT+Sans:400,700' rel='stylesheet' type='text/css'>
        <#include "styles.ftl">
        <script type="text/javascript" src="/static/d3.v3.min.js"></script>
    </head>
    <body>
        <div id="container">
            <div id="menu">
                <ul>
                    <li><a href="/status/">Dashboard</a></li>
                    <li><a href="/status/?queues=true">Queues</a></li>
                    <li><a href="/cluster/">Cluster Status</a></li>

                </ul>
            </div>
            <div id="content">
                <#nested>
            </div>
        </div>
    </body>
</html>
</#macro>