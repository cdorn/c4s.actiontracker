"use strict";

var express = require('express');
var bodyParser = require('body-parser');
var errorHandler = require('errorhandler');
var http = require('http');
var c = require('./config.js');
var initEBZ = require('./extractors/crawlEclipseBugzilla.js');

var app = express();
var port = 3001;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: true
}));
app.use(errorHandler({
    dumpExceptions: true,
    showStack: true
}));

var ebz = undefined;
var bzconfig = {
    dbName: c.config.bugzilla_dbName,
    cleanUp: 'false',
    dbURL: c.config.couchDB_url,
    bugzillaUrl : c.config.bugzillaUrl,
    bugzillaUser : c.config.bugzillaUser,
    bugzillaPassword : c.config.bugzillaPassword
};

initEBZ(bzconfig, function(err, inst) {
    if (!err) {
        ebz = inst;
        console.log("Eclipse Bugzilla Extractor Ready");
    }
    else {
        throw new Error("Fatal error from Eclipse Bugzilla Extractor \r\n" + err);
    }
});

var testBugId = "187156";

app.get("/crawl_bug", function(req, res) {
    var bugId = req.query.bugId;
    if (!bugId)
        res.status(404).end();
    else
        ebz.extractBug(bugId, function(err, result){
            if (!err) {
                console.log(result);
                return res.status(200).end();
            } else {console.log(err);
                return res.status(500).json({'errors': err });
            }
        });
});

app.get("/blockingBugs", function(req, res) {
    ebz.mergeBugs( function (err, result) {
        if (err) {
            return res.status(500).json({
                        'errors': err
                    });
        }
        else  return res.status(200).end();
    });
    
    // ebz.extractBugIds( function(err, result){
    //         if (!err) {
    //             processItem(result, 0, res);    
    //         } else {console.log(err);
    //             return res.status(500).json({'errors': err });
    //         }
    //     });
});


function processItem(items, pos, res) {
    if (pos < items.length) {
        setTimeout(function() {
            ebz.extractBug(items[pos], function(err, result) {
                if (err) {
                    console.log(err);
                    console.log("Last Processed: " + items[pos]);
                    return res.status(500).json({
                        'errors': err
                    });
                }
                else {
                    console.log("Processed: " + items[pos]);
                    pos++;
                    processItem(items, pos, res);
                }
            });
        }, 2000);
    }
    else
    {
        console.log("All processed");
        return res.status(200).end();
    }
}


app.get("*", function(request, response) {
    response.status(200).send('<html><body>' +
        '<form action = "http://192.168.65.129:'+port+'/crawl_bug" method = "GET">' +
        ' BugId <input type = "text" name = "bugId"/> ' +
        ' <input type = "submit" value = "Submit"/>' +
        ' </form><br>' +

        '</body></html>');
});


var connected = function() {
    console.log("Bugzilla Crawler started on port %s : %s", port, Date(Date.now()));
};

http.createServer(app).listen(port, connected);

function jsonError(res, err) {
    var message = err.message || err.statusMessage || err;
    //return error message to tracking client
    res.status(500).json({
        'error': message
    });
    // return error message to tracking host 
    return message;
};