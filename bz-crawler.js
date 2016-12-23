"use strict";

var express = require('express');
var bodyParser = require('body-parser');
var errorHandler = require('errorhandler');
var http = require('http');
var async = require('async');
var c = require('./config.js');
var CBZ = require('./extractors/crawlEclipseBugzilla.js');
var MT = require('./transformation/mylyn-transformation.js');
var initBT = require("./transformation/bugzilla-transformation");
var DBU = require('./util/dbutil.js');

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

var bugdbConfig = {
    dbName: c.config.bugzilla_dbName,
    cleanUp: 'false',
    dbURL: c.config.couchDB_url,
};

var flatbugdbConfig = {
    dbName: c.config.flatbug_dbName,
    cleanUp: 'false',
    dbURL: c.config.couchDB_url,
}

var attachmentdbConfig = {
    dbName: c.config.bugAttachments_dbName,
    cleanUp: 'false',
    dbURL: c.config.couchDB_url,
};

//var dbUtil = new DBU.DBUtil();

var ebz = null;
var myl = null;
var bt = null;
var mea = null;
async.parallel([
        async.apply(DBU.initCouchDB, bugdbConfig),
        async.apply(DBU.initCouchDB, attachmentdbConfig),
        async.apply(DBU.initCouchDB, flatbugdbConfig)
    ],
    // now do something with the results
    function(err, results) {
        if (err)
            throw new Error("Fatal error initializing DBs for Eclipse Bugzilla Extractor \r\n" + err);
        else {
            var mylynAttConfig = {
                attachmentsDB : results[1]
            };
            myl = MT.initAtt2Mylyn(mylynAttConfig);
            
            mea = MT.initMylynEventAnalysis({flatBugDB : results[2]});
            
            var bzconfig = {
                bugDB: results[0],
                attachmentsDB: results[1],
                bugzillaUrl: c.config.bugzillaUrl,
                bugzillaUser: c.config.bugzillaUser,
                bugzillaPassword: c.config.bugzillaPassword
            };
            
            var btConfig = {
                fullBugDB : results[0],
                flatBugDB : results[2],
                attachmentsDB : results[1]
            }
            bt = initBT(btConfig);
            
            CBZ.initEBZ(bzconfig, function(err, inst) {
                if (!err) {
                    ebz = inst;
                    console.log("Eclipse Bugzilla Extractor Ready");
                }
                else {
                    throw new Error("Fatal error from Eclipse Bugzilla Extractor \r\n" + err);
                }
            });
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

app.get("/extractAttachments", function(req, res) {
    //ebz.convertBlobs2Attachments(req.query.bugId, function (err, result) {
    //ebz.convertAllBlobs2Attachments(function (err, result) {
    //ebz.removeBugsWithoutMylynContext(function (err, result){
    // ebz.removeIllnamedAttachments(function (err, result) {
    // ebz.checkExtractedAttachments( function (err, result) {
    //ebz.generateBugPairs( function (err, result) {
   //bt.transformFullBugs2ActRefBugs(function (err, result) {
     bt.transformActivityRefBugs2FlatBugs(function (err, result) {
        if (err) {
            return res.status(500).json({
                        'errors': err
                    });
        }
        else  return res.status(200).end();
    });
});

//ActivityAttachment-83035_Bug-116487

app.get("/processAttachments", function(req, res) {
    //myl.loadAttachment(req.query.attachmentId, function (err, result) {
    //myl.allAttachmentStats2CSV(function (err, result) {
    // mea.analyseBugDependencies(function (err, result) {
      mea.analyseAllBugDependencies(function (err, result) {
        if (err) {
            return res.status(500).json({
                        'errors': err
                    });
        }
        else  return res.status(200).end();
    });
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
        '<form action = "http://192.168.65.129:'+port+'/extractAttachments" method = "GET">' +
        ' Extract Attachments for BugId <input type = "text" name = "bugId"/> ' +
        ' <input type = "submit" value = "Submit"/>' +
        ' </form><br>' +
        '<form action = "http://192.168.65.129:'+port+'/processAttachments" method = "GET">' +
        ' Process Attachment (Id) <input type = "text" name = "attachmentId"/> ' +
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