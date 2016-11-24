"use strict";

var express = require('express');
var path = require('path');
var http = require('http');
var bodyParser = require('body-parser');
var errorHandler = require('errorhandler');
var _ = require('lodash');
var initPh2E = require('./transformation/phabricator-transformation.js');
var request = require('request');
var createCanduit = require('./js/canduit.js');
var config = require('./config.json');

var app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
    extended: true
}));
app.use(errorHandler({
    dumpExceptions: true,
    showStack: true
}));


var ph2e = undefined;
var dbconfig = {
    dbName: config.crawler_dbName,
    cleanUp: 'false',
    dbURL: config. couchDB_url
};

initPh2E(dbconfig, function(err, inst) {
    if (!err) {
        ph2e = inst;
        console.log("Phabricator Transformer Ready");
    }
    else {
        throw new Error("Fatal error from Phabricator Transformer \r\n" + err);
    }
});

var canduit = undefined;
var cconfig = {
    user: 'phabricator.bot',
    api: 'http://192.168.65.131:80/api/',
    token: config.phabricator_token,
    client: 'crawler'
};
// Create and authenticate client
createCanduit(cconfig, function(err, inst) {
    if (!err) {
        canduit = inst;
        console.log("Candiut Ready");
    }
    else {
        throw new Error("Fatal error from Canduit: unable to initialize Conduit API \r\n Error: " + err);
    }
});


const taskdict = {
    1: "PHID-TASK-7kq3sj42qopqzchnn4lx",
    2: "PHID-TASK-fmrts7f56efppidfg7ti",
    3: "PHID-TASK-6g2yzvcy5g3xixr3mal5",
    4: "PHID-TASK-umbq3v2kt23xggp2woao",
    5: "PHID-TASK-pfk3j5cz5mo3ttn34gpe",
    6: "PHID-TASK-eay64karmv66eqhvjsib",
    7: "PHID-TASK-ffwauyqbvxapz6spc4hn",
    8: "PHID-TASK-wwiguwnei5ss5uxqkl7e"
}

const chatdict = {
    1: "PHID-CONP-zdgsbyp3wyfmg2hcyezj",
    2: "PHID-CONP-r7da5qjtih44umazfy74",
    3: "PHID-CONP-ljmdiiq5hvkcsh6imqx4"
}

function getPhIDforTask(taskIntID) {
    return taskdict[taskIntID];
}

function getPhIDforChat(intID) {
    return chatdict[intID];
}

app.get("/crawl_history", function(req, res) {
    canduit.exec('feed.query', { limit:100 },  function(err, transactions) {
        if (!err) {
            var counter = 0;
            _.values(transactions).forEach(function(entry) {
                var phid = entry.data.objectPHID;
                counter++;
                console.log("Feed entry about: "+phid);
                //TODO: trigger crawling for those elements that have changed
                //TODO: doesn't return changes to conpherence rooms whatsoever, thus need to query available rooms by conpherence.querythread without params
            });
            console.log(counter);
        }
        else {
            console.error("Error Getting Feed: "+err);
        }
    })
    return res.status(200).end();
})


//Configure crawler end point
app.get("/crawl_maniphest", function(req, res) {
    var taskId = req.query.taskId;
    if (!taskId)
        res.status(404).end();
    // Execute a conduit API call
    canduit.exec('maniphest.gettasktransactions', {
        ids: [taskId]
    }, function(err, transactions) {
        if (!err) {
            var phid = getPhIDforTask(taskId);
            ph2e.getCrawlingStateForId(phid, function(state) {
                var maxTransactionId = state.lastProcessedTx;

                var docs = [];
                transactions[taskId].forEach(function(singleDoc) {
                    singleDoc.taskPhID = getPhIDforTask(taskId);
                    if (state.lastProcessedTx < singleDoc.transactionID) // no yet processed
                    {
                        if (singleDoc.transactionID > maxTransactionId)
                            maxTransactionId = singleDoc.transactionID; // new highest tx id
                        var resp = ph2e.transformTaskTransaction(singleDoc);
                        if (!resp.error) {
                            resp.raw = singleDoc;
                            docs.push(resp);
                        }
                        else {; // console.log("Insufficient data for transforming condiut client data: " + resp.msg + "\r\n in raw event: " + JSON.stringify(resp.raw));
                        }
                    }
                    else {
                        console.log("Transaction already processed in prior session: " + singleDoc.transactionID + " of phid: " + phid);
                    }
                });
                //console.log(JSON.stringify(jsonPayload));
                ph2e.storeBulkEvents(docs, function(err, body) {
                    if (err) {
                        console.log(err);
                        return res.status(500).json({
                            'errors': err
                        });
                    }
                    else {
                        console.log(body);
                    }
                });
                state.lastProcessedTx = maxTransactionId; // will be the same as last time if no changes inbetween
                state.lastProcessedTS = new Date();
                ph2e.updateCrawlingState(state, function(result) {
                    console.log(result);
                });
            });
        }
        else
            console.log(err);
    });
    return res.status(200).end();
});

//Configure crawler end point
app.get("/crawl_conpherence", function(req, res) {
    var roomId = req.query.roomId;
    if (!roomId)
        res.status(404).end();
    // Execute a conduit API call
    canduit.exec('conpherence.querytransaction', {
        roomID: roomId
    }, function(err, transactions) {
        if (!err) {
            var phid = getPhIDforChat(roomId);
            ph2e.getCrawlingStateForId(phid, function(state) {
                var maxTransactionId = state.lastProcessedTx;
                var docs = [];
                _.values(transactions).forEach(function(singleDoc) {

                    if (state.lastProcessedTx < singleDoc.transactionID) // no yet processed
                    {
                        if (singleDoc.transactionID > maxTransactionId)
                            maxTransactionId = singleDoc.transactionID; // new highest tx id

                        var resp = ph2e.transformChatTransaction(singleDoc);
                        if (!resp.error) {
                            resp.raw = singleDoc;
                            docs.push(resp);
                        }
                        else {; //console.log("Insufficient data for transforming condiut client data: " + resp.msg + "\r\n in raw event: " + JSON.stringify(resp.raw));
                        }
                    }
                });
                ph2e.storeBulkEvents(docs, function(err, body) {
                    if (err) {
                        console.log(err);
                        return res.status(500).json({
                            'errors': err
                        });
                    }
                    else {
                        console.log(body);
                    }
                });
                state.lastProcessedTx = maxTransactionId; // will be the same as last time if no changes inbetween
                state.lastProcessedTS = new Date();
                ph2e.updateCrawlingState(state, function(result) {
                    console.log(result);
                });
            });
        }
        else
            console.log(err);
    });
    return res.status(200).end();

});


app.get("*", function(request, response) {
    response.status(200).send('<html><body>' +
        '<form action = "http://192.168.65.129:3001/crawl_maniphest" method = "GET">' +
        ' TaskId <input type = "text" name = "taskId"/> ' +
        ' <input type = "submit" value = "Submit"/>' +
        ' </form><br>' +
        '<form action = "http://192.168.65.129:3001/crawl_conpherence" method = "GET">' +
        ' RoomId <input type = "text" name = "roomId"/> ' +
        ' <input type = "submit" value = "Submit"/>' +
        ' </form>' +
        '<form action = "http://192.168.65.129:3001/crawl_history" method = "GET">' +
        ' History (Feed) <input type = "submit" value = "Submit"/>' +
        ' </form>' +
        '</body></html>');
});

var port = 3001;
var connected = function() {
    console.log("Crawler started on port %s : %s", port, Date(Date.now()));
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