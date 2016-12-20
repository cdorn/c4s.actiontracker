"use strict";

var express = require('express');
var bodyParser = require('body-parser');
var errorHandler = require('errorhandler');
var http = require('http');
var _ = require('lodash');
var c = require('./config.js');
var B2N = require('./transformation/bugzilla2neo4j.js');
var M2N = require('./transformation/mylyn2neo4j.js');
var DBU = require('./util/dbutil.js');
var async = require('async');

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

var attachmentdbConfig = {
    dbName: c.config.bugAttachments_dbName,
    cleanUp: 'false',
    dbURL: c.config.couchDB_url,
};

var graphdbConfig = {
   neo4jUser: c.config.neo4jUser, 
   neo4jPW: c.config.neo4jPW,
   neo4jURL: c.config.neo4jURL,
   cleanUp : 'false'
}

var m2n = undefined;
async.parallel([
        async.apply(DBU.initNeo4jDB, graphdbConfig),
        async.apply(DBU.initCouchDB, attachmentdbConfig)
    ],
    function(err, results) {
        if (err)
            throw new Error("Fatal error initializing DBs for Eclipse Bugzilla Extractor \r\n" + err);
        else {
            m2n = new M2N({ neo4jDB : results[0],
                            attachmentsDB : results[1] });
        }
    });




c.config['cleanUp'] = 'false'; // for neo4j only!!

// var tex = new B2N.TasksExtractor(c.config);

// var n4j = new B2N.N4J(c.config);

// var trn = new B2N.TaskTransformer();

app.get("/bugs2neo4j",  function(req, res) {
    
    //insertTestBZData();
    //insertAllBZData();
    m2n.user2bugViaSessions(function (err, result) {
        if (err) {
            return res.status(500).json({
                        'errors': err
                    });
        }
        else  return res.status(200).end();
    });
});

function insertAllBZData() {
    tex.collectTasks(function(err, graph) {
        if (err)
           console.log(err); 
        if (graph) {
            n4j.insertTaskNodes(graph.nodes, function(err) {
                    if (!err) {
                        n4j.insertTaskRelations(graph.relations, function(err) {
                            if (err)
                                console.log(err);
                            else
                                console.log("Completed Transfer of Tasks into Graph DB");
                        }); 
                    } else
                        console.log(err);
                });
        }
    });
}

function insertTestBZData() {
    tex.getCoreTicketData(function(err, rows){
            if (!err) {
                var nodes = []
                var relations = [];
                _.values(rows).forEach( function(row) {
                    var graph = trn.task2nodeAndRelations(row.doc, null);
                    nodes = nodes.concat(graph.nodes);
                    relations = relations.concat(graph.relations);
                });
                n4j.insertTaskNodes(nodes, function(err) {
                    if (!err)
                        n4j.insertTaskRelations(relations, function(err) {
                            if (err)
                                console.log(err);
                        }); 
                    else
                        console.log(err);
                });
            } else {
                console.log(err);
            }
        });
}

// function insertTaskNodes(tasks) {
//     tasks.forEach(function (task){
//         n4j.storeTask(task);
//     });
// }

// function insertTaskRelations(relations) {
//     relations.forEach(function (rel) {
//         n4j.createRelation(rel);
//     });
// }

function insertTestData() {
    var task1 = {id: 1};
    var task2 = {id: 2};
    var task3 = {id: 3};
    n4j.storeTask(task1);
    n4j.storeTask(task2);
    n4j.storeTask(task3);
    
    n4j.createRelation({taskIdFrom: task1.id , taskIdTo: task2.id, relationTypeR: "Blocks"});
    n4j.createRelation({taskIdFrom: task2.id , taskIdTo: task1.id, relationTypeR: "BlockedBy"});
    n4j.createRelation({taskIdFrom: task2.id , taskIdTo: task3.id, relationTypeR: "Blocks"});
    n4j.createRelation({taskIdFrom: task3.id , taskIdTo: task2.id, relationTypeR: "BlockedBy"});
}

app.get("*", function(request, response) {
    response.status(200).send('<html><body>' +
        '<form action = "http://192.168.65.129:'+port+'/bugs2neo4j" method = "GET">' +
        ' Bugs 2 Graph <input type = "submit" value = "Submit"/>' +
        '</body></html>');
});


var connected = function() {
    console.log("Task ETL started on port %s : %s", port, Date(Date.now()));
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