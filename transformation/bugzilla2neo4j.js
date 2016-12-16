var neo4j = require('neo4j-driver').v1;
var _ = require('lodash');
var async = require('async')

const TASK_REL_BLOCKING = "Blocks";
const TASK_REL_BLOCKED = "BlockedBy";

const TASK_KEY_COUCHDB_ID = "_id";
const TASK_KEY_TASK_ID = "id";
const TASK_KEY_STATUS = "status";
const TASK_KEY_CREATION_TIME = "creation_time";
const TASK_KEY_LAST_CHANGED_TIME = "last_change_time";
const TASK_KEY_BLOCKS = "blocks";
const TASK_KEY_DEPENDS_ON = "depends_on";

const TASK_KEY_N4JGRAPHDB_ID = "couchdb_id";
const TASK_KEY_N4JGRAPHDB_REV = "couchdb_rev";

const NODE_TASK_ID = "taskId";


var node2taskMap = {
                    [NODE_TASK_ID]            : TASK_KEY_TASK_ID,
                    [TASK_KEY_STATUS]         : TASK_KEY_STATUS,
                    [TASK_KEY_CREATION_TIME]  : TASK_KEY_CREATION_TIME,
                    [TASK_KEY_LAST_CHANGED_TIME] : TASK_KEY_LAST_CHANGED_TIME,
                    [TASK_KEY_N4JGRAPHDB_ID]  : TASK_KEY_COUCHDB_ID
               //     [TASK_KEY_N4JGRAPHDB_REV] : '_rev'
    } ;

// read each doc from local bugzilla couch db
// insert each ticket node
//  for each ticket node: read blocked and blocking fields
//  insert relation to that node (insert if does not exists yet)

module.exports = {TasksExtractor,N4J,TaskTransformer};

function TasksExtractor(opts) {
    this.dbName = opts.bugzilla_dbName || 'ebz_db';
    this.createDBIDfromBugID = function(bugId) { return "EBZId-"+bugId; }
    var dbURL = opts.couchDB_url || 'http://localhost:5984';
    
    var nano = require('nano')(dbURL);
    var self = this;
    
    nano.db.create(self.dbName, function(err, body) {
            if (err)
                console.log("Existing Database (" + self.dbName + ") ready");
            else
                console.log("New database (" + self.dbName + ") ready");
            self.db = nano.use(self.dbName);
       });
    return self;
}


TasksExtractor.prototype.getCoreTicketData = function getCoreTicketData(cb) {
    // return no commments or attachements
    var inclKeys =  ["_id", "id", "blocks", "depends_on", "status", "creation_time", "last_change_time" ];
    var self = this;
    
    // self.list(null, inclKeys, null, 10, 5, 
    //     function(err, body){
    //         if (!err) {
    //             return cb(null, body.rows);    
    //         }
    //         else
    //             return cb(err, null);
    //     }
    // );
    /**/
    self.db.list( 
        { 
           // selector : { "_id": { "$gt": null }},
          //  keys: inclKeys,
           include_docs: true,
           limit: 10
        },
        function(err, body){
            if (!err) {
                return cb(null, body.rows);    
            }
            else
                return cb(err, null);
    }); /**/
}

TasksExtractor.prototype.collectTasks = function collectTasks(cb) {
    _collectTasks(this.db, 0, null, cb);
}

function _collectTasks(db, offset, graph, cb) {
    offset = offset || 0;
    db.list({
            include_docs: true,
            limit: 10,
            skip: offset
        },
        function(err, data) {
            var total, offset, rows;
            if (err) {
                rows = [];
                return cb(err, graph);
            }
            total = data.total_rows;
            offset = data.offset;
            rows = data.rows;
            if (offset === total) {
                return cb(null, graph);
            }
            _.values(rows).forEach(function(row) {
                graph = _task2nodeAndRelations(row.doc, graph);
            });
            _collectTasks(db, offset + 10, graph, cb);
        });
}



// COUCH DB 1.6.1 does not support FIND!!!
/*TasksExtractor.prototype.list = function list(selector, fields, sort, limit, skip, cb) {
    var self = this;
    var params = {};
        params.selector = selector || { "_id": { "$gt": null }};
        if (fields) params.fields = fields;
        if (sort)   params.sort = sort;
        if (limit)  params.limit = limit;
        if (skip)   params.skip = skip;
    self.nano.request(  {   db: self.dbName,
                            path: '_find',
                            method: 'POST',
                            qs: params
                        } , cb);
}*/


function TaskTransformer() {
    //var task2nodeAndRelations = function task2nodeAndRelations;
}

TaskTransformer.prototype.task2nodeAndRelations = _task2nodeAndRelations;

function _task2nodeAndRelations(taskDoc, graph) {
    var fromId = taskDoc.id;
    var graph = graph || {
        nodes: [],
        relations: []
    };
    if (taskDoc.depends_on) {
        taskDoc.depends_on.forEach(function(value) {
            graph.relations.push({
                taskIdFrom: fromId,
                taskIdTo: value,
                relationTypeR: TASK_REL_BLOCKED
            });
        });
    }
    if (taskDoc.blocks) {
        taskDoc.blocks.forEach(function(value) {
            graph.relations.push({
                taskIdFrom: fromId,
                taskIdTo: value,
                relationTypeR: TASK_REL_BLOCKING
            });
        });
    }
    var node = _.chain(taskDoc)
        //         .mapKeys( function( value, key ){
        //    switch(key) {
        //     case TASK_KEY_COUCHDB_ID: //rename for neo4j
        //         return TASK_KEY_N4JGRAPHDB_ID;
        //     case "_rev":
        //         return TASK_KEY_N4JGRAPHDB_REV;
        //     default:
        //         return key;   
        //    }
        //  })
        .pickBy(function(value, key) {
           return _.includes(_.values(node2taskMap), key) ;
        })
        // .omitBy(function(value, key) {
        //     return _.includes(["blocks", "depends_on", "comments", "attachements"], key); // filter out data that is represented as relations anyway
        // })
        .value();
    graph.nodes.push(node);
    return graph;
}



function N4J(opts) {
    this.dbURL = opts.neo4jURL || "bolt://localhost";
    this.dbUser = opts.neo4jUser || "neo4j";
    this.dbPW = opts.neo4jPW || "neo4j";
    this.driver = neo4j.driver(this.dbURL, neo4j.auth.basic(this.dbUser, this.dbPW));
    if (opts.cleanUp == true) {
        var session = this.driver.session();    
         session 
        .run("MATCH (x) DETACH DELETE x") 
        .then( function(result) {
            console.log("Successfully cleared Neo4J Database");
            session.close();
        })
        .catch(function(error) {
            console.log("Error clearing Neo4J Database");
            console.log(error);
        });
    }
    return this;
}


N4J.prototype.insertTaskNodes = function insertTaskNodes(tasks, cb) {
    var params = [];
    _.forOwn(node2taskMap, function(value, key) { // BEWARE NOT TO EXTEND THAT MAP VIA PROTOTYPE, WONT SHOW UP HERE
            params.push(key + ": task." + value + " ");
        });
    var batchInsert = ("UNWIND { tasks } AS task MERGE (t:Task {" + params.join(', ') + "})");
    var input = { tasks: tasks};
    var session = this.driver.session();
        // creates a task node only if there is no task node with given id
    session
            .run(batchInsert, input)
            .then(function(result) {
                console.log("Inserted " + tasks.length+ " nodes");
                session.close();
                return cb(null);
            })
            .catch(function(error) {
                  console.log("Error inserting " + tasks.length+ " nodes");
                  session.close();
                  return cb(error);
            });
}

N4J.prototype.storeTasks = function storeTasks(tasks, pos, session, cb) {
    if (pos < tasks.length) {
        var task = tasks[pos];
        var params = [];
        _.forOwn(node2taskMap, function(value, key) { // BEWARE NOT TO EXTEND THAT MAP VIA PROTOTYPE, WONT SHOW UP HERE
            if (task[value]) // the task contains a key in the node2task mapping thus include it in the merge command as node property
            {
                params.push(key + ": {" + value + "}");
            }
        });
        var mergeCommand = ("MERGE (t:Task {" + params.join(', ') + "})");
        if (!session) 
            session = this.driver.session();
        // creates a task node only if there is no task node with given id
        session
            .run(mergeCommand, task)
            .then(function(result) {
                console.log("Inserted Node with TaskId: " + task.id);
                pos++;
                storeTasks(tasks, pos, session, cb);
            })
            .catch(function(error) {
                  console.log("Error inserted Node with TaskId: " + task.id+" while processing task "+pos+" out of "+tasks.length);
                  session.close();
                  return cb(error);
            });
    }
    else
    {
        if (session)
            session.close();
        return cb(null);
    }
}


N4J.prototype.insertTaskRelations = function insertTaskRelations(rels, cb) {
    // sort relations dynamically according to relation type
    var relMap = {};
    var self = this;
    rels.forEach( function(rel) {
        if (!relMap[rel.relationTypeR]) {
            relMap[rel.relationTypeR] = [];
        }
        relMap[rel.relationTypeR].push(rel);
    });
    async.forEachOf(relMap, function(value, key, callback) {
        _insertTaskRelations(self.driver, value, key, callback);
    }, cb);
}


function _insertTaskRelations(driver, rels, relType, cb) {
    var session = driver.session();
    // create a relation only if there is no such relation (by type)
    var input = {rels : rels};
    session 
        .run("UNWIND {rels} AS rel "+ 
            "MATCH (t1:Task {taskId: rel.taskIdFrom}) "+
            "MERGE (t2:Task {taskId: rel.taskIdTo}) "+
                "CREATE UNIQUE (t1)-[r: "+relType+"]->(t2) ",
                input) 
        .then( function(result) {
            if (result.records.length > 0)
                console.log("Inserted "+result.records.length+" relations ");
            else     
                console.log("Failed to inserted relations");
            session.close();
            cb(null);
        }).catch(function(error) {
            session.close();
            console.log(error);
            return cb(error);
        });
    
}

N4J.prototype.createRelation = function createRelation(rel) {
    var session = this.driver.session();
    // create a relation only if there is no such relation (by type)
    session 
        .run("MATCH (t1:Task {taskId: {taskIdFrom}}) "+
            "MERGE (t2:Task {taskId: {taskIdTo}}) "+
                "CREATE UNIQUE (t1)-[r: "+rel.relationTypeR+"]->(t2) "+
                "RETURN r", 
                rel) 
        .then( function(result) {
            if (result.records.length > 0)
                console.log("Inserted Relation: "+rel.taskIdFrom+"<>"+rel.taskIdTo+"#"+rel.relationTypeR);
            else     
                console.log("Failed to inserted Relation: "+rel.taskIdFrom+"<>"+rel.taskIdTo+"#"+rel.relationTypeR);
            session.close();
        }).catch(function(error) {
            console.log(error);
        });
}