var async = require('async');
var neo4j = require('neo4j-driver').v1;

module.exports = {initCouchDB,initNeo4jDB,iterateAllDocuments,iterateAllDocumentsOneByOne};

// function DBUtil() {
//     var self = this;
//     return self;
// } 

function initNeo4jDB(opts, cb) {
    var dbURL = opts.neo4jURL || "bolt://localhost";
    var dbUser = opts.neo4jUser || "neo4j";
    var dbPW = opts.neo4jPW || "neo4j";
    var cleanUp = opts.cleanUp || false;
    var driver = neo4j.driver(dbURL, neo4j.auth.basic(dbUser, dbPW));
    if (cleanUp == true) {
        var session = driver.session();    
         session 
        .run("MATCH (x) DETACH DELETE x") 
        .then( function(result) {
            console.log("Successfully cleared Neo4J Database");
            session.close();
            cb(null, driver);
        })
        .catch(function(error) {
            console.log("Error clearing Neo4J Database");
            console.log(error);
            cb(error);
        });
    }
    else cb(null, driver);
}

//DBUtil.prototype.initCouchDB =
function initCouchDB(opts, cb) {
    if (!opts.dbName)
        return cb("Fatal error from CouchDB Util: no 'dbName' provided", null);
    var dbName = opts.dbName;
    var cleanUp = opts.cleanUp || 'false';
    var dbURL = opts.dbURL || 'http://localhost:5984';
    var nano = require('nano')(dbURL);
    var db = null;
    //var self = this;
    // setup couchDB
    if (this.cleanUp == 'true')
    {
        // for development we clean up the database we created previously
        nano.db.destroy(dbName, function(err, body) {
            if (err)
            {
                console.log("Database (" + dbName + ") doesn't exit - nothing to destroy");
            }
            // create a new database
            nano.db.create(dbName, function(err, body) {
                if (!err) {
                    console.log("Database (" + dbName + ") created!");
                    db = nano.use(dbName);
                    console.log("New Database (" + dbName + ") ready");
                    return cb(null, db);
                }
                else {
                    return cb(err, null);
                }
            });
        });
    } else {
       // check if existing
       nano.db.create(dbName, function(err, body) {
            if (err)
                console.log("Existing Database (" + dbName + ") ready");
            else
                console.log("New database (" + dbName + ") ready");
            db = nano.use(dbName);
            return cb(null, db);
       });
    }
}


//DBUtil.prototype.iterateAllDocuments =
function iterateAllDocuments(db, iteratee, ctx, offset, results, cb) {
    results = results || [];
    console.log("Calling with offset %d", offset);
    db.list({
            include_docs: true,
            limit: 10,
            skip: offset
        },
        function(err, data) {
            var total, offset, rows;
            if (err) {
                rows = [];
                console.error('Error iterating through documents '+err);
                return cb(err, results);
            }
            total = data.total_rows;
            offset = data.offset;
            rows = data.rows;
            if (offset === total) {
                console.log('Completed iterating through all %d documents', total);
                return cb(null, results);
            }
            
            //need to unwarp row to access doc: row.doc
            async.concatSeries(rows, 
                        iteratee.bind(ctx), 
                        function(err, result) {
                if (!err)
                {
                    iterateAllDocuments(db, iteratee, ctx, offset + 10, results.concat(result), cb);    
                }
                else {
                    return cb(err, results);
                }
                });
        });
    }
    
function iterateAllDocumentsOneByOne(db, iteratee, ctx, offset, results, cb) {
    results = results || [];
    db.list({
            include_docs: true,
            limit: 1,
            skip: offset
        },
        function(err, data) {
            var total, offset, rows;
            if (err) {
                rows = [];
                console.error('Error iterating through documents '+err);
                return cb(err, results);
            }
            total = data.total_rows;
            offset = data.offset;
            rows = data.rows;
            if (offset === total) {
                console.log('Completed iterating through all %d documents', total);
                return cb(null, results);
            }
            
            //need to unwarp row to access doc: row.doc
            async.concatSeries(rows, 
                        iteratee.bind(ctx), 
                        function(err, result) {
                if (!err)
                {
                    iterateAllDocuments(db, iteratee, ctx, offset + 1, results.concat(result), cb);    
                }
                else {
                    return cb(err, results);
                }
                });
        });
    }    