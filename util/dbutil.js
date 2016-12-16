
module.exports = initCouchDB;

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