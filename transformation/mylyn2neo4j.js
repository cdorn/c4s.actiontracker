var _ = require('lodash');
var DBU = require('../util/dbutil.js');

const TASK_USER_REL_ACTIVE = "ActiveIn";
const ACTIVEIN_SESSION_COUNTER = "SessionCount";

module.exports = M2N;

function M2N(opts) {
    if (!opts.attachmentsDB ||
        !opts.neo4jDB)
        throw new Error("Cannot Init Mylyn2Neo4J without AttachmentsDB or Neo4J DB");
    this.db = opts.attachmentsDB;
    this.neo4j = opts.neo4jDB;
    
    return this;
}

M2N.prototype.user2bugViaSessions = function user2bugViaSessions(cb) {
    // extract all attachments from DB and add to graph DB,
    // for each attachment, its user, a link to the bug, and 
    // a relation of not existing, otherwise increase counter
    var self = this;
    DBU.iterateAllDocuments(self.db, _iterateeProcessAttachment, self, 0, null, function(err, results) {
        if (err) {
            console.log("Error iterating through attachments: "+err);
            return cb(err);
        }
        if (results)
        {
            console.log('There are %d items to be processed', results.length);
            // now insert relations here
            _insertTaskUserRelations(self.neo4j, results, TASK_USER_REL_ACTIVE, cb);
        }
    });
}

function _iterateeProcessAttachment(row, cb) {
    var doc = row.doc;
    return cb(null, [ { taskIdTo: doc.bug_id, 
                        userIdFrom :  doc.attacher,
                        relationType: TASK_USER_REL_ACTIVE
                    } ]);
}

function _insertTaskUserRelations(driver, rels, relType, cb) {
    var session = driver.session();
    // create a relation only if there is no such relation (by type)
    var input = {rels : rels};
    session 
        .run("UNWIND {rels} AS rel "+ 
            "MATCH (t1:Task {taskId: rel.taskIdTo}) "+
            "MERGE (u2:User {userId: rel.userIdFrom}) "+
            "MERGE (u2)-[r: "+relType+"]->(t1) "+
            "ON CREATE SET r."+ACTIVEIN_SESSION_COUNTER+"=1 "+
            "ON MATCH SET r."+ACTIVEIN_SESSION_COUNTER+"=r."+ACTIVEIN_SESSION_COUNTER+"+1 ",
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