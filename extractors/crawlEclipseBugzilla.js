// JavaScript File
var bz = require("./bz-json-ext.js");
var _ = require('lodash');

module.exports = initEBZ;

function initEBZ(opts, cb) {
    if (!opts)
        throw new Error("Fatal error from EclipseBugzillaCrawler: no options provided");
   return new EBZ(opts, cb);     
}

function EBZ(opts, cb) {
    this.dbName = opts.dbName || 'ebz_db';
    this.cleanUp = opts.cleanUp || 'false';
    this.createDBIDfromBugID = function(bugId) { return "EBZId-"+bugId; }
    var bzUrl = opts.bugzillaUrl || "https://bugzilla.mozilla.org/jsonrpc.cgi";
    var bzUser = opts.bugzillaUser;
    var bzPassword = opts.bugzillaPassword;
    var dbURL = opts.dbURL || 'http://localhost:5984';
    
    var nano = require('nano')(dbURL);
    
    
    
    var self = this;
    // setup couchDB
    if (this.cleanUp == 'true')
    {
        // for development we clean up the database we created previously
        nano.db.destroy(self.dbName, function(err, body) {
            if (err)
            {
                console.log("Database (" + self.dbName + ") doesn't exit - nothing to destroy");
            }
            // create a new database
            nano.db.create(self.dbName, function(err, body) {
                if (!err) {
                    console.log("Database (" + self.dbName + ") created!");
                    self.db = nano.use(self.dbName);
                    console.log("New Database (" + self.dbName + ") ready");
                }
                else {
                    return cb(err, null);
                }
            });
        });
    } else {
       // check if existing
       nano.db.create(self.dbName, function(err, body) {
            if (err)
                console.log("Existing Database (" + self.dbName + ") ready");
            else
                console.log("New database (" + self.dbName + ") ready");
            self.db = nano.use(self.dbName);
       });
    }
    
    this.bzClient = bz.createClient({
        url: bzUrl,
        username: bzUser,
        password: bzPassword
    });
    
    return cb(null, self);
}

function result2singleBug(result) {
    if (result && result.result && result.result.bugs && result.result.bugs.length > 0)
    {
        var bug = result.result.bugs[0];
        return bug;
    }
    return null;
}

function result2comments(result, bugId) {
    if (result && result.result && result.result.bugs && 
            result.result.bugs[bugId] &&
            result.result.bugs[bugId].comments)
            {
                return result.result.bugs[bugId].comments;
            }
    return null;
}

function result2history(result, bugId) {
    if (result && result.result && result.result.bugs && 
            result.result.bugs.length > 0 &&
            result.result.bugs[0].history)
            {
                return result.result.bugs[0].history;
            }
    return null;
}

function result2attachments(result, bugId) {
    if (result && result.result && result.result.bugs && 
            result.result.bugs[bugId] &&
            result.result.bugs[bugId].length > 0)
            {
                return result.result.bugs[bugId];
            }
    return null;
}

EBZ.prototype.extractBug = function extractBug(bugId, cb) {
    var self = this;
    var docId = self.createDBIDfromBugID(bugId);
    self.db.head(docId, function(err, headers) {
        if (err) {
            self.bzClient.getBug(bugId, function(error, result) {
                if (!error) {
                    var bug = result2singleBug(result);
                    self.bzClient.getComments(bugId, function(error, comments) {
                        if (!error)
                            bug.comments = result2comments(comments, bugId);
                        // in any case continue with history
                        self.bzClient.getHistory(bugId, function(error, history) {
                            if (!error)
                                bug.history = result2history(history, bugId);
                            // in any case continue with attachments
                            self.bzClient.getAttachments(bugId, function(error, attachments) {
                                if (!error)
                                    bug.attachments = result2attachments(attachments, bugId);
                                // in any case continue with storing bug structure
                                self.db.insert(bug, docId, cb);
                            });
                        });
                    });
                }
                else {
                    cb(error);
                }
            });
        }
        else cb(null, "Bug: "+bugId+" already in DB, WILL NOT crawl and store it");
    });
};

EBZ.prototype.extractBugIds = function extractBugsFromFile(filename, cb) {
 
    var fs = require('fs');
    var cheerio = require('cheerio');
    
    var self = this;
    
    fs.readFile('./data/'+filename, function (err, html) {
        if (err)
            cb(err);
        else {
            var $ = cheerio.load(html);
            
            var ids = [];
            $(':input')
                .filter(function(i, el) {
                    return $(this).attr('name') === 'id';
                })
                .each(function(i, elem) {
                    ids[i] = $(this).attr('value');
                });
            cb(null, ids);
        }
    });
}

EBZ.prototype.mergeBugs = function mergeBugs(cb) {
    
    var blockedHTML = 'blockedbugs.html';
    var blockingHTML = 'blockingbugs.html';
    var self = this;
    this.extractBugIds(blockedHTML, function (err, result) {
        if (!err) {
            console.log("Blocked Bugs Count: "+result.length);
            self.extractBugIds(blockingHTML, function(err, result2) {
                if (!err) {
                    console.log("Blocking Bugs Count: "+result2.length);
                    var intersectBugs = _.intersection(result, result2);
                    console.log("Overlapping Bugs Count: "+intersectBugs.length);
                    console.log(intersectBugs);
                    return cb(null, 'ok');
                }
                else 
                    return cb(err);
            });
        }
        else
            return cb(err);
    });
    
}