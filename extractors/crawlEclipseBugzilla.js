// JavaScript File
var bz = require("./bz-json-ext.js");
var JSZip = require("jszip");
var X2J = require('xml2js');
//var Readable = require('stream').Readable;
var _ = require('lodash');
var async = require('async');

module.exports = initEBZ;

function initEBZ(opts, cb) {
    if (!opts || !opts.bugDB || !opts.attachmentsDB)
        throw new Error("Fatal error from EclipseBugzillaCrawler: no options AND/OR no CouchDBs provided");
   return new EBZ(opts, cb);     
}

function EBZ(opts, cb) {
    this.db = opts.bugDB;
    this.attachmentsDB = opts.attachmentsDB;
    
    this.createDBIDfromBugID = function(bugId) { return "EBZId-"+bugId; }
    var bzUrl = opts.bugzillaUrl || "https://bugzilla.mozilla.org/jsonrpc.cgi";
    var bzUser = opts.bugzillaUser;
    var bzPassword = opts.bugzillaPassword;
    var self = this;
    
    this.bzClient = bz.createClient({
        url: bzUrl,
        username: bzUser,
        password: bzPassword
    });
    
    this._collectBlobs = function(db, offset, results, cb) {
    var self = this;
    results = results || [];
    //offset = offset || 0;
    console.log("Calling with offset %d", offset);
    db.list({
            include_docs: true,
            limit: 5,
            skip: offset
        },
        function(err, data) {
            var total, offset, rows;
            if (err) {
                rows = [];
                return cb(err, results);
            }
            total = data.total_rows;
            offset = data.offset;
            rows = data.rows;
            if (offset === total) {
                return cb(null, results);
            }
            
            //need to unwarp row to access doc: row.doc
            async.concat(_.flatMap(rows, _unwrapRow), 
                        function (item, cb) {
                            self.storeXMLatt2JSONdoc(item, cb);
                        }, 
                        function(err, result) {
                if (!err)
                {
                    results.concat(result);
                    self._collectBlobs(db, offset + 5, results, cb);    
                }
                else
                    return cb(err, results);
                });
        });
        }
    
    cb(null, self);
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

EBZ.prototype.convertBlobs2Attachments = function convertBlobs2Attachments(bugId, cb) {
    // iterates through all bug documents and extracts attachment blobs, 
    //                                  removes json entry and 
    //                                  stors blob as couchdb attachment for that document
    
    // for testing now just use a single doc
    var self = this;
    this.db.get(bugId, function(err, body) {
        if (err)
            return cb(err);
        else {
            self.storeXMLatt2JSONdoc(body, cb);
            //return cb(null, "BlobExtraction completed");
        }
    });
    
}

EBZ.prototype.convertAllBlobs2Attachments = function convertAllBlobs2Attachments(cb) {
    this._collectBlobs(this.db, 0, null, cb);
}



function _unwrapRow(row) {
    return row.doc;
}

function _unwrapEvent(event) {
    return event['$'];
}

function _unzipAndParse(att, cb) {
    JSZip.loadAsync(att.data, {
        base64: true
    }).then(function(zip) {
        zip.forEach(function(relativePath, file) {
            if (!relativePath.endsWith('.xml'))
            {
                console.log("ZIP doesn't contain XML, continuing after: "+relativePath);
            } else {
                file.async("nodebuffer").then(function success(content) {
                    var parser = new X2J.Parser();
                    parser.parseString(content, function(err, result) {
                        if (err) 
                            return cb(err, null);
                        else {
                            att.data = null;
                            att.jsondata = _.flatMap(result.InteractionHistory.InteractionEvent, _unwrapEvent);
                            att['_id'] = 'ActivityAttachment-'+att.id+'_Bug-'+att.bug_id;
                            return cb(null, att);
                        }
                    })
                    parser = null;
                },
                function error(err) {
                    return cb(err, null);
                });
            }
        });
    });
    cb(null, att);
}

EBZ.prototype.storeBulkAttachments = function storeBulkAttachments(attachments, cb) {
    var bulk = {
        docs : []
    };
    var self = this;
    _.values(attachments).forEach( function(element) {
       bulk.docs.push(element);
    });
    if (bulk.docs.length > 0)
    {
        self.attachmentsDB.bulk(bulk, function(err, ok) {
            if (err)
                return cb(err);
            else
                return cb(null, bulk.docs.length);
        });
    }
    else
        cb(null, 0);
};

EBZ.prototype.storeXMLatt2JSONdoc = function storeXMLatt2JSONdoc(doc, cb) {
   // var attachmentsToStore = [];
    var self = this;
    if (doc && doc.attachments) {
        // first filter out incomplete and keep appl/octet i.e. mylyn docs
        var xmlAtts = _.chain(doc.attachments)
            .filter(function(att) {
                return (att.id && att.file_name && att.data && att.content_type);
            })
            .filter( function(att) {
                return (att.content_type == "application/octet-stream"); 
            })
            .value();
        
        async.map(xmlAtts, _unzipAndParse, function(err, results) {
            if (err)
                return cb(err, null);
            else {
                console.log('%s Processed '+results.length+' out of '+doc.attachments.length+' attachements', doc._id);
                // bulk store
                self.storeBulkAttachments(_.filter(results, function(attachment) {
                    if (!attachment) return false; else return true;
                }), cb);
                //return cb(null, results);
            }
        });
    }
    else {
        console.log('No attachments, ignoring bug: '+doc.id);
        return cb(null, []);
    }
    //                     case "text/plain" :
    //                         const buffer = Buffer.from(att.data, 'base64');
    //                         console.log(buffer.toString('utf8',0,10));
    //                         // var streamIn = new Readable;
    //                         // streamIn.setEncoding('utf8');
    //                         // streamIn.push(att.data);
    //                         // streamIn.push(null);
}



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