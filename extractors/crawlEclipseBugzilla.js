// JavaScript File
var bz = require("./bz-json-ext.js");
var JSZip = require("jszip");
var X2J = require('xml2js');
var fs = require('fs');
//var Readable = require('stream').Readable;
var _ = require('lodash');
var async = require('async');
var DBU = require('../util/dbutil.js');

const attachmentPrefix = 'ActivityAttachment-';

module.exports = {initEBZ,readBugsAndTuplesFile, createDBIDfromBugID};

function initEBZ(opts, cb) {
    if (!opts || !opts.bugDB || !opts.attachmentsDB)
        throw new Error("Fatal error from EclipseBugzillaCrawler: no options AND/OR no CouchDBs provided");
   return new EBZ(opts, cb);     
}

function EBZ(opts, cb) {
    this.db = opts.bugDB;
    this.attachmentsDB = opts.attachmentsDB;
    
    
    var bzUrl = opts.bugzillaUrl || "https://bugzilla.mozilla.org/jsonrpc.cgi";
    var bzUser = opts.bugzillaUser;
    var bzPassword = opts.bugzillaPassword;
    var self = this;
    
    // this.bzClient = bz.createClient({
    //     url: bzUrl,
    //     username: bzUser,
    //     password: bzPassword
    // });
    
    this._collectBlobs = function(offset, results, cb) {
    var self = this;
    results = results || 0.00;
    //offset = offset || 0;
    console.log("Calling with offset %d", offset);
    self.db.list({
            include_docs: true,
            limit: 5,
            skip: offset
        },
        function(err, data) {
            var total, offset, rows;
            if (err) {
                rows = [];
                console.error('Error iterating through bug documents '+err);
                return cb(err, results);
            }
            total = data.total_rows;
            offset = data.offset;
            rows = data.rows;
            if (offset === total) {
                console.log('Completed iterating through all %d documents and %d db inserts', total, results);
                return cb(null, results);
            }
            
            //need to unwarp row to access doc: row.doc
            async.concatSeries(_.flatMap(rows, _unwrapRow), 
                        //function (item, cb) {
                            //self.storeXMLatt2JSONdoc.(item, cb);
                            self.storeXMLatt2JSONdoc.bind(self),
                        //}, 
                        function(err, result) {
                if (!err)
                {
                    //results.concat(result);
                    results = _.reduce(result, function(sum, n) { return sum+n; }, results);
                    
                    self._collectBlobs(offset + 5, results, cb);    
                }
                else {
                    return cb(err, results);
                }
                });
        });
    }
    
    cb(null, this);
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

EBZ.prototype.convertBlob2Attachments = function convertBlob2Attachments(bugId, cb) {
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
    this._collectBlobs(435, null, cb);
}

function _createAttachmentDocName(att) {
    return attachmentPrefix+att.id+'_Bug-'+att.bug_id;
}

function _unwrapRow(row) {
    return row.doc;
}

function _unwrapEvent(event) {
    return event['$'];
}

function _unzip(att, cb) {
    // if(att.summary == 'mylyn/context/zip' ||
    //     att.summary == 'activity_data')
    // {
    JSZip.loadAsync(att.data, {
        base64: true
    }).then(function(zip) {
        var files = [];
        zip.forEach(function(relativePath, file) {
            if (relativePath.endsWith('.xml'))
            {
                files.push(file);
            } 
        });
        if (files.length > 0)
        {
            if (files.length < 1) 
                console.log('Ignoring %d further xml files in attachment %s of bug %s', files.length-1, att.id, att.bug_id);
            _parse(files[0], att, cb);
        }
        else {
          //  console.log('No xml files in attachment %s of bug %s', att.id, att.bug_id);
            return cb('No xml files in attachment');
        }
    }).catch(function (error) {
        //console.log('Error loading attachment data: '+error)
        return cb(error);
    });
    // } else {
    //     console.log('Wont process %s of attachment %s of bug %s', att.file_name, att.id, att.bug_id);
    //     return cb('No suitable zip file in attachment');
    // }
    
}

function _parse(file, att, cb) {
    file.async("nodebuffer").then(function success(content) {
                    var parser = new X2J.Parser();
                    parser.parseString(content, function(err, result) {
                        if (err) {
                            console.err("Error parsing content: "+err);
                            return cb(err, null);
                        } else {
                            file = null;
                            parser = null;
                            if (result && result.InteractionHistory && result.InteractionHistory.InteractionEvent)
                            {
                                att.data = null;
                                att.jsondata = _.flatMap(result.InteractionHistory.InteractionEvent, _unwrapEvent);
                                att['_id'] = _createAttachmentDocName(att);
                                return cb(null, att);
                            }
                            else
                                return cb('Unknown XML content');
                        }
                    });
                },
                function error(err) {
                    console.err('Error loading XML file '+err);
                    return cb(err, null);
                });
}

EBZ.prototype.storeSingleAttachment = function storeSingleAttachment(attRaw, cb) {
    var attachmentsDB = this.attachmentsDB;
    var origAtt = attRaw;
    try {
        _unzip(attRaw, function(err, att) {
            if (err) {
                console.log('Continuing after hidding error in attachment %s of bug %s: ' + err, origAtt.id, origAtt.bug_id);
                return cb(null, [0.00]);
            }
            else {
                attachmentsDB.insert(att, function(err, result) {
                if (err) {
                    if (err.statusCode != 409) {
                        console.log('Continuing after Error inserting attachment %s of bug %s into db: '+err, att.id, att.bug_id);
                    }
                    return cb(null, [0.01]);
                } else
                    return cb(null, [1.00]);
                });
            }
        });
    }
    catch (err) {
        console.log('Continuing after catching undeclared error and hidding it when processing attachment %s of bug %s: ' + err, origAtt.id, origAtt.bug_id);
        return cb(null, [0.00]);
    }
   
    // _catchToNull(attachment, function(err, attParsed) {
    //     // err should not happen, thus
    //     if (err)
    //         return cb(null, 0);
    //     if (attParsed) {
    //         attachmentsDB.insert(attParsed, function(err, result) {
    //             if (err) {
    //                 console.log('Continuing after Error inserting attachment %s of bug %s into db: '+err, attParsed.id, attParsed.bug_id);
    //                 return cb(null, 0);
    //             } else
    //                 return cb(null, 1);
    //         });
    //     }
    //     else {
    //         return cb(null, 0);
    //     }
    // });
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
            if (err) {
                console.log('Error storing bulk documents: '+err);
                return cb(err);
            }
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
        var xmlAtts = _filterValidAttachments(doc);
        console.log('%s Processing '+xmlAtts.length+' out of '+doc.attachments.length+' attachments', doc._id);
        async.concatSeries(xmlAtts, self.storeSingleAttachment.bind(self), function(err, count) {
                if (err) { // all errors should by caught by store single 
                    console.log('Error iterating through attachments: '+err);
                    return cb(err, [0]);
                }
                else {
                    var succ = _.reduce(count, function(sum, n) { return sum+n; }, 0);
                    console.log('Inserted %d attachments for bug %s', succ, doc.id);
                    return cb(null, [succ]);
                }    
        });
        
        
        // async.map(xmlAtts, _catchToNull, function(err, results) {
        //     if (err) // all errors should by caught by catchToNull
        //         return cb(err, null);
        //     else {
        //         console.log('%s Processed '+results.length+' out of '+doc.attachments.length+' attachments', doc._id);
        //         // bulk store
        //         self.storeBulkAttachments(_.filter(results, function(attachment) {
        //             if (!attachment) return false; else return true;
        //         }), cb);
        //         //return cb(null, results);
        //     }
        // });
    }
    else {
        console.log('No attachments, ignoring bug: '+doc.id);
        return cb(null, 0);
    }
    //                     case "text/plain" :
    //                         const buffer = Buffer.from(att.data, 'base64');
    //                         console.log(buffer.toString('utf8',0,10));
    //                         // var streamIn = new Readable;
    //                         // streamIn.setEncoding('utf8');
    //                         // streamIn.push(att.data);
    //                         // streamIn.push(null);
}

EBZ.prototype.checkExtractedAttachments = function checkExtractedAttachments(cb) {
    //  check if all the attachments found in the bugs are also found in the couchdb
    // generate all attachment names: then iterate though attachment db and check retrieve names
    var self = this;
    DBU.iterateAllDocuments(self.db, _iterateeExpectedAttachmentNames, self, 0, null, function(err, expected) {
        if (err) {
            console.log("Error iterating through bugs for expected attachment check: "+err);
            return cb(err);
        }
        if (expected)
        {
           console.log('There are %d expected attachment docs', expected.length);
           DBU.iterateAllDocuments(self.attachmentsDB, _iterateeActualAttachmentName, self, 0, null, function(err, attNames) {
                if (err) {
                    console.log("Error iterating through attachments for expected attachment check: "+err);
                    return cb(err);
                }
                if (attNames) {
                    console.log('There are %d actual attachment docs', attNames.length);
                    var overlap = _.intersection(expected, attNames);
                    var nonExisting = _.difference(expected, overlap);
                    if (nonExisting.length > 0)
                        console.log('NonExisting but Expected Attachements:'+nonExisting);  
                    
                    var notExpected = _.difference(attNames, overlap);
                    if (notExpected.length > 0)
                        console.log('Not Expected but Existing Attachments: '+notExpected);
                    
                    return cb(null, 'ok');
                }
           });
        }
    });
}

function _iterateeActualAttachmentName(row, cb) {
    return cb(null, [row.doc._id]);
}

function _iterateeExpectedAttachmentNames(row, cb) {
    var doc = row.doc;
    var names = [];
    _.forEach(_filterValidAttachments(doc), function(value) {
        names.push(_createAttachmentDocName(value));
    });
    return cb(null, names);
}

function _filterValidAttachments(doc) {
    var filtered = [];
    if (doc && doc.attachments) {
        filtered = _.chain(doc.attachments)
            .filter(function(att) {
                return (att.id && att.file_name && att.data && att.content_type);
            })
            .filter( function(att) {
                return (att.content_type == "application/octet-stream"); 
            })
            .filter( function(att) {
                return (att.summary == 'mylyn/context/zip' || 
                        att.summary == 'mylar/context/zip' ||
                        att.summary == 'activity_data' || 
                        att.summary == 'activity data'
                        );        
            })
            .value();
    }        
    return filtered;
}

EBZ.prototype.removeBugsWithoutMylynContext = function removeBugsWithoutMylynContext(cb) {
    // check if at least one of the attachments found in a bug is a mylyn context, if not, then remove the bug
    var self = this;
    DBU.iterateAllDocuments(self.db, _iterateeNoAttachments, self, 0, null, function(err, results) {
        if (err) {
            console.log("Error iterating through documents for attachment analysis: "+err);
            return cb(err);
        }
        if (results)
        {
            console.log('There are %d items to be processed', results.length);
           async.concatSeries(results, 
                            _deleteDocument.bind(self),
                            function(err, results) {
                                if (err) {
                                    console.log('Error Deleting %d docs',results.length);
                                    return cb(err);
                                } else {
                                    console.log('Successfully Deleted %d docs',results.length);
                                    return cb(null, 'ok');
                                }
                            });
        }
    });
}

function _deleteDocument(doc, cb) {
    var db = this.db;
    if (!doc || !doc._id || !doc._rev)
        return cb("Insufficient details for removing document "+doc);
    db.destroy(doc._id, doc._rev, function(err, result) {
        if (err) {
            console.log('Failed to delete doc: '+doc._id);
            return cb(err);
        } else {
            console.log('Deleted doc: '+doc._id);
            return cb(null, [result]);
        }
    });
                        
}

function _iterateeNoAttachments(row, cb) {
    //var ctx = this;  //needs to be bound by caller
    var doc = row.doc;
    var count = _filterValidAttachments(doc).length;
    if (count <= 0) { // no suitable attachments
        console.log("Bug %s without mylyn attachments", doc.id);
        return cb(null, [doc]);
    }
    else {
       // console.log ("Bug %s with %d attachments", doc.id, count);
        return cb(null, []);
    }
}

EBZ.prototype.removeIllnamedAttachments = function removeIllnamedAttachments(cb) {
    // check each Attachment whether it has a correctly formated name, otherwise remove it from the db
    var self = this;
    DBU.iterateAllDocuments(self.attachmentsDB, _iterateeIllNamed, self, 0, null, function(err, results) {
        if (err) {
            console.log("Error iterating through attachment documents for illformed attachment name: "+err);
            return cb(err);
        }
        if (results)
        {
            console.log('There are %d items to be processed', results.length);
            async.concatSeries(results, 
                            _deleteDocument.bind({db: self.attachmentsDB}),
                            function(err, results) {
                                if (err) {
                                    console.log('Error Deleting %d docs',results.length);
                                    return cb(err);
                                } else {
                                    console.log('Successfully Deleted %d docs',results.length);
                                    return cb(null, 'ok');
                                }
                            });
        }    
    });
    return;
}

function _iterateeIllNamed(row, cb) {
    var doc = row.doc;
    if (!doc._id.startsWith(attachmentPrefix))
    {
        return cb(null, [doc]);
    }
    else
        return cb(null, []);
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

EBZ.prototype.generateBugPairs = function generateBugPairs(cb) {
    var self = this;
    DBU.iterateAllDocuments(self.db, _iterateeBugDependencies, self, 0, null, function(err, results) {
        if (err) {
            console.log("Error iterating through bug documents for bug dependencies: "+err);
            return cb(err);
        }
        if (results)
        {
            console.log('There are %d items to be processed', results.length);
            // need to unwrap structures from result to obtain list of 
            //produce list of tuples <blocking, blocked>
            //produce list of all bugs with attachment
            // filter tuple list where both bugs are in buglist
            // store filtered list as json document
            var bugCount = 0;
            var bugs = { };
            var tuples = _.chain(results)
                .forEach(function(structure){
                    bugCount++;
                    bugs[structure.bug_id] = 0; // store all bugs with attachment (id only)
                })
                .flatMapDeep(function(structure) { // concat all tuples from structure into one single array
                    return structure.dependencies;
                }) 
                .filter(function(tuple) { // filter out those where either bug is without mylyn attachment
                    return (bugs[tuple.from] !== undefined && bugs[tuple.to] !== undefined);
                })
                .filter(function(tuple) { // reduce bidirection links == inverse tuples to single
                    return tuple.from > tuple.to; // we don't care about direction, filters also accidental self references
                })
                .value();
            console.log('There are %d unique bugs, and %d full tuples', bugCount, tuples.length);    
            
            var bugStats = _.chain(tuples)
                .reduce(function(bugs, tuple) {
                  bugs[tuple.from]++;
                  bugs[tuple.to]++;
                  return bugs;
                }, bugs) // having bugs as output, next map to array for sorting
                .reduce(function(result, value, key) {
                    result.push({id: key, count:value}); 
                    return result;
                }, []) // now having array
                // .mapValue(function(value){ // should not be necessary
                //     return (value/2);
                // })
                .orderBy(['count','id'],['desc', 'asc']) // now sort it by count and bugid
                .value();
            
            var bugWdepCount = _.reduce(bugStats, function(result, value) {
                if (value.count >0) 
                    result++;
                return result;
            }, 0);    
            console.log('Bugs w Depdencies: '+bugWdepCount);
                
            fs.writeFile ("bugsAndTuples.json", JSON.stringify({ 'bugstats':bugStats, 'tuples':tuples}), function(err) {
                if (err) {
                    console.log(err);
                    return cb(err);
                }
                else {
                    console.log('complete writing file');
                    return cb(null, 'ok');
                }
            });
        }    
    });
}

function createDBIDfromBugID(bugId) { return "EBZId-"+bugId; }

function readBugsAndTuplesFile(cb) {
    fs.readFile('./data/bugsAndTuplesAll.json', function(err, result) {
        if (err) {
            console.log('Error reading file: '+err);
            return cb(err);
        }
        return cb(null, JSON.parse(result));
    })
}

function _iterateeBugDependencies(row, cb) {
    var  doc = row.doc;
    var structure = { bug_id: doc.id,
                      dependencies: []
    };
    _.forEach(doc.blocks, function(value) {
        structure.dependencies.push({'from' : doc.id,
                                    'to' : value});
    });
    _.forEach(doc.depends_on, function(value) {
        structure.dependencies.push({'to' : value,
                                    'from' : doc.id});
    })
    return cb(null, [structure]);
}

