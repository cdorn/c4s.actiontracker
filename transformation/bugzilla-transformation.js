var _ = require('lodash');
var async = require('async');
var DateTime = require('date-and-time');
var DBU = require('../util/dbutil.js');
var MT = require('./mylyn-transformation.js');

module.exports = initBT;

function initBT(opts) {
    if (!opts || !opts.fullBugDB || !opts.flatBugDB || !opts.attachmentsDB)
        throw new Error("Fatal error from EclipseBugzillaCrawler: no options AND/OR no CouchDBs provided");
   return new BT(opts);     
}

function BT(opts) {
    this.fullBugDB = opts.fullBugDB;
    this.flatBugDB = opts.flatBugDB;
    this.attachmentsDB = opts.attachmentsDB;
}

BT.prototype.transformFullBugs2ActRefBugs = function transformFullBugs2ActRefBugs(cb)
{
    var self = this;
    DBU.iterateAllDocuments(self.fullBugDB, _iterateeCleanAndRefBug, self, 0, null, function(err, results) {
        if (err) {
            console.log("Error iterating through documents for moving to new db: "+err);
            return cb(err);
        }
        if (results)
        {
            console.log('There were %d items processed', results.length);
            return cb(null, 'ok');
        }
    });
}

const uninterestingBugProperties = ['_rev', 'alias', 'attachments', 'comments', 
                                    'flag', 'groups', 'history', 'is_cc_accessible', 'is_confirmed',
                                    'is_creator_accessible', 'is_open', 'op_sys', 'platform', 'qa_contact',
                                    'see_also', 'target_milestone', 'url', 'version', 'whiteboard'];

function _iterateeCleanAndRefBug(row, cb) {
    // remove comments (not needed for now)
    // filter attachments
    // add doc name reference instead of attachment
    // insert in new db
    var doc = row.doc;
    var newDoc = _.chain(doc)
    //     .pickBy( function(value, key){
		  //  return _.includes(interestingKeys, key);
		  //})
		.omitBy( function(value, key){
		  return _.includes(uninterestingBugProperties, key); // for now we filter out what we definitely dont want while developing, later we only include explicitly what we need once we know what we need
		})
		.value();
		
    newDoc.attachmentDBDocRefs = _.reduce(_filterValidAttachments(doc), function(result, value) {
        result.push(_createAttachmentDocName(value));        
        return result;
    }, []);
    
    this.flatBugDB.insert(newDoc, function(err, result) {
                if (err) {
                    if (err.statusCode != 409) {
                        console.log('Continuing after Error inserting clean bug %s into flatbugdb: '+err, newDoc.bug_id);
                    }
                    return cb(null, [0.01]);
                } else
                    return cb(null, [1.00]);
                });
}



const attachmentPrefix = 'ActivityAttachment-';

function _createAttachmentDocName(att) {
    return attachmentPrefix+att.id+'_Bug-'+att.bug_id;
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

BT.prototype.transformActivityRefBugs2FlatBugs = function transformActivityRefBugs2FlatBugs(cb) {
    var self = this;
    DBU.iterateAllDocuments(self.flatBugDB, _iterateeMergeActivitiesIntoBug, self, 0, null, function(err, results) {
        if (err) {
            console.log("Error iterating through documents for flattening attachments into bug: "+err);
            return cb(err);
        }
        if (results)
        {
            console.log('There were %d items processed', results.length);
            return cb(null, 'ok');
        }
    });
}

function _iterateeMergeActivitiesIntoBug(row, cb) {
    // load bit by bit attachments from db
    // filter out at file level,
    // merge into one structure
    // add to bug
    // update bug
    var self = this;
    async.concatSeries(row.doc.attachmentDBDocRefs, 
                            _loadFilterAndParseAttachmentEvents.bind({db: self.attachmentsDB}),
                            function(err, results) {
                                if (err) {
                                    console.log('Error collecting %d mylyn events: %s',results.length, err);
                                    return cb(null, null);
                                } else {
                                    console.log('Successfully collected %d attachments',results.length);
                                    var events = _.flattenDeep(results);
                                    row.doc.events = events;
                                    self.flatBugDB.insert(row.doc, function(err, result) {
                                        if (err)
                                            console.log('Error storing flattened bug '+err);
                                        else
                                            console.log('Stored flattened bug');
                                        return cb(null, [results.length]);        
                                    })
                                }
                            });
}

const uninterestingEventProperties = ['Delta','Interest','Navigation','OriginId','StartDate'];

function _loadFilterAndParseAttachmentEvents(attId, cb){
    var db = this.db;
    db.get(attId, function(err, att) {
        if (err) {
            console.log('Error loading attachment $s: $s ', attId, err);
            return cb(null, null);
        }
        else {
            var user = att.attacher;
            // extract and transform events out of attachment for later inserting into activity
            var filteredEvents = _.chain(att.jsondata)
            .filter(function(event) { // keep only read and write events
                return (event.Kind == 'selection' || event.Kind == 'edit');
            })
            .map(function(event) { // transform structure handle
                return MT.parseEventResourceHandle(event);
            })
            .map(function(event) { // remove uninteresting keys from event
                return _.omitBy(event, function(value, key) {
                    return _.includes(uninterestingEventProperties, key); // for now we filter out what we definitely dont want while developing,
                });
            })
            .map(function(event) { // transform date into json readable date
                var date =  DateTime.parse(event.EndDate, 'YYYY-MM-DD HH:mm:ss:SSS Z');
                event.epoch = date.valueOf(); 
                event.date = date.toJSON();
                return event;
            })
            .map(function(event) { // add user and bug ids to single event
                event.bug_id = att.bug_id;
                event.user = user;
                return event;
            })
            .value();
            
            return cb(null, [filteredEvents]);
        }
    });
}

