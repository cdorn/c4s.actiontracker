
var _ = require('lodash');
var async = require('async');
var csvWriter = require('csv-write-stream');
var fs = require('fs');
var DBU = require('../util/dbutil.js');
var CBZ = require('../extractors/crawlEclipseBugzilla.js');

module.exports = {initAtt2Mylyn, initMylynEventAnalysis, parseEventResourceHandle};

function initAtt2Mylyn(opts) {
    if (!opts.attachmentsDB)
        throw new Error("Cannot Init Att2Mylyn without AttachmentsDB");
    return new A2M(opts);
}

function A2M(opts) {
    this.db = opts.attachmentsDB;
    
}

// each attachment represents a user session with multiple interaction event (inside jsondata)

function filterEvents(events) {
    var fEvents = _.chain(events)
    .reject( function(event) {
            return _.includes(['propagation','prediction','command','manipulation'], event.Kind);
        })
    .flatMap(parseEventResourceHandle)
    .value();
    return fEvents;
}

A2M.prototype.parseStructure = parseEventResourceHandle;

function parseEventResourceHandle(event) {
    var handle = event.StructureHandle;
    if (event.StructureKind == 'java') {
        var stats = {
            project : '',
            packagepath : '',
            classname : '',
            method : '',
            resourceName : '',
            path : ''
        };
        event.structure = stats;
        // =org.eclipse.mylyn.tests/src&lt;org.eclipse.mylyn.tests.integration{ChangeDataDirTest.java
        //=org.eclipse.mylyn.tasks.tests/src&lt;org.eclipse.mylyn.tasks.tests
        // =org.eclipse.mylyn.tests
        
        var projectEnd = handle.indexOf('/');
        if (projectEnd <= 0) projectEnd = handle.length;
        stats.project = handle.substring(1,projectEnd); //we remove the '=' at the beginning
        
        // between projeEnd and packageStart are further paths within project
        var packageStart = handle.lastIndexOf('&lt;');
        if (packageStart <= 0) // accessed only path, no package or classes
        {
            stats.type = 'PROJECT';
            if (projectEnd+1 == handle.length) {// root of the project
                stats.resourceName = stats.project;
                stats.path = '';
            } else {
                stats.resourceName = handle.substring(projectEnd);
                stats.path = stats.project;
            }
            return event;
        } // else just set the current path until here and continue
        stats.path = stats.path+handle.substring(1,packageStart);
        // if there are packages, processes these, we don't split packages but just keep them as the whole path
        
        var classPos = handle.indexOf('{');
        if (classPos <= 0) { // just package access/manipulation
            stats.type = "PACKAGE";
            stats.packagepath = handle.substring(packageStart+4); //+4 to skip the package separator
            stats.resourceName = stats.packagepath;
            return event;
        } // else just ste the current path until here, thus change package path to file system path
        stats.packagepath = handle.substring(packageStart+4, classPos);
        stats.path = stats.path + '/' + stats.packagepath.split(".").join("/");
        // continue with class and method
        var classEnd = handle.indexOf('.java', classPos);
        stats.classname = handle.substring(classPos+1, classEnd);
        // if there is a method beyond the class
        if (classEnd+5 == handle.length) { // no method part
            stats.type = "CLASS";
            stats.resourceName = handle.substring(classPos+1);
        } else {
            stats.type = "METHOD";
            stats.method = handle.substring(classEnd+6); // .java + [ that denotes method start
            stats.resourceName = stats.method;
            // further separators include '~' intrafile class, method, and ';' for parameters
            // ^ for class consts/vars
        }
        return event;
    }
    else {
        var lastPos = handle.lastIndexOf('/'); //path will include training /
        event.structure = { type : 'FILE',
                            project : handle.substring(1, handle.indexOf('/', 1)),
                            path : handle.substring(1, lastPos),
                            resourceName : handle.substring(lastPos+1)}
    }
    return event;
}

A2M.prototype.loadAllAttachments = function loadAllAttachments(cb) {
    _collectAttachments(this.db, 0, null, cb);
}

function _collectAttachments(db, offset, results, cb) {
    var self = this;
    results = results || [];
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
                return cb(err, results);
            }
            total = data.total_rows;
            offset = data.offset;
            rows = data.rows;
            if (offset === total) {
                return cb(null, results);
            }
            
            //need to unwarp row to access doc: row.doc
            _.values(rows).forEach( function(row){
                 console.log("Processing: %s by %s", row.doc._id, row.doc.creator);
                 console.log(getKindStats(row.doc.jsondata));
            //});
            //async.concat(_.flatMap(rows, _unwrapRow), 
            //            self.storeXMLatt2JSONdoc, 
            //            function(err, result) {
            //    if (!err)
            //    {
            //        results.concat(result);
                    _collectAttachments(db, offset + 10, results, cb);    
             //   }
             //   else
            //        return cb(err, results);
            });
    });
}

function _unwrapRow(row) {
    return row.doc;
}

A2M.prototype.loadAttachment = function loadAttachment(id, cb) {
    this.db.get(id, function(err, body) {
        if (!err) {
        //   console.log(getKindStats(body.jsondata));
        //   var events = filterEvents(body.jsondata);
        //   console.log('Filtered '+body.jsondata.length+" events down to "+events.length);
        // //   _.forEach(events, function(value) {
        // //       console.log(value.structure);
        // //   });
        //     var stats = getResourceStats(events);
        //     // Read and Write per FILE!!! not overall
        //     stats.readCount = _.chain(stats.read)
        //                         .mapValues( function(value, key) {
        //                             return value.length;
        //                         })
        //                         .value();
        //     stats.writeCount = _.chain(stats.write)
        //                         .mapValues( function(value, key) {
        //                             return value.length;
        //                         })
        //                         .value();
        //     stats.onlyReads = _.difference(_.keys(stats.read),_.keys(stats.write));                    
        //     console.log(stats);

        //     return cb(null, events);
        
        
            var writer = csvWriter({ headers: csvAttKeys.concat(csvEventKeys)});
            writer.pipe(fs.createWriteStream('stats1.csv'));
            writer.write(_compileCSVLine(body));
            writer.end();
           return cb(null, 'ok');
        
        }
        else return cb(err);
    });
}

function getKindStats(events) {
    return _.reduce(events, function(result, event) {
        if (!result[event.Kind])
            result[event.Kind] = 0;
        result[event.Kind]++;    
        return result;
    }, { 
        propagation : 0, 
        manipulation : 0,
        edit : 0,
        selection : 0,
        prediction : 0,
        command : 0
    });
}

const csvEventKeys = ['uniqueArtifactReads', 'uniqueArtifactWrites','readOnlyArtifactCount','uniqueJavaArtifacts','uniqueResourceArtifacts', 
                'edit','selection','command','manipulation','prediction','propagation'];


const csvAttKeys = ['id', 'user', 'bug', 'timestamp'];

function getEventStats(events) {
    // store for each artifact the number of occurences for selected and manipulation
    var stats = {
        read : {},
        write : {},
        java : {},
        resource : {},
        propagation : 0, 
        manipulation : 0,
        edit : 0,
        selection : 0,
        prediction : 0,
        command: 0
    };
    stats = _.reduce(events, function(result, event){
        if (event.Kind == 'selection') {
            (result.read[event.StructureHandle] || (result.read[event.StructureHandle] = [])).push(event);     
        } else if (event.Kind == 'edit') {
            (result.write[event.StructureHandle] || (result.write[event.StructureHandle] = [])).push(event);
        }
        if (event.StructureKind == 'java') {
             (result.java[event.StructureHandle] || (result.java[event.StructureHandle] = [])).push(event);
        } else if (event.StructureKind == 'resource') {
             (result.resource[event.StructureHandle] || (result.resource[event.StructureHandle] = [])).push(event);
        }
        if (!result[event.Kind])
            result[event.Kind] = 0;
        result[event.Kind]++; 
        return result;
    }, stats);
    stats.readOnlyArtifacts = _.difference(_.keys(stats.read),_.keys(stats.write));
    
    stats.uniqueArtifactReads = _.keys(stats.read).length;
    stats.uniqueArtifactWrites = _.keys(stats.write).length;
    stats.readOnlyArtifactCount = stats.readOnlyArtifacts.length; 
    stats.uniqueJavaArtifacts = _.keys(stats.java).length;
    stats.uniqueResourceArtifacts = _.keys(stats.resource).length;
    
    return stats;
}

A2M.prototype.allAttachmentStats2CSV = function allAttachmentStats2CSV(cb) {
    DBU.iterateAllDocuments(this.db, _iterateeCSVLine, this, 0, null, function(err, results){
        if (err) {
            console.log("Error iterating through attachments for stats collection: "+err);
            return cb(err);
        }
        if (results)
        {
           console.log('There are %d items to be processed', results.length);
           var writer = csvWriter({ headers: csvAttKeys.concat(csvEventKeys)});
           writer.pipe(fs.createWriteStream('stats.csv'));
           _.forEach(results, function(csvLine) {
               writer.write(csvLine);
           });
           writer.end();
           return cb(null, 'ok');
        }
    });
}


function _iterateeCSVLine(row, cb) {
    return cb(null, [_compileCSVLine(row.doc)]);
}

function _compileCSVLine(att)
{
    var stats = getEventStats(att.jsondata);
    var attStats = [att.id,
                    att.attacher, 
                    att.bug_id,
                    att.creation_time,
                  ];
    var eventStats = [];
    _.forEach(csvEventKeys, function(value) {
        eventStats.push(stats[value]);
    });
    return (attStats.concat(eventStats));
}


// function _calcUserStats(stats, attachment) {
//     stats.userSessionCount[attachment.attacher] ? stats.userSessionCount[attachment.attacher]++ : stats.userSessionCount[attachment.attacher] = 1;
    
//     //stats.userBugs[attachment.attacher] ? stats.userSession[attachment.attacher] = stats.userBugs[attachment.attacher]. : stats.userSession[attachment.attacher] = 1;
// }

// function _collectUserData(data, attachment) {
//     (data.userBugs[attachment.attacher] || (data.userBugs[attachment.attacher] = [])).push(attachment.bug_id);
// }

//         statistics for:

// for pairs of blocking blocked:
// # of tuples where A and B have mylyn context = full tuple
// for each full tuple:
// overlap of active/session developers
// # of A.edited and B.viewed
// % of subset that was A.edited and Viewed in B,
// % of viewed in B that was edited in A
// for multidependencies: % of viewed in B that was edited in any blocking A (union of edited in all As)
// overlap of A.edited and B.edited


// FILTERING DOWN
// consider only reading and writing of java class files and general files
// --> remove any reading of project or package level
// --> raise reading/editing of method up to class level

function initMylynEventAnalysis(opts) {
    if (!opts.flatBugDB)
        throw new Error("Cannot Init MylynEventAnalys without flatBugDB");
    return new MEA(opts);
}

function MEA(opts) {
    this.db = opts.flatBugDB;
    
}



MEA.prototype.analyseBugDependencies = function analyseBugDependencies(cb) {
    var db = this.db;
// function load pairs from file
    CBZ.readBugsAndTuplesFile(function (err, stats) {
        if (!err)
        {
            async.concat(stats.tuples, 
                function(tuple, cb) {
                 async.parallel([
                     // for each pair load both flatBugs
                        async.apply(db.get, CBZ.createDBIDfromBugID(tuple.from)),
                        async.apply(db.get, CBZ.createDBIDfromBugID(tuple.to))
                        ],
                    function(err, results) {
                         if (err) return cb(err);
                         // figure out which one is dependent which one is blocker
                         var bug1 = results[0][0];
                         var bug2 = results[1][0];
                         var stats1 = _getBugStats(bug1);     
                         var stats2 = _getBugStats(bug2);
                         var tuplestats = {};
                         var overlapStats = null;
                         if (bug1.blocks.indexOf(tuple.to) >= 0) // bug1 is blocker
                         {
                             overlapStats = _getOverlapStats(stats1, stats2);
                             tuplestats.blockingBugId = bug1.id;
                             tuplestats.dependentBugId = bug2.id;
                             tuplestats.blockingStats = stats1;
                             tuplestats.dependentStats = stats2;
                             tuplestats.overlapStats = overlapStats;
                         }
                         else {
                             overlapStats = _getOverlapStats(stats2, stats1);
                             tuplestats.blockingBugId = bug2.id;
                             tuplestats.dependentBugId = bug1.id;
                             tuplestats.blockingStats = stats2;
                             tuplestats.dependentStats = stats1;
                             tuplestats.overlapStats = overlapStats;
                         }
                         return cb(null, [tuplestats]);
                    });
                },
                function(err, results) {
                                if (err) {
                                    console.log('Error Getting BugStatistics');
                                    return cb(err);
                                } else {
                                    console.log('Successfully analysed %d dependencies',results.length);
                                    _writeOverlapStatsToCSV(results);
                                    return cb(null, 'ok');
                                }
                });
        }
        else
            return cb(err);
    });
}

MEA.prototype.analyseAllBugDependencies = function analyseAllBugDependencies(cb) {
    
    var db = this.db;
    var self = this;
// function load pairs from file to check againts the 160 tuples we don't want to compare
// dont care, calc now 
    // CBZ.readBugsAndTuplesFile(function (err, stats) {
    //     if (!err)
    //     {
            // var ctx = { db : self.db,
            //             offset : 0,
            //         };
            // start with One bug and load all others behind it in the db            
            //DBU.iterateAllDocumentsOneByOne(db, _iterateeFlatBug, ctx, 0, null, function(err, results) {
            DBU.iterateAllDocuments(db, _iterateeCalcFlatBugStats, self, 0, null, function(err, results) {
            if (err) {
                console.log("Error iterating through bug documents for bug independencies: "+err);
                return cb(err);
            }
            if (results) // contains the stats of all bugs
            {
                var writer = csvWriter({ headers: ['bug1','bug2', 
                'overlap_readOnly', 'overlap_writeSet', 'overlap_w1r2', 'overlap_w2r1',
                '1_ArtR', '1_ArtW', '1_Ronly', '2_ArtR', '2_ArtW', '2_Ronly' 
                ]});
                //hwriter.pipe(fs.createWriteStream('alltuplestats.csv'));
                //hwriter.end();
                
                console.log('There are %d items to be processed', results.length);
                var len = results.length;
                //var len = 5; // for testing
                for (var i = 0; i < len-1; i++) {
                    console.log('Processing Stats Starting at item %d', i);
                    // //var writer = csvWriter({ headers: ['bug1','bug2', 
                    // 'overlap_readOnly', 'overlap_writeSet', 'overlap_w1r2', 'overlap_w2r1',
                    // '1_ArtR', '1_ArtW', '1_Ronly', '2_ArtR', '2_ArtW', '2_Ronly' 
                    // ] , sendHeaders: false});
                    
                    //writer.pipe(fs.createWriteStream('alltuplestats.csv', {flags : 'a'}));
                
                    for (var j = i+1; j < len; j++) {
                        var stats1 = results[i];
                        var stats2 = results[j];
                        var tuplestats = {};
                             tuplestats.blockingBugId = stats1.bugId;
                             tuplestats.dependentBugId = stats2.bugId;
                             tuplestats.blockingStats = stats1;
                             tuplestats.dependentStats = stats2;
                        tuplestats.overlapStats = _getOverlapStats(stats1, stats2);
                        writer.write(_tupleStats2CVSline(tuplestats));
                        tuplestats = null;
                    }
                    //writer.end();
                    results[i] = null;
                }
                writer.end();
                console.log('completed writing stats');
                return cb(null, 'ok');
            }
            });
    //    }
    //     else
    //         return cb(err);
    // });
}

function _iterateeCalcFlatBugStats(row, cb) {
    var stats1 = _getBugStats(row.doc);
    stats1.bugId = row.doc.id;
    return cb(null, stats1);
}



// function _iterateeFlatBug(row, cb) {
//     var db = this.db;
//     var nextOffset = this.offset+1;
//     var bug = row.doc;
//     var stats1 = _getBugStats(bug); 
//     // now compare with all other further down the db
//     var ctx = { db : db,
//                 offset : nextOffset
//                     };
//     console.log('At localoffset %d calling iterateeFlatBugTo with offset %d',this.offset, nextOffset);
//     DBU.iterateAllDocumentsOneByOne(db, _iterateeFlatBugTo, ctx, nextOffset, null, function(err, results) {
//             if (err) {
//                 console.log("Error iterating through bug documents for bug independencies: "+err);
//                 return cb(err);
//             }
//             if (results)
//             {
//                 console.log('There are %d items to be processed', results.length);
            
//             }
//     });
// }

// function _iterateeFlatBugTo(row, cb) {
//     // loads the bugs to compare to
// }

function _writeOverlapStatsToCSV(statsArray) {
    var writer = csvWriter({ headers: ['blockingBugId','dependentBugId', 
        'overlap_readOnly', 'overlap_writeSet', 'overlap_wBrD', 'overlap_wDrB',
        'b_ArtR', 'b_ArtW', 'b_Ronly', 'd_ArtR', 'd_ArtW', 'd_Ronly' 
        ]});
           writer.pipe(fs.createWriteStream('tuplestats.csv'));
           _.forEach(statsArray, function(sta) {
               writer.write(_tupleStats2CVSline(sta));
           });
    writer.end();
}

function _tupleStats2CVSline(sta) {
    var csv = [
        sta.blockingBugId,
        sta.dependentBugId,
        sta.overlapStats.readOnlyOverlapCount,
        sta.overlapStats.writeSetOverlapCount,
        sta.overlapStats.writeInB_readOnlyInDcount,
        sta.overlapStats.writeInD_readOnlyInBcount,
        sta.blockingStats.uniqueArtifactReads,
        sta.blockingStats.uniqueArtifactWrites,
        sta.blockingStats.readOnlyArtifactCount,
        sta.dependentStats.uniqueArtifactReads,
        sta.dependentStats.uniqueArtifactWrites,
        sta.dependentStats.readOnlyArtifactCount,
        ];
     return csv;
}

// extract writes only from BlockerBug 
// extract readOnly from DependentBug
// extract writes from DependentBug
// store overlap set of B.w with D.r
// calculate overlap of B.w with D.w
// store overlap set of B.r with D.w (just for interest)
// calculate overall overlap B.r+w with D.r+w
function _getOverlapStats(statsBlocker, statsDependent) {
    var stats = {
        readOnlyOverlap : _.intersection(statsBlocker.readOnlyArtifacts, statsDependent.readOnlyArtifacts),
        writeSetOverlap : _.intersection(_.keys(statsBlocker.write), _.keys(statsDependent.write)),
        writeInB_readOnlyInD : _.intersection(_.keys(statsBlocker.write), statsDependent.readOnlyArtifacts),
        writeInD_readOnlyInB : _.intersection(_.keys(statsDependent.write), statsBlocker.readOnlyArtifacts)
    };
    stats.readOnlyOverlapCount = stats.readOnlyOverlap.length;
    stats.writeSetOverlapCount = stats.writeSetOverlap.length;
    stats.writeInB_readOnlyInDcount = stats.writeInB_readOnlyInD.length;
    stats.writeInD_readOnlyInBcount = stats.writeInD_readOnlyInB.length;
    return stats;
}

function _getBugStats(bug) {
    
    var stats = {
        read : {},
        write : {},
    };
    stats = _.reduce(bug.events, function(result, event){
        var resource = null;
        // (reduce events to filebasis)
        if (event.structure.type == 'FILE') {
            resource = event.StructureHandle;
        }
        if (event.StructureKind == 'java' && event.structure.classname.length > 0)
        {
            resource = event.structure.path+'/'+event.structure.classname;
        }
        if (resource == null) // no event on file level or below
            return result;
            
        if (event.Kind == 'selection') {
            (result.read[resource] || (result.read[resource] = [])).push(event);     
        } else if (event.Kind == 'edit') {
            (result.write[resource] || (result.write[resource] = [])).push(event);
        }
        return result;
    }, stats);
    stats.readOnlyArtifacts = _.difference(_.keys(stats.read),_.keys(stats.write));
    
    stats.uniqueArtifactReads = _.keys(stats.read).length;
    stats.uniqueArtifactWrites = _.keys(stats.write).length;
    stats.readOnlyArtifactCount = stats.readOnlyArtifacts.length; 
    return stats;
}




