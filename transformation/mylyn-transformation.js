
var _ = require('lodash');

module.exports = initAtt2Mylyn;

function initAtt2Mylyn(opts) {
    if (!opts.attachmentsDB)
        throw new Error("Cannot Init Att2Mylyn without AttachmentsDB");
    return new A2M(opts);
}

function A2M(opts) {
    this.db = opts.attachmentsDB;
    
}

// each attachment represents a user session with multiple interaction events (inside jsondata)

function filterEvents(events) {
    var fEvents = _.chain(events)
    .reject( function(event) {
            return _.includes(['propagation','prediction','command','manipulation'], event.Kind);
        })
    .flatMap(parseStructure)
    .value();
    return fEvents;
}

function parseStructure(event) {
    var handle = decodeURI(event.StructureHandle);
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
        
        var classPos = handle.lastIndexOf('{');
        if (classPos <= 0) { // just package access/manipulation
            stats.type = "PACKAGE";
            stats.packagepath = handle.substring(packageStart+4); //+4 to skip the package separator
            stats.resourceName = stats.packagepath;
            return event;
        } // else just ste the current path until here, thus change package path to file system path
        stats.packagepath = handle.substring(packageStart+4, classPos);
        stats.path = stats.path + '/' + stats.packagepath.split(".").join("/");
        // continue with class and method
        var classEnd = handle.lastIndexOf('.java');
        stats.classname = handle.substring(classPos+1, classEnd);
        // if there is a method beyond the class
        if (classEnd+5 == handle.length) { // no method part
            stats.type = "CLASS";
            stats.resourceName = handle.substring(classPos+1);
        } else {
            stats.type = "METHOD";
            stats.method = handle.substring(classEnd+6); // .java + [ that denotes method start
            stats.resourceName = stats.method;
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
           console.log(getKindStats(body.jsondata));
           var events = filterEvents(body.jsondata);
           console.log('Filtered '+body.jsondata.length+" events down to "+events.length);
        //   _.forEach(events, function(value) {
        //       console.log(value.structure);
        //   });
            var stats = getResourceStats(events)
            stats.readCount = _.chain(stats.read)
                                .mapValues( function(value, key) {
                                    return value.length;
                                })
                                .value();
            stats.writeCount = _.chain(stats.write)
                                .mapValues( function(value, key) {
                                    return value.length;
                                })
                                .value();
            stats.onlyReads = _.difference(_.keys(stats.read),_.keys(stats.write));                    
            console.log(stats);

            return cb(null, events);
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
    }, {});
}

function getResourceStats(events) {
    // store for each artifact the number of occurences for selected and manipulation
    var stats = {
        read : {},
        write : {},
        java : 0,
        resource : 0
    };
    return _.reduce(events, function(result, event){
        if (event.Kind == 'selection') {
            (result.read[event.StructureHandle] || (result.read[event.StructureHandle] = [])).push(event);     
        } else if (event.Kind == 'manipulation') {
            (result.write[event.StructureHandle] || (result.write[event.StructureHandle] = [])).push(event);
        }
        if (event.StructureKind == 'java') {
            result.java++;
        } else if (event.StructureKind == 'resource') {
            result.resource++;
        }
        return result;
    }, stats);
}