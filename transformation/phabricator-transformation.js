var _ = require('lodash');

module.exports = initPh2E;

// from https://github.com/phacility/phabricator/blob/master/src/applications/transactions/constants/PhabricatorTransactions.php
// CORE
  const TYPE_COMMENT      = 'core:comment';
  const TYPE_SUBSCRIBERS  = 'core:subscribers';
  const TYPE_VIEW_POLICY  = 'core:view-policy';
  const TYPE_EDIT_POLICY  = 'core:edit-policy';
  const TYPE_JOIN_POLICY  = 'core:join-policy';
  const TYPE_EDGE         = 'core:edge';
  const TYPE_CUSTOMFIELD  = 'core:customfield';
  const TYPE_BUILDABLE    = 'harbormaster:buildable';
  const TYPE_TOKEN        = 'token:give';
  const TYPE_INLINESTATE  = 'core:inlinestate';
  const TYPE_SPACE        = 'core:space';
  const TYPE_CREATE       = 'core:create';
  const TYPE_COLUMNS      = 'core:columns';
// CONPHERENCE
  const CONP_TYPE_FILES           = 'files';
  const CONP_TYPE_TITLE           = 'title';
  const CONP_TYPE_PARTICIPANTS    = 'participants';
  const CONP_TYPE_DATE_MARKER     = 'date-marker';
  const CONP_TYPE_PICTURE         = 'picture';
  const CONP_TYPE_PICTURE_CROP    = 'picture-crop';
// MANIPHEST
  const TASK_TYPE_TITLE = 'title';
  const TASK_TYPE_STATUS = 'status';
  const TASK_TYPE_PRIORITY = 'priority';
  const TASK_TYPE_DESCRIPTION = 'description';
 

function initPh2E(opts, cb) {
    if (!opts)
        opts = {};
    return new Ph2E(opts, cb);
}

function Ph2E (opts, cb){
    this.prefix = "transform";
    this.dbName = opts.dbName || 'monitor_db';
    this.cleanUp = opts.cleanUp || 'false';
    this.createIdFromEvent = function(event) {
        return event.date.toJSON()+"-"+event.actionPrimitive+"-"+event.resourceId+"#"+event.userId;
    };
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
    return cb(null, self);
    
    
}

function calculateDelta(oldState, newState) // assumes arrays of values in each parameter
{
    var delta = {
        newEntries : [],
        removedEntries : [],
        sameEntries : []
    };
    if (!oldState || oldState.length == 0) // only added elements
    {
        delta.newEntries = newState;
    }
    else if ( !newState || newState.length == 0) // only removed elements
    {
        delta.removedEntries = oldState;
    } else { // calc added/removed elements
        delta.sameEntries = _.intersection(oldState, newState); // try intersectionBy if that doesn't work
        delta.newEntries = _.difference(newState, delta.sameEntries); // newstate - same
        delta.removedEntries = _.difference(oldState, delta.sameEntries); //oldstate - same
    }
    return delta;
}

function createEdge(value) // assumes object
{ // see type ids at: http://192.168.65.131/config/module/edge/
    var edge = {};
    edge.phTypeId = value.type;
    edge.dstPhID = value.dst;
    switch(value.type)
    {
        case 3: 
            edge.phTypeName = 'ManiphestTaskDependsOnTaskEdgeType';
            edge.dstShortType = 'TASK';
            break;
        case 4: 
            edge.phTypeName = 'ManiphestTaskDependedOnByTaskEdgeType';
            edge.dstShortType = 'TASK';
            break;
        case 41:
            edge.phTypeName ='PhabricatorProjectObjectHasProjectEdgeType';
            edge.dstShortType = 'PROJ';
            break; 
        case 42:
            edge.phTypeName ='PhabricatorProjectProjectHasObjectEdgeType';
            edge.dstShortType = value.dst.substring(5,9);
            break; 
        case 51:
            edge.phTypeName = 'PhabricatorObjectMentionedByObjectEdgeType';
            edge.dstShortType = value.dst.substring(5,9);
            break;
        case 52:
            edge.phTypeName ='PhabricatorObjectMentionsObjectEdgeType';
            edge.dstShortType = value.dst.substring(5,9);
            break;
        default:
            edge.error = 'Unsupported EdgeType: '+value.type;
    }
    return edge;
}

function createLink(srcUserPhID, dstObjectPhID, linkPhTypeName) 
{
    var link = {
        dstPhId : dstObjectPhID, // always the object
        dstShortType : dstObjectPhID.substring(5,9),
        srcPhId : srcUserPhID, // always the user
        srcShortType : srcUserPhID.substring(5,9),
        phTypeName : linkPhTypeName
    };
    return link;
}

Ph2E.prototype.transformTaskTransaction = function transformTaskTransaction(phtx) {
    
    if (!phtx.authorPHID || !phtx.authorPHID.startsWith('PHID-USER'))
        return new function() { this.msg = 'No valid UserID found', this.raw = phtx, this.error = '422'};
    
    var event = {};
    event.userId = phtx.uid;
    event.date = new Date((phtx.dateCreated*1000));        
    event.epoch = event.date.valueOf();   
    event.userId = phtx.authorPHID;    
    var typeShort = 'TASK';
    event.resourceShortType = typeShort;
    event.resourceType = this.shorthand2Type(typeShort);
    event.resourceId  = phtx.taskPhID;
    
    switch(phtx.transactionType)
        {
            // MANIPHEST SPECIFIC:
            case TASK_TYPE_TITLE: // fallthrough as just an edit
            case TASK_TYPE_STATUS: // fallthrough as just an edit
            case TASK_TYPE_PRIORITY: // fallthrough as just an edit
            case TASK_TYPE_DESCRIPTION:
                // all changes should be aggregated here (when we don't need to distingusih between type of changes)
                event.actionPrimitive = "UPDATE";
                event.changedProperty = phtx.transactionType;
                break;
            // GENERAL ONES BELOW
            case TYPE_CREATE: 
                event.actionPrimitive = "CREATE";
                break;
            case TYPE_EDGE:
                // check the type of edge --> not really an update but a linking of artifact
                // need to check diff of oldValue and newValue
                // need to iteratie through old Value and newValue
                event.actionPrimitive = "RELATING";
                
                event.relations = [];
                var rdelta = calculateDelta(phtx.oldValue, phtx.newValue);
                _.values(rdelta.newEntries).forEach(function (element) {
                    var rel = createEdge(element);
                    rel.change = 'NEW';
                    event.relations.push(rel);
                });
                rdelta.removedEntries.forEach(function (element) {
                    var rel = createEdge(element);
                    rel.change = 'REMOVED';
                    event.relations.push(rel);
                });
                break;
            case TYPE_SUBSCRIBERS: 
                event.links = [];
                var delta = calculateDelta(phtx.oldValue, phtx.newValue);
                delta.newEntries.forEach(function(element) {
                    var link = createLink(element, phtx.taskPhID, TYPE_SUBSCRIBERS);
                    link.change = 'NEW';
                    event.links.push(link);
                });
                delta.removedEntries.forEach(function(element) {
                    var link = createLink(element, phtx.taskPhID, TYPE_SUBSCRIBERS);
                    link.change = 'REMOVED';
                    event.links.push(link);
                });
                event.actionPrimitive = "LINKING";
                break;
            case TYPE_COMMENT:
                event.actionPrimitive = ["COMMENT","UPDATE"]; // this needs to be done differently
                break;
            case TYPE_TOKEN:
                event.actionPrimitive = "UPDATE";
                event.changedProperty = phtx.transactionType;
                break;
            case TYPE_COLUMNS: //fallthrough
                // need to check what this implies
            case TYPE_CUSTOMFIELD: //fallthrough    
            case TYPE_EDIT_POLICY: //fallthrough
            case TYPE_JOIN_POLICY: //fallthrough
            case TYPE_VIEW_POLICY: //fallthrough
            case TYPE_BUILDABLE: // fallthrough
            case TYPE_INLINESTATE: //fallthrough
            case TYPE_SPACE : //fallthrough to here
                return new function() { this.msg = 'Uninteressing TransactionType', this.raw = phtx, this.error = '422'};
            default:
                return new function() { this.msg = 'Unknown TransactionType', this.raw = phtx, this.error = '501'};
        }    
    return event;
};

Ph2E.prototype.transformChatTransaction = function transformChatTransaction(phtx) {
    if (!phtx.authorPHID || !phtx.authorPHID.startsWith('PHID-USER'))
        return new function() { this.msg = 'No valid UserID found', this.raw = phtx, this.error = '422'};
     if (!phtx.roomPHID || !phtx.roomPHID.startsWith('PHID-CONP'))
        return new function() { this.msg = 'No valid RoomID found', this.raw = phtx, this.error = '422'};
    
    var event = {};
    event.userId = phtx.uid;
    event.date = new Date((phtx.dateCreated*1000));        
    event.epoch = event.date.valueOf();   
    event.userId = phtx.authorPHID;    
    var typeShort = 'CONP';
    event.resourceShortType = typeShort;
    event.resourceType = this.shorthand2Type(typeShort);
    event.resourceId  = phtx.roomPHID;
    
    switch(phtx.transactionType)
    {
        case CONP_TYPE_PARTICIPANTS:
            event.links = [];
            var delta = calculateDelta(phtx.transactionOldValue, phtx.transactionNewValue);
            delta.newEntries.forEach(function(element) {
                var link = createLink(element, phtx.roomPHID, CONP_TYPE_PARTICIPANTS);
                link.change = 'NEW';
                event.links.push(link);
            });
            delta.removedEntries.forEach(function(element) {
                var link = createLink(element, phtx.roomPHID, CONP_TYPE_PARTICIPANTS);
                link.change = 'REMOVED';
                event.links.push(link);
            });
            event.actionPrimitive = "LINKING";
            break;
        case CONP_TYPE_TITLE:
            event.actionPrimitive = "UPDATE";
            event.changedProperty = phtx.transactionType;
            break;
        case CONP_TYPE_DATE_MARKER: //fallthrough
        case CONP_TYPE_FILES: //fallthrough
        case CONP_TYPE_PICTURE: //fallthrough
        case CONP_TYPE_PICTURE_CROP: //fallthrough to here
            return new function() { this.msg = 'Uninteressing TransactionType', this.raw = phtx, this.error = '422'};
        // core transactions:
        case TYPE_COMMENT: 
            //TODO: extract WIKI links from comment
            event.actionPrimitive = ["COMMENT","UPDATE"]; // this needs to be done differently
            break;
        case TYPE_EDGE:
            event.actionPrimitive = "RELATING";
            event.relations = [];
            var rdelta = calculateDelta(phtx.transactionOldValue, phtx.transactionNewValue);
            _.values(rdelta.newEntries).forEach(function (element) {
                var rel = createEdge(element);
                rel.change = 'NEW';
                event.relations.push(rel);
            });
            rdelta.removedEntries.forEach(function (element) {
                var rel = createEdge(element);
                rel.change = 'REMOVED';
                event.relations.push(rel);
            });
            break;
        case TYPE_EDIT_POLICY:
        case TYPE_JOIN_POLICY:
        case TYPE_VIEW_POLICY:
            return new function() { this.msg = 'Uninteressing TransactionType', this.raw = phtx, this.error = '422'};
        default:
            return new function() { this.msg = 'Unknown TransactionType', this.raw = phtx, this.error = '501'};
            
    }
    return event;
};

Ph2E.prototype.transformWebInteraction = function transformWebInteraction (interaction) {
    
    if (!interaction.uid || !interaction.uid.startsWith('PHID-USER'))
        return new function() { this.msg = 'No valid UserID found', this.raw = interaction, this.error = '422'};
    if (!interaction.url)
        return new function() { this.msg = 'No valid URL found', this.raw = interaction, this.error = '422'};


    var event = {};
    event.userId = interaction.uid;
    event.actionPrimitive = "READ";
    
    if (!interaction.dimension2) { // no timestamp
        event.date = new Date();
        event.epoch = event.date.valueOf();
    } else {
        event.date = new Date(interaction.dimension2);        
        event.epoch = interaction.dimension2;
    }
    
    if (interaction.dimension1) { // accessing identifiable resource
        event.resourceId = interaction.dimension1;
        // determine the resource type
        var typeShort = event.resourceId.substring(5,9);
        event.resourceShortType = typeShort;
        event.resourceType = this.shorthand2Type(typeShort);
    } else {
         return new function() { this.msg = 'No Identifiable Resource found', this.raw = interaction, this.error = '422'};
    }
    var resp = {event : event, raw : interaction};
    return resp;
};


Ph2E.prototype.storeEvent = function storeEvent(event, cb) {
    var docId = this.createIdFromEvent(event);
    this.db.insert(event, docId, cb);
};

Ph2E.prototype.storeBulkEvents = function storeBulkEvents(events, cb) {
    var bulk = {
        docs : []
    };
    var self = this;
    _.values(events).forEach( function(element) {
        element['_id'] = self.createIdFromEvent(element); 
       bulk.docs.push(element);
    });
    if (bulk.docs.length > 0)
        this.db.bulk(bulk, cb);
    else
        cb(null, "No events to store");
};

Ph2E.prototype.getCrawlingStateForId = function getCrawlingStateForId(phId, cb) {
    // check if PhID exists in DB
    // createNew or Retreive State object (new object will have missing revision)
    this.db.get(phId, function(err, body) {
        if (err)
        {
           var state = {
               _id : phId,
               intId : -1,
               lastProcessedTx : -1,
               lastProcessedTS : new Date(0)
           };
           cb(state);
        }
        else
        {
            cb(body);
        }
    } );
};

Ph2E.prototype.updateCrawlingState = function updateCrawlingState(newState, cb) {
    // add new revision, catch conflict where revision is not matching --> should not really happen
    this.db.insert(newState, function(err, body) {
        if (err)
            cb(err);
        else    
            return cb(body);
    });
};

// Ph2E.prototype.createIdFromEvent = function createIdFromEvent(event) {
//     return event.date.toJSON()+"-"+event.actionPrimitive+"#"+event.userId;
// };

Ph2E.prototype.shorthand2Type = function shorthand2Type(shorthand) {
    switch(shorthand)
        {
            case 'WIKI': return 'PhrictionDocumentPHIDType'; 
            case 'XACT': return 'PhabricatorApplicationTransactionPHIDType';
            case 'USER': return 'PhabricatorPeopleUserPHIDType';
            case 'TASK': return 'ManiphestTaskPHIDType';
            case 'CONP': return 'PhabricatorConpherenceThreadPHIDType';
            case 'PROJ': return 'PhabricatorProjectPHIDType'; //project and milestone and subprojects
            default:
                return 'unknown';
        }
};




// Ph2E.prototype.path2CollectionType = function path2CollectionType(path) {
    
    
//     if (path.startsWith('conference/'))
//         return 'PhabricatorConpherenceThreadCollection';
//     if (path.startsWith('project/'))
//         return 'PhabricatorProjectCollection';
    
//      if (path.startsWith('maniphest/')){
//          if(path.startsWith('maniphest/task/edit/')) {
//             return null;
//          } else {
//             return 'ManiphestTaskCollection'; 
//          }
//      }
     
//     // Task: T+number, e.g, T6
    
//     // Task edit page: /maniphest/task/edit/6/
    
//     // Task collection: maniphest/query/open/ , /project/query/all/
    
//     // Wiki: w = root or phriction/
//     // wiki: w/xxxxx/ccc
    
//     // for now return only: project, wiki, task, chatroom
    
// }