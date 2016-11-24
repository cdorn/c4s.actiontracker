"use strict";
// JavaScript File
// adaptation of https://github.com/ibm-cds-labs/metrics-collector/blob/master/server.js
// to work with local couchDB
// and remain independent of IBM bluemix 

var express = require('express');
var path = require('path');
var http = require('http');
var bodyParser = require('body-parser');
var errorHandler = require('errorhandler');
var _ = require('lodash');
var initPh2E = require('./transformation/phabricator-transformation.js');
var config = require('./config.json');

var app = express();
//  we set up Express to serve the static JavaScript files to serve the piwik and tracker js files from this server directly
// The following code  makes any file in the js directory web-accessible via the url http://localhost:port/<filename>
app.use(express.static(path.join(__dirname, 'js')));

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(errorHandler({ dumpExceptions:true, showStack:true }));

var ph2e = undefined;
var dbconfig = {
  dbName : config.tracker_dbName,
  cleanUp : 'false',
  dbURL : config.couchDB_url
};

initPh2E(dbconfig,  function (err, inst) {
    if (!err){
        ph2e = inst;
        console.log("Phabricator Transformer Ready");
    } else  {
      throw new Error("Fatal error from Phabricator Transformer \r\n" + err);
    }
});

var interestingKeys = ['action_name','idsite','url','urlref','uid','link'];
var keysToFilterOut = ['rec', 'r', '_id', '_idts', '_idvc', '_idn', '_refts', '_viewts', 'send_image', 'pdf', 'qt', 'realp', 'wma', 'dir', 'fla', 'java', 'gears', 'ag', 'cookie', 'res', 'gt_ms'];
//var reqCounter = 0;

//Configure tracker end point
app.get("/tracker", function( req, res ){
  var type = null;
  var jsonPayload = _.chain( req.query )
		.mapValues( function(value){
			try{
				return JSON.parse(value);
			}catch(e){
				return value;
			}
		})//.pickBy( function(value, key){
		//    return _.includes(interestingKeys, key);
		//  })
		.omitBy( function(value, key){
		  return _.includes(keysToFilterOut, key); // for now we filter out what we definitely dont want while developing, later we only include explicitly what we need once we know what we need
		} )
		.mapKeys( function( value, key ){
		// 	if ( key === "action_name"){
		// 		type = type_pageView;
		// 	}else if ( key === "link"){
		// 		type = type_link;
		// 	}
			if ( _.startsWith( key, '_') ){
				//ClouchDB doesn't allow key starting with _
				return "$" + key;
			}
			return key;
		}).value();
	
	 // if ( type ){
		//   jsonPayload.type = type;
  //   }
    
    //Capture the IP address
  	var ip = req.headers['x-client-ip'] || req.headers['x-forwarded-for'] || req.connection.remoteAddress;
	  if ( ip ){
	  	jsonPayload.ip = ip;
	  }

    
   // console.log(JSON.stringify(jsonPayload));
    //jsonPayload._id = 
  
    //var docId = reqCounter+'';
    //reqCounter++;
  // store payload in couchDB
    
   var resp = ph2e.transformWebInteraction(jsonPayload);
   if (!resp.error)
   {
        resp.event.raw = jsonPayload;
        ph2e.storeEvent(resp.event, function(err, body) {
          if (!err) 
          {
            console.log(body);
            return res.status(200).end();   
          } else {
            return console.log( "Error storing client tracking data: " + jsonError( res, resp.error ) );
          }
        });
   }
   else
   {
      if (resp.error == "422")
      {
        console.log( "Insufficient data for transforming client tracking data: "+resp.msg+"\r\n in raw event: "+JSON.stringify(resp.raw));
        return res.status(200).end();
      }
      else
       return console.log( "Error transforming client tracking data: " + jsonError( res, resp.error ) );
   }
    
    // trackerDB.insert(jsonPayload, docId, function(err, body) {
    //   if (err) {
    //     return console.log( "Error storing client tracking data: " + jsonError( res, err ) );
    //   }
    //   else {
    //     console.log(body);
    //     return res.status(200).end();
    //   }
    // });
});


// Default Endpoint/Route returns error as nothing done there
app.get("*", function(request, response){
    console.log("GET request url %s : headers: %j", request.url, request.headers);
    
    response.status(500).send('<h1>Invalid Request</h1><p>Simple Metrics Collector captures web metrics data and stores it in a CouchDB. There are no web pages here. This is middleware.</p><p>For more information check out the sample app at <a href="https://github.com/ibm-cds-labs/metrics-collector/">the GitHub repo</a> from which this site was inspired</p>');
});

var port = 3000;
var connected = function() {
	console.log("Tracker Collector started on port %s : %s", port, Date(Date.now()));
};

http.createServer(app).listen(port,connected);

/*app.listen(3000, function () {
  console.log('Example app listening on port 3000!')
}) */

function jsonError(res, err) {
  var message = err.message || err.statusMessage || err;
  //return error message to tracking client
  res.status(500).json({'error':message});
  // return error message to tracking host 
  return message;
};