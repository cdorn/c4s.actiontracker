/**
 * Encapsulate the tracking logic
 */

//Init the piwik queue
var _paq = _paq || [];

//Asynchronous loading of the piwik tracking framework
(function(){
	
// 	var customDataFn = function(){
// 		var ret = "";
// 		//date
// 		var d = new Date();
// 		ret += "&date=" + d.getUTCFullYear() + "-" + (d.getUTCMonth() + 1) + "-" + d.getUTCDate();
// 		return ret;
// 	}
	
	// var extracturl = function( url ){
	// 	var a =  document.createElement('a');
	//     a.href = url;
	//     return a.protocol + "//" + a.hostname + ":" + a.port;
	// }
	

	
	//Get the site id from custom script data attribute
	var scripts = document.getElementsByTagName("script");
    var siteid = null;
	var defTrackerProtocol = "http";
	var defTrackerHost = "192.168.65.129:3000";
	var trackerUrl = defTrackerProtocol + "://" + defTrackerHost + "/tracker";
    if ( scripts && scripts.length > 0 ){
    	siteid = scripts[scripts.length - 1].getAttribute("siteid");
    //	trackerUrl = scripts[scripts.length - 1].getAttribute("trackerurl");
    }
    
    if ( !siteid ){
    	siteid = '13';
    	//console.log('siteid attribute missing in the script tag for tracker.js');
    }
	_paq.push(['setSiteId', siteid]);
	//_paq.push(['addPlugin', 'cds_custom_data', {'link': customDataFn, 'sitesearch':customDataFn, 'log': customDataFn}]);
	_paq.push(['setTrackerUrl', trackerUrl]);
	

	
	var d=document, g=d.createElement('script'), s=d.getElementsByTagName('script')[0];
    g.type='text/javascript'; g.async=true; g.defer=true; g.src=defTrackerProtocol + "://" + defTrackerHost+'/piwik.js'; 
    s.parentNode.insertBefore(g,s);
	
})();

document.addEventListener("DOMContentLoaded", function() {
  	var getUserUrl = function(d){
    	var loggedIn=d.getElementById("loggedas"); 
    	var user=loggedIn.getElementsByClassName('user active')[0];
    	return user.href;
    	//return "testUserForNow";
	}
	
	//var inputs = document.getElementsByTagName('input');
	//$("form").on('submit', function(){
	var forms = document.getElementsByTagName('form');
	console.warn('Found forms: '+forms.length);
	for (var i = 0; i < forms.length; i++) {
    	forms[i].addEventListener("submit", function() {
		//	_paq.push(['trackContentInteractionNode', this, 'submittedForm']);
			_paq.push(['trackEvent', 'Submit']);
		});
	};
	
	
	_paq.push(['setUserId', getUserUrl(document) ]);
	_paq.push(['trackPageView']);
//	 _paq.push(['trackAllContentImpressions']);
	_paq.push(['enableLinkTracking']);
});




//dynamically enable link tracking starting from provided DOM Element
// such a function would be called from within the instrumented website, e.g., each website that needs to dyanmicaly add a listener
// var enableLinkTrackingForNode = function( node ){
//   var _tracker = this;
//   node.find('a,area').each(function(link){
//       if ( _tracker.addClickListener) {
//           _tracker.addClickListener($(this)[0], true);
//       }
//   });
// };

