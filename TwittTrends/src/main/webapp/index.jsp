<%@page import="java.util.ArrayList"%>
<%@page import="java.sql.ResultSet"%>
<%@page import="java.sql.Statement"%>
<%@page import="java.sql.DriverManager"%>
<%@page import="java.sql.Connection"%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta name="viewport" content="initial-scale=1.0, user-scalable=no">
    <meta charset="utf-8">
    <title>TweetTrensds by HW Group 35</title>
    <style>
      html, body {
        height: 100%;
        margin: 0;
        padding: 0;
      }
      #map {
        height: 100%;
        width: 80%;
        float: right;
      }
      #searchPanel {
      	height: 100%;
        width: 20%;
        float: left;
        background-color: #EDE8E8;
      }
 
    </style>
    
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.4/jquery.min.js">
</script>





<script
	src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.4/jquery.min.js">
</script>
<script>
	var locations = [];
	var tweetsText = [];
	var map;
	var markers;
	var markerCluster;
	var interval;
	var scrollId="";
	var limit=10000;
	var lastCount=0;
	
	function loadNewLocations() {
		var keywordVal = document.getElementById('keyword').value;
		$.get("<%= request.getContextPath().toString()%>/gettweetselasticsearch?scrollId="+scrollId+"&keyword="+keywordVal, function(results, status){
			console.log("<%= request.getContextPath().toString()%>/gettweetselasticsearch?scrollId="+scrollId+"&keyword="+keywordVal);
			
        	var result = JSON.parse(results);
        	var tweets = result.tweets;
        	scrollId=result.scrollId;
        	console.log("scrollid:"+scrollId);
        	lastCount+=tweets.length;
        	//alert(lastCount)
        	if(lastCount>limit){
        		limit+=10000;
        		deleteMapMarkers();
        	}
   
        	
     		for (var i=0;i <tweets.length; i++){
     			var newLocation = {lat : tweets[i].lat, lng : tweets[i].lng};
     			locations.push(newLocation);
     			
     			var tweetText = tweets[i].text;
     			var sentiment = tweets[i].sentiment;
     			var image = '<%= request.getContextPath().toString()%>/images/';
     			
     			var newMarker = new google.maps.Marker({
     	              position: newLocation,
     	              icon: image+sentiment+'_emoji.png',
     	              title : decodeURIComponent((tweetText+'').replace(/\+/g, '%20'))
     	            });
     			
     			newMarker.setMap(map);
     			markers.push(newMarker);
     		}
     		markerCluster = new MarkerClusterer(map, markers,
     	            {imagePath: 'https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/m'});
    	});
	}
	
	
	function loadLocations() {
		if(interval !== null){
			clearInterval(interval);
			scrollId = "";
		}
		var keywordVal = document.getElementById('keyword').value;
		    	$.get("<%= request.getContextPath().toString()%>/gettweetselasticsearch?scrollId="+scrollId+"&keyword="+keywordVal, function(results, status){
	            		console.log("<%= request.getContextPath().toString()%>/gettweetselasticsearch?scrollId="+scrollId+"&keyword="+keywordVal);
		    			locations = [];
						tweetsText = [];
						
						deleteMapMarkers();
	            	
	            	var result = JSON.parse(results);
	            	var tweets = result.tweets;
	            	lastCount+=tweets.length;
	            	scrollId=result.scrollId;
	            	console.log("scrollid:"+scrollId);
	            	
	         		for (var i=0;i <tweets.length; i++){
	         			var newLocation = {location : {lat : tweets[i].lat, lng : tweets[i].lng}, text : tweets[i].text, sentiment : tweets[i].sentiment};
	         			locations.push(newLocation);
	         			tweetsText.push(tweets[i].text);
	         		}
	         		interval = setInterval(loadNewLocations, 10000);
	         		initMap();
	        	});
	}
	
	
      function initMap() {

        map = new google.maps.Map(document.getElementById('map'), {
          zoom: 2,
          center: {lat: 34.5133, lng: -94.1629}//{lat: -28.024, lng: 140.887}
        });
       
        var image = '<%= request.getContextPath().toString()%>/images/';

        markers = locations.map(function(location, i) {
            return new google.maps.Marker({
              position: location.location,
              icon: image+location.sentiment+'_emoji.png',
              title : decodeURIComponent((location.text+'').replace(/\+/g, '%20'))
            });
          });

        // Add a marker clusterer to manage the markers.
        markerCluster = new MarkerClusterer(map, markers,
            {imagePath: 'https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/m'});
      }
      
    	function deleteMapMarkers() {
    	    for(var i=0; i < this.markers.length; i++){
    	        this.markers[i].setMap(null);
    	    }
    	    this.markers = [];
    	    //alert("clear markers" + markers.length);
    	    //markerCluster.setMap(null);
    	    markerCluster.repaint();
    	    
    	    map = new google.maps.Map(document.getElementById('map'), {
    	          zoom: 2,
    	          center: {lat: 34.5133, lng: -94.1629}
    	        });
    	    markerCluster = new MarkerClusterer(map, markers,
    	            {imagePath: 'https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/m'});
    	
    	    markerCluster.clearMarkers();
    	    
    	}
      
    </script>
<script
	src="https://developers.google.com/maps/documentation/javascript/examples/markerclusterer/markerclusterer.js">
    </script>
<script async defer
	src="https://maps.googleapis.com/maps/api/js?key=<%=config.getServletContext().getInitParameter("googleMapAPIKey")%>&callback=initMap"> 
</script>
<script type="text/javascript">
    function hideAndShow() {
		var searchPanel = document.getElementById("searchPanel");
		var mapPanel = document.getElementById("map");
		var formObject = document.getElementById("searchForm");
		var btnHideShow = document.getElementById("searchForm");
		
		if(searchPanel.style.width == "5%"){
			formObject.style.display = "block";
			searchPanel.style.width = "20%";
			mapPanel.style.width = "80%";
		}
		else {
			formObject.style.display = "none";
			searchPanel.style.width = "5%";
			mapPanel.style.width = "95%";
			initMap();
		}
		
		
	}
    
    </script>
</head>

<body>
	<div id="searchPanel" align="center">
		<div onclick="hideAndShow()"
			style="direction: ltr; cursor: pointer; margin-top: 10px; margin-right: 4px; float: right; overflow: hidden; text-align: center; position: relative; color: rgb(0, 0, 0); font-family: Roboto, Arial, sans-serif; -moz-user-select: none; font-size: 11px; background-color: rgb(255, 255, 255); padding: 8px; border-bottom-left-radius: 2px; border-top-left-radius: 2px; background-clip: padding-box; box-shadow: 0px 1px 4px -1px rgba(0, 0, 0, 0.3); min-width: 22px; max-width: 30px; font-weight: 500;"
			draggable="false" title="Hide or Show">&lt;&lt;&gt;&gt;</div>
		<br> <br> <br> <br> <br> <br> <br>
		<br>

		<form id="searchForm" name="searchForm" method="get" style="margin-top: 35%">
			<table>
				<tr>
					<td>Select Keyword</td>
				</tr>
				<tr>
					<td><select id="keyword" name="keyword">
							<option value="">--select--</option>
							<option value="world">World</option>
							<option value="travel">Travel</option>
							<option value="friend">Friend</option>
							<option value="fun">Fun</option>
							<option value="game">Game</option>
							<option value="accident">Accident</option>
							<option value="job">Job</option>
							<option value="photo">Photo</option>
							<option value="work">Work</option>
							<option value="hiring">Hiring</option>
					</select></td>
				</tr>
				<tr>
					<td><input type="button" id="submit" value="Search"
						onclick="loadLocations();" /></td>
				</tr>
			</table>
		</form>
	</div>


	<div id="map"></div>
</body>
<script type="text/javascript">
loadLocations();
</script>
</html>