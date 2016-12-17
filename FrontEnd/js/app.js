var app = angular.module("myApp", ['elasticsearch'])


app.controller('namesCtrl', function($scope,$rootScope,$http,hashTable,$document) {
   var placeHash = undefined ; 
    $scope.names = [
        'Africa',
        'Antartica',
        'Asia',
        'Central America and Caribbean',
        'Europe',
        'Middle East',
        'North America',
        'South America'
    ];
    $scope.countryName = [
       'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 
'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 
'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 
'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
'West Virginia', 'Wisconsin', 'Wyoming'
    ];
     $scope.ngEnter = function(country,test){
        // if(event.keyCode==13){
         //   alert('Hello World! ' + event.keyCode);
            var myEl = angular.element( document.querySelector( '#repeat' ) );
            myEl.remove();
           
            placeHash = undefined ;
            var searchURL = 'http://localhost:9200/topicdata/docs/_search?q=topics:hike and state:'+country+'&size=10000';
               // activitySearch= 'hike or hiking';
            $http.get(searchURL).
                    then(function(response) {
                         var data = response.data;
                         var hits = new Array()
                         hits = data.hits.hits;
                         // add all result in hashtable to merge all activities in the area
                         if(hits != null && hits != "" ){
                             for(var i=0 ; i < hits.length; i++){
                                if( hits[i]._source != null  ){
                                    
                                    var place= {
                                        title :  hits[i]._source.title,
                                        url : hits[i]._source.url,
                                        country :  hits[i]._source.country,
                                        state : hits[i]._source.state,
                                        area : hits[i]._source.area,
                                        activity : 'Hike'
                                    }

                                    if(placeHash == undefined){
                                         placeHash = hashTable.createHashT();
                                    }
                                   
                                    var arg = {
                                        h : placeHash,
                                        key : hits[i]._source.area,
                                        val : place
                                    }

                                    hashTable.addHashTItem(arg);
                                }
                             }
                         }
                       var places = hashTable.getHashVal(placeHash);
                        if(places != null && places.length >0){
                            var count= 1;
                            getCoordinate(places[0]);
                            //while($scope.res == 0 && count < places.length ){
                            while(count < places.length ){
                                putMarker(places[count]);
                                count++;
                            }
                        }
                        
                    })
                    .catch(
                        function(error){
                            alert(error);
                               }
                   );

                searchURL = 'http://localhost:9200/topicdata/docs/_search?q=topics:dive and state:'+country+'&size=100';
                //activitySearch= 'dive or diving';

                $http.get(searchURL).
                    then(function(response) {
                         var data = response.data;
                         var hits = new Array()
                         hits = data.hits.hits;
                         // add all result in hashtable to merge all activities in the area
                         if(hits != null && hits != "" ){
                             for(var i=0 ; i < hits.length; i++){
                                if( hits[i]._source != null  ){
                                    
                                    var place= {
                                        title :  hits[i]._source.title,
                                        url : hits[i]._source.url,
                                        country :  hits[i]._source.country,
                                        state : hits[i]._source.state,
                                        area : hits[i]._source.area,
                                        activity : 'Dive'
                                    }

                                    if(placeHash == undefined){
                                         placeHash = hashTable.createHashT();
                                    }
                                   
                                    var arg = {
                                        h : placeHash,
                                        key : hits[i]._source.area,
                                        val : place
                                    }

                                    hashTable.addHashTItem(arg);
                                }
                             }
                         }
                        
                        var places = hashTable.getHashVal(placeHash);
                        if(places != null && places.length >0){
                            var count= 1;
                            getCoordinate(places[0]);
                            //while($scope.res == 0 && count < places.length ){
                            while(count < places.length ){
                                putMarker(places[count]);
                                count++;
                            }
                        }
                    })
                    .catch(
                        function(error){
                            alert(error);
                               });
           
               searchURL = 'http://localhost:9200/topicdata/docs/_search?q=topics:bike and state:'+country+'&size=100';
            
                $http.get(searchURL).
                    then(function(response) {
                         var data = response.data;
                         var hits = new Array()
                         hits = data.hits.hits;
                         // add all result in hashtable to merge all activities in the area
                         if(hits != null && hits != "" ){
                             for(var i=0 ; i < hits.length; i++){
                                if( hits[i]._source != null  ){
                                    
                                    var place= {
                                        title :  hits[i]._source.title,
                                        url : hits[i]._source.url,
                                        country :  hits[i]._source.country,
                                        state : hits[i]._source.state,
                                        area : hits[i]._source.area,
                                        activity : 'Bike'
                                    }

                                    if(placeHash == undefined){
                                         placeHash = hashTable.createHashT();
                                    }
                                   
                                    var arg = {
                                        h : placeHash,
                                        key : hits[i]._source.area,
                                        val : place
                                    }

                                    hashTable.addHashTItem(arg);
                                }
                             }
                         }

                        var places = hashTable.getHashVal(placeHash);
                        if(places != null && places.length >0){
                            var count= 1;
                            getCoordinate(places[0]);
                            //while($scope.res == 0 && count < places.length ){
                            while(count < places.length ){
                                putMarker(places[count]);
                                count++;
                            }
                        }
                    })
                    .catch(
                        function(error){
                            alert(error);
                               });
            
              searchURL = 'http://localhost:9200/topicdata/docs/_search?q=topics:snorkel and state:'+country+'&size=100';
              $http.get(searchURL).
                    then(function(response) {
                         var data = response.data;
                         var hits = new Array()
                         hits = data.hits.hits;
                         // add all result in hashtable to merge all activities in the area
                         if(hits != null && hits != "" ){
                             for(var i=0 ; i < hits.length; i++){
                                if( hits[i]._source != null  ){
                                    
                                    var place= {
                                        title :  hits[i]._source.title,
                                        url : hits[i]._source.url,
                                        country :  hits[i]._source.country,
                                        state : hits[i]._source.state,
                                        area : hits[i]._source.area,
                                        activity : 'Snorkel'
                                    }

                                    if(placeHash == undefined){
                                         placeHash = hashTable.createHashT();
                                    }
                                   
                                    var arg = {
                                        h : placeHash,
                                        key : hits[i]._source.area,
                                        val : place
                                    }

                                    hashTable.addHashTItem(arg);
                                }
                             }
                         }

                        var places = hashTable.getHashVal(placeHash);
                        if(places != null && places.length >0){
                            var count= 1;
                            getCoordinate(places[0]);
                            //while($scope.res == 0 && count < places.length ){
                            while(count < places.length ){
                                putMarker(places[count]);
                                count++;
                            }
                        }
                    })
                    .catch(
                        function(error){
                            alert(error);
                               });

              searchURL = 'http://localhost:9200/topicdata/docs/_search?q=topics:beach and state:'+country+'&size=100';
              $http.get(searchURL).
                    then(function(response) {
                         var data = response.data;
                         var hits = new Array()
                         hits = data.hits.hits;
                         // add all result in hashtable to merge all activities in the area
                         if(hits != null && hits != "" ){
                             for(var i=0 ; i < hits.length; i++){
                                if( hits[i]._source != null  ){
                                    
                                    var place= {
                                        title :  hits[i]._source.title,
                                        url : hits[i]._source.url,
                                        country :  hits[i]._source.country,
                                        state : hits[i]._source.state,
                                        area : hits[i]._source.area,
                                        activity : 'Beach'
                                    }

                                    if(placeHash == undefined){
                                         placeHash = hashTable.createHashT();
                                    }
                                   
                                    var arg = {
                                        h : placeHash,
                                        key : hits[i]._source.area,
                                        val : place
                                    }

                                    hashTable.addHashTItem(arg);
                                }
                             }
                         }

                        var places = hashTable.getHashVal(placeHash);
                        if(places != null && places.length >0){
                            var count= 1;
                            getCoordinate(places[0]);
                            //while($scope.res == 0 && count < places.length ){
                            while(count < places.length ){
                                putMarker(places[count]);
                                count++;
                            }
                        }
                    })
                    .catch(
                        function(error){
                            alert(error);
                               });

              searchURL = 'http://localhost:9200/topicdata/docs/_search?q=topics:yoga and state:'+country+'&size=100';
              $http.get(searchURL).
                    then(function(response) {
                         var data = response.data;
                         var hits = new Array()
                         hits = data.hits.hits;
                         // add all result in hashtable to merge all activities in the area
                         if(hits != null && hits != "" ){
                             for(var i=0 ; i < hits.length; i++){
                                if( hits[i]._source != null  ){
                                    
                                    var place= {
                                        title :  hits[i]._source.title,
                                        url : hits[i]._source.url,
                                        country :  hits[i]._source.country,
                                        state : hits[i]._source.state,
                                        area : hits[i]._source.area,
                                        activity : 'Yoga'
                                    }

                                    if(placeHash == undefined){
                                         placeHash = hashTable.createHashT();
                                    }
                                   
                                    var arg = {
                                        h : placeHash,
                                        key : hits[i]._source.area,
                                        val : place
                                    }

                                    hashTable.addHashTItem(arg);
                                }
                             }
                         }

                        var places = hashTable.getHashVal(placeHash);
                        if(places != null && places.length >0){
                            var count= 1;
                            getCoordinate(places[0]);
                            //while($scope.res == 0 && count < places.length ){
                            while(count < places.length ){
                                putMarker(places[count]);
                                count++;
                            }
                        }
                    })
                    .catch(
                        function(error){
                            alert(error);
                               });

                searchURL = 'http://localhost:9200/topicdata/docs/_search?q=topics:wildlife and state:'+country+'&size=100';
              $http.get(searchURL).
                    then(function(response) {
                         var data = response.data;
                         var hits = new Array()
                         hits = data.hits.hits;
                         // add all result in hashtable to merge all activities in the area
                         if(hits != null && hits != "" ){
                             for(var i=0 ; i < hits.length; i++){
                                if( hits[i]._source != null  ){
                                    
                                    var place= {
                                        title :  hits[i]._source.title,
                                        url : hits[i]._source.url,
                                        country :  hits[i]._source.country,
                                        state : hits[i]._source.state,
                                        area : hits[i]._source.area,
                                        activity : 'Wildlife'
                                    }

                                    if(placeHash == undefined){
                                         placeHash = hashTable.createHashT();
                                    }
                                   
                                    var arg = {
                                        h : placeHash,
                                        key : hits[i]._source.area,
                                        val : place
                                    }

                                    hashTable.addHashTItem(arg);
                                }
                             }
                         }

                        var places = hashTable.getHashVal(placeHash);
                        if(places != null && places.length >0){
                            var count= 1;
                            getCoordinate(places[0]);
                            //while($scope.res == 0 && count < places.length ){
                            while(count < places.length ){
                                putMarker(places[count]);
                                count++;
                            }
                        }
                    })
                    .catch(
                        function(error){
                            alert(error);
                               });

    };

   var getCoordinate =  function(place){
        var geocoder = new google.maps.Geocoder();
        geocoder.geocode( { 'address': place.country}, function(results, status) {

        if (status == google.maps.GeocoderStatus.OK) {
            var latitude = results[0].geometry.location.lat();
            var longitude = results[0].geometry.location.lng();
       //  alert(latitude+ " " + longitude);
        var arg=  {
            lat : latitude,
            long : longitude,
            title :  place.title,
            url : place.url,
            country :  place.country,
            state : place.state,
            area : place.area,
            activity : place.activity
           }
         $rootScope.$emit('createMarkerMethod', arg);
       }
      }); 
    }  

       var putMarker =  function(place){
        var geocoder = new google.maps.Geocoder();
        geocoder.geocode( { 'address': place.area}, function(results, status) {

        if (status == google.maps.GeocoderStatus.OK) {
            var latitude = results[0].geometry.location.lat();
            var longitude = results[0].geometry.location.lng();
       //  alert(latitude+ " " + longitude);
        var arg=  {
            lat : latitude,
            long : longitude,
            title :  place.title,
            url : place.url,
            country :  place.country,
            state : place.state,
            area : place.area,
            activity : place.activity
           }
         $rootScope.$emit('insertMarkerMethod', arg);
       } 
      }); 
    } 

});

app.controller('namesActivityCtrl', function($scope) {
    $scope.names = [
        'Hike',
        'Dive',
        'Beach',
        'Snorkel',
        'Bike',
        'Yoga',
        'Wildlife'
    ];
});
app.config(['$httpProvider', function($httpProvider) {
        $httpProvider.defaults.useXDomain = true;
        delete $httpProvider.defaults.headers.common['X-Requested-With'];
    }
]);



          //Angular App Module and Controller
         app.controller('MapCtrl', function ($scope, $rootScope) {

	     var mapOptions = {
                  zoom: 4,
                  center: new google.maps.LatLng(25,80),
                  mapTypeId: google.maps.MapTypeId.TERRAIN
              }

              $scope.map = new google.maps.Map(document.getElementById('map'), mapOptions);
            
              $scope.markers = [];

              var infoWindow = new google.maps.InfoWindow();

              var createMarker = function (info){

                  var marker = new google.maps.Marker({
                      map: $scope.map,
                      position: new google.maps.LatLng(info.lat, info.long),
                      title: info.city
                  });
                  marker.content = '<div class="infoWindowContent">' + 'activity: '+ info.activity 
                                                                     +'<p> blog url:'+ info.state+ '</p> '
                                                                     +'<p> blog url:'+ info.url+ '</p> '
                                                                     +'<p> title: '+ info.title+ '</p>'
                                                                     + '</div>';

                  google.maps.event.addListener(marker, 'click', function(){
                      infoWindow.setContent('<h2>' + marker.title + "("+ info.country+")" + '</h2>' + marker.content);
                      infoWindow.open($scope.map, marker);
                  });

                  $scope.markers.push(marker);

              }

              var createCityMarker = function (info){

                  var marker = new google.maps.Marker({
                      map: $scope.map,
                      position: new google.maps.LatLng(info.lat, info.long),
                      title: info.area
                  });
                  marker.content = '<div class="infoWindowContent">' + 'activity: '+ info.activity 
                                                                     +'<p> blog url:'+ info.state+ '</p> '
                                                                     +'<p> blog url:'+ info.url+ '</p> '
                                                                     +'<p> title: '+ info.title+ '</p>'
                                                                     + '</div>';

                  google.maps.event.addListener(marker, 'click', function(){
                      infoWindow.setContent('<h2>' + marker.title + "("+ info.country+")" + '</h2>' + marker.content);
                      infoWindow.open($scope.map, marker);
                  });

                  $scope.markers.push(marker);
              }

              var createMoveMarker = function (info){
                  var mapOptions = {
                  zoom: 4,
                  center: new google.maps.LatLng(info.lat, info.long),
                  mapTypeId: google.maps.MapTypeId.TERRAIN
                  }
                  $scope.markers = [];
                 $scope.map = new google.maps.Map(document.getElementById('map'), mapOptions);
              }

              /*for (i = 0; i < cities.length; i++){
                  createMarker(cities[i]);
              }*/

              $scope.openInfoWindow = function(e, selectedMarker){
                  e.preventDefault();
                  google.maps.event.trigger(selectedMarker, 'click');
              }

              $rootScope.$on('createMarkerMethod', function (event, args) {
                createMoveMarker (args);
                })

            $rootScope.$on('insertMarkerMethod', function (event, args) {
                createCityMarker (args);
                })

          });


          app.controller('hashControl', function($scope,$rootScope){

                function HashTable(obj)
                {
                    this.length = 0;
                    this.items = {};
                    for (var p in obj) {
                        if (obj.hasOwnProperty(p)) {
                            this.items[p] = obj[p];
                            this.length++;
                        }
                    }

                    this.setItem = function(key, value)
                    {
                        var previous = undefined;
                        if (this.hasItem(key)) {
                            previous = this.items[key];
                        }
                        else {
                            this.length++;
                        }
                        this.items[key] = value;
                        return previous;
                    }

                    this.getItem = function(key) {
                        return this.hasItem(key) ? this.items[key] : undefined;
                    }

                    this.hasItem = function(key)
                    {
                        return this.items.hasOwnProperty(key);
                    }
   
                    this.removeItem = function(key)
                    {   
                        if (this.hasItem(key)) {
                            previous = this.items[key];
                            this.length--;
                            delete this.items[key];
                            return previous;
                        }
                        else {
                            return undefined;
                        }
                    }

                    this.keys = function()
                    {
                        var keys = [];
                        for (var k in this.items) {
                            if (this.hasItem(k)) {
                            keys.push(k);
                        }
                    }
                        return keys;
                    }

                    this.values = function()
                    {
                        var values = [];
                        for (var k in this.items) {
                        if (this.hasItem(k)) {
                            values.push(this.items[k]);
                        }
                        }
                        return values;
                    }

                    this.each = function(fn) {
                        for (var k in this.items) {
                            if (this.hasItem(k)) {
                            fn(k, this.items[k]);
                            }
                        }
                    }

                    this.clear = function()
                    {
                        this.items = {}
                        this.length = 0;
                    }
                }

                $rootScope.$on('createHashTable', function (event, args) {
                    args = new HashTable ({});
                    return args;
                })

                $rootScope.$on('addHashTableItem', function (event, args) {
                    var val = args.h.getItem(arg.key);
                    if(val != undefined){
                        val.activity = val.activity+ ", "+ args.val.activity;
                    }
                    args.h.setItem (arg.key, val);
                })

          });



           app.factory('hashTable', function(){
                 function HashTable(obj)
                {
                    this.length = 0;
                    this.items = {};
                    for (var p in obj) {
                        if (obj.hasOwnProperty(p)) {
                            this.items[p] = obj[p];
                            this.length++;
                        }
                    }

                    this.setItem = function(key, value)
                    {
                        var previous = undefined;
                        if (this.hasItem(key)) {
                            previous = this.items[key];
                        }
                        else {
                            this.length++;
                        }
                        this.items[key] = value;
                        return previous;
                    }

                    this.getItem = function(key) {
                        return this.hasItem(key) ? this.items[key] : undefined;
                    }

                    this.hasItem = function(key)
                    {
                        return this.items.hasOwnProperty(key);
                    }
   
                    this.removeItem = function(key)
                    {   
                        if (this.hasItem(key)) {
                            previous = this.items[key];
                            this.length--;
                            delete this.items[key];
                            return previous;
                        }
                        else {
                            return undefined;
                        }
                    }

                    this.keys = function()
                    {
                        var keys = [];
                        for (var k in this.items) {
                            if (this.hasItem(k)) {
                            keys.push(k);
                        }
                    }
                        return keys;
                    }

                    this.values = function()
                    {
                        var values = [];
                        for (var k in this.items) {
                        if (this.hasItem(k)) {
                            values.push(this.items[k]);
                        }
                        }
                        return values;
                    }

                    this.each = function(fn) {
                        for (var k in this.items) {
                            if (this.hasItem(k)) {
                            fn(k, this.items[k]);
                            }
                        }
                    }

                    this.clear = function()
                    {
                        this.items = {}
                        this.length = 0;
                    }
                }
                var hashT = {
                            createHashT: function(){
                             return new HashTable ({});
                            },
                            addHashTItem: function(arg){
                                var val = arg.h.getItem(arg.key);
                                if(val != undefined && val.activity.indexOf(arg.val.activity) == -1 ){
                                    val.activity = val.activity+ ", "+ arg.val.activity;
                                }
                                else{
                                    val = arg.val;
                                }
                                arg.h.setItem (arg.key, val);
                                return arg.h.getItem(arg.key);
                            },
                            getHashVal: function(arg){
                                return arg.values();
                            }
                        }
                     return hashT;
        
         });