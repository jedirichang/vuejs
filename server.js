var http = require('http');
var firebase = require('firebase-admin');
var socketCluster = require('socketcluster-client');
var request = require('request');
var moment = require('moment');
var markets = [];

firebase.initializeApp({
    credential: firebase.credential.cert('serviceKey.json'),
    databaseURL: "https://volume-analytics.firebaseio.com"

})
const db = firebase.database();

var trades = db.ref('trades')

//Socket Clusters to fetch data in RealTime using various channels.
 var api_credentials =
 {
     "apiKey"    : "6c82cc3d47471bf81cb151446e31b23d",
     "apiSecret" : "26161c997747570a547da5cf91a1de2a"
 }

 var options = {
     hostname  : "sc-02.coinigy.com",    
     port      : "443",
     secure    : "true"
 };
 console.log(options);
var SCsocket = socketCluster.connect(options);


 SCsocket.on('connect', function (status) {

     console.log(status); 

    SCsocket.on('error', function (err) {
         console.log(err);
     });    


     SCsocket.emit("auth", api_credentials, function (err, token) {        

         if (!err && token) {                       
            //  SCsocket.emit("exchanges", null, function (err, data) {
            //      if (!err) {                  
            //         //  console.log(data);
            //      } else {
            //          console.log(err)
            //      }   
            //  });

             SCsocket.emit("channels", "BINA", function (err, data) {
                if (!err) {
                     data[0].forEach(element => {
                             if(element.channel.split('-')[0] === "TRADE")
                                {
                                     var scChannel = SCsocket.subscribe(element.channel);
                                     scChannel.watch(function (data) {
                                         data.timestamp=new Date(data.time_local).getTime();
                                         trades.push(data);
                                     });     
                                 }
                     });
                 } else {
                     console.log(err)
                 }   
             }); 
             SCsocket.emit("channels", "BTRX", function (err, data) {
                if (!err) {
                     data[0].forEach(element => {
                             if(element.channel.split('-')[0] === "TRADE")
                                {
                                     var scChannel = SCsocket.subscribe(element.channel);
                                     scChannel.watch(function (data) {
                                         data.timestamp=new Date(data.time_local).getTime();
                                         trades.push(data);
                                     });     
                                 }
                     });
                 } else {
                     console.log(err)
                 }   
             }); 
         } else {
             console.log(err)
         }   
     });   
 });






// Method for runing calculations on the data fetched from Firebase Trades node and saving Hourly and Daily aggregated data in two different nodes.
// Later in time this method will be shifted to firebase cloud function itself so that we don't have to worry about load balancing.

trades.on("value", function (snapshot) {
    // console.log(snapshot.val())
    if (snapshot.val() != null)
        makeCalculations(snapshot.val());

}, function (errorObject) {
    console.log("The read failed: " + errorObject.code);
});



function makeCalculations(data) {
    var visted = [];
    var currentDateObj = moment();
    var newArrDaily = [];
    var newArrHourly = [];
    Object.keys(data).forEach(function (trade, index) {
        trade = data[trade];
        if (!isVisited(trade) && trade.type=="BUY") {

            trade.price = checkExponentialNumber(trade.price);

            var newTradeDaily = {};
            var newTradeHourly = {};
            var calculatedEntity = calcTradeEntities(trade);
            newTradeDaily.exchange = trade.exchange;
            newTradeDaily.asset = trade.label;
            newTradeDaily.currentPrice = parseFloat(calculatedEntity.currentPrice)
            newTradeDaily.currentPeriodVolume = parseFloat(calculatedEntity.currentPeriodVolumeDaily)
            newTradeDaily.fifteenPeriodAvg = parseFloat(calculatedEntity.fifteenPeriodAvgDaily)
            newTradeDaily.percentageVolume = parseFloat(newTradeDaily.currentPeriodVolume / newTradeDaily.fifteenPeriodAvg).toFixed(3)
            newTradeDaily.volumeFlowRate = parseFloat(newTradeDaily.currentPeriodVolume / calculatedEntity.timeFromPeriodBeginningDaily).toFixed(3)

            newTradeHourly.exchange = trade.exchange;
            newTradeHourly.asset = trade.label;
            newTradeHourly.currentPrice = parseFloat(calculatedEntity.currentPrice)
            newTradeHourly.currentPeriodVolume = parseFloat(calculatedEntity.currentPeriodVolumeHourly)
            newTradeHourly.fifteenPeriodAvg = parseFloat(calculatedEntity.fifteenPeriodAvgHourly)
            newTradeHourly.percentageVolume = parseFloat(newTradeHourly.currentPeriodVolume / newTradeHourly.fifteenPeriodAvg).toFixed(3)
            newTradeHourly.volumeFlowRate = parseFloat(newTradeHourly.currentPeriodVolume / calculatedEntity.timeFromPeriodBeginningHourly).toFixed(3)

            visted.push({
                exchange: newTradeDaily.exchange,
                label: newTradeDaily.asset
            });
            newArrDaily.push(newTradeDaily);
            newArrHourly.push(newTradeHourly);
        }
    });
    console.log('Pushing to firebase nodes.')
    db.ref('hourly').set(newArrHourly);
    db.ref('daily').set(newArrDaily);
    // console.log(newArrHourly);

    function isVisited(trade) {
        var temp = visted.find(function (el) {
            return el.exchange === trade.exchange && el.label === trade.label;
        })

        return temp;
    }


    function calcTradeEntities(trade) {
        var temp = {
            currentPeriodVolumeHourly: 0,
            currentPeriodVolumeDaily: 0,
            currentPrice: 0,
            latestTimestamp: new Date('1971-1-1').getTime(),
            fifteenPeriodAvgHourly: 0,
            fifteenPeriodAvgDaily: 0
        };
        var countDaily = 0;
        var countHourly = 0;
        Object.keys(data).forEach(function (_e, index) {
            _e = data[_e];
            if (_e.exchange === trade.exchange && _e.label == trade.label) {
                //Calculating Current period volume
                if (getDateDiff(moment(_e.timestamp), currentDateObj, 'hours') <= 1) {
                        _e.quantity=Number(_e.quantity);
                        temp.currentPeriodVolumeHourly += _e.quantity;
                }


                //Calculating 15-period volume
                if (getDateDiff(moment(_e.timestamp), currentDateObj, 'hours') <= 15) {
                        temp.fifteenPeriodAvgHourly += _e.quantity;
                        ++countHourly;
                }

                //Calculating time from beginning of period
                if (temp.timeFromPeriodBeginning === undefined)
                    temp.timeFromPeriodBeginningHourly = parseInt(moment().format('mm'));

                //Last period closing price

                //Calculating current period volume
                if (currentDateObj.isSame(moment(_e.timestamp), 'd')) {
                    temp.currentPeriodVolumeDaily += _e.quantity;
                }
                //Calculating 15-period volume
                if (getDateDiff(moment(_e.timestamp), currentDateObj, 'days') <= 15) {
                    temp.fifteenPeriodAvgDaily += _e.quantity;
                    ++countDaily;
                }

                //Calculating time from beginning of period
                if (temp.timeFromPeriodBeginning === undefined)
                    temp.timeFromPeriodBeginningDaily = parseInt(moment().format('HH')) * 60;



                //Finding Current Price
                if (Math.abs(temp.latestTimestamp) <= Math.abs(_e.timestamp)) {
                    if (_e.price != undefined) {
                        temp.latestTimestamp = moment(_e.timestamp).unix();
                        temp.currentPrice = checkExponentialNumber(_e.price);
                    }
                }
            }
        })

        if(countHourly >0)
        temp.fifteenPeriodAvgHourly = temp.fifteenPeriodAvgHourly / countHourly;

        if(countDaily >0)
        temp.fifteenPeriodAvgDaily =  temp.fifteenPeriodAvgDaily / countDaily;

      
        return temp;
    }

}



function findIfTradeIsAlreadyPresent(trade) {
    var temp = newArr.find(function (_e) {
        return _e.exchange === trade.exchange && _e.label === trade.label && _e.key === trade.key
    })
    if (temp)
        return true
    else
        return false
}

function checkExponentialNumber(number) {
    if (number.toString().includes('-')) {
        number = number.toString().replace(/\-/g, "");
        var temp = number.split('e');
        number = parseFloat(temp[0]) * Math.pow(10, parseInt(temp[1]));

    }
    return number;
}

function getDateDiff(first, second, type) {
    return Math.abs(first.diff(second, type));
}

var dateCutoff=Date.now() - (20*24 * 60 * 60 * 1000) 
trades.orderByChild('timestamp').endAt(dateCutoff).on('value',function(snapshot){
    var updates = {};
    snapshot.forEach(function(child) {
      updates[child.key] = null
    });
    if(Object.keys(updates).length > 0)
        console.log('Old Data deleted!!!');
    // execute all updates in one go and return the result to end the function
    return trades.update(updates);
})

// BTS/BNB

// trades.orderByChild('label').equalTo("BTS/BNB").on('value',function(snapshot){
//     var temp=snapshot.val();
//     console.log(temp);
// })


http.createServer().listen('3000', () => {
    console.log('Feeling good at 3000')
})