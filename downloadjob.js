var kue = require('kue')
  , queue = kue.createQueue();
var rp = require('request-promise');
var config = require('../config.json');
var fs = require('fs');

var OPENSENSORS_API_BASE_URL = "https://api.opensensors.io"

queue.process('download', (job, done) => {
  // the download job is going to need the following parameters
  //    url       - the url to download
  //    save_path - the full path to where the result should be saved
  //    user_id   - the user id that made the request
  //    email     - the email address that should be notified on zip completed
  //    sequence  - the sequence number within this request chain


  var options = {
    uri: job.data.url,
    headers: {
       'Accept': 'application/json',
       'Authorization': 'api-key ' + config['api-key']
    },
    json: true,
    resolveWithFullResponse: true,
    simple: false 
  };  
  
  rp(options)
    .then((response) => {
      if(response.statusCode !== 200){
        done(new Error("OpenSensors returned status code " + response.statusCode));
      }
      else if(!response.body.messages){
        done(new Error("OpenSensors returned a body with no messages"));     
      }
      else{      
        if(response.body.messages.length == 0){
          console.log("Warning: response.body.messages.length was zero in response to " + job.data.url);        
        }
        
        let payload = response.body.messages.map((msg) => {
          // as it turns out nan is not valid JSON
          let body;
          try {
            body = msg.payload.text.replace(/':nan/g, '":null');
            body = body.replace(/nan/g, 'null');

            // workaround for malformation of uknown origin resulting in ' where " should be
            body = body.replace(/'/g, '"');

            let datum = JSON.parse(body);
            datum.timestamp = msg.date;
            datum.topic = msg.topic;
            return datum;
          }
          catch(exception){
            console.log(exception);
            console.log(body);
            return {
              timestamp: msg.date,
              topic: msg.topic
            };
          }
        });

        // if there is a next field then create a new download job modeled after this one
        if(response.body.next){
          let nextUrl = OPENSENSORS_API_BASE_URL + response.body.next;
          let job2 = queue.create('download', {
              title: 'downloading url ' + nextUrl
            , url: nextUrl
            , save_path: job.data.save_path
            , user_id: job.data.user_id
            , email: job.data.email
            , sequence: job.data.sequence + 1
          })
          .priority('high')
          .attempts(3)
          .backoff({delay: 60*1000, type:'fixed'})
          .save();                  
	}
	else{
        // otherwise create a new stitching job modeled after this one               
          let job2 = queue.create('stitch', {
              title: 'stitching data after ' + job.data.url 
            , save_path: job.data.save_path
            , user_id: job.data.user_id
            , email: job.data.email
          })
          .priority('high')
          .attempts(1)
          .save();     
        }
        
        // write the results to disk in the specified location
        let filepath = `${job.data.save_path}/${job.data.sequence}.json`;
        fs.writeFileSync(filepath, JSON.stringify(payload));
        done(null, payload);      
      }
    })
    .catch((err) => {
        console.log(err.stack);
        done(err);
    });   
});


process.once( 'uncaughtException', function(err){
  console.error( 'Something bad happened: ', err );
  queue.shutdown( 1000, function(err2){
    console.error( 'Kue shutdown result: ', err2 || 'OK' );
    process.exit( 0 );
  });
});
