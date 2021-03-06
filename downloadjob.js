var kue = require('kue')
  , queue = kue.createQueue();
var rp = require('request-promise');
var config = require('../config.json');
var fs = require('fs');
var path = require('path');
var moment = require('moment');

var OPENSENSORS_API_BASE_URL = "https://api.opensensors.io"

queue.process('download', (job, done) => {
  // the download job is going to need the following parameters
  //    original_serials - the original array of egg serial numbers
  //    serials   - the array of egg serial numbers remaining to process
  //    url       - the url to download
  //    save_path - the full path to where the result should be saved
  //    user_id   - the user id that made the request
  //    email     - the email address that should be notified on zip completed
  //    sequence  - the sequence number within this request chain
  //    compensated - whether to extract compensated values (true) or uncompensated values (false)
  //    instantaneous - whether to extract instantaneous values (true) or averaged values (false)
  //    utcOffset - the utcOffset for downstream moment conversion in the csv file
  //    zipfilename - the filename desired for the final zip
  //    bypassjobs - array of jobs that should not be run in this pipe


  if (!fs.existsSync(job.data.save_path)){
    fs.mkdirSync(job.data.save_path);
    fs.writeFileSync(`${job.data.save_path}/status.json`, JSON.stringify({complete: false}));
  }

  var options = {
    uri: job.data.url.replace('${serial-number}', job.data.serials[0].split("=")[0]),
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
      if(response.statusCode === 404){
        createDirFromJobIfNotExists();
        spawnNextSerialNumberJob();
        done();
      }
      else if(response.statusCode !== 200){
        if(response.statusCode === 400){
          console.log("Backing off for 1 minute because got 400 status code");
          setTimeout(() => {
            done(new Error(`OpenSensors returned status code ${response.statusCode}, with body ${JSON.stringify(response.body)}`));
          }, 60000);
        }
        else{
          done(new Error(`OpenSensors returned status code ${response.statusCode}, with body ${JSON.stringify(response.body)}`));
        }
      }
      else if(!response.body.messages){
        done(new Error("OpenSensors returned a body with no messages"));
      }
      else{
        // if the requisite subdirector doesn't exist, then create it
        let dir = createDirFromJobIfNotExists();

        if(response.body.messages.length == 0){
          console.log("Warning: response.body.messages.length was zero in response to " + options.uri);
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

        let nextUrl = "";
        let theNextUrl = "";
        if(response.body.next){
          nextUrl = OPENSENSORS_API_BASE_URL + response.body.next;
          theNextUrl = nextUrl.replace(job.data.serials[0].split("=")[0], '${serial-number}');
        }
        if(job.data.url === theNextUrl){
          console.log("theNextUrl would be the same as the current Url - ignoring it.", theNextUrl);
        }

        if(response.body.next && startAndEndAreDifferent(response.body.next) && (job.data.url !== theNextUrl)){
          // if there is a next field then create a new download job modeled after this one
          // console.log(`Next URL after ${options.uri} is ${nextUrl}`)
          let job2 = queue.create('download', {
              title: 'downloading url ' + decodeURIComponent(nextUrl)
            , original_serials: job.data.original_serials.slice()
            , serials: job.data.serials.slice()
            , url: theNextUrl
            , original_url: job.data.original_url
            , save_path: job.data.save_path
            , user_id: job.data.user_id
            , email: job.data.email
            , sequence: job.data.sequence + 1
            , compensated: job.data.compensated
            , instantaneous: job.data.instantaneous
            , utcOffset: job.data.utcOffset
            , zipfilename: job.data.zipfilename
            , bypassjobs: job.data.bypassjobs ? job.data.bypassjobs.slice() : []
            , stitch_format: job.data.stitch_format
          })
          .priority('high')
          .attempts(10)
          .backoff({delay: 60*1000, type:'exponential'})
          .save();
        }
        else {
          // pop the zero element out of the serials array
          // if there are any left, spawn a new job with the
          // reduced array of serial numbers
          spawnNextSerialNumberJob();
        }

        // write the results to disk in the specified location
        let filepath = `${dir}/${job.data.sequence}.json`;
        fs.writeFileSync(filepath, JSON.stringify(payload));
        // done(null, payload);
        done();
      }
    })
    .catch((err) => {
        console.log(err.stack);
        done(err);
    });

    let createDirFromJobIfNotExists = () => {
      let split = job.data.serials[0].split("=");
      let dir = `${job.data.save_path}/${job.data.serials[0]}`;
      if(split.length > 1){
        let cleanup_prefix = split.slice(1).join("_");
        cleanup_prefix += "_" + split[0];
        cleanup_prefix = cleanup_prefix.replace(/[^\x20-\x7E]+/g, ''); // no non-printable characters allowed
        ['\\\\','/',':','\\*','\\?','"','<','>','\\|',"-"," "].forEach(function(c){
          var regex = new RegExp(c, "g");
          cleanup_prefix = cleanup_prefix.replace(regex, "_"); // turn illegal characters into '_'
        });
        dir = `${job.data.save_path}/${cleanup_prefix}`;
      }

      if (!fs.existsSync(dir)){
        fs.mkdirSync(dir);
      }

      return dir;
    }

    let spawnNextSerialNumberJob = () => {
      let serials = job.data.serials.slice(1);
      if(serials.length > 0){
        let job2 = queue.create('download', {
            title: 'downloading url ' + decodeURIComponent(job.data.original_url.replace('${serial-number}',serials[0].split("=")[0]))
          , original_serials: job.data.original_serials.slice()
          , serials: serials
          , url: job.data.original_url
          , original_url: job.data.original_url
          , save_path: job.data.save_path
          , user_id: job.data.user_id
          , email: job.data.email
          , sequence: 1
          , compensated: job.data.compensated
          , instantaneous: job.data.instantaneous
          , utcOffset: job.data.utcOffset
          , zipfilename: job.data.zipfilename
          , bypassjobs: job.data.bypassjobs ? job.data.bypassjobs.slice() : []
          , stitch_format: job.data.stitch_format
        })
        .priority('high')
        .attempts(10)
        .backoff({delay: 60*1000, type:'exponential'})
        .save();
      }
      else {
        // otherwise create a new stitching job modeled after this one
        // populate serials with the list of directories in the working folder-  getDirectories(job.data.save_path).forEach( (dir) => {		 +  let dir = ${job.data.serials[0]};
        let getDirectories = (srcpath) => {
          return fs.readdirSync(srcpath).filter( (file) => {
            return fs.statSync(path.join(srcpath, file)).isDirectory();
          });
        }

        let directories = getDirectories(job.data.save_path);

        let job2 = queue.create('stitch', {
            title: 'stitching data for ' + job.data.original_serials[0]
          , save_path: job.data.save_path
          , original_serials: job.data.original_serials.slice()
          , original_url: job.data.original_url
          , serials: directories
          , user_id: job.data.user_id
          , email: job.data.email
          , compensated: job.data.compensated
          , instantaneous: job.data.instantaneous
          , utcOffset: job.data.utcOffset
          , zipfilename: job.data.zipfilename
          , bypassjobs: job.data.bypassjobs ? job.data.bypassjobs.slice() : []
          , stitch_format: job.data.stitch_format
        })
        .priority('high')
        .attempts(1)
        .save();
      }
    };
});

function startAndEndAreDifferent(url){
  // first of all url decode the url
  url = decodeURIComponent(url);

  // url should have a start-date querystring parameter, and end-date querystring parameter
  // extract them
  let start = url.match(/start-date=([0-9T\-Z:\.\+]+)&?/)[1];
  let end = url.match(/end-date=([0-9T\-Z:\.\+]+)&?/)[1];

  try{
    if(moment(start).isSame(moment(end))){
      console.log(`FYI, ${start} and ${end} are in fact not different dates`);
      return false;
    }
    else{
      return true;
    }
  }
  catch(e){
    console.log("This should never happen", e);
    return start != end;
  }
}

process.once( 'uncaughtException', function(err){
  console.error( 'Something bad happened: ', err );
  queue.shutdown( 1000, function(err2){
    console.error( 'Kue shutdown result: ', err2 || 'OK' );
    process.exit( 0 );
  });
});
