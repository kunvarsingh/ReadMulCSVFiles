var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var mongoose=require('mongoose');// for database
var cors = require('cors'); //For handling the server request
var parse = require('csv-parse');
const fs = require('fs');
var Promise  = require('promise');
var readline = require('readline');
var redis = require('redis');

var app = express();
var index = require('./routes/index');
var promises = [];

app.use(cors());
// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

//Radis configuration in Nodejs
var client = redis.createClient('6379','127.0.0.1'); //creates a new client

client.on('connect', function() {
    console.log('connected');
});

app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', index);
app.get('/readMultipleCsv', function(req, res) {
    var readFile = function (file) {
        return new Promise(function (resolve, reject) {
            var lines = [];
            var uniqueArray = [];
            var rl    = readline.createInterface({
                input: fs.createReadStream('C:/Users/Kunvar/Documents/Archive/'+file)
            });

            rl.on('line', function (line) {
                // Split line on comma and remove quotes
                var columns = line
                    .replace(/"/g, '')
                    .split(',');

                 client.get('npiId'+columns[16], function(err, reply) {
                     if(reply != null){
                        console.log("data: ",reply);
                        if(reply == columns[16]){
                            console.log("duplicate");
                        }
                        else{
                            client.set('npiId'+columns[16], columns[16], function(err, reply) {
                            // console.log(reply);
                            });
                        }
                     }
                     else{
                        console.log("data duplicate: ",reply);
                     }
                     
                                   
                });

                
               
                lines.push(columns);
            });

            rl.on('close', function () {
                // Add newlines to lines
                lines = lines.join("\n");
                resolve(lines)
            });
        });
    };

    var writeFile = function (data) {
        return new Promise(function (resolve, reject) {
            fs.appendFile('output.csv', data, 'utf8', function (err) {
                if (err) {
                    reject('Writing file error!');
                } else {
                    resolve('Writing file succeeded!');
                }
            });
        });
    };

    fs.readdir('C:/Users/Kunvar/Documents/Archive', function (err, files) {
        for (var i = 0; i < files.length; i++) {
            promises.push(readFile(files[i]));

            if (i == (files.length - 1)) {
                var results = Promise.all(promises);
                results.then(writeFile)
                    .then(function (data) {
                        console.log(data)
                    }).catch(function (err) {
                        console.log(err)
                    });
            }
        }
    });
});

app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
