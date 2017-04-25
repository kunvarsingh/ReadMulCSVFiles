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
var csvConfig = require('./config/config');
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

app.post('/GetcsvParse',function(req,res){
function parseCSVFile(sourceFilePath, columns, onNewRecord, handleError, done){
    var source = fs.createReadStream(sourceFilePath);

    var linesRead = 0;

    var parser = Parse({
        delimiter: ',', 
        columns:columns
    });

    parser.on("readable", function(){
        var record;
        while (record = parser.read()) {
            linesRead++;
            //onNewRecord(record);
        }
    });

    parser.on("error", function(error){
        handleError(error)
    });

    parser.on("end", function(){
        done(linesRead);
    });

    source.pipe(parser);
}
//We will call this once Multer's middleware processed the request
//and stored file in req.files.fileFormFieldName

function parseFile(req, res, next){
    var filePath = req.files.file.path;
    console.log(filePath);
    function onNewRecord(record){
        console.log(record)
    }

    function onError(error){
        console.log(error)
    }

    function done(linesRead){
        res.send(200, linesRead)
    }

    var columns = true; 
    parseCSVFile(filePath, columns, onNewRecord, onError, done);

}
});


app.get('/NewCSV',function(req,res){
var inputFile='C:/Users/Kunvar/Documents/Archive/nitishUnh.csv';
console.log("Processing csv data file");
 
var parser = parse({delimiter: ','}, function (err, data) {
    // when all countries are available,then process them
    // note: array element at index 0 contains the row of headers that we should skip
    data.forEach(function(line) {
      // create country object out of parsed fields
      var country = { "First Line" : line[0],"Second": line[1],"Third":line[2]};
     console.log(JSON.stringify(country));
    });    
});
 


// read the inputFile, feed the contents to the parser
fs.createReadStream(inputFile).pipe(parser);
});



app.get('/readMultipleCsv', function(req, res) {
var filename="C:/Users/Kunvar/Documents/Archive/";
    //For cleaning the db
    client.flushdb( function (err, succeeded) {
        console.log(succeeded); // will be true if successfull
    });

    fs.unlink(filename+'output.csv',function(){
            console.log("File deleted");
    });

    var readCandidate_keys=csvConfig.csvSettings.canditateKeys;
    
    var matched_primary_key=0,
        matched_canditateKeys=0,
        matched_canditateKeys1=0,
        matched_canditateKeys2=0,
        matched_canditateKeys3=0;

    var readFile = function (file) {
        return new Promise(function (resolve, reject) {
            var lines = [], duplicateLines = [], exceptionLines=[];
            var rl = readline.createInterface({
                input: fs.createReadStream(csvConfig.csvSettings.sourcePath+file)
            });
            var lineNumber = 1;

            rl.on('line', function (line) {
                // Split line on comma and remove quotes
                var columns = line
                    .replace(/"/g, '')
                    .split(',');

                if (lineNumber == 1) {
                    for(var i=0;i<columns.length;i++){
                        if (columns[i] == csvConfig.csvSettings.primaryKey) {
                            matched_primary_key=i;
                        }
                        if (columns[i] == csvConfig.csvSettings.canditateKeys[0]) {
                            matched_canditateKeys=i;
                           // console.log("matched_canditateKeys"+matched_canditateKeys);
                        }
                        // if (columns[i] == csvConfig.csvSettings.canditateKeys[1]) {
                        //     matched_canditateKeys1=i;
                        // }
                        // if (columns[i] == csvConfig.csvSettings.canditateKeys[2]) {
                        //     matched_canditateKeys2=i;
                        // }
                        // if (columns[i] == csvConfig.csvSettings.canditateKeys[3]) {
                        //     matched_canditateKeys3=i;
                        // }
                    }
                }

                if(columns[matched_primary_key].length >0 ){
                    client.get(csvConfig.csvSettings.primaryKey+columns[matched_primary_key], function(err, reply) {
                       // console.log("Reply PP: ",reply);
                         if(reply != null){
                            //console.log("duplicate data: ",reply);
                            if(reply == columns[matched_primary_key]){
                               console.log("Id duplicate");
                                // duplicateLines.push(columns);
                            }
                         } else{
                            //console.log("data: ",reply);
                            client.set(csvConfig.csvSettings.primaryKey+columns[matched_primary_key],
                                columns[matched_primary_key], function(err, reply) {
                            });
                            lines.push(columns);
                         }
                    });
                } else{
                   // client.set(csvConfig.csvSettings.canditateKeys[0]+columns[3],"abcd");
                   // console.log("Empty npi id"+csvConfig.csvSettings.canditateKeys[0]+columns[3])

                    client.get(csvConfig.csvSettings.canditateKeys[0]+columns[matched_canditateKeys], function(err, reply) {
                        console.log(csvConfig.csvSettings.canditateKeys[0]
                            +columns[matched_canditateKeys] + " : "+ reply);
                        // console.log("Reply: ",reply+",  set values: "+csvConfig.csvSettings.canditateKeys[0]+columns[3]);

                        // var exists=exceptionLines.filter(function(data){
                        //     return data.indexOf(columns[3]);

                        // });
                        // console.log("exists"+exists);
                        if(reply != null){
                            if(reply == columns[matched_canditateKeys]){
                                console.log("duplicate name exists");
                            }
                         } else{
                            //console.log("name:"+columns[3]);
                            client.set(csvConfig.csvSettings.canditateKeys[0]+columns[matched_canditateKeys],
                                columns[matched_canditateKeys], function(err, reply) {
                                   //console.log("name column:"+columns[3]);
                            });
                         }
                         exceptionLines.push(columns);
                    });
                }
            });

            rl.on('close', function () {
                // Add newlines to lines
                
                // for(var i=1;i<lines[0].length;i++){
                //     if(lines.columns[i]=="npi.npi_id"){
                //      console.log("yes i am in");
                //     }  
                // }
                lines = lines.join("\n");
                duplicateLines=duplicateLines.join("\n");
                      
                var obj={ "lines": lines, "duplicateLines": duplicateLines };
                
                resolve(lines);
              
            });
        });
    };

    var writeFile = function (data) {
        
        return new Promise(function (resolve, reject) {
            fs.appendFile(csvConfig.csvSettings.outputPath + 'output.csv', data, 'utf8', function (err) {
                if (err) {
                    reject('Writing file error!');
                } else {
                    resolve('Writing file succeeded!');
                }
            });
            //for duplicate 
            //  fs.appendFile(csvConfig.csvSettings.outputPath + 'duplicate.csv', data[1], 'utf8', function (err) {
            //     if (err) {
            //         reject('Writing file error!');
            //     } else {
            //         resolve('Writing file succeeded!');
            //     }
            // });

        });
    };

    // var writeDupFile = function (data) {
        
    //     return new Promise(function (resolve, reject) {
    //         fs.appendFile(csvConfig.csvSettings.outputPath + 'outputDup.csv', data, 'utf8', function (err) {
    //             if (err) {
    //                 reject('Writing file error!');
    //             } else {
    //                 resolve('Writing file succeeded!');
    //             }
    //         });
    //     });
    // };
    fs.readdir(csvConfig.csvSettings.sourcePath, function (err, files) {
        for (var i = 0; i < files.length; i++) {
            promises.push(readFile(files[i]));
            
             if (i == (files.length - 1)) {
                var results = Promise.all(promises);
                // console.log("Results",results)
                results.then(writeFile)
                    .then(function (data) {
                        console.log("data:",data)
                    }).catch(function (err) {
                        console.log(err)
                    });
                // var results1 = Promise.all(promises[1]);
                // console.log("Results",results)
                // results1.then(writeDupFile)
                //     .then(function (data) {
                //         console.log("data:",data)
                //     }).catch(function (err) {
                //         console.log(err)
                //     });
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
