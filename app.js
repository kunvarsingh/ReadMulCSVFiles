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
var cacheObj = {};

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
        matched_supportingsKey=0;
    
    var readFile = function (file,uniq_csv,dup_csv,exp_csv) {

        return new Promise(function (resolve, reject) {
            var lines = [], duplicateLines = [], exceptionLines=[];
            var rl = readline.createInterface({
                input: fs.createReadStream(csvConfig.csvSettings.sourcePath+file)
            });
            
            var lineNumber = 1;
            var getCsvHeader=[];

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
                        }
                        if (columns[i] == csvConfig.csvSettings.canditateKeys[1]) {
                            matched_canditateKeys1=i;
                        }
                        if (columns[i] == csvConfig.csvSettings.canditateKeys[2]) {
                            matched_canditateKeys2=i;
                        }
                        if (columns[i] == csvConfig.csvSettings.supportingKeys[0]) {
                            matched_supportingsKey=i;
                        }
                    }
                    lineNumber++;
                    getCsvHeader.push(columns);
                }

                if(columns[matched_primary_key].length >0 ){ //If npi.npi_id is found
                    //var primaryIndex = csvConfig.csvSettings.primaryKey+columns[matched_primary_key];
                    var primaryIndex = columns[matched_primary_key];
                    var primary_val=columns[matched_primary_key];

                    if(cacheObj[primaryIndex]){
                            if(cacheObj[primaryIndex] == primary_val){
                               console.log("Id duplicate..."+primary_val);
                               // when id is same. insert data into duplicate.csv file
                               duplicateLines.push(columns);
                            }
                    }
                    else{
                        cacheObj[primaryIndex] = primary_val;
                        lines.push(columns);// unique entry in output.csv file
                    }
                }
                 else{ //npi.npi_id not found          check fname+mname+lname+address is unique then insert into output.csv file
                    var candidate=getCandidateKeys(getCsvHeader[0],csvConfig.csvSettings.canditateKeys
                        ,columns,csvConfig.csvSettings.supportingKeys);

                   // console.log("canadiafdte keys..."+candidate.keys);
                    //console.log("canadiafdte values..."+candidate.values);
                    // var cand_key=columns[matched_canditateKeys]
                    //             +columns[matched_canditateKeys1]
                    //             +columns[matched_canditateKeys2];
                    var cand_key=candidate.keys;
                    var cand_value=candidate.values;
                    //var cand_value=columns[matched_supportingsKey];
                    
                    if(cacheObj[cand_key]){
                        if(cacheObj[cand_key] == cand_value){
                            duplicateLines.push(columns);
                        }
                        else{
                            exceptionLines.push(columns);
                        }
                    }
                    else{
                        cacheObj[cand_key]=cand_value;
                        lines.push(columns);
                    }

                    // if(cacheObj[cand_key]){
                    //         if(cacheObj[cand_key] == cand_value){
                    //            console.log("Name duplicate..."+cand_value);
                    //            //duplicateLines.push(columns); //When name is same. entry into duplicate.csv file
                    //            //var cand_address=csvConfig.csvSettings.canditateKeys[3]+columns[matched_canditateKeys3];
                    //            var cand_address=csvConfig.csvSettings.supportingKeys[0]+columns[matched_supportingsKey];
                    //            var cand_address_value=columns[matched_supportingsKey];
                    //            console.log("cand_address"+cand_address+"sfsdfsd"+cand_address_value)
                    //             if(cacheObj[cand_address]){
                    //                 if(cacheObj[cand_address]==cand_address_value){
                    //                     console.log("address is duplicate.."+cand_address_value);
                    //                    // when address is same. 
                    //                     duplicateLines.push(columns);
                    //                 }
                    //                 else{
                    //                     //Id missing and f+m+l(name+Address is same)
                    //                    // enter into exception files
                    //                      exceptionLines.push(columns);
                    //                 }
                    //             }
                    //             else{
                    //                 cacheObj[cand_address]=cand_address_value;
                    //                 duplicateLines.push(columns);
                    //            }
                    //         }
                    // }
                    // else{
                    //         cacheObj[cand_key] = cand_value;
                    //         lines.push(columns);
                    //        // insert into output.csv file
                    // }
                    //when npi_id not found at the first time
                    //lines.push(columns);
                }
               
                // check if name is not unique 
            });

            rl.on('close', function () {
                
                // duplicateLines.unshift(getCsvHeader);
                // exceptionLines.unshift(getCsvHeader);
                // lines.shift();
                // duplicateLines.shift();
                // exceptionLines.shift();
                
                lines = lines.join("\n");
                duplicateLines=duplicateLines.join("\n");
                exceptionLines=exceptionLines.join("\n");
              
                uniq_csv.push(lines);
                dup_csv.push(duplicateLines);
                exp_csv.push(exceptionLines);

                uniq_csv=uniq_csv.join("\n");
                dup_csv=dup_csv.join("\n");
                exp_csv=exp_csv.join("\n");

                var obj={ "lines": uniq_csv, "duplicateLines": dup_csv, "exceptionLines": exp_csv };
                resolve(obj);
            });
        });
    };
    
    var getCandidateKeysIndex=function(columns){
       
        return indexValue;
    }

    var getCandidateKeys=function(columns,candidateKey,columnsValue,supportingKeys){
        var keys="";
        var values="";

        var indexValue=[];
        var supportingIndexs=[];
        var keyAndValues={};
        if(candidateKey.length){
           //var indexValue=[];
            for(var i=0;i< candidateKey.length;i++){
                indexValue.push(columns.indexOf(candidateKey[i]));
                //
            }

            for(var j=0;j<indexValue.length;j++){
                keys=keys+columnsValue[indexValue[j]];
            }

            //looping for supporting keys:
            for(var i=0;i< supportingKeys.length;i++){
                supportingIndexs.push(columns.indexOf(supportingKeys[i]));
                //
            }

            for(var j=0;j<supportingIndexs.length;j++){
                values=values+columnsValue[supportingIndexs[j]];
            }

            keyAndValues={"keys":keys,"values":values};
            return keyAndValues;
        }
    }
    var writeFile = function (data) {
       console.log("Write function");
        return new Promise(function (resolve, reject) {
            var a=new Date().toLocaleString();
            
            var newDateappend=a.replace(/ /g,'_');
            var dateAppend=newDateappend.replace(/:/g,'_');
            var dateAppend1=dateAppend.split('/').join('_');
            //console.log(dateAppend+"sssss"+dateAppend);

            var fileArr = ["output(","duplicate(","exceptions("];
            var output = fileArr[0] +dateAppend1 +')'+ '.csv';
            //console.log(output);
            var duplicate = fileArr[1] +dateAppend1 + ')'+'.csv';
            var exceptions = fileArr[2] +dateAppend1 + ')'+'.csv';
            

            fs.appendFile(csvConfig.csvSettings.outputPath + output,  data[0].lines, 'utf8', function (err) {
                if (err) {
                    reject('Writing file error!');
                } else {
                    resolve('Writing file succeeded!');
                }
            });
            fs.appendFile(csvConfig.csvSettings.outputPath + duplicate,  data[0].duplicateLines, 'utf8', function (err) {
                if (err) {
                    reject('Writing file error!');
                } else {
                    resolve('Writing file succeeded!');
                }
            });
            fs.appendFile(csvConfig.csvSettings.outputPath + exceptions,  data[0].exceptionLines, 'utf8', function (err) {
                if (err) {
                    reject('Writing file error!');
                } else {
                    resolve('Writing file succeeded!');
                }
            });
        });
    };

   
    fs.readdir(csvConfig.csvSettings.sourcePath, function (err, files) {
        cacheObj = {};
        var uniq_csv=[];
        var dup_csv=[];
        var exp_csv=[];
        for (var i = 0; i < files.length; i++) {
            // This line is the issue.
           // console.log("FIle name:"+files[i]);

            promises.push(readFile(files[i],uniq_csv,dup_csv,exp_csv));
            console.log("promises :"+promises);
             if (i == (files.length - 1)) {
                var results = Promise.all(promises);
                results.then(writeFile)
                    .then(function (data) {
                        console.log("data:",data)
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
