var express = require('express');
var path = require('path');
//var favicon = require('serve-favicon');
//var logger = require('morgan');
//var cookieParser = require('cookie-parser');
//var bodyParser = require('body-parser');
//var mongoose=require('mongoose');// for database
var cors = require('cors'); //For handling the server request
//var parse = require('csv-parse');
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

app.use(express.static(path.join(__dirname, 'public')));

app.use('/', index);

app.get('/readMultipleCsv', function(req, res) {
    var readCandidate_keys=csvConfig.csvSettings.canditateKeys;
    var matched_primary_key=0;
    
        var readFile = function (file) {
        return new Promise(function (resolve, reject) {
            var lines = [], duplicateLines = [], exceptionLines=[];
            var rl = readline.createInterface({
                input: fs.createReadStream(csvConfig.csvSettings.sourcePath + file)
            });
            
            var lineNumber = 1;
            var getCsvHeader=[];

            rl.on('line', function (line, data) {
                if(lineNumber == 1 && getCsvHeader.length > 0){
                    return;
                }

                // Split line on comma and remove quotes
                var columns = line
                    .replace(/"/g, '')
                    .split(',');
                    
                if (lineNumber == 1) {
                    for(var i=0;i<columns.length;i++){
                        if (columns[i] == csvConfig.csvSettings.primaryKey) {
                            matched_primary_key=i;
                        }
                    }
                    lineNumber++;
                    getCsvHeader.push(columns);
                }

                if(columns[matched_primary_key].length >0 ){ //If npi.npi_id is found
                    var primaryIndex = columns[matched_primary_key];
                    var primary_val=columns[matched_primary_key];
                    if(cacheObj[primaryIndex]){
                            if(cacheObj[primaryIndex] == primary_val){
                               // console.log("Id duplicate..."+primary_val);
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

                    var cand_key=candidate.keys;
                    var cand_value=candidate.values;
                    
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
                }
            });

            rl.on('close', function () {
                // New Line
                lines.push('');
                duplicateLines.push('');
                exceptionLines.push('');

                lines = lines.join("\n");
                duplicateLines=duplicateLines.join("\n");
                exceptionLines=exceptionLines.join("\n");
              
               
                var obj={ "lines": lines, "duplicateLines": duplicateLines, "exceptionLines": exceptionLines };
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

            console.log('LENGTH' + data.length);
             for(var i = 0; i < data.length; i++){
                fs.appendFileSync(csvConfig.csvSettings.outputPath + output,  data[i].lines, 'utf8', function (err) {

                    if (err) {
                        reject('Writing file error!');
                    } else {
                        resolve('Writing file succeeded!');
                    }
                });
                fs.appendFileSync(csvConfig.csvSettings.outputPath + duplicate,  data[i].duplicateLines, 'utf8', function (err) {
                    if (err) {
                        reject('Writing file error!');
                    } else {
                        resolve('Writing file succeeded!');
                    }
                });
                fs.appendFileSync(csvConfig.csvSettings.outputPath + exceptions,  data[i].exceptionLines, 'utf8', function (err) {
                    if (err) {
                        reject('Writing file error!');
                    } else {
                        resolve('Writing file succeeded!');
                    }
                });
            }
        });
    
    };

   
    fs.readdir(csvConfig.csvSettings.sourcePath, function (err, files) {
        cacheObj = {};
        promises = [];
        for (var i = 0; i < files.length; i++) {
             promises.push(readFile(files[i]));
             if (i == (files.length - 1)) {
                var results = Promise.all(promises);
                
                results.then(writeFile)
                    .then(function (data) {
                        console.log("Write completed!!!!");
                    }).catch(function (err) {
                        console.log(err);
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
