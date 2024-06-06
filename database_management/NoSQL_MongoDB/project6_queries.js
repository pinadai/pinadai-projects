printjson("****************************************************************");
printjson("1) Returns pressures that are not too low or too high, between a range of 1008 and 1050. Sorts the results in ascending order. Limits the query results to 3.");
printjson(selection = { "pressure.value": { $gt: 1008, $lt: 1050 }});
printjson(projection = { "pressure": 1, "ts": 1, _id: 0 });
printjson(db.data.find(selection, projection).sort({ "pressure.value": 1 }).limit(3))

printjson("****************************************************************");
printjson("2) Returns those whose callLetters are either VC81 or TFBY.");
printjson(selection = {"$or": [{"callLetters": "VC81"},{"callLetters": "TFBY"}]});
printjson(projection = {"ts": 1,"callLetters": 1,"type": 1});
printjson(db.data.find(selection, projection).limit(3));

printjson("****************************************************************");
printjson("3) Returns weather data that has a wind speed rate greater than 12 and a dew point quality of '9'");
printjson(selection = {"wind.speed.rate": { $gt: 12 }, "dewPoint.quality": "9"});
printjson(projection = {"wind": 1,"dewPoint": 1});
printjson(db.data.find(selection, projection).sort({"wind.speed.rate": 1}).limit(3));

printjson("****************************************************************");
printjson("4) Returns weather data that either has a precipitation estimated observation discrepancy of '2' or air temperature greater than -5.0 but less than 0.");
printjson(selection = {"$or": [{"precipitationEstimatedObservation.discrepancy": "2"},{"airTemperature.value": { $gt: -5.0, $lt:0 }}]});
printjson(projection = {"precipitationEstimatedObservation": 1, "airTemperature": 1, "ts": 1});
printjson(db.data.find(selection, projection).sort({"airTemperature.value": 1}).limit(3));

printjson("****************************************************************");
printjson("5) Returns weather data that has a sky condition ceiling height less than 50000.");
printjson(selection = {"skyCondition.ceilingHeight.value": {$lt: 50000}});
printjson(projection = {"skyCondition": 1, "ts": 1, "visibility": 1});
printjson(db.data.find(selection, projection).sort({"skyCondition.ceilingHeight.value": 1}).limit(3));

printjson("****************************************************************");
printjson("6) Returns weather data that has an atmospheric condition value of '0' under past weather observational manual.");
printjson(selection = {"pastWeatherObservationManual.0.atmosphericCondition.value": "0"});
printjson(projection = {"pastWeatherObservationManual": 1,"precipitationEstimatedObservation": 1,"skyConditionObservation": 1, "presentWeatherObservationManual": 1, _id: 0});
printjson(db.data.find(selection, projection).limit(3));

printjson("****************************************************************");
printjson("7) Returns the weather data where the present weather observation manual condition is '05' and the period value for wave measurement is greater than 3.");
printjson(selection = {"presentWeatherObservationManual.0.condition": "05", "waveMeasurement.waves.period": {$gt: 3}});
printjson(projection = {"presentWeatherObservationManual": 1, "pastWeatherObservationManual": 1, "waveMeasurement": 1, "ts": 1});
printjson(db.data.find(selection, projection).limit(3));

printjson("****************************************************************");
printjson("8) Returns weather data that has 'MD1' for section 3, an air temperature value of more than 200, and a sky condition cavok of 'N'.");
printjson(selection = {"sections.3": "MD1", "airTemperature.value": { $gt: 200 },"skyCondition.cavok": "N"});
printjson(projection = {"sections": 1, "airTemperature": 1, "skyCondition": 1, "ts": 1, "wind": 1});
printjson(db.data.find(selection, projection).sort({"airTemperature.value": 1}).limit(3));

printjson("****************************************************************");
printjson("9) Returns weather data that either has a past weather observation manual period value between 2 and 6 or has a position coordinate 1 between 50 and 62.");
printjson(selection = {"$or": [{"pastWeatherObservationManual.0.period.value": {$gt: 2, $lt: 6}},{"position.coordinates.1": {$gt: 50, $lt: 62}}]});
printjson(projection = {"pastWeatherObservationManual": 1, "position": 1, "presentWeatherObservationManual": 1, _id: 0});
printjson(db.data.find(selection, projection).sort({"pastWeatherObservationManual.0.period.value": 1}).limit(3));

printjson("****************************************************************");
printjson("10) Retrieves weather data that was taken from regions in Canada.");
printjson(selection = {"position.coordinates.0": { $gt: -141.0, $lt: -52.6 }, "position.coordinates.1": { $gt: 41.6, $lt: 83.2 }});
printjson(projection = {"ts": 1, "position.coordinates": 1, "elevation": 1, "airTemperature": 1, "dewPoint": 1, "pressure": 1, "wind": 1, "visibility": 1, "skyCondition": 1, "sections": 1, "precipitationEstimatedObservation": 1});
printjson(db.data.find(selection, projection).sort({"position.coordinates.0": 1}).limit(3));