printjson("****************************************************************");
printjson("1) Inserts a new document into the sample_weatherdata data collection using insertOne().");
printjson(insert_doc_1 = {
  st: "x+12345-654321",
  elevation: 9999,
  callLetters: "DRRP",
  dataSource: "4",
  type: "DP-07"
});
printjson(db.data.insertOne(insert_doc_1));

printjson("****************************************************************");
printjson("2) Inserts 3 documents that have the call letters 'LAQU' and have 'VO20' as the quality control process using insertMany().");
printjson(insert_many_docs1 = [
      { st: "x+54601-019308", elevation:  9999, callLetters: "LAQU", qualityControlProcess: "VO20" , dataSource: "4"},
      { st: "x+37856-097561", elevation: 9999, callLetters: "LAQU" , qualityControlProcess: "VO20", dataSource: "4"},
      { st: "x+67439-032187", elevation: 9999, callLetters: "LAQU", qualityControlProcess: "VO20", dataSource: "4"}
   ]);
printjson(db.data.insertMany(insert_many_docs1));

printjson("****************************************************************");
printjson("3) Inserts 3 documents that have a data source of '5' and type 'AM-09' using insertMany().");
printjson(insert_many_docs2 = [
      { st: "x+80234-047891", elevation:  9999, callLetters: "CG26", dataSource: "5", type: "AM-09"},
      { st: "x+23567-087643", elevation: 9999, callLetters: "PLAT" , dataSource: "5", type: "AM-09"},
      { st: "x+94871-015432", elevation: 9999, callLetters: "VCNP", dataSource: "5", type: "AM-09"}
   ]);
printjson(db.data.insertMany(insert_many_docs2));

printjson("****************************************************************");
printjson("4) For object id '5553a998e4b02cf7151190c9' this updates the call letters information to 'UYOK' and data source information to '3' using updateOne().");
printjson(db.data.updateOne( 
    { _id : ObjectId("5553a998e4b02cf7151190c9") },
    { $set: { "callLetters": "UYOK", "dataSource": "3" } }));

printjson("****************************************************************");
printjson("5) For object id '5553a998e4b02cf7151190c9', push the value 'DP1' to the sections array using updateOne() and $push.");
printjson(db.data.updateOne( 
    { _id : ObjectId("5553a998e4b02cf7151190c9") },
    { $push: { "sections.4": "DP1"}}));

printjson("****************************************************************");
printjson("6) Sets a 'Review' flag to true for all documents that have a callLetters value of 'VC81' using updateMany().");
printjson(db.data.updateMany({ callLetters: "VC81" }, { $set: { "Review" : true } }));

printjson("****************************************************************");
printjson("7) Updates all documents where the quality control process is 'V020' to have a 'Review' flag set to 'in-progress' using updateMany().");
printjson(db.data.updateMany({ qualityControlProcess: "V020" }, { $set: { "Review" : "in-progress" } }));

printjson("****************************************************************");
printjson("8) Deletes the weather data with _id: ObjectId('5553a998e4b02cf7151190cb') using deleteOne().");
printjson(db.data.deleteOne({ "_id" : ObjectId("5553a998e4b02cf7151190cb") }));

printjson("****************************************************************");
printjson("9) Deletes all documents where the position coordinates 0 value is greater than -20 and less than -10 using deleteMany().");
printjson(db.data.deleteMany({ "position.coordinates.0" : { $gt: -20.0, $lt: -10.0 }}));

printjson("****************************************************************");
printjson("10) Deletes all documents where pressure.value is greater than 900 and less than 970 using deleteMany().");
printjson(db.data.deleteMany({ "pressure.value" : { $gt: 900, $lt: 970 }}));