db.listingsAndReviews.find( {name: {$regex : ".*a.*"}}, { _id : 0, name:1}).explain("executionStats").executionStats}


db.listingsAndReviews.aggregate([
{ $match: {"address.country": 
{ $eq: 'Turkey'}}},
 { $group: {
_id: null, avgP: { $avg: '$price'}}}
])



db.listingsAndReviews.aggregate([
 { $group: {
_id:  "$address.country", sumRooms: { $sum: '$bedrooms'}}}])


db.listingsAndReviews.aggregate([
 { $group: {
_id: null, avgP: { $avg: '$price'}}}
])


db.listingsAndReviews.aggregate([
 { $group: {
_id:  "$address.country", avgP: { $avg: '$price'}}}])


mongodump --host Cluster0-shard-0/cluster0-shard-00-00-lqqq1.mongodb.net:27017,cluster0-shard-00-01-lqqq1.mongodb.net:27017,cluster0-shard-00-02-lqqq1.mongodb.net:27017 --ssl --username ShyGuy118 --password fg1511973 --authenticationDatabase ShyGuy118 --db sample_airbnb


mongodump --host Cluster0-shard-0/cluster0-shard-00-00-lqqq1.mongodb.net:27017,cluster0-shard-00-01-lqqq1.mongodb.net:27017,cluster0-shard-00-02-lqqq1.mongodb.net:27017 --ssl --username ShyGuy118 --password fg1511973 --authenticationDatabase admin --db sample_airbnb

docker run --rm -v $(pwd):/workdir/ -w /workdir/ mongo:4.0 mongodump -h server -d $sample_airbnb --out /workdir/dump/

mongodump --forceTableScan -h Cluster0-shard-0/cluster0-shard-00-00-lqqq1.mongodb.net:27017,cluster0-shard-00-01-lqqq1.mongodb.net:27017,cluster0-shard-00-02-lqqq1.mongodb.net:27017 -u ShyGuy118 -p fg1511973 -d sample_airbnb  -o /