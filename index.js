const fs = require('fs');
const csv = require('fast-csv');
const MongoClient = require('mongodb').MongoClient;

const url = '';
const stream = fs.createReadStream('hours.csv');

const update = branch => {
  MongoClient.connect(url, (err, db) => {
    if (!branch) {
      db.close();
      process.exit(1);
    }

    const col = db.collection('branches');

    col.findOne({ branchName: Object.keys(branch)[0] })
      .then(doc => {
        col.updateOne(
          { _id: doc._id },
          { $set: { branchOpeningTimes: branch[Object.keys(branch)[0]] } }
        );
      });
  });
}

csv
  .fromStream(stream)
  .transform(data => {
    const branch = data[0];
    const openingTime = data[1].replace(/\r/gm, '\r\n');
    return {
      [branch]: openingTime
    };
})
.on('data', data => {
    console.log('updating: ', data);
    update(data);

})
.on('end', () => {
    console.log("done");
    update(null);
});
