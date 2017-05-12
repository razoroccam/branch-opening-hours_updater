const fs = require('fs');
const csv = require('fast-csv');
const MongoClient = require('mongodb').MongoClient,
      test = require('assert');

const url = '';
const stream = fs.createReadStream('hours.csv');

const update = branch => {
  MongoClient.connect(url, (err, db) => {
    const col = db.collection('branches');

    col.findOneAndUpdate(
      { branchName: Object.keys(branch)[0] },
      { $set: { branchOpeningTimes: branch[Object.keys(branch)[0]] } },
      { returnOriginal: false },
      (err, r) => {
        test.equal(null, err);
        db.close();
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
});
