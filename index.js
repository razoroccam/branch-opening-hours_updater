const fs = require('fs');
const csv = require('fast-csv');
const MongoClient = require('mongodb').MongoClient;
const ObjectId = require('mongodb').ObjectId;

const url = '';
const stream = fs.createReadStream('hours.csv');

const update = branch => {
  MongoClient.connect(url, (err, db) => {
    const col = db.collection('branches');

    try {
      col.findOne({ branchName: Object.keys(branch)[0].trim() })
        .then(doc => {
          console.log(`updating: ${ doc.branchName } with ${ branch[Object.keys(branch)[0]] }`);
          console.log('');
          col.updateOne(
            { _id: doc._id, branchName: Object.keys(branch)[0].trim() },
            { $set: { branchOpeningTimes: branch[Object.keys(branch)[0]] } }
          );
        });
    } catch(e) {
      db.close();
      process.exit(1);
    }
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
    update(data);
})
.on('end', () => {
    console.log("done");
    setTimeout(() => update(null), 1000)
});
