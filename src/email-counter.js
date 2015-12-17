var mongoose = require('mongoose'),
  moment = require('moment'),
  emailSchema = new mongoose.Schema({
    to: String,
    region: String,
    sent: { type: Date }
  }),
  Email = mongoose.models.Email;

module.exports = function(context, callback) {
  var connectionString = context.data.MONGO_CONN;

  if (!connectionString) {
    return callback('Your connection string is not set. Please re-create your Webtask including your MongoDb connection.');
  }

  if (!context.data.to || !context.data.region) {
    return callback('Invalid parameters sent. Please check that parameters "to" and "region" are correctly set.');
  }

  checkDatabaseInitialized(context, callback);
  var entry = new Email({
    to: context.data.to,
    region: context.data.region,
    sent: moment().utc()
  });

  entry.save(function (err) {
    if (err) {
      console.error('Unable to save email entry.', err);
    }

    return callback(err, 'done');
  });
}

function checkDatabaseInitialized(context, callback) {
	if (!mongoose.connection.readyState) {
    console.log('Connecting...');
		mongoose.connect(context.data.MONGO_CONN);
    if (!Email) {
      Email = mongoose.model('Email', emailSchema);
    }

		this.db = mongoose.connection;
		this.db.on('error', function (err) {
			if (err) {
				callback(err);
			}

			return;
		});

		this.db.once('open', function (cb) {
			console.log('Database initialized.');
		});
	}
}
