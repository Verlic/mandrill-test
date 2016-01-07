var mongoose = require('mongoose'),
  request = require('request'),
  mandrill = require('mandrill-api/mandrill'),
  reputationSchema = new mongoose.Schema({
    reputation: Number,
    hourly: Number
  }),
  Reputation = mongoose.models.Reputation,
  slackUrl,
  mandrillKey,
  timeout = 10,
  defaultTimeout = 10;

module.exports = function(context, callback) {
  var connectionString = context.data.MONGO_CONN,
    mandrillKey = context.data.MANDRILL_KEY,
    slackUrl = context.data.SLACK_URL;

  if (!connectionString) {
    return callback('Your connection string is not set. Please re-create your Webtask including your MongoDb connection.');
  }

  if (!mandrillKey) {
    return callback('Missing Mandrill API key. Please re-create your Webtask including your Mandrill key secret.');
  }

  checkDatabaseInitialized(context, callback);

  Reputation.find(function (err, data) {
    if (err) {
      console.log(err);
      return callback(err);
    }

    var reputation = data[0];
    var client = new mandrill.Mandrill(mandrillKey);
    client.users.info({}, function(result) {
      var mandrillReputation = {
        reputation: result.reputation,
        hourly: result.hourly_quota
      };

      console.log('Current reputation:', mandrillReputation);

      if (!reputation) {
        console.log('No reputation entries found in the database. Creating new...');
        reputation = new Reputation({
          reputation: mandrillReputation.reputation,
          hourly: mandrillReputation.hourly
        });
      }

      console.log('Previous reputation', reputation);

      // Compare DB and Mandrill reputations
      if (mandrillReputation.reputation < reputation.reputation || mandrillReputation.hourly < reputation.hourly) {
        console.log('WARNING: Reputation/Quota decreased from previous run');

        // Values changed. Notify alert
        notifySlack(slackUrl, reputation, mandrillReputation, function(err) {
          if (err) {
            return callback(err);
          }

          reputation.save(function(err) {
            return callback(err, { error: !!err, reputation: reputation });
          });
        });
      } else {
        console.log('Reputation/quota ok');
        reputation = new Reputation({
          reputation: mandrillReputation.reputation,
          hourly: mandrillReputation.hourly
        });

        reputation.save(function(err) {
          return callback(err, { error: !!err, reputation: reputation });
        });
      }
    }, function(err) {
      console.log(err);
      return callback(err);
    });
  });
}

function notifySlack(slackUrl, reputation, mandrillReputation, cb) {
  console.log('Notifying Slack...');
	var payload = {
		attachments: [{
      channel: '#release-mgmt',
			fallback: 'Mandrill reputation/quota decreased.',
			pretext: 'Mandrill reputation/quota decreased.',
			color: 'danger',
			fields: [{
        title: 'WARNING: Mandrill reputation/quota decreased',
        value: 'Reputation (prev/new): ' + reputation.reputation + '/' + mandrillReputation.reputation + '\nHourly quota (prev/new): ' + reputation.hourly + '/' + mandrillReputation.hourly,
        short: false
      }]
		}]
	};

	request.post({ url: slackUrl, form: { payload: JSON.stringify(payload) } }, function (error, response, body) {
		if (error) {
			return cb('Unable to send reply back to Slack.');
		}

		return cb();
	});
}

function checkDatabaseInitialized(context, callback) {
	if (!mongoose.connection.readyState) {
    console.log('Connecting...');
		mongoose.connect(context.data.MONGO_CONN);
    if (!Reputation) {
      Reputation = mongoose.model('Reputation', reputationSchema);
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
