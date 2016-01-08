var mongoose = require('mongoose'),
  moment = require('moment'),
  request = require('request'),
  mandrill = require('mandrill-api/mandrill'),
  async = require('async'),
  emailSchema = new mongoose.Schema({
    to: String,
    region: String,
    sent: { type: Date, default: moment().utc() }
  }),
  Email = mongoose.models.Email,
  slackUrl,
  mandrillKey,
  timeout = 10,
  defaultTimeout = 10;

module.exports = function(context, callback) {
  var connectionString = context.data.MONGO_CONN,
    timeout = context.data.TIMEOUT || defaultTimeout,
    to = context.data.to,
    region = context.data.region,
    fromDate = moment().utc().startOf('day'),
    toDate = moment().utc().endOf('day');

  mandrillKey = context.data.MANDRILL_KEY;
  slackUrl = context.data.SLACK_URL;

  if (!connectionString) {
    return callback('Your connection string is not set. Please re-create your Webtask including your MongoDb connection.');
  }

  if (!mandrillKey) {
    return callback('Missing Mandrill API key. Please re-create your Webtask including your Mandrill key secret.');
  }

  if (!to || !region) {
    return callback('Invalid parameters sent. Please check that parameters "to" and "region" are correctly set.');
  }

  checkDatabaseInitialized(context, callback);
  var aggregateOptions = { to: to, region: region, fromDate: fromDate, toDate: toDate};

  // Aggregate email entries by hour
  aggregateEmails(aggregateOptions, function(err, entries) {
    if (err) {
      console.error(err);
      return callback(err);
    }

    var lastHour;
    if (entries.mandrill.length === 0 && entries.wt.length === 0) {
      // No entries. Nothing to check here.
      return callback(null, 'Success');
    }

    console.log('Entries to compare', entries);
    var lastHour = entries.mandrill.length === 0 ? entries.wt[0].hour : entries.mandrill[0].hour;
    compareEntries(entries, lastHour, function(result) {
      notifySlack(result, entries.options, function(err, status) {
        return callback(err, status);
      });
    });
  });
}

function compareEntries(entries, hour, cb, secondPass) {
  var mandrillCount = entries.mandrill.filter(function(item) { return item.hour === hour; })[0] || 0;
  var emailCount = entries.wt.filter(function(item) { return item.hour === hour; })[0] || 0;

  mandrillCount = mandrillCount !== 0 ? mandrillCount.count : 0;
  emailCount = emailCount !== 0 ? emailCount.count : 0;

  console.log('Comparing entries...');
  console.log('Compare emails at hour:', hour);
  console.log('Mandrill count:', mandrillCount);
  console.log('Db count:', emailCount);
  console.log('Last sent:', entries.options.lastSent);

  if (mandrillCount == 0 && emailCount == 0) {
    if (secondPass) {
      // No emails were sent in two consecutive hours. Passed.
      return cb({
        success: true,
        mandrillCount: 0,
        emailCount: 0
      });
    }

    newHour = hour == 0 ? 23 : hour--;
    return compareEntries(entries, newHour, cb, true);
  }

  if (mandrillCount === emailCount) {
    // Success
    return cb({
      success: true,
      mandrillCount: mandrillCount,
      emailCount: emailCount
    });
  }

  if (mandrillCount < emailCount) {
    // check last email duration. If the time difference is lesser than the timeout value, return success
    var difference = Math.abs(moment(entries.options.lastSent).diff(moment().utc(), 'minutes'));
    console.log('Warning: Mandrill count less than emails registered in the database');
    console.log('Time difference between the last time we check emails and current time:', difference);
    return cb({
      success: difference <= timeout,
      lastSent: entries.options.lastSent,
      difference: difference,
      mandrillCount: mandrillCount,
      emailCount: emailCount
    });
  }

  return cb({
    success: true,
    mandrillCount: mandrillCount,
    emailCount: emailCount
  });
}

function aggregateEmails(options, cb) {
  if (!Email) {
    Email = mongoose.model('Email', emailSchema);
  }

  Email.aggregate([
    {
      $match: { $and: [
          { sent: { $gte: new Date(options.fromDate), $lte: new Date(options.toDate) }},
          { to: options.to },
          { region: options.region }
        ]
      }
    },
    { $sort: { sent: -1 } },
    { $group: { _id: { hour: { $hour: '$sent'}}, count: { $sum: 1 } }}
  ], function(err, wtEntries) {
    if (err) {
      console.error('Unable to retrieve email entries from database', err);
    }

    // Get email entries from Mandrill
    options.wtEntries = wtEntries.map(function(entry) {
      return {
        hour: entry._id.hour,
        count: entry.count
      };
    });

    Email.find({ sent: { $gte: new Date(options.fromDate), $lte: new Date(options.toDate) }, to: options.to, region: options.region })
      .sort({ sent: -1 })
      .exec(function(err, res) {
        console.log('Email found', res);
        if (err) {
          console.log(err);
        }

        if (res.length !== 0) {
          options.lastSent = res[0].sent;
        }

        aggregateMandrillEntries(options, cb);
      });
  });
}

function aggregateMandrillEntries(options, cb) {
  var client = new mandrill.Mandrill(mandrillKey),
    searchQuery = {'query': 'full_email:' + options.to, 'date_from': options.fromDate, 'date_to': options.toDate};

  client.messages.searchTimeSeries(searchQuery, function(result) {
    result.reverse();
    var mandrillEntries = result.map(function(entry) {
      return {
        hour: moment(entry.time).hour(),
        count: entry.sent
      };
    });

    return cb(null, { wt: options.wtEntries, mandrill: mandrillEntries, options: options });
  }, function(err) {
    console.log(err);
    return cb(err, { wt: options.wtEntries, mandrill: [] });
  });
}

function notifySlack(result, options, cb) {
  // Ignore notification if the result is Sucess or no slack webhook found
  if (result.success || !slackUrl) {
    return cb(null, result);
  }

  console.log('Notifying Slack...');
	var payload = {
		attachments: [{
      channel: '#release-mgmt',
			fallback: 'Mandrill test run completed.',
			pretext: 'Mandrill test run completed.',
			color: 'danger',
			fields: [{
        title: 'Mandrill API (' + options.region.toUpperCase() + ') status: Failed',
        value: 'Email not sent after ' + timeout + ' minutes.\nEmail count: ' + result.emailCount + '\nMandrill count: ' + result.mandrillCount,
        short: false
      }]
		}]
	};

	request.post({ url: slackUrl, form: { payload: JSON.stringify(payload) } }, function (error, response, body) {
		if (error) {
			cb('Unable to send reply back to Slack.');
		}

		cb(null, result);
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
