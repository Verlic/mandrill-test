var mongoose = require('mongoose'),
  moment = require('moment'),
  request = require('request'),
  mandrill = require('mandrill-api/mandrill'),
  async = require('async'),
  slackUrl,
  mandrillKey,
  emailSchema = new mongoose.Schema({
    to: String,
    region: String,
    sent: { type: Date, default: moment().utc() }
  }),
  Email = mongoose.models.Email,
  timeout = 10;

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

module.exports = function(context, callback) {
  var connectionString = context.data.MONGO_CONN,
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
      console.log('Notifying Slack...');
      notifySlack(result, entries.options, function(err, status) {
        return callback(err, status);
      });
    });

    // callback(err, entries);
  });
}

function compareEntries(entries, hour, cb, secondPass) {
  console.log(2);
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
      return cb(1);
    }

    newHour = hour == 0 ? 23 : hour--;
    return compareEntries(entries, newHour, cb, true);
  }

  if (mandrillCount === emailCount) {
    // Success
    return cb(1);
  }

  if (mandrillCount < emailCount) {
    // check last email duration. If the time difference is lesser than the timeout value, return success
    var difference = Math.abs(moment(entries.options.lastSent).diff(moment().utc(), 'minutes'));
    console.log('Warning: Mandrill count less than emails registered in the database');
    console.log('Time difference between the last time we check emails and current time:', difference);
    return cb(difference <= timeout);
  }

  // Mandrill count is greater than the email count. Add entries in the db to match the same number for future checks.
  var saveEntries = [];
  for (var i = 0; i < mandrillCount - emailCount; i++) {
    var entry = new Email({
      to: entries.options.to,
      region: entries.options.region,
      sent: moment().utc()
    });

    saveEntries.push(function(callback) { console.log('Saving entry...'); entry.save(); callback(); });
  }

  async.parallel(saveEntries, function() {
    // Entries saved. Return success
    return cb(1);
  });
}

function aggregateEmails(options, cb) {
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

function notifySlack(status, options, cb) {
  var color = status ? 'good' : 'danger',
    testStatus = status ? 'Success' : 'Failed',
    brief = 'Mandrill test run completed.',
    title = 'Mandrill API (' + options.region.toUpperCase() + ') status: ' + testStatus,
    body = status ? 'Test successful for region "' + options.region.toUpperCase() + '"' : 'Email not sent after ' + timeout + ' minutes.';

	var payload = {
		attachments: [{
      channel: '#release-mgmt',
			fallback: brief,
			pretext: brief,
			color: color,
			fields: [{ title: title, value: body, short: false }]
		}]
	};

  console.log('Slack payload', payload);
	request.post({ url: slackUrl, form: { payload: JSON.stringify(payload) } }, function (error, response, body) {
		if (error) {
			cb('Unable to send reply back to Slack.');
		}

		cb(null, 'done');
	});
}
