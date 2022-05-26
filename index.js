//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Listen to mqtt topic db_updates
// Update the db based on the data sent
//---------------------------------------------------------------------------
// Modifications
//===========================================================================
// 11-Jan-2022 mike
//  Added exit when we get an sql error, Error usually caused by sql connection
//  timeout, so exit and it will restart and reconnect... Simples
//---------------------------------------------------------------------------

const config = require("config")
const mysql = require('mysql')
const mqtt = require("mqtt");
const uuid = require('uuid');
const npEscape = require('mysql-named-params-escape');

const cli_args = require('commander');
cli_args.addHelpText('after', `
Here is what the config file should like like in ./config/default.toml

[sql]
username = ""
password = ""
database = ""
host = ""

[report]
logger_name = ""
log_file = ""

`);

require('./lib/logit.js')

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Update the song year
//---------------------------------------------------------------------------
function update_year(msg, db) {

  sql = npEscape('update songs set year = :year where id = :id', {year: msg.year, id: msg.id});
  db.query(sql, (error, results) => {
    if (error) {
      logger.info(ln() + error)
    }
  })
}
//---------------------------------------------------------------------------

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Update songs_extra.retire_until
//---------------------------------------------------------------------------
function update_retire_until(msg, db) {

  if (msg.retire_until == '') {
    sql = npEscape('update songs_extra set retire_until = null where song_id = :id', {retire_until: msg.retire_until, id: msg.id});
  } else
  {
    sql = npEscape('update songs_extra set retire_until = :retire_until where song_id = :id', {retire_until: msg.retire_until, id: msg.id});
  }
  db.query(sql, (error, results) => {
    if (error) {
      logger.info(ln() + error)
      process.exit(1)
    } else
    {
    if (results.changedRows == 0 && msg.retire_until != '') {
      sql = npEscape('insert into songs_extra (song_id, retire_until) values (:song_id, :retire_until)', {song_id: msg.id, retire_until: msg.retire_until})
      db.query(sql, (error, results) => {
        if (error) {
          logger.info(ln() + error)
        }
      })
    };
    }
  })

  logger.info(ln() + sql)
}
//---------------------------------------------------------------------------

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Year made number 1
//---------------------------------------------------------------------------
function update_year_made_number_1(msg, db) {

  if (msg.year == '') {
    sql = npEscape('update songs_extra set year_made_number_1 = null where song_id = :id', {id: msg.id});
  } else
    sql = npEscape('update songs_extra set year_made_number_1 = :year_made_number_1 where song_id = :id', {year_made_number_1: msg.year, id: msg.id});
  {
  }
  db.query(sql, (error, results) => {
    if (error) {
      logger.info(ln() + error)
    } else
    {
      if (results.changedRows == 0 && msg.year != '') {
        sql = npEscape('insert into songs_extra (song_id, year_made_number_1) values (:song_id, :year)', {song_id: msg.id, year: msg.year})
        db.query(sql, (error, results) => {
          if (error) {
            logger.info(ln() + error)
          }
        })
      };
    }
  })
}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Update song_type
//---------------------------------------------------------------------------
function update_song_type(msg, db) {

  sql = npEscape('select id from song_type where name = :name', {name: msg.song_type})
  db.query(sql, (error, rows) => {
    if (error) {
      logger.info(ln() + error)
    } else
    {
      song_type = rows[0].id
      sql = npEscape('update songs set song_type = :song_type where id = :id', {song_type: song_type, id: msg.id})
      db.query(sql, (error, results) => {
        if (error) {
          logger.info(ln() + error)
        }
      })
    }
  })
}
//---------------------------------------------------------------------------

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Update christmas_number_1
//---------------------------------------------------------------------------
function update_christmas_number_1(msg, db) {
  if (msg.christmas_number_1 == 'true') {
    msg.christmas_number_1 = 1
  } else {
    msg.christmas_number_1 = 0
  }
  sql = 'update songs_extra set christmas_number_1 = ? where song_id = ?'
  db.config.queryFormat = undefined;
  sql = db.format(sql, [msg.christmas_number_1, msg.id])
  db.query(sql, (error, results) => {
    if (error) {
      logger.info(ln() + error)
    }
  })
db.config.queryFormat = npEscape;
}
//---------------------------------------------------------------------------

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Update subcategory
//---------------------------------------------------------------------------
function update_subcategory(msg, db) {
  sql = npEscape('update songs set id_subcat = :subcat_id where id = :id', {subcat_id: msg.subcategory, id: msg.id})
  db.query(sql, (error, results) => {
    if (error) {
      logger.info(ln() + error)
    }
  })

}
//---------------------------------------------------------------------------

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Update enabled
//---------------------------------------------------------------------------
function update_enabled(msg, db) {
logger.info(ln() + msg.enabled);
  if (msg.enabled == 'true') {
    sql = npEscape('update songs set enabled = 1 where id = :id', {id: msg.id})
  } else {
    sql = npEscape('update songs set enabled = 0 where id = :id', {id: msg.id})
  }
  logger.info(ln() + sql);
  db.query(sql, (error, results) => {
    if (error) {
      logger.info(ln() + error)
    }
  })

}

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Connect to mosquitto
//---------------------------------------------------------------------------
function connect_to_mosquitto(config, db) {

logger.info(ln()+"Connecting to mosquitto");
mosquitto = mqtt.connect(config.get("mqtt.host"), {clientId:"np_feed_"+uuid.v4(), username:config.get("mqtt.username"), password:config.get("mqtt.password")})

mosquitto.on("message", (topic, raw, packet) => {
msg = JSON.parse(raw);
if (msg.type == 'year') {
  update_year(msg, db)
}
if (msg.type == 'retire_until') {
  update_retire_until(msg, db)
}
if (msg.type == 'year_made_number_1') {
update_year_made_number_1(msg, db)
}

if (msg.type == 'song_type') {
  update_song_type(msg, db)
}

if (msg.type == 'christmas_number_1') {
  update_christmas_number_1(msg, db)
}

if (msg.type == 'subcategory') {
  update_subcategory(msg, db)
}

if (msg.type == 'enabled') {
  update_enabled(msg, db)
}

});

mosquitto.on("connect", () => {
  logger.info(ln()+"Connected to mosquitto");
  logger.info(ln()+"Subscribing to db_update")
  mosquitto.subscribe("db_update");
});

mosquitto.on("subscribed", () => {
  logger.info(ln()+"Subscribed to db_update")
})

mosquitto.on("error", (error) => {
  logger.error(ln()+" (mqtt) "+error);
});

return mosquitto;
}
//---------------------------------------------------------------------------

//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//MAIN main Main
//---------------------------------------------------------------------------

cli_args
  .option('--log-to-screen', 'Log output to console')
  .option('--log-file <name>', 'Log filname and path', config.get("report.log_file"))
  .addOption(new cli_args.Option('--log-level <type>', 'Set logging level', 'error').choices(['error', 'verbose', 'info']));

cli_args.parse(process.argv);

logger = create_logger(cli_args);
logger.info(ln()+"Hello World!!")

const db = mysql.createPool({
  host : config.get("sql.host"),
  user : config.get("sql.username"),
  password : config.get("sql.password"),
  database : config.get("sql.database"),
})

//db.connect((err) => {
//  if (err) {
//    throw(err);
//  }
//});

db.config.queryFormat = npEscape;

mosquitto = connect_to_mosquitto(config, db);
