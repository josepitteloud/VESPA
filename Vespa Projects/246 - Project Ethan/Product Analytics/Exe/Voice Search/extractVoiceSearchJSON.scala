/*
spark-shell \
  --packages \
    com.databricks:spark-csv_2.10:1.4.0
*/


// --------------------------------------------------------------------
// Initialise
// --------------------------------------------------------------------
println("Load libraries and set logging level...")

import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.Calendar

Logger.getLogger("org").setLevel(Level.FATAL)
Logger.getLogger("akka").setLevel(Level.FATAL)


// --------------------------------------------------------------------
// Get target processing date (yesterday)
// --------------------------------------------------------------------
println("Set processing date...")

val cal = Calendar.getInstance()
cal.add(Calendar.DATE,-1)

val yyyy = cal.get(Calendar.YEAR)

val mm = if(cal.get(Calendar.MONTH) + 1 < 10){
    "0" + (cal.get(Calendar.MONTH) + 1)
    } else {
        cal.get(Calendar.MONTH) + 1
    }

val dd = if(cal.get(Calendar.DAY_OF_MONTH) < 10){
    "0" + cal.get(Calendar.DAY_OF_MONTH)
    } else {
        cal.get(Calendar.DAY_OF_MONTH)
    }


// Manually set processing for if required
// val yyyy = "2017"
// val mm = "02"
// val dd = "*"


// --------------------------------------------------------------------
// Define file path(s)
// --------------------------------------------------------------------
println("Define target data directory...")
val path = "/data/raw/ethan/stb-pa/year=" + yyyy + "/month=" + mm + "/day=" + dd + "/hr=*/*.txt"

println(path)


// --------------------------------------------------------------------
// Read JSON file into DataFrame
// --------------------------------------------------------------------
println("Read JSON logs into data frame...")
val df = sqlContext.read.json(path)


// --------------------------------------------------------------------
// Filter for Voice Search actions and flatten
// --------------------------------------------------------------------
println("Filter for Voice Search actions and flatten...")
val vs = 
  df.
    select(
      $"source",  // already a struct
      explode($"eventLog").as("eventLog_flat")  // explode the eventlog array
      ).
    filter($"eventLog_flat.trigger.input" === "Voice-Search").  // filter for Voice Search events only
    select(
      $"source.serialNumber".as("source_serialNumber"),
      $"eventLog_flat.action.asrConfidenceLevel".as("eventLog_action_asrConfidenceLevel"),
      $"eventLog_flat.action.error_msg".as("eventLog_action_error_msg"),
      $"eventLog_flat.action.id".as("eventLog_action_id"),
      $"eventLog_flat.action.oldQuery".as("eventLog_action_oldQuery"),
      $"eventLog_flat.action.query".as("eventLog_action_query"),
      $"eventLog_flat.action.suggestions".as("eventLog_action_suggestions"),
      $"eventLog_flat.ref.id".as("eventLog_ref_id"),
      $"eventLog_flat.timems".as("eventLog_timems"),
      $"eventLog_flat.trigger.id".as("eventLog_trigger_id"),
      $"eventLog_flat.trigger.input".as("eventLog_trigger_input"),
      $"eventLog_flat.trigger.remote.batterylevel".as("eventLog_trigger_remote_batterylevel"),
      $"eventLog_flat.trigger.remote.conntype".as("eventLog_trigger_remote_conntype"),
      $"eventLog_flat.trigger.remote.deviceid".as("eventLog_trigger_remote_deviceid"),
      $"eventLog_flat.trigger.remote.hwrev".as("eventLog_trigger_remote_hwrev"),
      $"eventLog_flat.trigger.remote.make".as("eventLog_trigger_remote_make"),
      $"eventLog_flat.trigger.remote.model".as("eventLog_trigger_remote_model"),
      $"eventLog_flat.trigger.remote.name".as("eventLog_trigger_remote_name"),
      $"eventLog_flat.trigger.remote.swrev".as("eventLog_trigger_remote_swrev")
      )


// --------------------------------------------------------------------
// Register as table for Spark SQL manipulation
// --------------------------------------------------------------------
println("Register temp table...")
vs.registerTempTable("vs")


// --------------------------------------------------------------------
// Add audit timestamp
// --------------------------------------------------------------------
println("Append audit timestamp...")
val vs_timestamped = 
  sql("""
        SELECT
                *
            ,   current_timestamp()     AS  auditTimestamp
        FROM    vs
    """)


// --------------------------------------------------------------------
// Write to hdfs
// --------------------------------------------------------------------
println("Write Voice Search data to hdfs...")
vs_timestamped.
  write.
  mode("overwrite").
  format("com.databricks.spark.csv").
  option("header", "false").
  option("delimiter","\\").
  save("/user/tanghoiy/PA_tactical")




// --------------------------------------------------------------------
// Finish and quit
// --------------------------------------------------------------------
println("Done.")

exit()


/* sqoop command for export into Netezza - ensure target table name is correct first!
sqoop export \
--connect jdbc:netezza://10.137.15.3/ETHAN_PA_PROD \
--username BD_SMI \
--password B1gData3DM \
--direct \
--export-dir /user/tanghoiy/PA_tactical \
--table PA_VOICE_SEARCH_RAW \
--num-mappers 8 \
--input-fields-terminated-by '\\' \
-- --nz-maxerrors 1
*/



