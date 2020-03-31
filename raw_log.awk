#!/bin/awk

# This script is used to filter logs in separate files per day
# based on start_placement field in the logs. The resultant generated daily log files
# are saved in 'daily_directory'. Also geo attributes for the subscribers found in
# raw log files are saved in a single file in 'geo_directory'

# Example usage:
#
# gunzip -c /data/logs/raw/VODDAI_201412211621.txt.gz | awk -f script/raw_log.awk
#     -v timestamp="1421352000000" \
#     -v daily_directory="/data/logs/daily" \
#     -v daily_file_prefix="abc_logs_" \
#     -v geo_directory="/data/logs/geo" \
#     -v geo_file_prefix="abc_geo_" \
#     -v stats_log_file="/tmp/stats_log.txt" \
#     -v unknown_networks_file="/tmp/unknown_networks.txt" \
#     -v progress_file="/tmp/log_progress.txt" \
#     -v log_file_name="VODDAI_201412211621.txt.gz" \
#     target/networks.txt
#     target/network_mapping.txt
#     -

function printStats( label, map ) {
  flag = 0;
  for (item in map) {
    if (flag == 0) {
      print label ":" > stats_log_file
      flag = 1;
    }
    print "  " item ": " map[item] > stats_log_file
  }
  if (flag == 1) {
    print "" > stats_log_file
  }
}

BEGIN {
  FS = "|"
  OFS = "|"
  geo_file_path = geo_directory "/" geo_file_prefix timestamp ".txt"
  print "SUBSCRIBER_ID","MARKET_NAME","ZONE","STATE" > geo_file_path

  # This is defining the valid ad lengths.  If a different value is ever encountered it shall
  # be set to 0 length.  Unless it is length of 5, those are dropped completely.
  AD_LENGTHS[10] = 1
  AD_LENGTHS[15] = 1
  AD_LENGTHS[30] = 1
  AD_LENGTHS[60] = 1
  AD_LENGTHS[90] = 1
  AD_LENGTHS[120] = 1
}

FNR == 1 {
  FILE_NUM++
}

FILE_NUM == 1 && FNR > 1 {
    NETWORK = $1
    NETWORKS[NETWORK] = 1
}

FILE_NUM == 2 && FNR > 1 {
    CONTENT_ASSET_ID = $1
    NETWORK = $2
    NETWORK_MAP[CONTENT_ASSET_ID] = NETWORK
}

FILE_NUM == 3 && FNR > 1 {
    CONTENT_ASSET_ID = $1
    PROVIDER_ID = $2
    PROVIDER_NAME = $3
    if (PROVIDER_NAME == "") {
      PROVIDER_NAME = "UNKNOWN"
    }
    CAI_TO_PROV_ID[CONTENT_ASSET_ID] = PROVIDER_ID
    CAI_TO_PROV_NAME[CONTENT_ASSET_ID] = PROVIDER_NAME
}


FILE_NUM == 4 && FNR > 1 {
    UNIT_ID = $1                  # This field isn't used
    SUBSCRIBER_ID = $2
    # NETWORK_DESC = $3           # This field isn't used
    # NETWORK = toupper($3)       # Only use mapping file provider names for the NETWORK field
    PROGRAM_TITLE_NAME = $4
    TV_RATING = $5
    START_PLACEMENT = $6
    END_PLACEMENT = $7
    CONTENT_ASSET_ID = $8
    POSITION = $9
    IMPRESSION_TYPE = $10
    AD_LENGTH_SECS = $11
    SPOT_ASSET_ID = $12           # This field isn't used
    MARKET_NAME = $13
    ZONE = $14
    STATE = $15
    # Following fields are not used
    SESSION_ID = $16
    START_SESSION_LOCAL = $17
    END_SESSION_LOCAL = $18
    TRACKING_ID = $19
    VOD_PAID_ASSET = $20
    ADS_IDENTITY = $21

    # Lookup the provider id and name
    PROVIDER_ID = CAI_TO_PROV_ID[CONTENT_ASSET_ID]
    PROVIDER_NAME = CAI_TO_PROV_NAME[CONTENT_ASSET_ID]

    if (PROVIDER_NAME != "" &&  PROVIDER_NAME != "\\n") {
      if (PROVIDER_NAME == "UNKNOWN") {
          MISSING_CONTENT_ASSET_ID_COUNT++

          if (MISSING_CONTENT_ASSET_ID_ARRAY[CONTENT_ASSET_ID] == "") {
            MISSING_CONTENT_ASSET_ID_ARRAY[CONTENT_ASSET_ID] = 1
          }
          else {
            MISSING_CONTENT_ASSET_ID_ARRAY[CONTENT_ASSET_ID]++
          }

      }
      NETWORK = PROVIDER_NAME
      NUM_RECS_WITH_NETWORK_RESOLVED++
    }
    else {
      NUM_RECS_WITH_NETWORK_NOT_RESOLVED++
    }

    # Determine the ad length in seconds
    ad_length = 0;
    if (AD_LENGTH_SECS != "" && AD_LENGTH_SECS != "\\N") {
      match(AD_LENGTH_SECS, /([0-9]+)/, arr)
      ad_length = arr[1]
    }

    if (SUBSCRIBER_ID == "" || SUBSCRIBER_ID == "\\N") {
      SUBSCRIBER_ID = "0"
      EMPTY_SUBSCRIBERS++
    } else {
      SUBSCRIBER_ID = substr(SUBSCRIBER_ID, 3)
    }

    if (START_PLACEMENT != "" && START_PLACEMENT != "\\N") {
      processing_date = START_PLACEMENT
    } else if (START_SESSION_LOCAL != "" && START_SESSION_LOCAL != "\\N") {
      processing_date = START_SESSION_LOCAL
    } else if (END_PLACEMENT != "" && END_PLACEMENT != "\\N") {
      processing_date = END_PLACEMENT
    } else if (END_SESSION_LOCAL != "" && END_SESSION_LOCAL != "\\N") {
      processing_date = END_SESSION_LOCAL
    }

    # We can't process a record if the processing date is invalid
    # and we don't want 5 second ads in the resulting files
    if (processing_date != "" && ad_length != 5) {
      CONTENT_ASSET_ID = toupper(substr(CONTENT_ASSET_ID, 1, 4))

      if (NETWORK != "\\N" && NETWORK != "") {
        # If the raw log has a NETWORK that is not null or empty
        #    USE the Network always even if it is not in the list of valid networks (from the networks input file)
        #    If the NETWORK is NOT in in the list of valid networks, add it to a list of unknown networks to log
        if (NETWORKS[NETWORK] == "") {
          UNKNOWN_NETWORKS[NETWORK]++
          if(UNKNOWN_NETWORKS_DESC[NETWORK] == "") {
            UNKNOWN_NETWORKS_DESC[NETWORK] = NETWORK
          }
        }
      } else {

        NETWORK = "UNKNOWN"
        }
      FINAL_NETWORKS[NETWORK]++
      FINAL_CONTENT_ASSET_ID_NETWORKS[CONTENT_ASSET_ID "|" NETWORK]++

      if (POSITION == "pre-roll") {
          POSITION = 1;
      } else if (POSITION == "mid-roll") {
          POSITION = 2;
      } else if (POSITION == "post-roll") {
          POSITION = 3;
      } else {
          POSITION = 0;
      }

      if (IMPRESSION_TYPE == "Spotlight") {
          IMPRESSION_TYPE = 1;
      } else {
          IMPRESSION_TYPE = 0;
      }

      # We only care about ad lengths that are defined in the AD_LENGTH array
      # if we find one that is not defined, set it to 0
      if (ad_length != 0) {
        if (AD_LENGTHS[ad_length] == "") {
          ad_length = 0
          AD_LENGTH_UNKNOWN[AD_LENGTH_SECS]++
        }
      }
      FOUND_AD_LENGTHS[ad_length]++

      split(processing_date, datetime, " ")
      date = datetime[1]
      time = datetime[2]
      if (time < "05:00:00") {
          day = prev[date]
          if (day == "") {
              cmd = "date -d'" date "-1 day' +%Y%m%d"
              cmd | getline day
              prev[date] = day
              close(cmd)
          }
      } else {
          day = substr(processing_date,1,4) substr(processing_date,6,2) substr(processing_date,9,2)
      }
      output_file = daily_directory "/" daily_file_prefix day "_" timestamp ".txt"

      if (END_PLACEMENT == "\\N") {
          END_PLACEMENT = ""
      }
      print SUBSCRIBER_ID, NETWORK, PROGRAM_TITLE_NAME, TV_RATING, processing_date, END_PLACEMENT, POSITION, IMPRESSION_TYPE, ad_length > output_file
      IMPRESSION_COUNT++
    } else {
        if (ad_length == "5") {
          SKIPPED_5_SEC_ADS++
        }
        SKIPPED_IMPRESSIONS++
    }

    if (SUBSCRIBER_ID != "0") {
      print SUBSCRIBER_ID, MARKET_NAME, ZONE, STATE > geo_file_path
    }

    if (FNR % 500000 == 0) {
      print strftime("%Y-%m-%d %H:%M:%S") ": Reading file: " log_file_name " line: " FNR > progress_file
    }
}

END {
  print "IMPRESSION COUNT: " IMPRESSION_COUNT "\n" > stats_log_file

  print "SKIPPED IMPRESSIONS: " SKIPPED_IMPRESSIONS "\n" > stats_log_file

  print "EMPTY SUBSCRIBERS: " EMPTY_SUBSCRIBERS "\n" > stats_log_file

  print "SKIPPED 5 SEC ADS: " SKIPPED_5_SEC_ADS "\n" > stats_log_file

  print "NUM_RECS_WITH_NETWORK_RESOLVED: " NUM_RECS_WITH_NETWORK_RESOLVED "\n" > stats_log_file

  print "NUM_RECS_WITH_NETWORK_NOT_RESOLVED: " NUM_RECS_WITH_NETWORK_NOT_RESOLVED "\n" > stats_log_file

  print "MISSING_CONTENT_ASSET_ID_COUNT: " MISSING_CONTENT_ASSET_ID_COUNT "\n" > stats_log_file

  # printStats( "MISSING_CONTENT_ASSET_ID_ARRAY", MISSING_CONTENT_ASSET_ID_ARRAY )

  printStats( "NETWORKS", FINAL_NETWORKS )

  printStats( "UNKNOWN NETWORKS", UNKNOWN_NETWORKS )

  printStats( "CONTENT ASSET ID|NETWORKS", FINAL_CONTENT_ASSET_ID_NETWORKS )

  printStats( "AD LENGTHS", FOUND_AD_LENGTHS )

  printStats( "UNKNOWN CONTENT ASSET IDS", UNKNOWN_CONTENT_ASSET_IDS )

  printStats( "UNKNOWN AD LENGTH", AD_LENGTH_UNKNOWN )

  # Write unknown networks so that it can be read as cvs to be inserted in db
  flag = 0;
  for (item in UNKNOWN_NETWORKS) {
    if (flag == 0) {
      print "UNKNOWN_NETWORKS" > unknown_networks_file
      flag = 1;
    }
    print UNKNOWN_NETWORKS_DESC[item] > unknown_networks_file
  }
}
