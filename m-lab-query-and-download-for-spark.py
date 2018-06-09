#!/usr/bin/python3

import os.path, sys
from datetime import datetime, timedelta
import calendar

import fastparquet
import pandas as pd
from pandas.io import gbq

from mlabnetdb import mlabnetdb

# script to download view from measurement-lab.release.ndt_all table, month at a time
#
# usage: script.py YYYY MM
# 
# Will write output to file named mlab_v1_YYYY_MM.parquet.
# Queries for US; change line in sql if you want something different.

project_id = 'mlab-185523'
max_limit = 1e8
ndt_table = 'measurement-lab.release.ndt_all'

def query_writer(start_time, end_time): 
      
    #DEFINING THE BASIC QUERIES FOR EACH METRIC

    #The basic query for download throughput
    #    "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CongSignals) "
    #    "\nAND web100_log_entry.snap.CongSignals > 0 "

    #The basic query for finding round trip time 
    #    "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.MinRTT) "
    #    "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.CountRTT) "
    #    "\nAND web100_log_entry.snap.CountRTT > 10 "

    #The basic query for packet retransmission 
    #    "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SegsRetrans) "
    #    "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.DataSegsOut) "
    #    "\nAND web100_log_entry.snap.DataSegsOut > 0 "


    basic_query = """
SELECT
  web100_log_entry.log_time AS log_time,
  FORMAT_DATETIME("%Y%m", PARSE_DATETIME("%s", CAST(web100_log_entry.log_time AS STRING))) AS yyyymm_partition,
  connection_spec.client_geolocation.city AS client_city,
  connection_spec.client_geolocation.area_code AS client_area_code,
  web100_log_entry.connection_spec.remote_ip AS client_ip,
  web100_log_entry.connection_spec.local_ip AS MLab_ip,
  8 * (web100_log_entry.snap.HCThruOctetsAcked / (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd)) AS download_Mbps
FROM
  `{}`
WHERE
  web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeRwin IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeCwnd IS NOT NULL
  AND web100_log_entry.snap.SndLimTimeSnd IS NOT NULL
  AND project = 0
  AND connection_spec.data_direction IS NOT NULL
  AND connection_spec.data_direction = 1
  AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
  AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) >= 9000000
  AND (web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd) < 3600000000
  AND (web100_log_entry.snap.State = 1
    OR (web100_log_entry.snap.State >= 5
      AND web100_log_entry.snap.State <= 11))
  AND connection_spec.client_geolocation.country_code='US'
"""

    basic_query = basic_query.format(ndt_table)

    #CONVERTING DATE TO UNIX TIMESTAMP
    start_time_unix = calendar.timegm(start_time.timetuple())
    end_time_unix = calendar.timegm(end_time.timetuple())

    #WRITING THE TIME CONDITION
    tstamp_var = "web100_log_entry.log_time"
    tframe_cond = ("\nAND " + tstamp_var + "<=" + "%d"%(end_time_unix) +
        "\nAND " + tstamp_var + ">=" + "%d"%(start_time_unix))

    #LIMIT
    limit_cond = "\nLIMIT " + str(int(max_limit))

    return basic_query + tframe_cond + limit_cond



def acquire_mlab_data(project_id, start_time, end_time):

    # generate the query
    querystring = query_writer(start_time, end_time)

    print("starting query.....")
    # read the query output into a pandas dataframe
    #   NOTE: the first time this runs, you will be prompted for an authorization key. 
    #    Click on the link provided, get the key string, paste it in, and go.
    df = gbq.read_gbq(querystring, project_id=project_id, verbose=True, dialect='standard')
    print("read", df.shape[0], "records")

    # use mlabnetdb to get ISP names
    print("getting ISP names.....")
    owner = []
    ispname = []
    asn = []
    for ip in df.client_ip:
        ipinfo = None
        try:
            ipinfo = mlabnetdb.lookup(ip, date=None)
        except Exception as e:
            print("error for ip {}: {}".format(ip, e))

        if ipinfo:
            owner.append(ipinfo['autonomous_system_organization'])
            asn.append(ipinfo['autonomous_system_number'])
            ispname.append(ipinfo['isp'])
        else:
            print("error: for ip %s, ipinfo==None"%(ip))
            owner.append('')
            asn.append(0)
            ispname.append('')


    # add IP_owner and IP_ASN columns to the dataframe
    df["IP_owner"] = owner
    df["IP_ASN"] = asn
    # get company name from owner string
    df["ISP_name"] = ispname
    
    return df

def query_and_append(t_from, t_to, filename):

    print('starting query for ', t1x,'-',t2x)

    df = acquire_mlab_data(project_id, t_from, t_to)

    print("saving results to {}.....".format(output_filename))
    append = os.path.isfile(filename)
    fastparquet.write(output_filename, df, append=append)

#############################################################################

if len(sys.argv) < 3:
    import os.path
    script = os.path.basename(__file__)
    print("usage:\n", script, "yyyy mm")
    print("\nqueries month's worth of data from '"+ndt_table+"' starting\nfrom first day in yyyy/mm.  Saves to mlab_view_yyyy_mm.parquet")
    sys.exit(0)

ts = datetime.now()

year = int(sys.argv[1])
month = int(sys.argv[2])

output_filename = 'mlab_v1_{}_{}.parquet'.format(year, month)

t1 = datetime(year, month, 1)
month += 1
if month == 13:
    year += 1
    month = 1
t2 = datetime(year, month, 1)

# break up query into N parts, otherwise pandas dataframe is too much for memory
N = 4
days_per_query = int((t2 - t1).days / N) + 1
t1x = t1
for _ in range(N):
    t2x = t1x + timedelta(days=days_per_query)
    if t2x > t2:
        t2x = t2
    query_and_append(t1x, t2x, output_filename)
    t1x = t2x

te = datetime.now()
print('completed queries in', (te - ts))
