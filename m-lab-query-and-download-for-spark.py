# python3

import datetime
import calendar

import pandas as pd
from pandas.io import gbq

from mlabnetdb import mlabnetdb

# This function takes as input the metric, mlab_location, AS number, 
# start_time, end_time and the optional country (the default 
# country is set to US)
# Check out the MLabServers.csv file to look up possible values for the
# mlab_location and AS variables.  The mlab_location should be entered using 
# quotation marks, the AS should be entered as an integer.
# The choices for the metric are: "dtp", "rtt", and "prt" for download 
# throughput, round trip time and packet retransmission respectively
# The start_time, end_time info should be entered in the 'mm/dd/yy' format
# The output of the function, when successful, is a text file, called
# query.txt

def query_writer(start_time, end_time, country = 'US', limit=-1): 
      
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


    basic_query = ("SELECT "
        "\nweb100_log_entry.log_time AS log_time, "
        "\nconnection_spec.client_geolocation.city  AS client_city, "
        "\nconnection_spec.client_geolocation.area_code As client_area_code, "
        "\nweb100_log_entry.connection_spec.remote_ip AS client_ip, "
        "\nweb100_log_entry.connection_spec.local_ip AS MLab_ip, "
        "\n8 * (web100_log_entry.snap.HCThruOctetsAcked / "
        "\n(web100_log_entry.snap.SndLimTimeRwin + "
        "\nweb100_log_entry.snap.SndLimTimeCwnd + "
        "\nweb100_log_entry.snap.SndLimTimeSnd)) AS download_Mbps "
        "\nFROM "
        "\n[plx.google:m_lab.ndt.all] "
        "\nWHERE "
        "\nIS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.remote_ip) "
        "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.connection_spec.local_ip) "
        "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.HCThruOctetsAcked) "
        "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeRwin) "
        "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeCwnd) "
        "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.snap.SndLimTimeSnd) "
        "\nAND project = 0 "
        "\nAND IS_EXPLICITLY_DEFINED(connection_spec.data_direction) "
        "\nAND connection_spec.data_direction = 1 "
        "\nAND IS_EXPLICITLY_DEFINED(web100_log_entry.is_last_entry) "
        "\nAND web100_log_entry.is_last_entry = True "
        "\nAND web100_log_entry.snap.HCThruOctetsAcked >= 8192 "
        "\nAND (web100_log_entry.snap.SndLimTimeRwin + "
        "\nweb100_log_entry.snap.SndLimTimeCwnd + "
        "\nweb100_log_entry.snap.SndLimTimeSnd) >= 9000000 "
        "\nAND (web100_log_entry.snap.SndLimTimeRwin + "
        "\nweb100_log_entry.snap.SndLimTimeCwnd +   "
        "\nweb100_log_entry.snap.SndLimTimeSnd) < 3600000000 "
        "\nAND (web100_log_entry.snap.State == 1 "
        "\nOR (web100_log_entry.snap.State >= 5 "
        "\nAND web100_log_entry.snap.State <= 11))")


    

    #CONVERTING DATE TO UNIX TIMESTAMP
    try:
        dt = datetime.datetime.strptime(start_time, "%m/%d/%y")
        start_time_unix = calendar.timegm(dt.timetuple())
    except:
        print("The start_time entered is invalid!")
        return

    try:
        dt = datetime.datetime.strptime(end_time, "%m/%d/%y")
        end_time_unix = calendar.timegm(dt.timetuple())
    except:
        print("The end_time entered is invalid!")
        return

    #WRITING THE TIME CONDITION
    tstamp_var = "web100_log_entry.log_time"
    tframe_cond = ("\nAND " + tstamp_var + "<=" + "%d"%(end_time_unix) +
        "\nAND " + tstamp_var + ">=" + "%d"%(start_time_unix))
    #print(tframe_cond)

    #WRITING THE COUNTRY CONDITION
    country_cond = ''
    if country:
        country_string = "'" + country + "'" 
        country_var = "connection_spec.client_geolocation.country_code"
        country_cond = "\nAND " + country_var + "==" + country_string
        #print(country_cond)

    #LIMIT
    limit_cond = ''
    if limit >= 0:
        limit_cond = "\nLIMIT " + str(int(limit))


    #WRITING THE QUERY
    the_query = basic_query + country_cond + tframe_cond + limit_cond
    #with open("querypy.txt", "w") as text_file:
    #    text_file.write(the_query)

    return the_query



def acquire_mlab_data(project_id, start_time, end_time, country = 'US'):

    # generate the query
    querystring = query_writer(start_time, end_time, country)

    # read the query output into a pandas dataframe
    #   NOTE: the first time this runs, you will be prompted for an authorization key. 
    #    Click on the link provided, get the key string, paste it in, and go.
    df = gbq.read_gbq(querystring, project_id=project_id)

    # use mlabnetdb to get ISP names
    print("\ngetting ISP names.....")
    owner = []
    ispname = []
    asn = []
    for ip in df.client_ip:
        ipinfo = mlabnetdb.lookup(ip, date=None)
        if ipinfo:
            owner.append(ipinfo['autonomous_system_organization'])
            asn.append(ipinfo['autonomous_system_number'])
            ispname.append(ipinfo['isp'])
        else:
            print("error: for ip %s, ipinfo==None"%(ip))
            owner.append('')
            asn.append(0)
            ispname.append('')
    print("\n  DONE getting ISP names")

    # add IP_owner and IP_ASN columns to the dataframe
    df["IP_owner"] = owner
    df["IP_ASN"] = asn
    # get company name from owner string
    df["ISP_name"] = ispname
    
    return df

#############################################################################


the_query = query_writer("06/15/14", "05/13/15", limit=999)
print(the_query)


project_id = 'mlab-185523'
df = acquire_mlab_data(project_id, "01/01/13", "02/01/13")


from fastparquet import write
write('mlab-test-data-0.parquet', df)
