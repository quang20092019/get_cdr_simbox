import datetime
import time
from datetime import datetime
import requests
from requests.auth import HTTPDigestAuth
from datetime import date, timedelta
import pymysql
import pandas as pd
from pandas import json_normalize
import re
import json
from datetime import datetime,date, timedelta
try :
    while True :
        try:
            now = datetime.now()
            print(now)
            f = open("time.cfg", "r")
            times = f.read()
            f.close()
            timenext = datetime.strptime(times, '%Y-%m-%d %H:%M:%S') +timedelta(minutes=5)
            print(timenext)
            if timenext < now:
                db = pymysql.connect(host="127.0.0.1",    # your host, usually localhost
                            user="simboxcdr",         # your username
                            passwd="KJAhsd09094940999",  # your password
                            db="simbox_cdr",
                            port=3306
                            )
                cursor = db.cursor()
                sqlurl ="SELECT * FROM simbox WHERE STATUS =1"
                dfurl = pd.read_sql(sqlurl,db)
                print(dfurl)
                url = 0
                while url < len(dfurl) :
                    print(dfurl.iloc[url,2])
                    i= 0
                    while i <=31 :
                        urls =str(dfurl.iloc[url,2])
                        payload = json.dumps({"port":[i],
                                "time_after":str(times),
                                "time_before":str(timenext)
                        })
                        headers = {'Content-Type': 'application/json'}
                        response = requests.request("POST", urls, headers=headers, data=payload,auth=HTTPDigestAuth(str(dfurl.iloc[url,3]),str(dfurl.iloc[url,4])))
                        response =response.json()
                        df = pd.json_normalize(response)
                        if str(df.iloc[0,0])=="200" :
                            df = pd.json_normalize(response["cdr"])
                            print(df)
                            insert = 0
                            while insert < len(df) :
                                if str(df.iloc[insert,4]) == "0":
                                    print("ko insert")
                                else :
                                    sqlinsert ="INSERT INTO simbox_log(simbox_id,port, number, start_date, answer_date, duration, source_number, destination_number, direction, ip, codec, hangup, gsm_code, bcch, reason) VALUES('"+str(dfurl.iloc[url,0])+"', '"+str(df.iloc[insert,0])+"', '"+str(df.iloc[insert,1])+"', '"+str(df.iloc[insert,2])+"', '"+str(df.iloc[insert,3])+"', '"+str(df.iloc[insert,4])+"', '"+str(df.iloc[insert,5])+"', '"+str(df.iloc[insert,6])+"', '"+str(df.iloc[insert,7])+"', '"+str(df.iloc[insert,8])+"', '"+str(df.iloc[insert,9])+"', '"+str(df.iloc[insert,10])+"', '"+str(df.iloc[insert,11])+"', '"+str(df.iloc[insert,12])+"', '"+str(df.iloc[insert,13])+"')"
                                    cursor.execute(sqlinsert)
                                insert = insert +1
                        else :
                            print("co loi")
                        i=i+1
                    url =url +1
                f = open("time.cfg", "w")
                f.write(str(timenext))
                f.close()
                db.commit()
                db.close()
            else : 
                print("chua den luc")
            time.sleep(1)
        except :
            print("co loi")
except :
    print("co loi")