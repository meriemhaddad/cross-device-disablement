#!/usr/bin/env python
# coding: utf-8

# In[42]:


import os
import pandas as pd
from ands.query_utils import QueryManager
import numpy as np
from datetime import datetime, date, timedelta
from send_pretty_email import send_pretty_email
from send_pretty_email import custom_body
import matplotlib.pyplot as plt
from sqlalchemy.engine import create_engine
import requests
import getpass
import json
import uuid
from IPython.display import Image, display_javascript, display_html, display, HTML
rs = requests.Session()
# ------------------------------------------#
# (1) Database Connections                  #
# ------------------------------------------#


def pull_from_mysql(query):
    df = qm.mysql_query(query=query,
                        host='mysql-slave.prod.nym1.adnxs.net',
                        database='',
                        user='xxxxxx',
                       pd='xxxx')
    return df

#reload(sys)
#sys.setdefaultencoding('utf8')
pd.options.display.float_format = '{:.2f}'.format
pd.set_option('display.max_columns', 500)
MYSQL_PROD_USER = 'xxxxxxxx'
MYSQL_PROD_PD = 'xxxxxx'
os.environ['LDAP_USER']=MYSQL_PROD_USER
os.environ['LDAP_PASSWD']=MYSQL_PROD_PASSWD
qm = QueryManager() 


# In[43]:


# ------------------------------------------#
# (2) API Connection                       #
# ------------------------------------------#
post_request_authentication = rs.post('https://api.appnexus.com/auth', 
                              data=json.dumps({'auth':{'username': 'xxxxxx','pd': 'xxxxxx'}}))
post_request_authentication
token = post_request_authentication.json()["response"]["token"].encode("ascii")


# In[44]:


#GET Profiles for member 10594

GET_PROFILE= """select cg.profile_id 
from bidder.campaign_group cg 
where cg.member_id=10594 and cg.created_on>='2024-09-01' and cg.deleted=0"""
df_profile= pull_from_mysql(GET_PROFILE)
df_profile


# In[45]:


#
profile_ids=df_profile["profile_id"].tolist()
print(profile_ids)


# In[ ]:


headers = {
    'cache-control': "no-cache",
    'Authorization': token
    }
for p in profile_ids:
    apiurl='https://api.appnexus.com/profile?id=%d&member_id=10594'%(p)
    getsprofile=rs.get(apiurl)
    getsprofile=json.loads(getsprofile.text)
    graph_id=getsprofile["response"]["profile"]["graph_id"]
    print(graph_id)
    if graph_id==4:
        new_profile={"profile":{
            "id":p,
            "graph_id":0}
        }
        new_profile=json.dumps(new_profile)     
        print(new_profile)
        response = requests.put(apiurl, headers=headers, data=new_profile)
        print(json.loads(response.text))
   


# In[ ]:




