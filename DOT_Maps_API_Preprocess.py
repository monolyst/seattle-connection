
# coding: utf-8

# In[ ]:


# coding: utf-8
"""
Dot Maps API Preprocess data from the Seattle DOT Project and Construction Coordination map.
The data feed an interactive visualization for the Seattle Connection project, 
www.seattleconnection.com. A draft of the visualization can be accessed at:

https://public.tableau.com/views/Seattle_Connection_DotMapsApp/Dashboard?:embed=y&:display_count=yes

Processed data are organized into two files, one with merged normalized data at the sement 
level (projects.csv) and one with custom polygon format for use in Tableau dashboards.

Data are from the Project and Construction Coordination map. Visit 
http://www.seattle.gov/transportation/projects-and-programs/programs
/project-and-construction-coordination-office/project-and-construction-coordination-map 
for more information, to pull data, and to interact with the full DotMapsApp application.
"""


# In[1]:


# Libraries for API ETL
import json
import os
from urllib.request import Request, urlopen
from pandas.io.json import json_normalize 

import csv
from datetime import datetime
from dateutil import tz
import pandas as pd
import numpy as np


# In[6]:


import sys
import requests
from urllib.request import Request, urlopen
import json 

# Obtain session token by passing credentials to login page
def obtainCredentials():
    EMAIL = sys.argv[1]
    PASSWORD = sys.argv[2]

    loginURL = 'https://seattle.dotmapsapp.com/api-auth/login/?next=/api/'
    client = requests.session()

    # Retrieve the CSRF token first
    client.get(loginURL)  # sets cookie
    if 'csrftoken' in client.cookies:
        csrftoken = client.cookies['csrftoken']
    else:
        # older versions
        csrftoken = client.cookies['csrf']

    # Pass token to login page
    login_data = dict(username=EMAIL, password=PASSWORD, csrfmiddlewaretoken=csrftoken, next='/')
    r = client.post(loginURL, data=login_data, headers=dict(Referer=loginURL))
    return


# In[12]:


# Access API and save project information as needed. API limits and offset are in effect
# so getMapsData recursivly calls the API as long as a "next" URL is returned. 

def getMapsData(URL):
    r2 = client.get(URL)
    data = r2.json()

    # Create lists to temporarily store project information
    ID = []
    name = []
    agency_name = []
    description = []
    start_date = []
    end_date = []
    active = []
    conflicts = []
    geohash = []
    display_from = []
    display_to = []
    from_street = []
    to_street = []
    segment_number = []

    # Create lists to temporarily store polygon information
    ID_POLYGON = []
    lat = []
    lon = []
    sort_order = []
    geohash_polygon = []

    for result in data['results']:
    
        for i, segment in enumerate(result['segments']):
        
            ID.append(result['id'])
            name.append(result['name'])
            agency_name.append(result['agency_name'])
            description.append(result['description'])
            start_date.append(result['start_date'])
            end_date.append(result['end_date'])
            active.append(result['active'])
            conflicts.append(result['conflicts'])
            geohash.append(segment['geohash'])
            display_from.append(segment['display_from'])
            display_to.append(segment['display_to'])
            from_street.append(segment['from_street'])
            to_street.append(segment['to_street'])
            segment_number.append(str(i))

            # process point coordinates from polygons. Alternatively, decode geohash
            if segment['shape']['type'] == 'LineString':
                for j, shape in enumerate(segment['shape']['coordinates']):
                    ID_POLYGON.append(result['id'])
                    lat.append(shape[1])
                    lon.append(shape[0])
                    sort_order.append(str(j))
                    geohash_polygon.append(segment['geohash'])
        
            if segment['shape']['type'] == 'Point':
                ID_POLYGON.append(result['id'])
                lat.append(segment['shape']['coordinates'][1])
                lon.append(segment['shape']['coordinates'][0])
                sort_order.append(0)
                geohash_polygon.append(segment['geohash'])    
    
    # Build dataframes for transformation or export
    df_projects = pd.DataFrame()
    df_projects['Id'] = ID
    df_projects['name'] = name
    df_projects['agency_name'] = agency_name
    df_projects['description'] = description
    df_projects['start_date'] = start_date
    df_projects['end_date'] = end_date
    df_projects['active'] = active
    df_projects['conflicts'] = conflicts
    df_projects['geohash'] = geohash
    df_projects['display_from'] = display_from
    df_projects['display_to'] = display_to
    df_projects['from_street'] = from_street
    df_projects['to_street'] = to_street
    df_projects['segment_number'] = segment_number

    df_polygons = pd.DataFrame()
    df_polygons['Id'] = ID_POLYGON
    df_polygons['lat'] = lat
    df_polygons['lon'] = lon
    df_polygons['sort_order'] = sort_order
    df_polygons['geohash'] = geohash_polygon

    # Export to file
    if os.path.isfile('data/projects.csv'):
        df_projects.to_csv('data/projects.csv', mode='a', header=False, index=False, encoding="utf-8")
    else:
        df_projects.to_csv('data/projects.csv', mode='w', header=True, index=False, encoding="utf-8")
    
    if os.path.isfile('data/df_polygons.csv'):
        df_polygons.to_csv('data/df_polygons.csv', mode='a', header=False, index=False, encoding="utf-8")
    else:
        df_polygons.to_csv('data/df_polygons.csv', mode='w', header=True, index=False, encoding="utf-8")
    
    # Recursively call API for the next URL offset and limit
    if data["next"]:
        print (data["next"])
        getMapsData(data["next"])
    


# In[5]:


URL = "https://seattle.dotmapsapp.com/api/project/?format=json&limit=500&offset=0"
obtainCredentials():
getMapsData(URL):

