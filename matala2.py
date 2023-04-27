#!/usr/bin/env python
# coding: utf-8

# In[1]:


## matala 2


# In[2]:


import pandas as pd
import numpy as np
import datetime


# In[4]:


data = pd.read_csv("C:\\Users\\קארין\\Desktop\\הנדסת תעון - קארין\\שנה ג\\סמסטר ב\\כרייה וניתוח נתונים בפייתון\\מטלות\\מטלה 2\matala2_cosmetics_2019-Nov.csv")


# In[5]:


data.head()


# In[6]:


DATA = data.iloc[0:1000,:]


# In[7]:


copie_data=DATA.copy()


# In[8]:


#### 1


# In[9]:


# Convert the column to datetime format
copie_data['event_time'] = pd.to_datetime(copie_data['event_time'])

# Sort the data by 'user_session' and 'event_time' to creat the differences
copie_data.sort_values(["user_id",'user_session', 'event_time'], inplace=True)

# Create a new column 'next_event_time' with the event_time of the next event in the same session
copie_data['next_TIME_e'] = copie_data.groupby('user_session')['event_time'].shift(-1)

# Calculate the duration between the current event and the next event in the same session
copie_data['duration_to_next_event'] = (copie_data['next_TIME_e'] - copie_data['event_time']).dt.total_seconds()

# Replace the NaN values in 'duration_to_next_event' with 0 for the last event in each session
copie_data['duration_to_next_event'].fillna(0, inplace=True)

# Drop the 'next_event_time' column
copie_data.drop('next_TIME_e', axis=1, inplace=True)


# In[10]:


# we can cheake the changes
DATA.head()


# In[13]:


#### 2


# In[14]:


from datetime import timedelta


# In[17]:


# Change the column to datetime type
copie_data['event_time']= pd.to_datetime(copie_data['event_time'])

# Add a new column to the dataframe with the funnel number for each user
copie_data['funnel_number'] = (copie_data.groupby('user_id')['event_time'].apply(lambda x: ((x - x.shift()) > timedelta(days=5)).cumsum())
                        .reset_index(level=0, drop=True) + 1)


# In[18]:


copie_data


# In[ ]:


#### 3


# In[21]:


# Add a new column that counting the visiting for each funnel
copie_data['index_in_funnel'] = copie_data.groupby(['user_id', 'funnel_number'])['user_session'].transform(lambda x: x.factorize()[0] + 1)


# In[22]:


copie_data


# In[23]:


#### 4


# In[25]:


import re


# In[34]:


# 4. Clean up the price column
def clean_price(price):
    match = re.search(r'\d+(\.\d+)?', price) # find the price pattern in the string
    if match:
        return float(match.group()) # return the price as a float
    else:
        return None

# try the fanction i create to see if work
list= ["2.44 sale", "net 5.44"]
for i in list:
    print(clean_price(i))


# In[35]:


copie_data['price'] = copie_data['price'].apply(clean_price) # apply the clean_price function to the price column


# In[33]:


copie_data


# In[36]:


#### 5


# In[39]:


# Create a chart that depicts the number of events of each type
event_counts = copie_data.groupby("event_type")["user_id"].count()  
event_counts.plot(kind='bar', title='Number of events by type')


# In[40]:


#### 6


# In[41]:


# Group the data by user_id and user_session
data_group = copie_data.groupby(['user_id', 'user_session'])

# Calculate the number of events per session
events_per_session = data_group.size()

# Calculate the duration of each session
session_durations = data_group['event_time'].agg(lambda x: (x.max() - x.min()).total_seconds())

# Get a list of viewed, added to cart, and purchased products per session
viewed_per_session = data_group.apply(lambda x: x.loc[x['event_type'] == 'view', 'product_id'].tolist())
added_to_cart_per_session = data_group.apply(lambda x: x.loc[x['event_type'] == 'cart', 'product_id'].tolist())
purchased_per_session = data_group.apply(lambda x: x.loc[x['event_type'] == 'purchase', 'product_id'].tolist())

# Create the session_data dataframe
session_data = pd.DataFrame({
    'user_id': events_per_session.index.get_level_values('user_id'),
    'user_session': events_per_session.index.get_level_values('user_session'),
    'total_events': events_per_session.values,
    'duration': session_durations.values,
    'list_of_viewed': viewed_per_session.values,
    'list_of_added_to_cart': added_to_cart_per_session.values,
    'list_of_purchased': purchased_per_session.values
})


# In[42]:


session_data


# In[47]:


# merge session_data with data_copy
merged_data = pd.merge(session_data, copie_data[["user_id",'user_session', 'funnel_number', 'index_in_funnel']], on=["user_id",'user_session'], how='left')

# select columns
session_data_new = merged_data[["user_id",'user_session', 'total_events', 'duration', 'list_of_viewed', 'list_of_added_to_cart', 'list_of_purchased', 'funnel_number', 'index_in_funnel']]


# In[48]:


session_data_new


# In[ ]:




