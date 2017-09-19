
# coding: utf-8

# Try to find what nepal and south africa are labelled as

# In[58]:

import csv
country = "nepal"
np_codes = ["NEP", "NPL", "Nepal"]
za_codes = ["ZAF", "South Africa"]
# za_codes = ["ZA"]
fname = "/Users/hughchen/Documents/Work/EventsResearch/code/processed_2014-2016.csv"
header = ["database", "file_name", "file_line", "latitude", "longitude", 
          "date", "actor1", "actor2", "source", "description", 
          "num_affected", "event_type", "violent"]
actor1_ind = header.index("actor1")
actor2_ind = header.index("actor2")
actor_lst = []


# In[59]:

country_fname = "nepal_filter.csv"
with open(country_fname, 'w') as write_file:
    csv_writer = csv.writer(write_file)
    csv_writer.writerow(header)
country_codes = np_codes
with open(country_fname, 'a') as write_file:
    csv_writer = csv.writer(write_file)
    with open(fname, 'rb') as f:
        reader = csv.reader(f, delimiter=",")
        for line in reader:
            is_a1_country = any([code in line[actor1_ind] for code in country_codes])
            is_a2_country = any([code in line[actor2_ind] for code in country_codes])
            if (is_a1_country or is_a2_country):
                csv_writer.writerow(line)


# In[60]:

country_fname = "southafrica_filter.csv"
with open(country_fname, 'w') as write_file:
    csv_writer = csv.writer(write_file)
    csv_writer.writerow(header)
country_codes = za_codes
with open(country_fname, 'a') as write_file:
    csv_writer = csv.writer(write_file)
    with open(fname, 'rb') as f:
        reader = csv.reader(f, delimiter=",")
        for line in reader:
            is_a1_country = any([code in line[actor1_ind] for code in country_codes])
            is_a2_country = any([code in line[actor2_ind] for code in country_codes])
            if (is_a1_country or is_a2_country):
                csv_writer.writerow(line)


# ## Testing the country code

# In[53]:

country_codes = ["NEP", "NPL", "Nepal"]
actor_lst = []
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter=",")
    for line in reader:
        is_a1_country = any([code in line[actor1_ind] for code in country_codes])
        is_a2_country = any([code in line[actor2_ind] for code in country_codes])
        if (is_a1_country or is_a2_country):
            if (is_a1_country):
                actor_lst.append(line[actor1_ind])
            if (is_a2_country):
                actor_lst.append(line[actor2_ind])


# In[54]:

len(actor_lst)


# In[55]:

actor_lst


# In[56]:

set(actor_lst)


# In[46]:

country_codes = ["ZAF", "South Africa"]
actor_lst = []
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter=",")
    for line in reader:
        is_a1_country = any([code in line[actor1_ind] for code in country_codes])
        is_a2_country = any([code in line[actor2_ind] for code in country_codes])
        if (is_a1_country or is_a2_country):
            if (is_a1_country):
                actor_lst.append(line[actor1_ind])
            if (is_a2_country):
                actor_lst.append(line[actor2_ind])


# In[50]:

len(actor_lst)


# In[51]:

actor_lst


# In[52]:

set(actor_lst)


# In[ ]:



