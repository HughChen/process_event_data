
# coding: utf-8

# # Process GDELT Data
# https://www.acleddata.com/wp-content/uploads/2015/01/ACLED_Codebook_2015.pdf

# In[2]:

# GDELT Script
# Read the headers
GDELTPATH = "../GDELT/"
print("\n")
f = open("{0}CSV.header.dailyupdates.txt".format(GDELTPATH),"r") 
header = f.readline().split('\t')
print("Header length: {0}".format(len(header)))
print(header)
f.close()

# Read the first two lines
import csv
with open('{0}events/20140101.export.csv'.format(GDELTPATH), 'rb') as f:
    reader = csv.reader(f, delimiter="\t")
    next_line = next(reader)
    print("Line length: {0}".format(len(next_line)))
    print(next_line)
    print("Line length: {0}".format(len(next_line)))
    print(next_line)


# ## Asia Data

# Location

# In[26]:

lat_index, long_index = [header.index("Actor1Geo_Lat"), header.index("Actor1Geo_Long")]


# Date
# 
# Example format: '20040104'

# In[31]:

date_index = header.index("SQLDATE")


# Description
# 
# They don't have a description - instead they have NumMentions, NumSources, NumArticles, and AvgTone

# In[9]:

[header.index("NumMentions"), header.index("AvgTone")]


# Number of people affected
# 
# No obvious surrogate in this dataset

# In[11]:

# 


# Event Type
# 
# CAMEO Code
# 
# http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf

# In[28]:

event_index = header.index("EventCode")


# Violent Event - Binary
# 
# 18 - ASSAULT
# 
# 19 - FIGHT
# 
# 20 - UNCONVENTIONAL MASS VIOLENCE
# 
# Count different types of events - simply select the ones with violent prefixed

# In[18]:

fname = '{0}events/20140101.export.csv'.format(GDELTPATH)
import csv
counts = dict()
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter="\t")
    for line in reader:
        counts[line[event_index]] = counts.get(line[event_index], 0) + 1


# In[25]:

from itertools import islice

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))
take(20, counts.iteritems()) # Print subset


# In[ ]:

def is_violent(event_code):
    if (event_code.startswith("18") or event_code.startswith("19") or event_code.startswith("20")):
        return True
    else:
        return False


# In[61]:

fname = '{0}events/20140101.export.csv'.format(GDELTPATH)
indices = [lat_index, long_index, date_index, -1, -1, -1, event_index]
import csv
csv_writer = csv.writer(open('sample_out.csv', 'a'))
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter="\t")
    count = 0
    for line in reader:
        if (count == 0):
            count = count + 1
            continue
        if (count > 20):
            break
        count = count + 1
        write_line = [line[index] if index is not -1 else 'NA' for index in indices]
        write_line.append(str(is_violent(line[event_index])))
        csv_writer.writerow(write_line)


# In[58]:

write_line


# In[ ]:



