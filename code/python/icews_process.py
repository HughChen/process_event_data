
# coding: utf-8

# # Process ICEWS Data
# https://www.acleddata.com/wp-content/uploads/2015/01/ACLED_Codebook_2015.pdf

# In[20]:

# ICEWS Script
# Test out reading 
import csv
ICEWSPATH = "../ICEWS/"
def print_header_first_line(fname):
    print("Current File: {0}".format(fname))
    with open(fname, 'rb') as f:
        reader = csv.reader(f, delimiter="\t")
        next_line = next(reader)
        print("Header length: {0}".format(len(next_line)))
        print(next_line)
        next_line = next(reader)
        print("Line length: {0}".format(len(next_line)))
        print(next_line)
    print("\n")

print_header_first_line('{0}events.2016.20170615135114.tab'.format(ICEWSPATH))


# ## Asia Data

# In[21]:

# Load header
import csv
fname = '{0}events.2016.20170615135114.tab'.format(ICEWSPATH)
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter="\t")
    header = next(reader)
    print("Header length: {0}".format(len(header)))
    print(header)


# Location

# In[32]:

lat_index, long_index = [header.index("Latitude"), header.index("Longitude")]


# Date

# In[33]:

date_index = header.index("Event Date")


# Description and source

# In[42]:

source_ind, source_notes = [header.index("Publisher"), header.index("Event Text")]


# Number of people affected
# 
# No obvious surrogate in this dataset - Maybe intensity?

# In[27]:

header.index("Intensity")


# Event Type

# In[35]:

event_index = header.index("CAMEO Code")


# Violent Event - Binary
# 
# 18 - ASSAULT
# 
# 19 - FIGHT
# 
# 20 - UNCONVENTIONAL MASS VIOLENCE
# 
# Count different types of events - simply select the ones with violent prefixed

# In[29]:

fname = '{0}events.2016.20170615135114.tab'.format(ICEWSPATH)
import csv
counts = dict()
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter="\t")
    for line in reader:
        counts[line[event_index]] = counts.get(line[event_index], 0) + 1


# In[36]:

from itertools import islice

def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))
take(20, counts.iteritems()) # Print subset


# In[43]:

def is_violent(event_code):
    if (event_code.startswith("18") or event_code.startswith("19") or event_code.startswith("20")):
        return True
    else:
        return False


# In[45]:

fname = '{0}events.2016.20170615135114.tab'.format(ICEWSPATH)
indices = [lat_index, long_index, date_index, source_ind, source_notes, -1, event_index]
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


# In[ ]:



