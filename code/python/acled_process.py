
# coding: utf-8

# # Process ACLED Data
# https://www.acleddata.com/wp-content/uploads/2015/01/ACLED_Codebook_2015.pdf

# In[3]:

# Print header and first line of different files
ACLEDPATH = "../ACLED/"
def print_header_first_line(fname):
    print("Current File: {0}".format(fname))
    import csv
    with open(fname, 'rb') as f:
        reader = csv.reader(f, delimiter=",")
        next_line = next(reader)
        print("Header length: {0}".format(len(next_line)))
        print(next_line)
        print(next_line[6])
        next_line = next(reader)
        print("Line length: {0}".format(len(next_line)))
        print(next_line)
        print(next_line[6])
    print("\n")

# Test out reading all the asia data
# print_header_first_line('{0}ACLED-Asia-Running-File-January-May-2017.csv'.format(ACLEDPATH))
print_header_first_line('{0}ACLED-Asia-Running-file-January-to-December-2015-V2.csv'.format(ACLEDPATH))
print_header_first_line('{0}ACLED-Asia-Running-File-2016.csv'.format(ACLEDPATH))

# Test out reading all the africa data
# print_header_first_line('{0}ACLED All Africa File_20170101 to 20170617_csv.csv'.format(ACLEDPATH))
print_header_first_line('{0}ACLED-Version-7-All-Africa-1997-2016_actordyad_csv.csv'.format(ACLEDPATH))


# ## Asia Data

# In[4]:

ACLEDPATH = "../ACLED/"
fname = '{0}ACLED-Asia-Running-file-January-to-December-2015-V2.csv'.format(ACLEDPATH)
fname = '{0}ACLED-Asia-Running-File-2016.csv'.format(ACLEDPATH)
print("Current File: {0}".format(fname))
import csv
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter=",")
    header = next(reader)
    print("Header length: {0}".format(len(header)))
    print(header)
print("\n")


# Location

# In[6]:

[lat_index, long_index, geo_precision] = [header.index("LATITUDE"), 
                                          header.index("LONGITUDE"), 
                                          header.index("GEO_PRECISION")]


# Date
# 
# Example format: '01-January-2015'

# In[7]:

[date_index, date_precision] = [header.index("EVENT_DATE"), header.index("TIME_PRECISION")]


# Description

# In[8]:

[source_ind, source_notes] = [header.index("SOURCE"), header.index("NOTES")]


# Number of people affected

# In[9]:

aff_index = header.index("FATALITIES")


# Event Type

# In[10]:

event_index = header.index("EVENT_TYPE")


# Violent Event - Binary

# In[11]:

fname = '{0}ACLED-Asia-Running-file-January-to-December-2015-V2.csv'.format(ACLEDPATH)
import csv
counts = dict()
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter=",")
    for line in reader:
        counts[line[event_index]] = counts.get(line[event_index], 0) + 1

fname = '{0}ACLED-Asia-Running-File-2016.csv'.format(ACLEDPATH)
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter=",")
    for line in reader:
        counts[line[event_index]] = counts.get(line[event_index], 0) + 1
        
fname = '{0}ACLED-Version-7-All-Africa-1997-2016_actordyad_csv.csv'.format(ACLEDPATH)
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter=",")
    for line in reader:
        counts[line[event_index]] = counts.get(line[event_index], 0) + 1


# In[12]:

counts


# In[13]:

violent_events = ["Battle-Government regains territory",
                  "Battle-Government regains territory",
                  "Battle-No change of territory",
                  "Battle-Non-state actor overtakes territory",
                  "Battle-Non-state actors overtake territory",
                  "RIots/Protests",
                  "Remote violence",
                  "Riots/Protests",
                  "Violence Against Civilians",
                  "Violence against civilians",
                  "Violence against civilians"]


# Violent events are:
# 
# Battle-Government regains territory
# 
# Battle-Government regains territory
# 
# Battle-No change of territory
# 
# Battle-Non-state actor overtakes territory
# 
# Battle-Non-state actors overtake territory
# 
# RIots/Protests
# 
# Remote violence
# 
# Riots/Protests
# 
# Violence Against Civilians
# 
# Violence against civilians
# 
# Violence against civilians

# Write to CSV

# In[31]:

fname = '{0}ACLED-Asia-Running-file-January-to-December-2015-V2.csv'.format(ACLEDPATH)
write_header = ["latitude", "longitude", "date", "source", "description", "num_affected", "event_type", "violent"]
indices = [lat_index, long_index, date_index, source_ind, source_notes, aff_index, event_index]
import csv
csv_writer = csv.writer(open('sample_out.csv', 'w'))
DATE_IND = 2

with open('sample_out.csv', 'w') as write_file:
    csv_writer = csv.writer(write_file)
    csv_writer.writerow(write_header)
    with open(fname, 'rb') as f:
        reader = csv.reader(f, delimiter=",")
        count = 0
        for line in reader:
            print('\n')
            print(count)
            print(line)
            if (count == 0):
                count = count + 1
                continue
            if (count > 20):
                break
            count = count + 1
            write_line = [line[index] for index in indices]
            print(write_line)
            write_line[DATE_IND] = str(datetime.strptime(write_line[DATE_IND], '%d-%B-%Y'))
            write_line.append(str(line[event_index] in violent_events))
            csv_writer.writerow(write_line)


# In[29]:

write_line


# In[16]:

write_line[2]


# In[21]:

from datetime import datetime
str(datetime.strptime('04-January-2015', '%d-%B-%Y'))


# In[ ]:



