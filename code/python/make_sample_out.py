
# coding: utf-8

# # Create sample output file

# # Initialize file

# In[21]:

import csv
from datetime import datetime

file_cutoff = 30
write_header = ["database", "file_name", "file_line", "latitude", "longitude", 
                "date", "actor1", "actor2", "source", "description", 
                "num_affected", "event_type", "violent"]
date_ind = write_header.index("date")
event_ind = write_header.index("event_type")
with open('sample_out.csv', 'w') as write_file:
    csv_writer = csv.writer(write_file)
    csv_writer.writerow(write_header)


# ## ACLED

# In[6]:

# Grab header
ACLEDPATH = "../ACLED/"
fname = '{0}ACLED-Asia-Running-file-January-to-December-2015-V2.csv'.format(ACLEDPATH)
fname = '{0}ACLED-Asia-Running-File-2016.csv'.format(ACLEDPATH)
print("Current File: {0}".format(fname))
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter=",")
    header = next(reader)
    print("Header length: {0}".format(len(header)))
    print(header)
print("\n")

[lat_index, long_index, geo_precision] = [header.index("LATITUDE"), 
                                          header.index("LONGITUDE"), 
                                          header.index("GEO_PRECISION")]
[date_index, date_precision] = [header.index("EVENT_DATE"), header.index("TIME_PRECISION")]
[actor1_index, actor2_index] = [header.index("ACTOR1"), header.index("ACTOR2")]
[source_ind, source_notes] = [header.index("SOURCE"), header.index("NOTES")]
aff_index = header.index("FATALITIES")
event_index = header.index("EVENT_TYPE")
violent_events_acled = ["Battle-Government regains territory",
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


# In[12]:

fname = '{0}ACLED-Asia-Running-file-January-to-December-2015-V2.csv'.format(ACLEDPATH)
indices = [lat_index, long_index, date_index, actor1_index, actor2_index, 
           source_ind, source_notes, aff_index, event_index]

with open('sample_out.csv', 'a') as write_file:
    csv_writer = csv.writer(write_file)
    with open(fname, 'rb') as f:
        reader = csv.reader(f, delimiter=",")
        line_num = -1
        count = 0
        for line in reader:
            if (count == 0):
                count = count + 1
                continue
            if (count > file_cutoff):
                break
            line_num = line_num + 1
            write_line = [line[index] for index in indices]
            write_line = ["ACLED", fname, str(line_num)] + write_line
            date_time = datetime.strptime(write_line[date_ind], '%d-%B-%Y')
            write_line[date_ind] = str(date_time)
            
            print(date_time.year<2016)
            
            write_line.append(str(line[event_index] in violent_events_acled))
            if (write_line.count('') >= 3):
                continue
            csv_writer.writerow(write_line)
            count = count + 1


# ## GDELT

# In[17]:

import csv
from datetime import datetime
# Grab header
GDELTPATH = "../GDELT/"
print("\n")
f = open("{0}CSV.header.dailyupdates.txt".format(GDELTPATH),"r") 
header = f.readline().split('\t')
print("Header length: {0}".format(len(header)))
print(header)
f.close()

lat_index, long_index = [header.index("Actor1Geo_Lat"), header.index("Actor1Geo_Long")]
date_index = header.index("SQLDATE")
[actor1_index, actor2_index] = [header.index("Actor1Code"), header.index("Actor2Code")]
[header.index("NumMentions"), header.index("AvgTone")]
event_index = header.index("EventCode")
def is_violent_gdelt(event_code):
    if (event_code.startswith("18") or event_code.startswith("19") or event_code.startswith("20")):
        return True
    else:
        return False


# In[23]:

fname = '{0}events/20140101.export.csv'.format(GDELTPATH)
indices = [lat_index, long_index, date_index, actor1_index, 
           actor2_index, -1, -1, -1, event_index]

import shutil
with open('sample_out.csv', 'a') as write_file:
    csv_writer = csv.writer(write_file)
    with open(fname, 'rb') as f:
        reader = csv.reader(f, delimiter="\t")
        line_num = -1
        count = 0
        for line in reader:
            if (count == 0):
                count = count + 1
                continue
            if (count > file_cutoff):
                break
            line_num = line_num + 1
            write_line = [line[index] if index is not -1 else 'NA' for index in indices]
            write_line = ["GDELT", fname, str(line_num)] + write_line
            write_line[date_ind] = str(datetime.strptime(write_line[date_ind], '%Y%m%d'))
            write_line[event_ind] = "CAMEO:"+write_line[event_ind]
            write_line.append(str(is_violent_gdelt(line[event_index])))
            if (write_line.count('') >= 3):
                continue
            csv_writer.writerow(write_line)
            count = count + 1            


# In[25]:

fname = '{0}events/20150325.export.CSV.zip'.format(GDELTPATH)
indices = [lat_index, long_index, date_index, actor1_index, 
           actor2_index, -1, -1, -1, event_index]
import shutil
with open('sample_out.csv', 'a') as write_file:
    csv_writer = csv.writer(write_file)
    with open("tmpfile", "wb") as f:
        shutil.copyfileobj(gzip.open(fname), fname)
        reader = csv.reader(f, delimiter="\t")
        line_num = -1
        count = 0
        for line in reader:
            if (count == 0):
                count = count + 1
                continue
            if (count > file_cutoff):
                break
            line_num = line_num + 1
            write_line = [line[index] if index is not -1 else 'NA' for index in indices]
            write_line = ["GDELT", fname, str(line_num)] + write_line
            write_line[date_ind] = str(datetime.strptime(write_line[date_ind], '%Y%m%d'))
            write_line[event_ind] = "CAMEO:"+write_line[event_ind]
            write_line.append(str(is_violent_gdelt(line[event_index])))
            if (write_line.count('') >= 3):
                continue
            csv_writer.writerow(write_line)
            count = count + 1            


# In[36]:

fname = '{0}events/20150325.export.CSV.zip'.format(GDELTPATH)
indices = [lat_index, long_index, date_index, actor1_index, 
           actor2_index, -1, -1, -1, event_index]
zip_ref = zipfile.ZipFile(fname, 'r')
zip_ref.extractall(GDELTPATH+"tmp/")
zip_ref.close()


# In[42]:

processor = GDELT_processor(GDELTPATH+"tmp/"+os.listdir(GDELTPATH+"tmp/")[0])
processor.process(process_fname,file_cutoff)


# In[43]:

folder = GDELTPATH+"tmp/"
for the_file in os.listdir(folder):
    file_path = os.path.join(folder, the_file)
    try:
        if os.path.isfile(file_path):
            os.unlink(file_path)
        #elif os.path.isdir(file_path): shutil.rmtree(file_path)
    except Exception as e:
        print(e)


# ## ICEWS

# In[3]:

# Load header
ICEWSPATH = "../ICEWS/"
fname = '{0}events.2016.20170615135114.tab'.format(ICEWSPATH)
with open(fname, 'rb') as f:
    reader = csv.reader(f, delimiter="\t")
    header = next(reader)
    print("Header length: {0}".format(len(header)))
    print(header)

lat_index, long_index = [header.index("Latitude"), header.index("Longitude")]
date_index = header.index("Event Date")
[actor1_index, actor2_index] = [header.index("Source Name"), header.index("Target Name")]
source_ind, source_notes = [header.index("Publisher"), header.index("Event Text")]
header.index("Intensity")
event_index = header.index("CAMEO Code")
def is_violent_icews(event_code):
    if (event_code.startswith("18") or event_code.startswith("19") or event_code.startswith("20")):
        return True
    else:
        return False


# In[4]:

fname = '{0}events.2016.20170615135114.tab'.format(ICEWSPATH)
indices = [lat_index, long_index, date_index,  actor1_index, 
           actor2_index, source_ind, source_notes, -1, event_index]
with open('sample_out.csv', 'a') as write_file:
    csv_writer = csv.writer(write_file)
    with open(fname, 'rb') as f:
        reader = csv.reader(f, delimiter="\t")
        line_num = -1
        count = 0
        for line in reader:
            if (count == 0):
                count = count + 1
                continue
            if (count > file_cutoff):
                break
            line_num = line_num + 1
            write_line = [line[index] if index is not -1 else 'NA' for index in indices]
            write_line = ["ICEWS", fname, str(line_num)] + write_line
            write_line[date_ind] = str(datetime.strptime(write_line[date_ind], '%Y-%m-%d'))
            write_line[event_ind] = "CAMEO:"+write_line[event_ind]
            write_line.append(str(is_violent_icews(line[event_index])))
            if (write_line.count('') >= 3):
                continue
            csv_writer.writerow(write_line)
            count = count + 1


# In[ ]:



