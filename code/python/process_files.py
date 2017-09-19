### Initialize file
import csv
from datetime import datetime

# file_cutoff = 20
write_header = ["database", "file_name", "file_line", "latitude", "longitude", 
                "date", "actor1", "actor2", "source", "description", 
                "num_affected", "event_type", "violent"]
date_ind = write_header.index("date")
event_ind = write_header.index("event_type")
process_fname = "processed.csv"
with open(process_fname, 'w') as write_file:
    csv_writer = csv.writer(write_file)
    csv_writer.writerow(write_header)
    
ACLEDPATH = "../ACLED/"
GDELTPATH = "../GDELT/"
ICEWSPATH = "../ICEWS/"


### ACLED
class ACLED_processor():
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
    

    def __init__(self, fname):
        self.fname = fname
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
        self.event_index = event_index
        self.indices = [lat_index, long_index, date_index, actor1_index, actor2_index, 
           source_ind, source_notes, aff_index, event_index]        
    
    def process(self, output_fname, is_africa=False, file_cutoff = float('inf'),
               year_cutoffs=[2014,2016]):
        with open(output_fname, 'a') as write_file:
            csv_writer = csv.writer(write_file)
            with open(self.fname, 'rb') as f:
                reader = csv.reader(f, delimiter=",")
                line_num = -1
                is_header = True
                count = 0
                for line in reader:
                    if (count == 0):
                        count = count + 1
                        continue
                    if (count > file_cutoff):
                        break                    
                    if (is_header): # We're on the header
                        is_header = False
                        continue
                    line_num = line_num + 1
                    write_line = [line[index] for index in self.indices]
                    write_line = ["ACLED", self.fname, str(line_num)] + write_line
                    if (is_africa):
                        date_time = datetime.strptime(write_line[date_ind], '%d/%m/%Y')
                        write_line[date_ind] = str(date_time)
                        if ((date_time.year < year_cutoffs[0]) or (date_time.year > year_cutoffs[1])):
                            continue
                    else:
                        date_time = datetime.strptime(write_line[date_ind], '%d-%B-%Y')
                        write_line[date_ind] = str(date_time)
                        if ((date_time.year < year_cutoffs[0]) or (date_time.year > year_cutoffs[1])):
                            continue
                    write_line.append(str(line[self.event_index] in self.violent_events_acled))
                    if (write_line.count('') >= 3):
                        continue
                    csv_writer.writerow(write_line)
                    count = count + 1



processor = ACLED_processor('{0}ACLED-Asia-Running-File-2016.csv'.format(ACLEDPATH))
processor.process(process_fname,False,file_cutoff)


processor = ACLED_processor('{0}ACLED-Asia-Running-file-January-to-December-2015-V2.csv'.format(ACLEDPATH))
processor.process(process_fname,False,file_cutoff)


### Process all ACLED files
ACLED_fnames = ["ACLED-Asia-Running-File-2016.csv",
                "ACLED-Asia-Running-file-January-to-December-2015-V2.csv",
                "ACLED-Version-7-All-Africa-1997-2016_actordyad_csv.csv"]
is_africa_lst = [False,False,True]
for i in range(0,len(ACLED_fnames)):
    processor = ACLED_processor(ACLEDPATH+ACLED_fnames[i])
    processor.process(process_fname,is_africa_lst[i])


### GDELT
class GDELT_processor():

    def is_violent_gdelt(self, event_code):
        if (event_code.startswith("18") or event_code.startswith("19") or event_code.startswith("20")):
            return True
        else:
            return False

    def __init__(self, fname):
        self.fname = fname
        # Grab header
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
        self.event_index = event_index
        self.indices = [lat_index, long_index, date_index, actor1_index, 
                        actor2_index, -1, -1, -1, event_index]
    
    def process(self, output_fname, file_cutoff = float('inf'), year_cutoffs=[2010,2016]):
        with open(output_fname, 'a') as write_file:
            csv_writer = csv.writer(write_file)
            with open(self.fname, 'rb') as f:
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
                    write_line = [line[index] if index is not -1 else 'NA' for index in self.indices]
                    write_line = ["GDELT", self.fname, str(line_num)] + write_line
                    date_time = datetime.strptime(write_line[date_ind], '%Y%m%d')
                    write_line[date_ind] = str(date_time)
                    if ((date_time.year < year_cutoffs[0]) or (date_time.year > year_cutoffs[1])):
                        continue
                    write_line[event_ind] = "CAMEO:"+write_line[event_ind]
                    write_line.append(str(self.is_violent_gdelt(line[self.event_index])))
                    if (write_line.count('') >= 3):
                        continue
                    csv_writer.writerow(write_line)
                    count = count + 1



processor = GDELT_processor('{0}events/20140101.export.csv'.format(GDELTPATH))
# processor.process(process_fname)
processor.process(process_fname, file_cutoff)


### Process all GDELT files
import os
year_cutoffs = [2014,2016]
GDELT_fnames = os.listdir("/Users/hughchen/Documents/Work/EventsResearch/GDELT/events")
GDELT_fnames = [name for name in GDELT_fnames if name.startswith("2")]
GDELT_fnames = [name for name in GDELT_fnames if ((int(name[0:4]) >= year_cutoffs[0]) and (int(name[0:4]) <= year_cutoffs[1]))]

import zipfile
import shutil
import random
import os

def unzip_to_tmp(fname):
    zip_ref = zipfile.ZipFile(fname, 'r')
    zip_ref.extractall(GDELTPATH+"tmp/")
    zip_ref.close()
    
def delete_tmp_files(folder):
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)    

random.shuffle(GDELT_fnames)
for i in range(0,len(GDELT_fnames)):
    unzip_to_tmp(GDELTPATH+"events/"+GDELT_fnames[i])
    processor = GDELT_processor(GDELTPATH+"tmp/"+os.listdir(GDELTPATH+"tmp/")[0])
    processor.process(process_fname,float('inf'),year_cutoffs)
    delete_tmp_files(GDELTPATH+"tmp/")


### ICEWS
class ICEWS_processor():

    def is_violent_icews(self, event_code):
        if (event_code.startswith("18") or event_code.startswith("19") or event_code.startswith("20")):
            return True
        else:
            return False
            
    def __init__(self, fname):
        self.fname = fname
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
        self.event_index = event_index
        self.indices = [lat_index, long_index, date_index,  actor1_index, 
                        actor2_index, source_ind, source_notes, -1, event_index]
    
    def process(self, output_fname, file_cutoff = float('inf'), year_cutoffs=[2010,2016]):
        with open(output_fname, 'a') as write_file:
            csv_writer = csv.writer(write_file)
            with open(self.fname, 'rb') as f:
                reader = csv.reader(f, delimiter="\t")
                line_num = -1
                is_header = True
                count = 0
                for line in reader:
                    if (count == 0):
                        count = count + 1
                        continue
                    if (count > file_cutoff):
                        break
                    if (is_header): # We're on the header
                        is_header = False
                        continue
                    line_num = line_num + 1
                    write_line = [line[index] if index is not -1 else 'NA' for index in self.indices]
                    write_line = ["ICEWS", self.fname, str(line_num)] + write_line
                    date_time = datetime.strptime(write_line[date_ind], '%Y-%m-%d')
                    write_line[date_ind] = str(date_time)
                    if ((date_time.year < year_cutoffs[0]) or (date_time.year > year_cutoffs[1])):
                        continue
                    write_line[event_ind] = "CAMEO:"+write_line[event_ind]
                    write_line.append(str(self.is_violent_icews(line[self.event_index])))
                    if (write_line.count('') >= 3):
                        continue
                    csv_writer.writerow(write_line)
                    count = count + 1



processor = ICEWS_processor('{0}events.2016.20170615135114.tab'.format(ICEWSPATH))
# processor.process(process_fname)
processor.process(process_fname, file_cutoff)


### Process all ICEWS files
# ICEWS_fnames = ["events.2010.20150313084533.tab",
#                 "events.2011.20150313084656.tab",
#                 "events.2012.20150313084811.tab",
#                 "events.2013.20150313084929.tab",
#                 "events.2014.20160121105408.tab",
#                 "events.2015.20170206133646.tab",
#                 "events.2016.20170615135114.tab"]
ICEWS_fnames = ["events.2014.20160121105408.tab",
                "events.2015.20170206133646.tab",
                "events.2016.20170615135114.tab"]
for fname in ICEWS_fnames:
    processor = ICEWS_processor(ICEWSPATH+fname)
    processor.process(process_fname)





