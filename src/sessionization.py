

# coding: utf-8

# import libraries
import pandas
from datetime import datetime
import sys
import os
import csv

# import arguments from the command line
input_file = sys.argv[1]
inactivity_file = sys.argv[2]
output_file = sys.argv[3]

def import_log():
    log_file = pandas.read_csv(input_file, 
                           header=0,
                           dtype = {'cik': object},
                           parse_dates = {'datetime':['date','time']},
                           usecols=["ip", 
                                    "date", 
                                    "time",
                                    "cik",
                                    "accession",
                                    "extention"])
    return log_file

def import_inactivity():
    with open(inactivity_file) as file:
        inactivity_period = int(file.read())
        file.close()
    return inactivity_period    
    
def data_processing(df):
    df['doc_name'] = df.cik + df.accession + df.extention
    df = df.drop(['cik', 'accession','extention'], axis=1)
    return df

def write_output(output_list):

    fileout = output_file

    if os.path.exists(fileout):
        append_write = 'a' # append if already exists
    else:
        append_write = 'w' # make a new file if not

    with open(fileout, append_write) as f:
        writer = csv.writer(f)
        writer.writerows(output_list)

    f.close()

# import data
log_file = import_log()
inactivity_period = import_inactivity()

# process data
log_file = data_processing(log_file)

start_time = datetime.now()
output_list = []
ip_dict = {}
counter = 0
prev_timestamp = None

# iterate over log_file
for row in log_file.itertuples(): # this outputs a tuple 
    
    counter += 1
    
    # retrieve ip address & time stamp of request
    ip_address = row[2]
    current_timestamp = row[1]
    
    # detect time change
    if prev_timestamp is not None: 
        sec_diff = prev_timestamp - current_timestamp
        if sec_diff.seconds > 0:
            check_dict = True
            prev_timestamp = current_timestamp # assign new prev_timestmap
        else:
            check_dict = False
    else:
        prev_timestamp = current_timestamp
        check_dict = False

    # check if ip_address has made document request & if time elapsed since
    # exceeds session threshold else increment documents requested by 1
    if ip_address in ip_dict:
        
        last_timestamp = ip_dict[ip_address]['last_request']
        time_elapsed = current_timestamp - last_timestamp
        if time_elapsed.seconds > inactivity_period:
            duration = current_timestamp - ip_dict[ip_address]['first_request']
            duration = duration.seconds + 1
            output = [ip_address,
                     str(ip_dict[ip_address]['first_request']),
                     str(current_timestamp),
                     duration,
                     ip_dict[ip_address]['n_doc']]
            output_list.append(output)
            del ip_dict[ip_address]
        else:
            ip_dict[ip_address]['last_request'] = current_timestamp
            ip_dict[ip_address]['n_doc'] = ip_dict[ip_address]['n_doc'] + 1
            
    # record ip_address request
    else:
        ip_dict[ip_address] = {}
        ip_dict[ip_address]['first_request'] = current_timestamp
        ip_dict[ip_address]['last_request'] = current_timestamp
        ip_dict[ip_address]['n_doc'] = 1
        ip_dict[ip_address]['index'] = row[0]
    
    # close out all sessions if last row of log_file
    if len(log_file.index) == counter:
        ip_address_list = []
        for _ip_adress in list(ip_dict):
            ip_address_list.append(_ip_adress)
        ip_address_list_sorted = sorted(ip_address_list,
                                        key=lambda x: (ip_dict[x]['index']))
        
        for _ip_adress in ip_address_list_sorted:
            duration = (ip_dict[_ip_adress]['last_request'] - ip_dict[_ip_adress]['first_request'])
            duration = duration.seconds + 1
            _output = [_ip_adress,
                     str(ip_dict[_ip_adress]['first_request']),
                     str(ip_dict[_ip_adress]['last_request']),
                     duration,
                     ip_dict[_ip_adress]['n_doc']]
            output_list.append(_output)
            del ip_dict[_ip_adress]
    
    # check if any recorded sessions in dictionary exceed session time treshold
    if check_dict == True:
        for _ip_adress in list(ip_dict):
            last_request = ip_dict[_ip_adress]['last_request']
            time_elapsed = current_timestamp - last_request
            if time_elapsed.seconds > inactivity_period:
                duration = (last_request - ip_dict[_ip_adress]['first_request'])
                duration = duration.seconds + 1
                _output = [_ip_adress,
                         str(ip_dict[_ip_adress]['first_request']),
                         str(last_request),
                         duration,
                         ip_dict[_ip_adress]['n_doc']]
                output_list.append(_output)
                del ip_dict[_ip_adress]
            
# write results to output file
write_output(output_list)
            
end_time = datetime.now()

sessionization_time = end_time - start_time
print("Sessionization took ", sessionization_time," seconds")






