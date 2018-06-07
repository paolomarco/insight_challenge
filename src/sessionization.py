
# coding: utf-8

# import libraries
import pandas
from datetime import datetime
import sys
import os

# import arguments from the command line
log_file = sys.argv[1]
inactivity_file = sys.argv[2]
output_file = sys.argv[3]

# import data
log_file = pandas.read_csv(log_file, 
                           header=0,
                           dtype = {'cik': object},
                           parse_dates = {'datetime':['date','time']},
                           usecols=["ip", 
                                    "date", 
                                    "time",
                                    "cik",
                                    "accession",
                                    "extention"])

with open(inactivity_file) as file:
    inactivity_period = int(file.read())
    file.close()

# data formatting
log_file['doc_name'] = log_file.cik + log_file.accession + log_file.extention

log_file = log_file.drop(['cik', 'accession','extention'], axis=1)

# create empty dictionary to store ip session data
ip_dict = {}

# file to write session information to
fileout = output_file

if os.path.exists(fileout):
    append_write = 'a' # append if already exists
else:
    append_write = 'w' # make a new file if not

output_file = open(fileout,
                   append_write)

counter = 0

# iterate over log_file
for row in log_file.itertuples():
    
    # retrieve ip address & time stamp of request
    counter = counter + 1
    ip_address = row[2]
    current_timestamp = row[1]
            
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
            output_file.write("%s\n" % ','.join(str(i) for i in output))
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
        ip_dict[ip_address]['index'] = counter
    
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
            output_file.write("%s\n" % ','.join(str(i) for i in _output))
            del ip_dict[_ip_adress]
    
    # check if any recorded sessions in dictionary exceed session time treshold
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
            output_file.write("%s\n" % ','.join(str(i) for i in _output))
            del ip_dict[_ip_adress]

output_file.close()




