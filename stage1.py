# Importing the required libraries
import ray
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import os
import datetime as dt
import boto3

#  Initializing Ray Multiprocessing Library
ray.init()

# Initializing Boto3 AWS API Library. Important to note that 
# AWS Credentials should already be configured 
# using the AWS CLI
s3 = boto3.resource('s3')

# BISON KV6 Archive URL
base_archive_url = 'https://dt.trein.fwrite.org/DE-Co'

# Adding correct names for the columns in the data archive
maincolumns=['TIME', 'Data', 'Hash']

# Defining a date from where to start cleaning the XML archives
start_date = dt.datetime(2020, 4, 6)

# Defining the number of files to process
day_count = 24

# Here, we define a Ray remote function which will be running the task
# of extracting data from the messages passed to it. The messages will 
# be passed as a list, where wach list is picked up from a cell of the 
# raw data table column 'Data'

@ray.remote
def process_xml(data):
    cleaned_message_list = ''
    # We replace all the namespaces to make the data uniform, since 
    # some messages contained a namespace, and some did not. We also 
    # removed the '\n' and '\r' line endings since they were present
    # non-uniformly accross the data
    data = data.replace('xmlns', 'id')
    data = data.replace(':ns2', '1')
    data = data.replace('tmi8:', '')
    data = data.replace('\n', '')
    data = data.replace('\r', '')
    
    # We then import the data into a BeautifulSoup object to read the 
    # XML data
    message_XML_data = BeautifulSoup(StringIO(data), "xml")
    # And we find all the XML tags with name KV6posinfo to get all 
    # the messages
    message_list_container = message_XML_data.find_all('KV6posinfo')
    
    # We then start reading the messages one by one 
    for message_list in message_list_container:
        for message in message_list.children:
            if message == ' ' or message == '':
                # Discarding any whitespaces
                continue
                
            # Adding message type to a comma separated row
            temp_message_row = message.name + ', '
            
            # Setting new message flag to true to keep track if we are 
            # continuing reading a message that was being read in the 
            # previous pass or not
            new_message = True
            
            # We now read the columns inside a message to extract the data
            for column in message.children:
                if column == ' ' or column == '':
                    # Discarding any whitespaces
                    continue
                    
                if not new_message:
                    # Handling a case of reading the previous message
                    temp_message_row += ', '
                    
                if column.contents.__len__() == 0:
                    # Discarding an empty column
                    continue
                    
                # Setting the flag to remember that we are reading the
                # same message
                new_message = False
                
                # Adding the column contents to the comma separated 
                # message row
                temp_message_row += column.contents[0]
                
            # Adding a completed message row to the cleaned message list
            if cleaned_message_list == '':
                cleaned_message_list = temp_message_row
            else:
                cleaned_message_list = '\n'.join(
                    [cleaned_message_list, temp_message_row])

    return cleaned_message_list

# In this loop, we read the files one by one, according to the dates. The 
# files are then split and sent to the Ray remote function for processing.
# After prcessing, the processed data is collected and put in a CSV file, 
# which is then store in an AWS S3 bucket

for single_date in (start_date + dt.timedelta(n) for n in range(day_count)):
    # We first build the filename according to the format of the archive
    # using the date
    filename = 'BISON_{}.csv'.format(single_date.strftime("%Y-%m-%d"))
    
    # To download and operate on these archive files, we using CLI tools,
    # which make the task very easy to handle
    
    # We download the file from the archive using wget
    os.system('cd ./Dataset/BISON/ && wget {}/{}/{}.xz'
              .format(base_archive_url, 
                      single_date.strftime("%Y-%m"), 
                      filename))
    
    # Once the file is downloaded, we expand it using XZ
    os.system('cd ./Dataset/BISON/ && xz -d {}.xz'.format(filename))
    
    # And when we have the file, we read it using the Pandas library and store 
    # it in a dataframe
    df = pd.read_csv('./Dataset/BISON/{}'.format(filename), 
                     names=maincolumns, 
                     header=None, 
                     encoding='unicode_escape')
    
    # And then we print to the console that the data has been successfully 
    # imported
    print('data imported')
    
    # We then split up the files row-wise, and send the Data column of each of 
    # the rows to the Ray remote function for processing. 
    futures = [process_xml.remote(df.Data[index]) for index in df.index]
    
    # This is where the magic happens. We call pass the split up dataset
    # to the Ray library, which processes the whole thing in parallel 
    # over multiple cores. This processed data is then returned as a list, 
    # which is then stored. A side note here, the Ray remote function is 
    # asynchronous, therefore the data will not be processed in any particular 
    # order. Ordinarily, indices can be used to keep track of the data, 
    # but the order of the data that we sent in does not matter since 
    # it has timestamps, which can later be used to organize the data.
    processed_data_list = ray.get(futures)
    
    # Here, we merge the processed data to prepare it for storage in a CSV file
    merged_data = '\n'.join(processed_data_list)
    
    # The CSV file is then written and stored on the disk
    f = open('./Processed/Processed_{}'.format(filename), "w")
    f.write(merged_data)
    f.close()
    
    # Here, we try to upload the processed file that we exported above into 
    # an AWS S3 bucket. If the uploading fails due to any reason, a message
    # is printed to the console
    try:
        s3.meta.client
            .upload_file('./Processed/Processed_{}'.format(filename), 
                           'eu-gtfs-data', 
                           'Processed_{}'.format(filename))
    except:
        print('S3 upload failed')
        continue
        
            
    # We then remove the file that we downloaded from the archive
    os.system('rm ./Dataset/BISON/{}'.format(filename))
    # and the file we CSV file we created, since space is limited on an AWS 
    # EC2 instance
    os.system('rm ./Processed/Processed_{}'.format(filename));
    
    # And then we print to console that the file has been processed.
    # The process is then repeated for other dates.
    print('processed {}'.format(filename))
    