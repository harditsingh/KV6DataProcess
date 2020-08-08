# Data Cleanup
As has been metioned in the thesis, the data cleanup was done in two major parts: one was converting all the data from the very inefficient XML format into something more efficient, like CSV (this was also done since CSV files are much easier to work with in different Python libraries), and second was extracting only the useful messages that would be pertinent to our research.

## Step 1: The raw to CSV conversion

To properly understand how this process might go, we should first understand how one of the messages might look in the raw XML dataset. Here's a few sample messages from a raw KV6 XML file:

#### Sample ONSTOP message

`<ONSTOP>
    <dataownercode>CXX</dataownercode>
    <lineplanningnumber>A005</lineplanningnumber>
    <operatingday>2019-12-01</operatingday>
    <journeynumber>7065</journeynumber>
    <reinforcementnumber>0</reinforcementnumber>
    <userstopcode>40019640</userstopcode>
    <passagesequencenumber>0</passagesequencenumber>
    <timestamp>2019-12-01T23:59:59+01:00</timestamp>
    <source>VEHICLE</source>
    <vehiclenumber>5266</vehiclenumber>
    <punctuality>0</punctuality>
</ONSTOP>`

#### Sample ONROUTE message

`<ONROUTE>
    <dataownercode>CXX</dataownercode>
    <lineplanningnumber>M300</lineplanningnumber>
    <operatingday>2019-12-01</operatingday>
    <journeynumber>7246</journeynumber>
    <reinforcementnumber>0</reinforcementnumber>
    <userstopcode>56230610</userstopcode>
    <passagesequencenumber>0</passagesequencenumber>
    <timestamp>2019-12-01T23:59:59+01:00</timestamp>
    <source>VEHICLE</source>
    <vehiclenumber>9346</vehiclenumber>
    <punctuality>-60</punctuality>
    <distancesincelastuserstop>3057</distancesincelastuserstop>
    <rd-x>106738</rd-x>
    <rd-y>484060</rd-y>
</ONROUTE>`

The KV6 data archive that were obtained were in tabular form such that there were three columns: Time, Data and Hash. Time contained the timestamp when a message was received, Data contained one or more messages received at the given timestamp, and the Hash column contained the checksum. In the Data column for each of the rows, the messages were present one or more parent tag 'KV6posinfo', like such:

`<KV6posinfo>
    <ONSTOP>...</ONSTOP>
    <DEPARTURE>...</DEPARTURE>
    ......
</KV6posinfo>
<KV6posinfo>
    ......
</KV6posinfo>
<KV6posinfo>
    ......
</KV6posinfo>`

Now that we know what the messages look like, let us take a look at the code.


```python
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
    
```

After a raw file is processed, it is stored in an AWS S3 bucket. The main reason for choosing AWS S3 for storage solutions was that since the data was being processed on an AWS EC2 instance, there would be no file transfer fee, and the upload and download speeds would be high.

This is the head of a processed file which is store in the S3 bucket:

`
ONROUTE, CXX, A300, 2020-04-13, 7064, 0, 60000070, 0, 2020-04-13T23:59:59+02:00, VEHICLE, 5443, -37, 1328, 188221, 429736
ARRIVAL, CXX, Z160, 2020-04-13, 7031, 0, 53480210, 0, 2020-04-13T23:59:59+02:00, SERVER, 5878, -60
ONSTOP, CXX, M341, 2020-04-13, 7100, 0, 57142225, 0, 2020-04-13T23:59:59+02:00, VEHICLE, 2730, 60
ONROUTE, CXX, Z436, 2020-04-13, 7034, 0, 74380150, 0, 2020-04-13T23:59:59+02:00, VEHICLE, 5902, 1, 3556, 86323, 413290
ONSTOP, CXX, L322, 2020-04-13, 7062, 0, 64000010, 0, 2020-04-13T23:59:59+02:00, VEHICLE, 1239, 0
ONSTOP, CXX, F301, 2020-04-13, 7063, 0, 59405020, 0, 2020-04-14T00:00:00+02:00, VEHICLE, 5843, 0
ARRIVAL, RET, M008, 2020-04-13, 934703, 0, HA8135, 0, 2020-04-14T00:00:00.7298993+02:00, SERVER, 0, -15
ARRIVAL, GVB, 2, 2020-04-13, 273, 0, 06071, 0, 2020-04-13T23:59:59.895936+02:00, SERVER, 2107, -62
DEPARTURE, GVB, 2, 2020-04-13, 273, 0, 06071, 0, 2020-04-13T23:59:59.895936+02:00, SERVER, 2107, -62
ONROUTE, QBUZZ, g503, 2020-04-13, 7087, 0, 10005910, 0, 2020-04-13T23:59:58.000+02:00, VEHICLE, 7434, 19, 562, 230731, 580061
ONSTOP, QBUZZ, u007, 2020-04-13, 7121, 0, 50000132, 0, 2020-04-13T23:59:59.000+02:00, VEHICLE, 4022, -300, 135926, 455594
DEPARTURE, QBUZZ, u028, 2020-04-13, 7161, 0, 51119800, 0, 2020-04-13T23:59:59.000+02:00, VEHICLE, 4215, 297, 131620, 454787
ONROUTE, QBUZZ, m078, 2020-04-13, 7009, 0, 51940060, 0, 2020-04-13T23:59:59.000+02:00, VEHICLE, 6656, 194, 2441, 122995, 436161
ONSTOP, QBUZZ, u008, 2020-04-13, 7123, 0, 50000680, 0, 2020-04-13T23:59:59.000+02:00, VEHICLE, 4049, -60, -1, -1
`


As we can see, this file is still far from perfect: there were no proper column names, some files were missing coorinate values, and many such issues. Moreover, we were not able to filter out the Arrival and Departure messages that were pertinent to our research, since going any deeper than we did in Step 1 breached the limits os cyclomatic complexity allowed by the Ray library. Therefore arose the need for Step 2.

## Step 2: Data Finalization
In this step, we basically did all the things that we were unable to do during the initial converision and cleanup process. We basically loaded the files produced in Step 1 from the S3 bucket, and did advanced cleanup to make the files much more usable during the analysis processes. 

Since we are already familiar with the files that we are working with, we will directly jump into the code for the data finalization step:


```python
# Importing the required libraries
import boto3
import pandas as pd
import urllib.request
import io
import os

# Defining the input and output S3 bucket details
base_bucket_url = 'https://{}.s3.eu-central-1.amazonaws.com/'
input_bucket_name = 'eu-gtfs-data'
output_bucket_name = 'finalized-bison-data'

# And creating other objects relevant to the S3
s3 = boto3.resource('s3')
input_bucket = s3.Bucket(input_bucket_name)
output_bucket = s3.Bucket(output_bucket_name)

# This function cleans and simplifies the dataset, and then saves it 
# in a local directory
def simplify_file(file_name):
    # Building the URL for the file in the S3 bucket
    file_url = base_bucket_url.format(input_bucket_name) + file_name
    
    # And then loading the file from the bucket
    lines = urllib.request.urlopen(file_url)
    print('Loaded file ' + file_name)
    
    # Here, we loop over the rows of the file that we loaded into memory 
    # from the S3 bucket. We clean this file and make is such that it 
    # can be loaded into a Pandas dataframe.
    finalized_lines = []
    for line in lines:
        line = line.decode('utf-8')
        
        # Filtering out only the Departure and the Arrival messages since 
        # they are the most relevant to our analysis
        if line.startswith('DEPARTURE') or line.startswith('ARRIVAL'):
            if line.count(',') == 11:
                # In here, we fix the rows which are missing the coordinate 
                # data. Since this is a CSV, we simply add two commas before
                # the end of line. This way, the columns would be empty for 
                # the rows with no coordinate data, but the CSV file would be 
                # valid, and therefore we would be able to import it into a 
                # Pandas dataframe. We can decide whether to ignore these 
                # rows or to ignore the coordinate columns much later into 
                # the analysis process.
                last_char_index = line.rfind("\n")
                line = line[:last_char_index] + 
                        ' , , ' + 
                        line[last_char_index:]
                
            # Once the lines are cleaned and filtered, they are added to 
            # a list of rows.
            finalized_lines.append(line) 
            
    # A message is printed to the console when the whole file has been cleaned
    print('Cleaned file')
    
    # The finalzed lines are then used to create a StringIO string stream, 
    # and the bad lines are simply ignored. This string stream is then 
    # used to create a Pandas dataframe. This way, any problems in the file 
    # being processed can be caught at this stage. 
    df = pd.read_csv(io.StringIO('\n'.join(finalized_lines)), 
                     error_bad_lines=False)
    
    # The names of columns are added in accordance with the KV6 standards
    df.columns = ['Type', 'Operator', 'LineNo', 'OperatingDay', 
                  'JourneyNo', 'ReinfNo', 'UserStopCode', 
                  'PassageSeqNo', 'Timestamp', 'Source', 'VehicleNo', 
                  'Punctuality']

    # The dataframe is then saved in a CSV file locally. A message 
    # is then printed to the console.
    df.to_csv(file_name)
    print('Saved temp CSV')


# In this function, we try to upload the processed file that we 
# exported above into an AWS S3 bucket. If the uploading fails 
# due to any reason, a message is printed to the console
def upload_file(filename, s3_object):
    try:
        s3_object.upload_file(filename, 
                              output_bucket_name, 
                              'finalized_{}'.format(filename))
    except:
        print('S3 upload failed')
        return
    
    # A message is printed to the console if file has been uploaded 
    # successfully
    print('File uploaded to S3\n\n')
    # And then the file which was created locally is removed.
    os.system('rm {}'.format(filename))

# A list of already finalized files present in the output bucket is 
# queried
finalized_files = [item.key for item in output_bucket.objects.all()]

# We then loop over all the unprocessed files in the input bucket
for item in input_bucket.objects.all():
    # We check if any of those files have already been processed so
    # as to avoid duplicates
    if 'finalized_{}'.format(item.key) not in finalized_files:
        # And we then call the functions that we defined earlier
        simplify_file(item.key, s3.meta.client)
        upload_file(item.key)
    else:
        print('Already processed file: {}'.format(item.key))
```

This script finalizes the file and exports a much cleaner and much more ready to use dataset. The files at this point each contain the ```DEPARTURE``` and ```ARRIVAL``` messages from all the public transport vehicles in the Netherlands which send out their data into the KV6 stream, for any given day. This is what the a sample file looks like after being processed in Step 2:


```python
import pandas as pd
df = pd.read_csv('./finalized_Processed_BISON_2020-04-14.csv', 
                 index_col=0)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Type</th>
      <th>Operator</th>
      <th>LineNo</th>
      <th>OperatingDay</th>
      <th>JourneyNo</th>
      <th>ReinfNo</th>
      <th>UserStopCode</th>
      <th>PassageSeqNo</th>
      <th>Timestamp</th>
      <th>Source</th>
      <th>VehicleNo</th>
      <th>Punctuality</th>
      <th>Lat</th>
      <th>Long</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ARRIVAL</td>
      <td>RET</td>
      <td>M008</td>
      <td>2020-04-13</td>
      <td>934703</td>
      <td>0</td>
      <td>HA8135</td>
      <td>0</td>
      <td>2020-04-14T00:00:00.7298993+02:00</td>
      <td>SERVER</td>
      <td>0</td>
      <td>-15</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>1</th>
      <td>ARRIVAL</td>
      <td>GVB</td>
      <td>2</td>
      <td>2020-04-13</td>
      <td>273</td>
      <td>0</td>
      <td>06071</td>
      <td>0</td>
      <td>2020-04-13T23:59:59.895936+02:00</td>
      <td>SERVER</td>
      <td>2107</td>
      <td>-62</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>2</th>
      <td>DEPARTURE</td>
      <td>GVB</td>
      <td>2</td>
      <td>2020-04-13</td>
      <td>273</td>
      <td>0</td>
      <td>06071</td>
      <td>0</td>
      <td>2020-04-13T23:59:59.895936+02:00</td>
      <td>SERVER</td>
      <td>2107</td>
      <td>-62</td>
      <td></td>
      <td></td>
    </tr>
    <tr>
      <th>3</th>
      <td>DEPARTURE</td>
      <td>QBUZZ</td>
      <td>u028</td>
      <td>2020-04-13</td>
      <td>7161</td>
      <td>0</td>
      <td>51119800</td>
      <td>0</td>
      <td>2020-04-13T23:59:59.000+02:00</td>
      <td>VEHICLE</td>
      <td>4215</td>
      <td>297</td>
      <td>131620</td>
      <td>454787</td>
    </tr>
    <tr>
      <th>4</th>
      <td>DEPARTURE</td>
      <td>HTM</td>
      <td>3</td>
      <td>2020-04-13</td>
      <td>38073</td>
      <td>0</td>
      <td>2010</td>
      <td>0</td>
      <td>2020-04-13T23:59:57.994+02:00</td>
      <td>VEHICLE</td>
      <td>4047</td>
      <td>28</td>
      <td>78829</td>
      <td>455037</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>1043010</th>
      <td>DEPARTURE</td>
      <td>RET</td>
      <td>43</td>
      <td>2020-04-14</td>
      <td>943974</td>
      <td>0</td>
      <td>HA1297</td>
      <td>0</td>
      <td>2020-04-14T23:59:59.03+02:00</td>
      <td>VEHICLE</td>
      <td>2103</td>
      <td>25</td>
      <td>89771</td>
      <td>436302</td>
    </tr>
    <tr>
      <th>1043011</th>
      <td>DEPARTURE</td>
      <td>QBUZZ</td>
      <td>u073</td>
      <td>2020-04-14</td>
      <td>8145</td>
      <td>0</td>
      <td>50200110</td>
      <td>0</td>
      <td>2020-04-14T23:59:58.000+02:00</td>
      <td>VEHICLE</td>
      <td>4560</td>
      <td>53</td>
      <td>144846</td>
      <td>457171</td>
    </tr>
    <tr>
      <th>1043012</th>
      <td>DEPARTURE</td>
      <td>QBUZZ</td>
      <td>g503</td>
      <td>2020-04-14</td>
      <td>6104</td>
      <td>0</td>
      <td>10009019</td>
      <td>0</td>
      <td>2020-04-14T23:59:58.000+02:00</td>
      <td>VEHICLE</td>
      <td>7434</td>
      <td>54</td>
      <td>233898</td>
      <td>581204</td>
    </tr>
    <tr>
      <th>1043013</th>
      <td>DEPARTURE</td>
      <td>QBUZZ</td>
      <td>m003</td>
      <td>2020-04-14</td>
      <td>8071</td>
      <td>0</td>
      <td>53607400</td>
      <td>0</td>
      <td>2020-04-14T23:59:59.000+02:00</td>
      <td>VEHICLE</td>
      <td>6102</td>
      <td>0</td>
      <td>105066</td>
      <td>423107</td>
    </tr>
    <tr>
      <th>1043014</th>
      <td>DEPARTURE</td>
      <td>RET</td>
      <td>M008</td>
      <td>2020-04-14</td>
      <td>953147</td>
      <td>0</td>
      <td>HA8138</td>
      <td>0</td>
      <td>2020-04-14T23:59:59.793954+02:00</td>
      <td>SERVER</td>
      <td>0</td>
      <td>5</td>
      <td></td>
      <td>9</td>
    </tr>
  </tbody>
</table>
<p>1043015 rows × 14 columns</p>
</div>



## Some relevant information about the cleanup process
- Each of the raw XML files that we picked up from the KV6 were approximately `6GB` in size, which was not a surprise since they were in XML format, not the most ideal format to store data.

- At the end of the first Step 1, the file that we put up here as an example, was around `390MB` in size, which an approximately `90%` reduction in file size.

- We initially tried to process this file on a MacBook with an `i7-7700HQ` processor and `16GB` of RAM, and despite the code using the Ray multiprocessing library, it took approximately 30 minutes to perform Step 1 on a raw XML file. This directly translates to approximately 15 hours of non-stop processing time to just process a month's worth of data.

- To speed up the process, the operation was moved to an AWS EC2 instance of type c5.9xlarge, which employ Intel Xeon Platinum 8000 series processors. This instance had `36 vCPUs`, `72GB` of RAM and `30GB` of storage. One such instance was able to run two of these Step 1 scripts simultaneously.

- Moving to the EC2 instance gave a huge bump in processing speeds, with each file only taking `3 minutes` to complete Step 1. Also, since one of the EC2 instances were able to run two instances of the Step 1 script, we were now able to process `2 files every 3 minutes`, or `40 files an hour`, which is clearly a huge increase compared to the previous 2 files an hour on a personal computer.

- The EC2 instance costs were also relatively low. To run the `c5.9xlarge` instance, it cost us about `$1.78` an hour, which means less than `4 cents` for a single file.

- After the completion of Step 2, the files were further reduced to about `150MB`, which directly translates to an approximate `98%` reduction in the file size when compared to the initial raw XML dataset


## Some stats for the files

- Initial raw XML files contained about 6 million messages

- The files after the Step 1 contained a very similar amount of messages

- The files after Step 2 contained about 1 million messages, only of type `DEPARTURE` and `ARRIVAL`


## Step 3: Reducing the dataset for the our research
After step 2, we have daily `DEPARTURE` and `ARRIVAL` data from all over the Netherlands. For the sake of this research, we feel that analyzing the transport data from all of the Netherlands would be too big of a scope. So we narrow it down to only contain data from the EBS transport company from Haaglanden.

This is where the KV1 database is first use. Since this is a static dataset, and it does not change for long periods of time, we can manually download the dataset related to the months that we are going to be analyzing. 

To filter out the required data from the whole dataset, we will have to use the `LINEXXXXXX.TMI` and `PUJOPASSXX.TMI` from the KV1 dataset that we obtain from the ndovloket.nl. Let us first take a look at the columns of the files that we will be using.

The files that we obtained from `ndovloket.nl` are all in a 'pipe-separated' format, so to say, so they can easily be imported using the Pandas library. This is how the `PUJOPASSXX.TMI` looks like:

`
[Recordtype]|[Version number]|[Implicit/Explicit]|[DataOwnerCode]|{OrganizationalUnitCode]|[ScheduleCode]|[ScheduleTypeCode]|[LinePlanningNumber]|[JourneyNumber]|[StopOrder]|[JourneyPatternCode]|[UserStopCode]|[TargetArrivalTime]|[TargetDepartureTime]|[WheelChairAccessible]|[DataOwnerIsOperator]
PUJOPASS|1|I|EBS|h030|31202|Sunday|3030|7001|1|15|54410053|05:54:00|05:54:00|ACCESSIBLE|TRUE
PUJOPASS|1|I|EBS|h030|31202|Sunday|3030|7001|2|15|54418916|05:54:51|05:54:51|ACCESSIBLE|TRUE
PUJOPASS|1|I|EBS|h030|31202|Sunday|3030|7001|3|15|54418503|05:55:06|05:55:06|ACCESSIBLE|TRUE
PUJOPASS|1|I|EBS|h030|31202|Sunday|3030|7001|4|15|54418504|05:55:28|05:55:28|ACCESSIBLE|TRUE
PUJOPASS|1|I|EBS|h030|31202|Sunday|3030|7001|5|15|54412320|05:56:29|05:56:29|ACCESSIBLE|TRUE
PUJOPASS|1|I|EBS|h030|31202|Sunday|3030|7001|6|15|54410010|05:57:23|05:57:23|ACCESSIBLE|TRUE
PUJOPASS|1|I|EBS|h030|31202|Sunday|3030|7001|7|15|54417001|05:57:34|05:57:34|ACCESSIBLE|TRUE
PUJOPASS|1|I|EBS|h030|31202|Sunday|3030|7001|8|15|54317003|06:00:14|06:00:14|ACCESSIBLE|TRUE
PUJOPASS|1|I|EBS|h030|31202|Sunday|3030|7001|9|15|54110430|06:01:00|06:01:00|ACCESSIBLE|TRUE
.....
`

The `PUJOPASSXX.TMI` basically contains the mapping between the LineNo (or the LinePlanningNo) and the user stop codes (which are collected under the Central Halte Bestand). Now, the stop codes can be used to filter out the entries which are in the database of EBS HGL. We can then use the mappings to directly link them to the bus numbers and destinations.

The code this time is much simpler, we will just be reading all the files from the finalized S3 bucket for a given time period, copy all the relevant messages into a temporary set, and will then create a single file for all the messages for the selected period. Let us take a look at the code:


```python
# Importing the relevant libraries
import pandas as pd
import requests
import boto3
import io
import os
import datetime as dt

# Importing the PUJOPASSXX.TMI file, creating the UserStopCode to 
# LinePlanningNumber mapping and getting a unique list of stop codes.
journeys = pd.read_csv(
    'KV1_EBS_Corona_Haaglanden_2020-04-26_2020-06-26_2/PUJOPASSXX.TMI', 
    delimiter='|')
journey_stop_mappings = dict(zip(
    journeys['[UserStopCode]'], 
    journeys['[LinePlanningNumber]']))
stop_codes = journeys['[UserStopCode]'].unique()

# Setting the starting date and the time period for which to filter
# and merge the finalized files from Step 2
start_date = dt.datetime(2020, 4, 1)
day_count = 29

# Initializing the S3 object
s3 = boto3.resource('s3')

# Defining the URL and name of the input and output buckets. They are
# the same in this case since the files are being loaded from and 
# stored in different directories of the same bucket
base_url = "https://finalized-bison-data.s3.eu-central-1.amazonaws.com/"
output_bucket_name = 'finalized-bison-data'

# Creating an empty list for all the daily filtered dataframes
df_list = []

# Here, we will be looping over all the dates in the tinme period and
# will be downloading the finalized datasets. The relevant data would
# then be filtered out from all these datasets.

for single_date in (start_date + dt.timedelta(n) for n in range(day_count)):
    # Building the filename from the date
    filename = 'finalized_Processed_BISON_{}.csv'
                    .format(single_date.strftime("%Y-%m-%d"))
    
    # and then getting the dataset from the S3 bucket
    content = requests.get(base_url + filename).content
    
    # Once the data is received from the S3 bucket, the files are
    # converted to a string stream, which is used to create a Pandas
    # dataframe.
    df = pd.read_csv(io.StringIO(s.decode('utf-8')), index_col=0)
    
    # All the rows containing null values are dropped
    df.dropna(inplace=True)
    
    # Converting all the user stop codes to numerals, removing strings
    df = df[pd.to_numeric(df.UserStopCode, errors='coerce').notnull()]
    df['UserStopCode'] = [int(x[1:]) for x in df['UserStopCode'].values]
    
    # Filtering out the messages which have a user stop code which
    # matches one of the stop codes from the EBS HGL KV1 database
    filtered_df = df[df['UserStopCode'].isin(stop_codes)]
    
    # Adding the column LinePlanningNo correctly to the table
    filtered_df['LinePlanningNo'] = 
                    [journey_stop_mappings[key] 
                     for key in filtered_df['UserStopCode']]
        
    # And then finally dropping all the columns that we think are either
    # not useful or pertinent to our analysis
    filtered_df = filtered_df
                    .drop(
                        ['ReinfNo', 'PassageSeqNo', 'Source', 
                         'OperatingDay', 'LineNo']
                    , axis=1)
                    .reset_index()
                    .drop('index', axis=1)
    
    # Finally we add the current dataframe of a single day to the list of
    # daily filtered dataframes
    df_list.append(filtered_df)
    
    # And then print to the console that a file has been read and filtered
    print('Filtered file:' + filename)

# The name for the merged file is built
merged_filename = 'merged_EBS_HGL_KV6_{}.csv'
                    .format(start_date.strftime("%Y-%m"))

# Once all the datasets have been filtered, we simply concatenate all of
# them into a single dataset, which is then exported as a CSV
finalized_df = pd.concat(df_list, ignore_index=True)
finalized_df.to_csv(merged_filename, index=False)

    
# And then we upload the file to the S3 bucket
try:
    s3.meta.client.upload_file(merged_filename, 
                               output_bucket_name, 
                               './MergedData/{}'.format(merged_filename))
except:
    print('S3 upload failed')
    
# And then ultimately delete the file that was exported locally
os.system('rm ' + merged_filename)
```

So, to summarize, this piece of code would basically be taking all the finalized datasets for a specified period, will filter out only the messages that we need for our analysis, in this case that would be the one related to EBS Haaglanden transit network, and will give us a merged dataset for the time period. 

This is how a merged dataset looks after performing the Step 3:


```python
import pandas as pd
df = pd.read_csv('merged_EBS_HGL_KV6_2020-04.csv')
df.drop('LineNo', axis=1).to_csv('merged_EBS_HGL_KV6_2020-04_1.csv', index=False)
```

At this point, we can say that the dataset is ready for the analysis that we plan to conduct, therefore we can conclude the data cleanup phase.


```python
journeys = pd.read_csv(
    'KV1_EBS_Corona_Haaglanden_2020-04-26_2020-06-26_2/PUJOPASSXX.TMI', 
    delimiter='|')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>[Recordtype]</th>
      <th>[Version number]</th>
      <th>[Implicit/Explicit]</th>
      <th>[DataOwnerCode]</th>
      <th>{OrganizationalUnitCode]</th>
      <th>[ScheduleCode]</th>
      <th>[ScheduleTypeCode]</th>
      <th>[LinePlanningNumber]</th>
      <th>[JourneyNumber]</th>
      <th>[StopOrder]</th>
      <th>[JourneyPatternCode]</th>
      <th>[UserStopCode]</th>
      <th>[TargetArrivalTime]</th>
      <th>[TargetDepartureTime]</th>
      <th>[WheelChairAccessible]</th>
      <th>[DataOwnerIsOperator]</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>PUJOPASS</td>
      <td>1</td>
      <td>I</td>
      <td>EBS</td>
      <td>h030</td>
      <td>31202</td>
      <td>Sunday</td>
      <td>3030</td>
      <td>7001</td>
      <td>1</td>
      <td>15</td>
      <td>54410053</td>
      <td>05:54:00</td>
      <td>05:54:00</td>
      <td>ACCESSIBLE</td>
      <td>True</td>
    </tr>
    <tr>
      <th>1</th>
      <td>PUJOPASS</td>
      <td>1</td>
      <td>I</td>
      <td>EBS</td>
      <td>h030</td>
      <td>31202</td>
      <td>Sunday</td>
      <td>3030</td>
      <td>7001</td>
      <td>2</td>
      <td>15</td>
      <td>54418916</td>
      <td>05:54:51</td>
      <td>05:54:51</td>
      <td>ACCESSIBLE</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2</th>
      <td>PUJOPASS</td>
      <td>1</td>
      <td>I</td>
      <td>EBS</td>
      <td>h030</td>
      <td>31202</td>
      <td>Sunday</td>
      <td>3030</td>
      <td>7001</td>
      <td>3</td>
      <td>15</td>
      <td>54418503</td>
      <td>05:55:06</td>
      <td>05:55:06</td>
      <td>ACCESSIBLE</td>
      <td>True</td>
    </tr>
    <tr>
      <th>3</th>
      <td>PUJOPASS</td>
      <td>1</td>
      <td>I</td>
      <td>EBS</td>
      <td>h030</td>
      <td>31202</td>
      <td>Sunday</td>
      <td>3030</td>
      <td>7001</td>
      <td>4</td>
      <td>15</td>
      <td>54418504</td>
      <td>05:55:28</td>
      <td>05:55:28</td>
      <td>ACCESSIBLE</td>
      <td>True</td>
    </tr>
    <tr>
      <th>4</th>
      <td>PUJOPASS</td>
      <td>1</td>
      <td>I</td>
      <td>EBS</td>
      <td>h030</td>
      <td>31202</td>
      <td>Sunday</td>
      <td>3030</td>
      <td>7001</td>
      <td>5</td>
      <td>15</td>
      <td>54412320</td>
      <td>05:56:29</td>
      <td>05:56:29</td>
      <td>ACCESSIBLE</td>
      <td>True</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>138580</th>
      <td>PUJOPASS</td>
      <td>1</td>
      <td>I</td>
      <td>EBS</td>
      <td>h456</td>
      <td>31282</td>
      <td>Saturday</td>
      <td>3456</td>
      <td>7069</td>
      <td>19</td>
      <td>11</td>
      <td>54391230</td>
      <td>22:48:45</td>
      <td>22:48:45</td>
      <td>ACCESSIBLE</td>
      <td>True</td>
    </tr>
    <tr>
      <th>138581</th>
      <td>PUJOPASS</td>
      <td>1</td>
      <td>I</td>
      <td>EBS</td>
      <td>h456</td>
      <td>31282</td>
      <td>Saturday</td>
      <td>3456</td>
      <td>7069</td>
      <td>20</td>
      <td>11</td>
      <td>54398479</td>
      <td>22:48:53</td>
      <td>22:48:53</td>
      <td>ACCESSIBLE</td>
      <td>True</td>
    </tr>
    <tr>
      <th>138582</th>
      <td>PUJOPASS</td>
      <td>1</td>
      <td>I</td>
      <td>EBS</td>
      <td>h456</td>
      <td>31282</td>
      <td>Saturday</td>
      <td>3456</td>
      <td>7069</td>
      <td>21</td>
      <td>11</td>
      <td>54398480</td>
      <td>22:49:19</td>
      <td>22:49:19</td>
      <td>ACCESSIBLE</td>
      <td>True</td>
    </tr>
    <tr>
      <th>138583</th>
      <td>PUJOPASS</td>
      <td>1</td>
      <td>I</td>
      <td>EBS</td>
      <td>h456</td>
      <td>31282</td>
      <td>Saturday</td>
      <td>3456</td>
      <td>7069</td>
      <td>22</td>
      <td>11</td>
      <td>54391250</td>
      <td>22:49:59</td>
      <td>22:49:59</td>
      <td>ACCESSIBLE</td>
      <td>True</td>
    </tr>
    <tr>
      <th>138584</th>
      <td>PUJOPASS</td>
      <td>1</td>
      <td>I</td>
      <td>EBS</td>
      <td>h456</td>
      <td>31282</td>
      <td>Saturday</td>
      <td>3456</td>
      <td>7069</td>
      <td>23</td>
      <td>11</td>
      <td>54390156</td>
      <td>22:52:00</td>
      <td>22:52:00</td>
      <td>ACCESSIBLE</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
<p>138585 rows × 16 columns</p>
</div>




```python

```
