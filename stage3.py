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