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