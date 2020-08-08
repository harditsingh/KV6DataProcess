import boto3
import pandas as pd
import urllib.request
import io
import os

input_bucket_url = 'https://eu-gtfs-data.s3.eu-central-1.amazonaws.com/'
output_bucket = 'finalized-bison-data'

def simplify_file(file_name):
    file_url = input_bucket_url + file_name
    lines = urllib.request.urlopen(file_url)
    print('Loaded file ' + file_name)
    finalized_lines = []
    for line in lines:
        line = line.decode('utf-8')
        if line.startswith('DEPARTURE') or line.startswith('ARRIVAL'):
            if line.count(',') == 11:
                line = line + ' , , '
            finalized_lines.append(line)
    print('Cleaned file')
    print('Cleaned lines: ' + str(finalized_lines.__len__()))
    df = pd.read_csv(io.StringIO('\n'.join(finalized_lines)), error_bad_lines=False)
    print('Acceptable DF rows: ' + str(df.shape[0]))
    df.columns = ['Type', 'Operator', 'LineNo', 'OperatingDay', 'JourneyNo', 'ReinfNo', 'UserStopCode', 'PassageSeqNo', 'Timestamp', 'Source', 'VehicleNo', 'Punctuality']
    #df = df.drop(['Empty'], axis=1)
    print('Cleaned df')
    df.to_csv(file_name)
    print('Saved temp CSV')
    return df

def upload_file(file_name):
    try:
        s3.meta.client.upload_file(file_name, output_bucket, 'finalized_' + file_name)
    except:
        print('S3 upload failed')
        return
    print('File uploaded to S3\n\n')
    os.system('rm ' + file_name)

s3 = boto3.resource('s3')
bucket = s3.Bucket('eu-gtfs-data')

single_filename = 'Processed_BISON_2020-04-15.csv'
simplify_file(single_filename)
upload_file(single_filename)
