#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pandas as pd
import datetime 
import os
import glob
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import time
import csv


    
# Specify the file path and extension
folder_path = '/home/baba/MyPythonProjects/LoggerNet/AWS-LATEST'
file_path2 = '/home/baba/MyPythonProjects/init.csv'
extension = '_Table10m.dat'
awsdf = pd.read_csv(file_path2)
awsdf = awsdf.set_index('TIMESTAMP')
files = glob.glob(os.path.join(folder_path, f'*{extension}'))
#print(files)

#The mail loop to be executed hourly
while True:
    #awsdf = pd.read_csv(file_path2)
    #awsdf = awsdf.set_index('TIMESTAMP')
    

    #Loop through files in the folder

    for file_path in files:
        file_name = os.path.basename(file_path)

        #check if the current item is a file and has the specified extension
        if '_RG' in file_name:
            continue
            #create the relevant dataframe
            #try:
        try:
            df = pd.read_csv(file_path, header=1)#with open(file_name,'r', encoding='utf-8') as f:
                #csv_reader = csv.reader(f)
                #header = csv.header

            #except UnicodeDecodeError:
                #continue

            #df = pd.read_csv(file_path, header=1)#, parse_dates=True,)
            # except: UnicodeDecodeError

            #to_drop = ['RECORD', 'JobID', 'WSpd_Min', 'WSpd_Max', 'WSpd_Std', 'WDir_Std', 'BPressUnits', 'BPress_Success_Tot']
            #df.drop(to_drop, inplace=True, axis=1)
            df = df.set_index('TIMESTAMP')
            df = df.iloc[2:]
            df.index = pd.to_datetime(df.index)
            #append the dataframe to the awsdf dataframe 
            awsdf = pd.concat([awsdf, df.tail(1)])
            #print(awsdf)
        except UnicodeDecodeError:
            continue

    #convert dataframe to a csv file
    awsdf.to_csv('awsdata.csv')

    #create and send the email with the csv file attachment
    # Email configuration
    sender_email = 'awszimgcf@gmail.com'
    receiver_email = ['babamaketa@gmail.com','metcfo@gmail.com','isaacmasawana@gmail.com','james.ngomajr@gmail.com','tambupasi@yahoo.com', 'vmamombe@gmail.com']
    subject = 'GCF AWS Data'
    message = 'Please find the attached awsdata file.'
    attachment_file = 'awsdata.csv'

    # Create a multipart message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(receiver_email)
    msg['Subject'] = subject

    # Attach the message to the email
    msg.attach(MIMEText(message, 'plain'))

    # Read the attachment file
    with open(attachment_file, 'rb') as attachment:
        # Create a base object to encode the attachment
        attachment_part = MIMEBase('application', 'octet-stream')
        attachment_part.set_payload(attachment.read())

    # Encode the attachment and add headers
    encoders.encode_base64(attachment_part)
    attachment_part.add_header(
        'Content-Disposition',
        f'attachment; filename={attachment_file}'
    )

    # Attach the encoded attachment to the email
    msg.attach(attachment_part)

    # SMTP server configuration (Gmail example)
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    smtp_username = 'awszimgcf@gmail.com'
    smtp_password = 'szrgvyaneuzsynaj'

    # Create a secure connection to the SMTP server
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)

    # Send the email
    server.sendmail(sender_email, receiver_email, msg.as_string())
    server.quit()
    
    print('data sent at:')
    print(datetime.datetime.now().time())
    #os.remove('awsdata.csv')
    
    #wait for another hour
    time.sleep(60*60)
    

        


# In[ ]:




