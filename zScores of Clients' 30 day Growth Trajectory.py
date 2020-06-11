#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
from datetime import date
import configparser
import sqlalchemy as db
import pygsheets
from oauth2client.service_account import ServiceAccountCredentials



db_config = configparser.ConfigParser()
db_config.read('dwdbconfig.ini')
db_host = db_config['mysql']['host']
db_database = db_config['mysql']['database']
db_user = db_config['mysql']['user']
db_pass = db_config['mysql']['password']
db_port = db_config['mysql']['port']

############### Pulling daily totals from data warehouse #######################
SQL = '''
    SELECT
        leads.DealerID,
        leads.DealerName,
        leads.`Date`,
        leads.Sessions,
        leads.TotalUniqueGoals,
        leads.TotalForms,
        leads.TotalCalls,
        leads.TotalChats,
        emp.FullName
    FROM `data_5d67cfa96d8c0`.`Total Conversions by Client and Date - CACHED (161)` AS leads
    JOIN `data_5d67cfa96d8c0`.`Client Accounts (22)` AS accounts ON leads.DealerID = accounts.DealerID
    JOIN `data_5d67cfa96d8c0`.`Employees (61)` AS emp ON accounts.PerformanceManagerID = emp.EmployeeID
    WHERE accounts.TerminationDate IS NULL
        '''

sql_alc_string = 'mysql+pymysql://'+db_user+':'+db_pass+'@'+db_host+':'+db_port+'/'+db_database
print("The SQL Alchemy Call: " + sql_alc_string)

db_engine = db.create_engine(sql_alc_string)
db_connection = db_engine.connect()
db_metadata = db.MetaData()

df = pd.read_sql_query(SQL, db_engine)


##################### Cleaning the master dataFrame ###########################

def lookup(s):
    """
    This is an extremely fast approach to datetime parsing.
    For large data, the same dates are often repeated. Rather than
    re-parse these, we store all unique dates, parse them, and
    use a lookup to convert all dates.
    """
    dates = {date:pd.to_datetime(date) for date in s.unique()}
    return s.map(dates)

df['Date'] = lookup(df['Date'])


"""
We now have a dataFrame with formatted dates.
Our plan will be to get the sum aggregates for each dealer
in two different dataFrames. One frame will have the previous 30 day
total of leads. The other will have the previous period. One important
factor to note is that our systems pull the data on a weekly basis so
we will need to factor that in. To deal with this the code subtracts
30 days from the most recent day of data as seen in the code below.
"""

############################# Authorize GSheets ################################

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

"""
Note that you need to go into Google Cloud Services and create a service app to
aquire the json creds. Don't forget to 'enable' google sheets and/or google drive.
You will then need to add the email in the json key to the gsheet with edit permissions.
"""

credentials = ServiceAccountCredentials.from_json_keyfile_name('PersonalGoogleDriveAPICreds.json', scope)

gc = pygsheets.authorize(service_file='PersonalGoogleDriveAPICreds.json')

######################### Begin Processing ####################################

def CalculateZScores(IntervalLengths):
    ActiveGSheet = -1
    for IntervalLength in IntervalLengths:
        ActiveGSheet += 1
        LastImportDay = df['Date'].max()
        FirstIntervalDate = LastImportDay - pd.Timedelta(days=IntervalLength*2)


        print("Being processing "+str(IntervalLength)+" day interval")

        #Removes Clients that are not active nor have enough data based on the IntervalLength
        # https://stackoverflow.com/questions/27965295/dropping-rows-from-dataframe-based-on-a-not-in-condition
        print("Last Import Day: "+str(LastImportDay))
        print("Two Intervals ago: "+str(FirstIntervalDate))
        ValidClients = df[df['Date'] <= str(FirstIntervalDate)].groupby('DealerName')['Date'].min().index.tolist()
        df_valid = df[df['DealerName'].isin(ValidClients)]
        #df

        SelectedInterval = (df_valid['Date'] >= LastImportDay - pd.Timedelta(days=IntervalLength))
        PreviousPeriod = (df_valid['Date'] < (df_valid[SelectedInterval]['Date'].min())) & (df_valid['Date'] >= (df_valid[SelectedInterval]['Date'].min() - pd.Timedelta(days=IntervalLength)))

        """
        Great! We now have the last time interval as one dataFrame and the previous
        as another dataFrame. We are going to now reduce each of these dataFrames to their
        sum totals, then merge them back together for difference calculations
        """

        # You'll notice with the way that the code works this report can be ran with different intervals.
        DealerLeads_SelectedInterval = df_valid[SelectedInterval].groupby('DealerName')[['TotalUniqueGoals','TotalForms','TotalCalls','TotalChats']].sum()
        DealerLeads_PrevPeriod = df_valid[PreviousPeriod].groupby('DealerName')[['TotalUniqueGoals','TotalForms','TotalCalls','TotalChats']].sum()

        #Renaming the columns since both dataFrames have the same column titles
        DealerLeads_PrevPeriod.columns = ['TotalUniqueGoals_prev','TotalForms_prev','TotalCalls_prev','TotalChats_prev']


        #Merging the two dataFrames
        df_merged = pd.merge(DealerLeads_SelectedInterval, DealerLeads_PrevPeriod, on='DealerName')

        #Calculating the percentage difference
        df_merged['Calculated Diff'] = (df_merged.TotalUniqueGoals - df_merged.TotalUniqueGoals_prev) / df_merged.TotalUniqueGoals_prev

        #Reducing to needed columns and sorting Values
        df_merged = df_merged[['TotalUniqueGoals','TotalUniqueGoals_prev','Calculated Diff']].sort_values(by=['Calculated Diff'], ascending=False)

        #Replacing infinite values from bad data
        df_merged.replace([np.inf, -np.inf], np.nan, inplace=True)

        #Calculating Z-Scores
        mean = df_merged['Calculated Diff'].mean()
        std = df_merged['Calculated Diff'].std()
        print('The mean is: '+str(mean))
        print('The Standard Deviation is: '+str(std))
        df_merged['Z Score'] = (df_merged['Calculated Diff'] - mean) / std


        """
        Because we used a groupby function earlier in our dataFrames we lost the names
        of the performance managers. The code below joins the names back.
        """
        df_final = pd.merge(df_merged, df[['DealerName','FullName']].drop_duplicates(), on='DealerName', how='inner')



        #This provides the image urls to be placed in our googlesheet for the thumbnails
        PM_Pics = {
            "Mark Ferguson": "https://dealerworldfiles.s3.amazonaws.com/PMs+Profile+Pics/Mark.png",
            "Cassidy Spring": "https://dealerworldfiles.s3.amazonaws.com/PMs+Profile+Pics/Cassidy.jpg",
            "Miranda Milillo": "https://dealerworldfiles.s3.amazonaws.com/PMs+Profile+Pics/Miranda.jpg",
            "Abby Frey": "https://dealerworldfiles.s3.amazonaws.com/PMs+Profile+Pics/Abby.jpg",
            "Troy Spring": "https://dealerworldfiles.s3.amazonaws.com/PMs+Profile+Pics/Troy.png"
        }

        for key in PM_Pics:
                df_final['FullName'].replace(key, '=IMAGE("'+PM_Pics[key]+'")', inplace=True)


        #Remaing columns again to make prettier for end users
        #Note that we change the percentage float to a string with % character
        df_final.columns = ['Dealer Name','Leads','Leads (Previous Period)','Calculated Diff','Z Score','PM']
        df_final['Calculated Diff'] = df_final['Calculated Diff'].map(lambda n: '{:,.2%}'.format(n))

        print('There are '+str(df_final.count())+' records in this dataFrame')
        print()
        print()
        ############################ Sending to GSheet ################################


        #open the google spreadsheet, [0] selects first sheet
        gsheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/13V8TGGw4z1aEB0hQ-NMLr9GFoBpjTS9RkFOg4Tklrws/edit?usp=sharing')[ActiveGSheet]
        print("PyGSheet Editing Sheet "+str(ActiveGSheet))

        #clear current values in selected range
        gsheet.clear(start = 'B4')

        #update the first sheet with df, (1,1) begins at A1, the first number is verticle starting with 1 (not 0)
        gsheet.set_dataframe(df_final,(3,2))

        #update a single value in the gsheet variable
        gsheet.update_value('B1', "Last Update: "+str(pd.to_datetime('today').date()))




ChosenIntervals = [30,60,90]

CalculateZScores(ChosenIntervals)
