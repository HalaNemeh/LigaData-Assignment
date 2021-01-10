import pandas as p
import io
import DataManipulation
from DataBase import Database
from GithubApi import GithubApi
from pandas import DataFrame
import logging

logging.basicConfig(level=logging.DEBUG, filename='LoggingFile.log', format='%(asctime)s %(name)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

#connect to github API using private access token to load repository and source file
api = GithubApi()

#connect to my postgresql DB
db = Database()

#Excecuted in first Run Only to load repo last 10 commits history to "Commits" Table in DB
logger.info("Check if Commits Table exist on DB")
msg="exist"
try:
    db.cur.execute('select * from "Commits"')
except Exception as err:
    msg = str(err)

#if this is the first run ("Commits" table doesn't exist):
#1 Create Commits table on DB with last 10 repo commits
#2 Store source CSV file to Dataframe
#3 Data manipulation 
#4 Create tables and Push the manipulated data to my DB
if "does not exist" in msg:
    logger.info("Commits Table Doesn't exist on DB")

    #Calling create_commits_table Method to create "Commits" table
    db.create_commits_table(api.commits,api.repo)

    #Store source CSV file to Dataframe, and Data Manuplation using (DataManipulation.py) methods
    logger.info("Start Data Manipulation...")
    df = p.read_csv(io.StringIO(api.file.decoded_content.decode('utf-8')))
    df = DataManipulation.Non_Comulative(df)
    columns_to_remove=['Lat','Long','Province/State']
    df = DataManipulation.Remove_Columns(df,columns_to_remove)
    df = DataManipulation.Country_Aggregation(df,"Country/Region")
     
    #Call Insert_Tables methods to create tables
    db.Insert_Tables(df)

#this is not the first run ("Commits" table exists):
#1 compare source last commit date with data last commit date("update date" column in "Commits" table in DB)
#2 Store the source new CSV file to Dataframe
#3 Data manipulation 
#4 Update tables by Pushing the new manipulated data
#5 insert last file commit data (commit date, author, commit message) to "Commits" table in DB (to be compare with in the next run)
else:
    logger.info("Commits Table exist on DB")
    
    #call method get_last_commit_date to get source last commit date
    api_last_commit_date = api.get_last_commit_date()
    #call method get_data_last_update_date to get data last commit date
    db_last_commit_date = db.get_data_last_update_date()

    #source last commit date gretter than DB last commit date, then we should update DB according to source
    if (api_last_commit_date>db_last_commit_date):
        logger.info(".......Data in DB needs Update........")

        #Store source CSV file to Dataframe, and Data Manuplation using (DataManipulation.py) methods
        df = p.read_csv(io.StringIO(api.file.decoded_content.decode('utf-8')))
        logger.info("Start Data Manipulation...")
        df = DataManipulation.Non_Comulative(df)
        columns_to_remove=['Lat','Long','Province/State']
        df = DataManipulation.Remove_Columns(df,columns_to_remove)
        df = DataManipulation.Country_Aggregation(df,"Country/Region")
     
        #Call Insert_Tables methods to replace tables with new data according to data source
        db.Insert_Tables(df)

        #insert last file commit data (commit date, author, commit message) to "Commits" table in DB
        # (to be compare with in the next run)
        # using last_commit_data_details method to get file commit data
        logger.info("start updating Commits table to include the last commit date...") 
        last_commit_data_details=api.get_last_commit_details()
        last_commit_data_df = DataFrame(last_commit_data_details,columns=['Update Date','Author','Message'])
        last_commit_data_df.to_sql('Commits', con=db.engine, if_exists='append', index=False)
        logger.info("Commits table is updated successfuly to include the last commit date...") 

    #source last commit date less than or equal to DB last commit date, then data in DB is uptodate
    else:
        logger.info("DataBase is Already Updated To the Last Version Of The File...")
    