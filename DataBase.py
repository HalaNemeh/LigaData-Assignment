import psycopg2
from sqlalchemy import create_engine
from pandas import DataFrame
import time
import logging
import MyConfig as c
from datetime import datetime

logging.basicConfig(level=logging.INFO, filename='LoggingFile.log', format='%(asctime)s %(name)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

class Database():
    def __init__(self, db=c.db, user=c.user,password=c.password, host=c.host, port= c.port):
        try:
            logger.info("Trying to connect to postgres DB...")
            self.conn = psycopg2.connect(database=db, user=user, password= password, host=host, port= port)
            self.cur = self.conn.cursor()
            self.engine = create_engine(c.engine) 
            logger.info("Connected to DB successfuly")
        except psycopg2.DatabaseError as err:
            logger.error(err)


    def query(self, query):
        try:
            self.cur.execute(query)
        except psycopg2.DatabaseError as err:
            logger.error(err)

    def close(self):
        self.cur.close()
        self.conn.close()

    #this method creates tables if not exists, or replace them if exist
    #storing df to DB as three tables:
    #1 Fact table which contains (data,country) forgein keys and the major (covid-19 total cases)
    #2 Date dimension which contains (index as date primary key) and date value
    #3 Country dimension which contains (index as country primary key) and country value
    def Insert_Tables(self,df):
        logger.info("Start loading data to DB...")
        #unpivote the dataframe
        df = df.melt(id_vars = 'Country/Region', var_name = 'Date', value_name = 'Total')
        ndf = df.rename(columns={'Country/Region': 'CountryRegion'})
        try:
            #looping df to create dimensions or replace them
            for i in range(len(ndf.columns) - 1):
                tmp_df = DataFrame (ndf[ndf.columns[i]].unique(),columns=[ndf.columns[i]])
                tmp_df.to_sql(ndf.columns[i], con=self.engine, if_exists='replace')
                logger.info(ndf.columns[i]+" Dimension is successfuly updated")
                tmp_dict=(tmp_df.to_dict())
                dict_tmp=tmp_dict[ndf.columns[i]]
                inv_map = {v: k for k, v in dict_tmp.items()}
                ndf[ndf.columns[i]].replace(inv_map, inplace=True)
            #create Fact table or replace it   
            ndf.to_sql('Fact', con=self.engine, if_exists='replace', index=False)
            logger.info("Fact Table is successfuly updated")
            logger.info("Data Tables in DB are successfuly updated according to source file...")
        except psycopg2.DatabaseError as err:
            logger.error(err)
        except Exception as err:
            logger.error(err) 

    #this method query "Commits" table on DB to fetch the max "update date"
    #used to be compared with repository last commit date 
    #in order to check if the DB is uptodate or needs update
    def get_data_last_update_date(self):
        try:
            logger.info("Fetching DB last update date")
            select_Query = 'select max(to_timestamp("Update Date",'+"'Mon/DD/YYYY HH24:MI:SS'"+')) as date from "Commits"'
            self.query(select_Query)
            db_last_commit_date = self.cur.fetchall() 
            db_last_commit_date = time.mktime(db_last_commit_date[0][0].timetuple())
            logger.info("DB last updated in "+  str(db_last_commit_date))
        except psycopg2.DatabaseError as err:
            logger.error(err)
        except Exception as err:
            logger.error(err)            
        return db_last_commit_date

    #this method creates "Commits" table on DB with last 10 repo commits
    #"Commits" table contains file commits data (commit date, author, commit message)
    #("Commits" table used to compare with source file last modified date to check if the DB is uptodate or needs update)
    def create_commits_table(self,commits,repo):
        try:
            logger.info("Creating Commits Table in DB with last 10 commits...")
            Commits_list = [[]]
            for i in range(10):
                g=[datetime.strptime(repo.get_git_commit(commits[i].sha).last_modified, '%a, %d %b %Y %H:%M:%S %Z').strftime('%b/%d/%Y %H:%M:%S'),
                repo.get_git_commit(commits[i].sha).author.name,
                repo.get_git_commit(commits[i].sha).message]
                Commits_list.append(g)

            new_Commits_list = Commits_list[1:]
            Commits_df = DataFrame(new_Commits_list,columns=['Update Date','Author','Message'])
            Commits_df.to_sql('Commits', con=self.engine, if_exists='replace', index=False)
            logger.info("Commits Table Created successfuly")
        except psycopg2.DatabaseError as err:
            logger.error(err)
        except Exception as err:
            logger.error(err)       