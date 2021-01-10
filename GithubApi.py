from github import Github
from github import GithubException
import os
import time
from datetime import datetime
import logging
import MyConfig as c

logging.basicConfig(level=logging.INFO, filename='LoggingFile.log', format='%(asctime)s %(name)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)
class GithubApi():
    def __init__(self):
        try:
            logger.info("Trying to connect to github API...")
            self.token = Github(os.getenv('GITHUB_TOKEN', c.token))
            self.repo = self.token.get_repo(c.repo)
            self.file = self.repo.get_contents(c.file_path, ref=c.branch)
            self.commits = list(self.repo.get_commits(c.branch))
            logger.info("Connected to github API successfuly")
        except GithubException as err:
            logger.error(err)

    #this method get repository last commit date
    #used to be compared with DB last commit date, in order to check if the DB is uptodate or needs update
    def get_last_commit_date(self):
        try:
            logger.info("Fetching source file last update date")
            api_last_commit_date=datetime.strptime(self.repo.get_git_commit(self.commits[0].sha).last_modified, '%a, %d %b %Y %H:%M:%S %Z')  
            api_last_commit_date=time.mktime(api_last_commit_date.timetuple())
            logger.info("Repository last updated in "+ str(api_last_commit_date))
        except Exception as err:
            logger.error(err)     
        return api_last_commit_date

    #this method get repository last commit date(commit date, author, commit message)
    #used to insert last file commit data into "Commits" table on DB
    def get_last_commit_details(self):
        try:
            last_commit_data=[datetime.strptime(self.repo.get_git_commit(self.commits[0].sha).last_modified, '%a, %d %b %Y %H:%M:%S %Z').strftime('%b/%d/%Y %H:%M:%S'),
            self.repo.get_git_commit(self.commits[0].sha).author.name,
            self.repo.get_git_commit(self.commits[0].sha).message]
            last_commit_data_tmp=[last_commit_data]
        except Exception as err:
            logger.error(err)    
        return last_commit_data_tmp    
