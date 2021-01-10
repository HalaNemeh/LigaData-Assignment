import logging

logging.basicConfig(level=logging.INFO, filename='LoggingFile.log', format='%(asctime)s %(name)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

def Non_Comulative(df):
    try:
        logger.info("Manipulating data to be non comulative...")
        i= len(df.columns)-1
        while i >=5:
            df[df.columns[i]]=df[df.columns[i]] - df[df.columns[i -1]]
            i-=1   
    except Exception as err:
        logger.error(err)        
    return df
 
def Remove_Columns(df,cols):
    try:
        logger.info("Removing Lat,Lang,Province columns...")
        for col in cols:
            df=df.drop(col,axis=1)
    except Exception as err:   
        logger.error(err)     
    return df

def Country_Aggregation(df,col):
    try:
        logger.info("Aggregating data based on country column...")
        df= df.groupby([col]).sum().reset_index()
    except Exception as err:   
        logger.error(err)          
    return df
