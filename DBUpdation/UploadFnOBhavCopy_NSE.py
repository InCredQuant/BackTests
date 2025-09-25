import os
import sys
import pandas as pd
# sys.path.insert(1,'G:\Shared drives\BackTests\pycode\MainLibs')
#current = os.path.dirname(os.path.realpath(__file__))
#current = 'C:/Users/bbg.quant_incredalts/Desktop/Nilesh/PnL-Exposure'
#parent = os.path.dirname(current)
#sys.path.append('C:/Users/bbg.quant_incredalts/Desktop/Nilesh/PnL-Exposure')

#os.chdir('C:/Users/bbg.quant_incredalts/Desktop/Nilesh/PnL-Exposure')
from config import config
from sqlalchemy import create_engine
#import glob
import psycopg2
#import psycopg2.extras as extras
from io import StringIO

conn = psycopg2.connect(dbname=config['histdb_name'], user=config['username'], password='postgres', host=config['host'], port='5432')
engine = create_engine(config['histdb_uri'])
                                           
def ReadFile(zipFileName):
    cols = ['INSTRUMENT', 'SYMBOL', 'EXPIRY_DT', 'STRIKE_PR', 'OPTION_TYP', 'OPEN','HIGH', 'LOW', 'CLOSE', 'SETTLE_PR', 'CONTRACTS', 'VAL_INLAKH','OPEN_INT', 'CHG_IN_OI', 'TIMESTAMP']
    oneDayData = pd.read_csv(zipFileName, compression='zip')
    if 'OPTIONTYPE' in oneDayData.columns:
        oneDayData.rename(columns={'OPTIONTYPE':'OPTION_TYP'}, inplace=True)
    oneDayData = oneDayData[cols]
    oneDayData.dropna(subset=['SYMBOL'], inplace=True)
    #----------------------------------------------------------------------------------------
    try:
        oneDayData['EXPIRY_DT'] = pd.to_datetime(oneDayData['EXPIRY_DT'], format="%d-%b-%Y").dt.strftime('%d%b%y').str.upper()
    except:
        try:
            oneDayData['EXPIRY_DT'] = pd.to_datetime(oneDayData['EXPIRY_DT'], format="%d-%b-%y").dt.strftime('%d%b%y').str.upper()
        except:
            raise
    # ----------------------------------------------------------------------------------------
    try:
        oneDayData['TIMESTAMP'] = pd.to_datetime(oneDayData['TIMESTAMP'], format="%d-%b-%Y").dt.date
    except:
        try:
            oneDayData['TIMESTAMP'] = pd.to_datetime(oneDayData['TIMESTAMP'], format="%d-%b-%y").dt.date
        except:
            raise
    # ----------------------------------------------------------------------------------------
    #new_col = oneDayData.SYMBOL + oneDayData.STRIKE_PR.astype('str') + oneDayData.OPTION_TYP + oneDayData.EXPIRY_DT#.astype('int')
    temp1 = oneDayData.loc[oneDayData.STRIKE_PR.astype('int') == oneDayData.STRIKE_PR].copy()
    temp1.STRIKE_PR = temp1.STRIKE_PR.astype('int').astype('str')
    
    temp2 = oneDayData[oneDayData.STRIKE_PR.astype('int') != oneDayData.STRIKE_PR].copy()
    temp2.STRIKE_PR = temp2.STRIKE_PR.astype('str')
    
    oneDayData = pd.concat([temp1, temp2], axis = 0)
    new_col = oneDayData.SYMBOL + oneDayData.EXPIRY_DT + oneDayData.OPTION_TYP + oneDayData.STRIKE_PR.astype('str')
    oneDayData.insert(loc=0, column='Ticker', value=new_col)
    #oneDayData['Ticker'] = 
    return oneDayData
    
    
def InsertoDB(fullData):
    try:
        fullData.to_sql('nsefno', engine, if_exists = 'append', index = False)
        dt = fullData['TIMESTAMP'][0]
        print(f'Data Upload Success: {dt}')
    except Exception as e:
        print(f'Error: {e}')


def copy_from_stringio(conn, df, table):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    # print(df.columns)
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()



#rootdir = 'G:\Shared drives\BackTests\DB\Bhavcopy'
# for path in glob.glob(f'{rootdir}/*/**/', recursive=True):
#     print(path)

def getCSVpath(rootdir):
    """
    Get Path of ZipFiles for each year 
    Parameters:
    -----------
    rootdir: String Path of root directory which contains all the years as subdirectory

    Returns:
    filesPath: dict of paths of all ZipFiles with keys == year
    """
    paths = dict({})
    for rootdir, dirs, files in os.walk(rootdir):
        for subdir in dirs:
            years_path = os.path.join(rootdir, subdir)
            #print(subdir)
            filesPath = []
            for years_path, ydirs, yfiles in os.walk(years_path):
                #print(yfiles)
                for ysubfiles in yfiles:
                    #print(ysubfiles)
                    filesPath.append(os.path.join(years_path, ysubfiles))
                    #print(os.path.join(years_path, ysubfiles))
            paths[subdir] = filesPath
    return paths


#filesPath = getCSVpath(rootdir=rootdir)

#filesPath = {'2023':[r'G:\Shared drives\BackTests\DB\Bhavcopy\temp\\']}
# for k,v in filesPath.items():
#     filesPath[k].sort()
#     print(filesPath[k])


# for k,v in filesPath.items():
#     if k == '2004':
#         pass
#         # print(f'Uploading data for year: {k}')
#         # for filename in v:
#         #     df = ReadFile(filename)
#         #     InsertoDB(df)
#         # print(f'Uploaded data for: {k}')
#     else:
#         print(f'Uploading data for year: {k}')
#         for filename in v:
#             df = ReadFile(filename)
#             copy_from_stringio(conn=conn,df=df, table='NSEFNO')
#         print(f'Uploaded data for: {k}')

#filename = 'G:/Shared drives/BackTests/DB/Bhavcopy/temp/fo01SEP2023bhav.csv.zip'
#filename = 'C:/Users/bbg.quant_incredalts/Downloads/fo28JUL2023bhav.csv.zip'

dirPath = 'G:/Shared drives/BackTests/DB/Bhavcopy/NSE/FnO/temp/'
for filename in os.listdir(dirPath):
    df = ReadFile(os.path.join(dirPath, filename))
    copy_from_stringio(conn=conn,df=df, table='nsefno')
    
#df = ReadFile(filename)
#copy_from_stringio(conn=conn,df=df, table='NSEFNO')