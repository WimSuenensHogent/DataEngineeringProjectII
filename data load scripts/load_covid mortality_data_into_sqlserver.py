from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Date
import pandas as pd
import sys
#Make connection to local sql server
#Ensure 'vaccination_data' database is created on your localhost

#connection variables
server='34.140.20.123'
database='vaccination_data'
driver='ODBC Driver 17 for SQL Server'
user=''
passw=''
database_con = f'mssql://{user}:{passw}@{server}/{database}?driver={driver}'


mortality_data_covid = 'https://epistat.sciensano.be/Data/COVID19BE_MORT.csv'

#connect
engine = create_engine(database_con)
meta = MetaData()
try:
    con = engine.connect
    print(f'connected to {database}')
    
    #read data from source
    df = pd.read_csv(mortality_data_covid)
    #print(df.head(10))
    
    # renaming pandas 'index' to 'ID'
    df.index.names = ['ID']
    #############################################
    # Here comes data cleansing later id needed #
    #############################################
    
    print("creating covid_mortality table")
    if not engine.dialect.has_table(engine, 'covid_mortality'):
        #create table in sqlserver
        covid_vaccinations = Table(
            'covid_mortality', meta,
            #setting 'ID' as primary key
            Column('ID', Integer, primary_key = True, nullable=False),
            Column('DATE', Date),
            Column('REGION', String),
            Column('AGEGROUP', String),
            Column('SEX', String),
            Column('DEATHS', Integer)
        )                
        print('table covid_mortality created')
        
        print('writing data might take some time')
        try:     
            df.to_sql('covid_mortality', con = engine, if_exists = 'append', chunksize = 1000)
            print("data written")
        except: 
            print("Oops!", sys.exc_info()[0], "occurred.")
               
    else:
        print("table covid_mortality already exists, not overwritten")
        
    #write data into sqlserver   
    #meta.create_all(engine)           
except:
    print('connection failed')



 