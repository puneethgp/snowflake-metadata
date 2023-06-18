import os
import glob
import csv
import pandas as pd
import shutil
import snowflake.connector
import configparser
import fnmatch
from configparser import ConfigParser
import os
from datetime import datetime



class Metaframework:
    
    def __init__(self,path,stage = None):
        self.path = path
        parser = ConfigParser()
        parser.read(f'{os.path.join(self.path,"SNOWFLAKE_CREDS.cfg")}')
        account = parser.get('my_api','account')
        user = parser.get('my_api','user')
        password = parser.get('my_api','password')
        self.database = parser.get('my_api','database')
        self.schema = parser.get('my_api','schema')
        role = parser.get('my_api','role')
        warehouse = parser.get('my_api','warehouse')
       
        
        conn = snowflake.connector.connect( user = user ,password = password,account = account ,warehouse = warehouse ,
                                           database = self.database,schema = self.schema,role = role)
        self.curs = conn.cursor()
        
        if stage is None:
            self.stage = 'internal'
            self.curs.execute(f'CREATE OR REPLACE STAGE {self.stage}')
        else:
            self.stage = stage
            self.curs.execute(f'CREATE STAGE IF NOT EXISTS {self.stage};')
        
        print(f'{self.stage} has been created')
        
        
    def create_table(self,target_table,file_path,delimiter=',',replace=False):
        file_format = f"CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = '{delimiter}'  PARSE_HEADER = TRUE;"
        self.curs.execute(file_format)
        statement = ' OR REPLACE TABLE' if replace else "TABLE IF NOT EXISTS "
        query = f"""
                    CREATE {statement} {target_table.upper()}
                    USING TEMPLATE (
                           SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                           WITHIN GROUP (ORDER BY ORDER_ID)
                           FROM TABLE(
                               INFER_SCHEMA(
                                     LOCATION=>'@{self.stage}/{file_path}',
                                     FILE_FORMAT=>'my_csv_format')));
                """
        print(query)
        self.curs.execute(query)
        print(f'\n{target_table.upper()} has been created\n')
        
        
    def ingest_csv_file(self, file, table_name, skip_header = 1, field_delimiter = ',', target_operation = 'append', *args):
         
         # Put file to stage 
        import os
        full_path = os.path.join(self.path,'inbound',file)
        put_file = f"PUT file://{full_path} @{self.stage} AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
        print(put_file)
        self.curs.execute(put_file)
        
        #creating table with path and delimiter from metadata
        print(f'self.create_table({table_name},{file},delimiter = "{field_delimiter}")')
        self.create_table(table_name,file,delimiter=field_delimiter)
        
        # append or overwrite
        if target_operation.casefold() == 'overwrite':
            print(f"{table_name.upper()} is truncated")
            truncate_query = f"TRUNCATE TABLE {table_name.upper()}"  
            curs.execute(truncate_query)
        
        
        # Copy CSV file data into Snowflake table
        copy_query = f"""
                            COPY INTO {table_name.upper()} 
                            FROM '@{database}.{schema}.{self.stage}/{file}'
                            FILE_FORMAT = (
                                    TYPE = CSV   
                                    SKIP_HEADER = {skip_header}
                                    FIELD_DELIMITER = '{field_delimiter}')
                            on_error = continue;"""
        print(copy_query)
        self.curs.execute(copy_query)
        add_file_name = f"""
                        BEGIN
                          IF (NOT EXISTS(SELECT * 
                                         FROM INFORMATION_SCHEMA.COLUMNS 
                                         WHERE TABLE_NAME = '{table_name.upper()}' 
                                           AND TABLE_SCHEMA = '{self.schema}'
                                           AND COLUMN_NAME = 'FILE_ADDED')) THEN
                            ALTER TABLE IF EXISTS {table_name.upper()} ADD COLUMN FILE_ADDED VARCHAR;
                          END IF;
                        END;"""
        curs.execute(add_file_name)
        curs.execute(f"UPDATE {table_name.upper()} set FILE_ADDED = '{file}' where FILE_ADDED IS NULL")
        
        add_date =      f"""
                        BEGIN
                          IF (NOT EXISTS(SELECT * 
                                         FROM INFORMATION_SCHEMA.COLUMNS 
                                         WHERE TABLE_NAME = '{table_name.upper()}' 
                                           AND TABLE_SCHEMA = '{self.schema}'
                                           AND COLUMN_NAME = 'DATE_ADDED')) THEN
                            ALTER TABLE IF EXISTS {table_name.upper()} ADD COLUMN DATE_ADDED VARCHAR;
                          END IF;
                        END;"""

        curs.execute(add_date)
        
        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        curs.execute(f"UPDATE  {table_name.upper()} set DATE_ADDED = '{current_time}' where DATE_ADDED IS NULL")

        
    def main(self):
        
        csv_folder = os.path.join(self.path,'inbound')
        archive_folder = os.path.join(self.path,'archive')
        missing_folder = os.path.join(self.path,'missing')
        failed_folder = os.path.join(self.path,'failed')

        df = pd.read_csv('mapping/metadata1.csv',header = 0)

        for file in os.listdir(os.path.join(csv_folder)):
            matching = False
            for ind in df.index:
                if fnmatch.fnmatch(file, df['src_file_prefix'][ind]+'*.csv'):
                    if df['is_active'][ind] == 'Y':
                        delim = df['src_file_delim'][ind]
                        
                        print(f"self.ingest_csv_file({file}),{df['tgt_table'][ind]},field_delimiter = delim)")
                        self.ingest_csv_file(file,df['tgt_table'][ind],field_delimiter = delim)
                        shutil.move(os.path.join(csv_folder, file),archive_folder)

                    else:
                        print(f'Skipping CSV file {file} for table {df["tgt_table"][ind]} due to inactive flag.')
                        # Move the skipped file to the missing mapping folder
                        shutil.move(os.path.join(csv_folder, file), failed_folder)
                        print(f'CSV file "{file}" moved to missing mapping folder "{failed_folder}".')
                    matching = True
                    break
            if not matching:
                    print(f'\n No file name pattern found for CSV file "{file}" . Skipping the file.\n')
                    shutil.move(os.path.join(csv_folder, file), missing_folder)
                    print(f'\nCSV file "{file}" moved to missing mapping folder {missing_folder}".\n')
        #           
        self.curs.close()

c1 = Metaframework(r'D:\snowpark')
c1.main()