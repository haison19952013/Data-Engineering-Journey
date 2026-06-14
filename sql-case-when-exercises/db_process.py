import psycopg2
from config import load_config
from utils import Logger

logger = Logger('logs/db_process.log')

class DB_Process():
    def __init__(self):
        self.config = load_config()
    
    @logger.log_errors(logger)
    def create_tables(self,table_dict):
        """ Create tables in the PostgreSQL database
        table dict will have the following format:
        (
            {
                table_name: ...,
                column_name: (column_name1, column_name1,...)
                column_type: (column_type1, column_type2,...)
            },...
        )
        """
        commands = []
        for table in table_dict:
            command = f"CREATE TABLE {table['table_name']} ("
            for idx, _ in enumerate(table['column_name']):
                if idx == len(table['column_name']) - 1:
                    command += f"{table['column_name'][idx]} {table['column_type'][idx]}"
                else:
                    command += f"{table['column_name'][idx]} {table['column_type'][idx]},"
            command += ");"
        commands.append(command)

        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cur:
                # execute the CREATE TABLE statement
                for command in commands:
                    cur.execute(command)

    
    @logger.log_errors(logger)
    def insert_table(self, table_dict):
        """ Insert a new vendor into the vendors table 
        
        table dict will have the following format:
        
            {
                table_name: ...,
                column_name: (column_name1, column_name2, ...),
                column_value: (column_value1, column_value2,...)
            }
        
        """

        sql = f"INSERT INTO {table_dict['table_name']} (" + ",".join([column_name for column_name in table_dict['column_name']]) + ") VALUES(" + ",".join(["%s" for column in table_dict['column_name']]) + ");"

        with psycopg2.connect(**self.config) as conn:
            with conn.cursor() as cur:
                # execute the INSERT statement
                cur.execute(sql, table_dict['column_value'])

                # commit the changes to the database
                conn.commit()

    
