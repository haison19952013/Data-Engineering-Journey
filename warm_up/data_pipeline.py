import pandas as pd
import numpy as np
import os
import re
from unidecode import unidecode
from sqlalchemy import create_engine
from config import settings

def wrangle_salary(df):
    df_wrangle = df.copy()
    # make sure
    df_wrangle['salary'] = df_wrangle['salary'].str.lower()
    # remove ',' in df_wrangle['salary'] to avoid problems when extracting numbers
    df_wrangle['salary'] = df_wrangle['salary'].str.replace(',', '')
    # Add negotiable column by checking if 'thoả thuận' in df_wrangle['salary']
    df_wrangle['negotiable'] = df_wrangle['salary'].map(lambda x: 'thoả thuận' in x)
    # Extract salary_min, salary_max  if the text has a format like '10 - 20 triệu'
    df_wrangle[['salary_min', 'salary_max']] = df_wrangle['salary'].str.extract(r'(\d+)\s*-\s*(\d+)', expand=True)
    # Extract salary_unit
    df_wrangle['salary_unit'] = df_wrangle['salary'].map(lambda x: 'USD' if 'usd' in x else 'M VND')
    # Extract salary_max if the text has 'tới' in it
    df_wrangle.loc[df_wrangle['salary'].str.contains('tới', na=False), 'salary_max'] = df_wrangle['salary'].str.extract(r'(\d+)')[0]
    # Extract salary_min if the text has 'trên' in it
    df_wrangle.loc[df_wrangle['salary'].str.contains('trên', na=False), 'salary_min'] = df_wrangle['salary'].str.extract(r'(\d+)')[0]
    # Convert salary_min, salary_max to numeric type if possible
    df_wrangle[['salary_min', 'salary_max']] = df_wrangle[['salary_min', 'salary_max']].apply(pd.to_numeric, errors='coerce')
    
    # Check if all rows have a salary unit
    null_salary_units = df_wrangle['salary_unit'].isnull().sum()
    assert null_salary_units == 0, f"Found {null_salary_units} rows with null salary_unit"
    
    # Check that if negotiable is false, salary_min and salary_max cannot be null simultaneously
    invalid_non_negotiable = ((df_wrangle['negotiable'] == False) & 
                             (df_wrangle['salary_min'].isnull()) & 
                             (df_wrangle['salary_max'].isnull())).sum()
    assert invalid_non_negotiable == 0, f"Found {invalid_non_negotiable} non-negotiable rows without salary ranges"
    
    # Count rows violating the rule (negotiable is True but salary ranges exist)
    invalid_negotiable = ((df_wrangle['negotiable'] == True) & 
                         (df_wrangle['salary_min'].notnull() | df_wrangle['salary_max'].notnull())).sum()
    
    # Assert there are no invalid rows
    assert invalid_negotiable == 0, f"Found {invalid_negotiable} rows where negotiable=True but salary ranges are specified"
    
    # drop 'salary' column
    df_wrangle.drop('salary', axis=1, inplace=True)
    
    return df_wrangle

def wrangle_address(df, df_support):
    cities_list = df_support['Tỉnh/Thành phố'].unique().tolist() + ['Hồ Chí Minh']
    cities_list = [unidecode(city.lower()) for city in cities_list]
    districts_list = df_support['Tên đơn vị hành chính'].unique().tolist() 
    districts_list = [unidecode(district.lower()) for district in districts_list]
    
    def replace_special_cases(text):
        text = text.replace('Thừa Thiên Huế: TP Huế', 'Huế')
        text = text.replace('Thừa Thiên Huế', 'Huế')
        return text.replace('TP', '')
    
    def extract_city_district(text):
        cities_districts = re.split(r'[:]', text)
        cities = []
        districts = []
        for idx, city_districts in enumerate(cities_districts):
            city_districts = city_districts.strip()
            city_districts = city_districts.split(',')
            for city_district in city_districts:
                city_district_lower = unidecode(city_district.lower())
                if city_district_lower in cities_list:
                    cities.append(city_district)
                elif city_district_lower in districts_list:
                    districts.append(city_district)
                elif city_district_lower == 'nuoc ngoai':
                    cities.append(city_district)
                elif city_district_lower == 'toan quoc':
                    cities.append(city_district)
        cities = list(set(cities))
        districts = list(set(districts))
        cities = ', '.join(cities)
        districts = ', '.join(districts)
        return cities, districts
        # return cities.strip(', '), districts.strip(', ')
    df_wrangle = df.copy()
    df_wrangle['address'] = df_wrangle['address'].apply(replace_special_cases).apply(pd.Series)
    df_wrangle[['city', 'district']] = df_wrangle['address'].apply(extract_city_district).apply(pd.Series)
    # replay '' by nan in city and district columns
    df_wrangle['city'] = df_wrangle['city'].replace('', np.nan)
    df_wrangle['district'] = df_wrangle['district'].replace('', np.nan)
    # drop 'address' column
    df_wrangle.drop('address', axis=1, inplace=True)
    return df_wrangle

def wrangle_job_title(df):
    def classify_job(title):
        def normalize_text(text):
            text = unidecode(text.lower())
            return text
        
        job_categories = {
            "IT": ["software","it","programmer" ,"developer", "lap trinh", "tester", "fullstack", "aws", "android", "java","javascript" ,"web", "system", "linux","may tinh","server", "cntt","he thong", "dotnet"],
            "Data Science": ["data", "database", "machine learning", "khoa hoc du lieu", "hoc may", "analyst", "thong tin", "ai"],
            "Designer": ["designer", "thiet ke", "artist", "animation", "ux/ui"],
            "Sales": ["sales", "kinh doanh"],
            "Human Resources": ["hr", "nhan su"]
        }
        
        title = normalize_text(title)  # Normalize Vietnamese accents
        for category, keywords in job_categories.items():
            if any(keyword in title for keyword in keywords):
                return category
        return "Other"
    df_wrangle = df.copy()
    df_wrangle['category'] = df_wrangle['job_title'].apply(classify_job)
    return df_wrangle

class SQLRepository:
    def __init__(self, connection ):
        self.connection = connection

    def insert_table(self, table_name, records, if_exists='fail'):
    
        """Insert DataFrame into SQLite database as table

        Parameters
        ----------
        table_name : str
        records : pd.DataFrame
        if_exists : str, optional
            How to behave if the table already exists.

            - 'fail': Raise a ValueError.
            - 'replace': Drop the table before inserting new values.
            - 'append': Insert new values to the existing table.

            Default: 'fail'

        Returns
        -------
        dict
            Dictionary has two keys:

            - 'transaction_successful', followed by bool
            - 'records_inserted', followed by int
        """
        
        n_inserted = records.to_sql(name = table_name, con = self.connection, if_exists = if_exists, index = False)
        return {
            "transaction_successful": True,
            "records_inserted": n_inserted
        }
    
    def read_table(self, table_name, limit=None):
    
        """Read table from database.

        Parameters
        ----------
        table_name : str
            Name of table in SQLite database.
        limit : int, None, optional
            Number of most recent records to retrieve. If `None`, all
            records are retrieved. By default, `None`.

        Returns
        -------
        pd.DataFrame
            Index is DatetimeIndex "created_date".
        """
        # Create SQL query (with optional limit)
        if limit is None:
            query = f"SELECT * FROM {table_name}"
        else:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
        
        # Retrieve data, read into DataFrame
        df = pd.read_sql(query, self.connection, parse_dates=["created_date"])
        
        # Return DataFrame
        return df

class DataPipeline:
    def __init__(self, file_path = None, table_name = "job_data", sp_file_path = None):
        self.file_path = file_path
        self.sp_file_path = sp_file_path
        self.table_name = table_name
        self.df_wrangle = None
        self.connection = None
    
    def wrangle_data(self):
        # Load data from CSV file
        current_dir = os.getcwd()
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        try:
            df_wrangle = pd.read_csv(self.file_path)
        except FileNotFoundError:
            print(f"File not found: {self.file_path}")
            exit()
        try:
            df_support = pd.read_csv(self.sp_file_path, usecols = ['Tên đơn vị hành chính','Tỉnh/Thành phố'])
        except FileNotFoundError:
            print(f"File not found: {self.sp_file_path}")
            exit()
        os.chdir(current_dir)
        df_wrangle = wrangle_salary(df_wrangle)
        df_wrangle = wrangle_address(df_wrangle, df_support = df_support)
        df_wrangle = wrangle_job_title(df_wrangle)
        self.df_wrangle = df_wrangle
    
    def conn_db(self):
        # MySQL connection details
        db_user = settings.db_user
        db_password = settings.db_password
        db_host = settings.db_host
        db = settings.db

        # Create SQLAlchemy engine
        try:
            connection = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db}")
        except Exception as e:
            print("Error creating connection:", e)
            exit()
        self.connection = connection
    
    def save_to_db(self, if_exists='replace'):
        # Create SQL repository
        sql_repository = SQLRepository(self.connection)

        # Insert data into SQL table
        result = sql_repository.insert_table(self.table_name, self.df_wrangle, if_exists=if_exists)
        print("Transaction successful:", result['transaction_successful'])
        print("Number of records inserted:", result['records_inserted'])
    
    def read_from_db(self, limit=None):
        # Create SQL repository
        sql_repository = SQLRepository(self.connection)

        # Read data from SQL table
        try:
            df = sql_repository.read_table(self.table_name, limit=limit)
        except Exception as e:
            print("Error reading data:", e)
            exit()
        return df
    
if __name__ == '__main__':
    # Load data data_raw/data.csv, wrangle, save to DB
    pipeline = DataPipeline(file_path = 'data_raw/data.csv', table_name='job_data', sp_file_path = 'data_raw/vn_city_district.csv')
    pipeline.wrangle_data()
    pipeline.conn_db()
    pipeline.save_to_db(if_exists='replace')
    
    # Read data from DB
    pipeline = DataPipeline(table_name='job_data')
    pipeline.conn_db()
    df = pipeline.read_from_db(limit=None)
    print(df)
    print("Done!")
