from airflow.hooks.base import BaseHook
from airflow.plugins_manager import AirflowPlugin
import psycopg


class UserReportHook(BaseHook):
    def __init__(self, conn_id='postgres_default', dbname='airflow', *args, **kwargs):
        super().__init__(*args, **kwargs)
        conn = self.get_connection(conn_id)

        self.db_conn = psycopg.connect(host=conn.host, port=conn.port, dbname=dbname, user=conn.login,
                                       password=conn.password)

    def report(self):
        # Open a cursor to perform database operations
        with self.db_conn.cursor() as cur:
            # Execute a command: this creates a new table
            cur.execute("""
                    CREATE TABLE IF NOT EXISTS user_reports (
                        gender text,
                        num_users bigint)
                    """)

            cur.execute("""
            TRUNCATE TABLE user_reports
            """)

            cur.execute("""
                INSERT INTO user_reports(gender, num_users)
                SELECT gender, count(1)
                FROM users
                group by gender
                """)

            # Make the changes to the database persistent
            self.db_conn.commit()


class UserReportPlugin(AirflowPlugin):
    name = 'user_report'
    hooks = [UserReportHook]
