import psycopg2
from config import load_config
import os

def read_blob(part_id, path_to_dir):
    """ Read BLOB data from a table """
    # read database configuration
    config = load_config()

    try:
        # connect to the PostgresQL database
        with  psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the SELECT statement
                cur.execute(""" SELECT part_name, file_extension, drawing_data
                                FROM part_drawings
                                INNER JOIN parts on parts.part_id = part_drawings.part_id
                                WHERE parts.part_id = %s """,
                            (part_id,))

                blob = cur.fetchone()
                
                part_name, file_extension, drawing_data = blob

                # Ensure the output directory exists
                os.makedirs(path_to_dir, exist_ok=True)
                
                # Create the file path
                file_path = os.path.join(path_to_dir, f"{part_name}.{file_extension}")

                # Write blob data into file
                with open(file_path, 'wb') as file:
                    file.write(drawing_data)

                print(f"✅ File saved: {file_path}")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
    read_blob(1, 'images/output/')
    read_blob(2, 'images/output/')                