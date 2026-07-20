## Overview

Now it's time to practice more with `connection`, `operator`, `sensor`, and `hook`.

Build a data pipeline on Airflow that fulfils the following requirements:

1. Extract data from the API [https://dummyjson.com/products](https://dummyjson.com/products). The API returns a list of `products`.
2. Save the extracted data to a `.csv` file.
3. Create a table named `products` in the Postgres database.
4. Load the data from the `.csv` file into the `products` table.