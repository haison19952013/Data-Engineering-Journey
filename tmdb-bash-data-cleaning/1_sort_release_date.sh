#!/bin/bash

# Check if file exists, download if not
if [ -f "data/tmdb-movies.csv" ]; then
    echo "File exists"
else
    echo "File does not exist, cloning from github"
    curl -o data/tmdb-movies.csv https://raw.githubusercontent.com/yinghaoz1/tmdb-movie-dataset-analysis/master/tmdb-movies.csv
fi

# Create temporary file for processed data
temp_file=$(mktemp)

# Save header with new date column
header=$(head -n 1 data/tmdb-movies.csv)
echo "release_date_iso,$header" > "$temp_file"

# Process data with awk - add date as first column only for rows with valid dates
awk -F, 'NR>1 {
    line = $0
    found_date = 0
    
    # Find if there is a field with MM/DD/YY pattern
    for(i=1; i<=NF; i++) {
        if ($i ~ /^[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{2}$/) {
            # We found the date field
            split($i, date_parts, "/")
            
            month = date_parts[1]
            day = date_parts[2]
            year = date_parts[3]
            
            # Determine century
            if (year > 30) {
                full_year = "19" year
            } else {
                full_year = "20" year
            }
            
            # Ensure leading zeros
            month = sprintf("%02d", month)
            day = sprintf("%02d", day)
            
            # Create new date
            new_date = full_year "-" month "-" day
            found_date = 1
            break
        }
    }
    
    if (found_date) {
        # Add the formatted date as the first column
        print new_date "," line
    }
}' data/tmdb-movies.csv >> "$temp_file"

# Extract header for the final file
head -n 1 "$temp_file" > solution/1-date-sorted-tmdb-movies.csv

# Sort the data (skip header) by date in reverse order and append to final file
tail -n +2 "$temp_file" | grep -v "^$" | sort -t, -k1,1r >> solution/1-date-sorted-tmdb-movies.csv

# Clean up temporary file
rm "$temp_file"

echo "Processing complete. Results saved to solution/1-date-sorted-tmdb-movies.csv"