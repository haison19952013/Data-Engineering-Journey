#!/bin/bash
# Input and output CSV file
input_file="tmdb-movies.csv"
output_file="best-director-tmdb-movies.csv"

# Temporary file to hold processed CSV
# temp_file="temp.csv"
temp_file=$(mktemp)

# Convert CSV to a tab-separated format (TSV) to handle embedded commas correctly
csvtool -t COMMA -u TAB cat "$input_file" > "$temp_file"

# Use awk to filter column 18 and keep all columns (now tab-separated)
best_director=$(csvcut -d $'\t' -c 9 "$temp_file" | csvstat --freq --freq-count 1)
echo "Best director: $best_director" > "$output_file"

# Cleanup temporary file
rm "$temp_file"

echo "Filtered data saved to $output_file"

