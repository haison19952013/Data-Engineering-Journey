#!/bin/bash
# Input and output CSV file
input_file="tmdb-movies.csv"
output_file="filtered-tmdb-movies.csv"

# Temporary file to hold processed CSV
# temp_file="temp.csv"
temp_file=$(mktemp)

# Convert CSV to a tab-separated format (TSV) to handle embedded commas correctly
csvtool -t COMMA -u TAB cat "$input_file" > "$temp_file"

# Use awk to filter column 18 and keep all columns (now tab-separated)
awk -F'\t' 'NR==1 || $18 > 7.5' "$temp_file" > "$output_file"

# Convert back to proper CSV format
csvtool -t TAB -u COMMA cat "$output_file" > "$output_file.tmp" && mv "$output_file.tmp" "$output_file"

# Cleanup temporary file
rm "$temp_file"

echo "Filtered data saved to $output_file"

