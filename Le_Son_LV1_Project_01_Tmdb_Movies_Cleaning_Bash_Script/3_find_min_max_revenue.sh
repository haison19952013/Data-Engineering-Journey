#!/bin/bash
# Input and output CSV file
input_file="data/tmdb-movies.csv"
output_file_min="solution/3-min-revenue-tmdb-movies.csv"
output_file_max="solution/3-max-revenue-tmdb-movies.csv"

# Temporary file to hold processed CSV
temp_file=$(mktemp)

# Convert CSV to a tab-separated format (TSV) to handle embedded commas correctly
csvtool -t COMMA -u TAB cat "$input_file" > "$temp_file"
# get min, max
head -n 1 "$temp_file" > "$output_file_min"
head -n 1 "$temp_file" > "$output_file_max"
tail -n +2 "$temp_file" | sort -t$'\t' -k21n | head -n 1 >> "$output_file_min"
tail -n +2 "$temp_file" | sort -t$'\t' -k21nr | head -n 1 >> "$output_file_max"


# Convert back to proper CSV format
csvtool -t TAB -u COMMA cat "$output_file_min" > "$output_file_min.tmp" && mv "$output_file_min.tmp" "$output_file_min"
csvtool -t TAB -u COMMA cat "$output_file_max" > "$output_file_max.tmp" && mv "$output_file_max.tmp" "$output_file_max"

# Cleanup temporary file
rm "$temp_file"

echo "Min-max data saved to $output_file_min and $output_file_max"

