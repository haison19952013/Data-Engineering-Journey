#!/bin/bash
# Input and output CSV file
input_file="data/tmdb-movies.csv"
output_file="solution/4-total-revenue-tmdb-movies.csv"

# Temporary file to hold processed CSV
# temp_file="temp.csv"
temp_file=$(mktemp)

# Convert CSV to a tab-separated format (TSV) to handle embedded commas correctly
csvtool -t COMMA -u TAB cat "$input_file" > "$temp_file"

# Use awk to filter column 18 and keep all columns (now tab-separated)
sum_rev=$(csvcut -d $'\t' -c 21 "$temp_file" | csvstat --sum)
echo "Total revenue: $sum_rev" > "$output_file"

# Cleanup temporary file
rm "$temp_file"

echo "Filtered data saved to $output_file"

