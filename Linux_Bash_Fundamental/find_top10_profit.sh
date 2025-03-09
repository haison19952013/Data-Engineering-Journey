#!/bin/bash
# Input and output CSV file
input_file="tmdb-movies.csv"
output_file="top10-profit-tmdb-movies.csv"

# Temporary file to hold processed CSV
# temp_file="temp.csv"
temp_file=$(mktemp)
temp_file1=$(mktemp)

# Convert CSV to a tab-separated format (TSV) to handle embedded commas correctly
csvtool -t COMMA -u TAB cat "$input_file" > "$temp_file"

# Use awk to filter column 18 and keep all columns (now tab-separated)
awk -F'\t' '
  NR==1 {
    # Print header and add new column for profit
    print $0 "\tprofit"
  }
  NR>1 {
    # Calculate profit and print all columns plus profit
    profit = $21 - $20
    printf "%s\t%d\n", $0, profit  # Format profit as an integer
  }' "$temp_file" > "$temp_file1"

# # Convert back to proper CSV format
head -n 1 "$temp_file1" > "$output_file"
tail -n +2 "$temp_file1" | sort -t$'\t' -k22nr | head -n 10 >> "$output_file"

csvtool -t TAB -u COMMA cat "$output_file" > "$output_file.tmp" && mv "$output_file.tmp" "$output_file"

# Cleanup temporary file
rm "$temp_file"

echo "Filtered data saved to $output_file"

