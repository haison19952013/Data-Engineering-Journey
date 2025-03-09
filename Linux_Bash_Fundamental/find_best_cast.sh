#!/bin/bash
# Input and output CSV file
input_file="tmdb-movies.csv"
output_file="best-cast-tmdb-movies.csv"

# Temporary file to hold processed CSV
temp_file=$(mktemp)

# Convert CSV to a tab-separated format (TSV) to handle embedded commas correctly
csvtool -t COMMA -u TAB cat "$input_file" > "$temp_file"

# Use awk to filter column 18 and keep all columns (now tab-separated)
csvcut -d $'\t' -c 7 "$temp_file" | tail -n +2 | tr ' ' '_' | tr '|' ' ' | tr ' ' '\n' | tr -d '"' | sed '/^$/d' > "$output_file"

{ 
    echo -e "frequency Name";  # Add headers
    sort "$output_file" | uniq -c | sort -nr | awk '{print $1 " " $2}' | head -n 1;
} > "$output_file.tmp" && mv "$output_file.tmp" "$output_file"


# echo "Best director: $best_director" > "$output_file"

# # Cleanup temporary file
rm "$temp_file"

echo "Best cast data saved to $output_file"
