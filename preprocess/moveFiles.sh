#!/bin/bash

# Source directory
source_dir="/home/jaber/TrueDetective/toomany"

# Base destination directory
base_destination="/home/jaber/TrueDetective/toomany1/"

# Ensure the source directory exists
if [ ! -d "$source_dir" ]; then
    echo "Source directory not found: $source_dir"
    exit 1
fi

# Loop to move files until the source directory is empty
counter=1
while [ "$(ls -A "$source_dir")" ]; do
    # Destination directory for this iteration
    destination_dir="${base_destination}${counter}/"

    # Create the destination directory if it doesn't exist
    mkdir -p "$destination_dir"

    # Move the first 5000 files from the source directory to the destination directory
    find "$source_dir" -maxdepth 1 -type f | head -n 1000 | xargs -I {} mv {} "$destination_dir"

    # Increment counter for the next iteration
    ((counter++))
done

echo "All files moved to destination directories."
