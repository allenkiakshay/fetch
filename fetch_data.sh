#!/bin/bash
# fetch_data.sh
# This script fetches data from an API endpoint using the provided x-api-key
# and saves the response to a file.

# Set your API endpoint, API key, and output file name
API_URL="https://events.vitap.ac.in/events/api/vtopia"
API_KEY="93a3b0b71ecc49b59b8d442c787e06b2cf7a3dfd88b0fa66be6fccf96b146a2"
OUTPUT_FILE="output.json"

# Fetch data from the API with the x-api-key header and save it to the output file
echo "Fetching data from ${API_URL}..."
curl -s -H "X-API-KEY: ${API_KEY}" "${API_URL}" -o "${OUTPUT_FILE}"

# Fetch data from API
echo "Fetching data from ${API_URL}..."
curl -s -H "X-API-KEY: ${API_KEY}" "${API_URL}" -o "${OUTPUT_FILE}"

# Check if the curl command was successful
if [ $? -eq 0 ]; then
    echo "Data successfully saved to ${OUTPUT_FILE}"

    echo "Executing process_data.py"
    
    # Initialize Conda before activating
    source ~/miniconda3/etc/profile.d/conda.sh
    conda activate check
    
    # Run Python script
    python3 "/home/ubuntu/fetch/process_data.py"
    
    # Deactivate Conda
    conda deactivate
else
    echo "An error occurred while fetching data."
    exit 1
fi

