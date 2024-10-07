"""
Module for fetching property transaction data from UK Land Registry,
processing it into a CSV format, and serving it via a FastAPI service.
"""

import os
import asyncio
import csv
from io import StringIO

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse

app = FastAPI()

# Base URL for fetching property transaction data from the UK Land Registry.
BASE_URL = (
    "https://landregistry.data.gov.uk/app/ppd/ppd_data.csv?"
    "et%5B%5D=lrcommon%3Afreehold&et%5B%5D=lrcommon%3Aleasehold&header=true"
)

# Path where the fetched CSV data will be saved.
SAVE_PATH = os.path.join("src", "pipeline", "fetch-data-2014-2024.csv")


async def fetch_year_data(client, start_date, end_date):
    """
    Fetch data for a specific year range from the UK Land Registry.

    Args:
        client (httpx.AsyncClient): The HTTP client for making async requests.
        start_date (str): The start date in the format YYYY-MM-DD.
        end_date (str): The end date in the format YYYY-MM-DD.

    Returns:
        str: CSV data fetched for the given date range.

    Raises:
        HTTPException: If the request fails, an exception is raised with
        the corresponding status code.
    """
    url = f"{BASE_URL}&from-date={start_date}&to-date={end_date}"
    response = await client.get(url, follow_redirects=True)

    # If the status code is not 200, raise an HTTP exception.
    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Failed to fetch data for {start_date} to {end_date}",
        )

    return response.text


async def save_file(content):
    """
    Save the fetched CSV data to a file asynchronously.

    Args:
        content (str): The content of the CSV file to be written.
    """
    os.makedirs(os.path.dirname(SAVE_PATH), exist_ok=True)
    with open(SAVE_PATH, 'w', newline='', encoding='utf-8') as file:
        file.write(content)


@app.get("/fetch-data-2014-2024")
async def fetch_data():
    """     
    Fetch property transaction data for the years 2014-2024 and return it
    as a CSV file.

    The data is fetched year by year, concatenated, and returned as a single
    CSV file. This function also asynchronously saves the data to a file on
    disk.

    Returns:
        StreamingResponse: A streaming response with the CSV data.
    """
    all_data = StringIO()  # In-memory stream to store concatenated CSV data.
    csv_writer = csv.writer(all_data)
    header_written = False  # To track if the header has been written.

    # Create an async HTTP client to fetch the data.
    async with httpx.AsyncClient() as client:
        for year in range(2014, 2025):  # Fetch data for each year from 2014 to 2024.
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"

            # Fetch data for the given year.
            year_data = await fetch_year_data(client, start_date, end_date)
            csv_reader = csv.reader(StringIO(year_data))

            if not header_written:
                # Write the header from the first year's data.
                header = next(csv_reader)
                csv_writer.writerow(header)
                header_written = True
            else:
                # Skip the header for subsequent years.
                next(csv_reader)

            # Write the rows for the current year to the combined CSV.
            for row in csv_reader:
                csv_writer.writerow(row)

    content = all_data.getvalue()  # Get the complete CSV content as a string.

    # Asynchronously save the data to a file on disk.
    asyncio.create_task(save_file(content))

    # Return the CSV data as a streaming response.
    return StreamingResponse(
        iter([content]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=fetch-data-2014-2024.csv"}
    )


@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify that the API is running.

    Returns:
        JSONResponse: A JSON response indicating that the API is operational.
    """
    return JSONResponse(
        content={"status": "OK", "message": "API is running"}, status_code=200
    )


if __name__ == "__main__":
    # Run the FastAPI application using Uvicorn server on port 8000.
    uvicorn.run(app, host="0.0.0.0", port=8000)
