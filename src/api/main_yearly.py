import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
import os
import asyncio
import csv
from io import StringIO
from datetime import datetime, timedelta

app = FastAPI()

BASE_URL = "https://landregistry.data.gov.uk/app/ppd/ppd_data.csv?et%5B%5D=lrcommon%3Afreehold&et%5B%5D=lrcommon%3Aleasehold&header=true"
SAVE_PATH = os.path.join("src", "pipeline", "fetch-data-2014-2024.csv")

async def fetch_year_data(client, start_date, end_date):
    url = f"{BASE_URL}&from-date={start_date}&to-date={end_date}"
    response = await client.get(url, follow_redirects=True)
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=f"Failed to fetch data for {start_date} to {end_date}")
    return response.text

async def save_file(content):
    os.makedirs(os.path.dirname(SAVE_PATH), exist_ok=True)
    with open(SAVE_PATH, 'w', newline='', encoding='utf-8') as file:
        file.write(content)

@app.get("/fetch-data-2014-2024")
async def fetch_data():
    all_data = StringIO()
    csv_writer = csv.writer(all_data)
    header_written = False

    async with httpx.AsyncClient() as client:
        for year in range(2014, 2025):
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            
            year_data = await fetch_year_data(client, start_date, end_date)
            csv_reader = csv.reader(StringIO(year_data))
            
            if not header_written:
                header = next(csv_reader)
                csv_writer.writerow(header)
                header_written = True
            else:
                next(csv_reader)  # Skip header for subsequent years
            
            for row in csv_reader:
                csv_writer.writerow(row)

    content = all_data.getvalue()
    
    # Save the file asynchronously
    asyncio.create_task(save_file(content))
    
    # Return streaming response
    return StreamingResponse(
        iter([content]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=fetch-data-2014-2024.csv"}
    )

@app.get("/health")
async def health_check():
    return JSONResponse(content={"status": "OK", "message": "API is running"}, status_code=200)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)