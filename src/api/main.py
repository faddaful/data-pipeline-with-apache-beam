"""
Module to fetch property transaction data from UK Land Registry
and serve it via a FastAPI service.
"""

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

# Create an instance of the FastAPI app
app = FastAPI()

# URL for the Land Registry property transaction data
LAND_REGISTRY_URL = "https://landregistry.data.gov.uk/app/ppd/ppd_data.csv"

@app.get("/fetch-data")
async def fetch_data():
    """
    Endpoint to fetch property transaction data from the UK Land Registry.
    It retrieves CSV data from the given URL and returns it as a streaming response.

    Returns:
        StreamingResponse: A streaming response that serves the CSV data.

    Raises:
        HTTPException: If the request to the Land Registry fails, an HTTPException is raised.
    """
    # Create an asynchronous HTTP client using httpx to fetch the data
    async with httpx.AsyncClient() as client:
        response = await client.get(LAND_REGISTRY_URL, follow_redirects=True)
        
        # If the response status is not 200 (OK), raise an HTTP exception
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="Failed to fetch data")
        
        # Return the fetched CSV data as a streaming response
        return StreamingResponse(response.iter_bytes(), media_type="text/csv")

# Entry point to run the FastAPI application using Uvicorn
if __name__ == "__main__":
    # Run the application on host 0.0.0.0 and port 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)