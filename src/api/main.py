import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

app = FastAPI()

LAND_REGISTRY_URL = "https://landregistry.data.gov.uk/app/ppd/ppd_data.csv"

@app.get("/fetch-data")
async def fetch_data():
    async with httpx.AsyncClient() as client:
        response = await client.get(LAND_REGISTRY_URL, follow_redirects=True)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="Failed to fetch data")
        return StreamingResponse(response.iter_bytes(), media_type="text/csv")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)