import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)

def test_fetch_data():
    response = client.get("/fetch-data")
    assert response.status_code == 200
    #assert response.headers["content-type"] == "text/csv"
    assert len(response.content) > 0  # Ensure some data is returned
    #assert "Transactions" in response.json()