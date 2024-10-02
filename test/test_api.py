import warnings
from fastapi.testclient import TestClient
from src.api.main import app

# Suppress DeprecationWarnings that may arise during testing.
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Create a test client for the FastAPI app.
client = TestClient(app)

def test_fetch_data():
    """
    Test case to verify that the '/fetch-data' endpoint is working correctly.
    
    - Ensures that the response status code is 200 (OK).
    - Ensures that the response content is not empty.
    """
    # Send a GET request to the '/fetch-data' endpoint.
    response = client.get("/fetch-data")
    
    # Check if the status code is 200 (success).
    assert response.status_code == 200
    
    # Ensure that some data is returned in the response body.
    assert len(response.content) > 0  # Verify non-empty content
    
    # Validate specific headers and data format.
    # assert response.headers["content-type"] == "text/csv"
    # assert "Transactions" in response.json()  # Expecting a JSON response with a 'Transactions' field in the file