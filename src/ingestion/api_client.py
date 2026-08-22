import requests
import time
from typing import Dict, Any, List, Optional

class WeatherApiClient:
    def __init__(self, api_key: str, base_url: str = "https://api.weatherapi.com/v1"):
        self.api_key = api_key
        self.base_url = base_url

    def _get(self, endpoint: str, params: Dict[str, Any], retries: int = 3, backoff_factor: float = 1.5) -> Dict[str, Any]:
        """Executes GET request with exponential backoff for rate limits / network errors."""
        params['key'] = self.api_key
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(retries):
            try:
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code in [429, 500, 502, 503, 504]:
                    # Rate limited or server error -> backoff and retry
                    time.sleep(backoff_factor ** attempt)
                else:
                    response.raise_for_status()
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise e
                time.sleep(backoff_factor ** attempt)
        raise RuntimeError(f"Failed to fetch data from {url} after {retries} attempts.")

    def fetch_current(self, location: str) -> Dict[str, Any]:
        return self._get("current.json", {"q": location})

    def fetch_history(self, location: str, date_str: str) -> Dict[str, Any]:
        """Fetches historical weather for a specific YYYY-MM-DD date."""
        return self._get("history.json", {"q": location, "dt": date_str})

    def fetch_forecast(self, location: str, days: int = 3) -> Dict[str, Any]:
        return self._get("forecast.json", {"q": location, "days": days})