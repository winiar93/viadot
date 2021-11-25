from typing import Any, Dict
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout, Timeout
from urllib3.exceptions import ProtocolError
import requests

from .exceptions import APIError


def slugify(name: str) -> str:
    return name.replace(" ", "_").lower()


def handle_api_response(
    url: str,
    auth: tuple = (None, None),
    params: Dict[str, Any] = None,
    headers: Dict[str, Any] = None,
):
    try:
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)

        session.mount("http://", adapter)
        session.mount("https://", adapter)
        response = session.get(url, params=params, auth=auth, headers=headers)
        response.raise_for_status()

    # TODO: abstract below and put as handle_api_response() into utils.py
    except ReadTimeout as e:
        msg = "The connection was successful, "
        msg += f"however the API call to {url} timed out after .... s "
        msg += "while waiting for the server to return data."
        raise APIError(msg)

    except HTTPError as e:
        raise APIError(
            f"The API call to {url} failed. "
            "Perhaps your account credentials need to be refreshed?",
        ) from e

    except (ConnectionError, Timeout) as e:
        raise APIError(f"The API call to {url} failed due to connection issues.") from e
    except ProtocolError as e:
        raise APIError(f"Did not receive any reponse for the API call to {url}.")
    except Exception as e:
        raise APIError("Unknown error.") from e

    return response
