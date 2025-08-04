import os

import google.auth
from google.auth.credentials import Credentials


def get_auth_default() -> tuple[Credentials, str]:
    """Set up Google authentication for the application.

    This function checks for the presence of the GOOGLE_APPLICATION_CREDENTIALS
    environment variable, which should point to a valid service account key
    file. It then initializes the Google Cloud credentials and project ID for
    use in the application and returns them. The project ID is determined from
    the credentials, and an error is raised if it cannot be determined.

    Returns
    -------
    credentials_and_project_id : tuple[Credentials, str]
        A tuple containing the Google Cloud credentials and the project ID.
    """
    # Check for required environment variable
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        raise RuntimeError("Environment variable GOOGLE_APPLICATION_CREDENTIALS is not set.")
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path or not os.path.exists(credentials_path):
        raise RuntimeError("Invalid GOOGLE_APPLICATION_CREDENTIALS path.")

    # Setup Google authentication
    try:
        credentials, project_id = google.auth.default()
        if not project_id:
            raise RuntimeError("Project ID could not be determined from the credentials.")
    except Exception as e:
        raise RuntimeError("Failed to setup Google credentials.") from e

    return credentials, project_id
