import requests
import pytest
import psycopg2


# Integration test that checks if the YouTube API is reachable and returns a successful response (status code 200)
# using the real API key and channel handle from Airflow variables.
def test_youtube_api_response(airflow_variable):
    api_key = airflow_variable("api_key")
    channel_handle = airflow_variable("channel_handle")

    url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

    try:
        response = requests.get(url)
        assert response.status_code == 200
    except requests.RequestException as e:
        pytest.fail(f"Request to YouTube API failed: {e}")



# Integration test that verifies the real Postgres database connection works correctly.
# It executes a simple SQL query ("SELECT 1") to ensure the connection is active
# and the database can respond. If any error occurs during the query, the test fails.
# The cursor is safely closed after the test to prevent resource leaks.
def test_real_postgres_connection(real_postgres_connection):
    cursor = None

    try:
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()

        assert result[0] == 1

    except psycopg2.Error as e:
        pytest.fail(f"Database query failed: {e}")

    finally:
        if cursor is not None:
            cursor.close()