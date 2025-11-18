import os
import pytest
import psycopg2
from unittest import mock
from airflow.models import Variable, Connection, DagBag

# Fixture to mock the API_KEY Variable
@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")


# Fixture to mock the Channel Handle Variable
@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="MRCHEESE"):
        yield Variable.get("CHANNEL_HANDLE")


# Fixture to mock a Postgres connection for testing
@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name",  # schema is the db name
    )
    conn_uri = conn.get_uri()

    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")

# Fixture to provide a DagBag instance for DAG integrity testing
@pytest.fixture()
def dagbag():
    yield DagBag()


# Fixture that provides a helper function to fetch the value of any Airflow Variable 
# from the environment, allowing tests to access real variables dynamically.
@pytest.fixture()
def airflow_variable():
    def get_airflow_variable(variable_name):
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)

    return get_airflow_variable



# Pytest fixture that establishes a real connection to the Postgres database using credentials
# from environment variables. It yields the connection for use in integration tests and ensures
# the connection is closed after the test, failing the test if the connection cannot be established.
@pytest.fixture
def real_postgres_connection():
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None

    try:
        conn = psycopg2.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )

        yield conn

    except psycopg2.Error as e:
        pytest.fail(f"Failed to connect to the database: {e}")

    finally:
        if conn:
            conn.close()
