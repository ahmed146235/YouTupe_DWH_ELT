# Test to verify that the API_KEY Variable returns the mocked value
def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"

# Test to verify that the Channel Handle Variable returns the mocked value
def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHEESE"


# Test to verify the mock Postgres connection
def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name"


# Test to verify the integrity of all DAGs in the DagBag
def test_dags_integrity(dagbag):

    # 1. Check that there are no import errors in the DAGs
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("===========")
    print(dagbag.import_errors)

    # 2. Verify that all expected DAGs are loaded
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())
    print("===========")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing."

    # 3. Verify the total number of DAGs loaded
    assert dagbag.size() == 3
    print("===========")
    print(dagbag.size())

    # 4. Verify that each DAG has the expected number of tasks
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2,
    }
    print("===========")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)
        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."
        print(dag_id, len(dag.tasks))
