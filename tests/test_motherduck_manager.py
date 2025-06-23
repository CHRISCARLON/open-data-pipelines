import pytest
import duckdb

from databases.motherduck import MotherDuckManager
from data_sources.os_usrn_uprn import OsUsrnUprn


@pytest.fixture
def local_duckdb_connection():
    """Create a temporary in-memory DuckDB connection for testing."""
    conn = duckdb.connect(database=":memory:", read_only=False)
    yield conn
    conn.close()


@pytest.fixture
def mock_motherduck_manager(local_duckdb_connection):
    """Create a MotherDuckManager with a local DuckDB connection."""
    manager = MotherDuckManager("fake_token", "test_db")
    manager.connection = local_duckdb_connection
    return manager


def test_setup_for_data_source(mock_motherduck_manager):
    """Test that setup_for_data_source creates the expected schema and tables..."""
    # Create a test configuration
    test_config = OsUsrnUprn.create_default_latest()
    print(test_config)

    # Call the method we want to test
    mock_motherduck_manager.setup_for_data_source(test_config)

    # Verify the schema was created
    result = mock_motherduck_manager.connection.execute(
        f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{test_config.schema_name}'"
    ).fetchall()
    assert len(result) == 1

    # Debug: Print out the schemas that actually exist
    all_schemas = mock_motherduck_manager.connection.execute(
        "SELECT schema_name FROM information_schema.schemata"
    ).fetchall()
    print(f"Available schemas: {all_schemas}")

    # Verify the table was created - add more debugging
    for table_name in test_config.table_names:
        # Debug: List all tables in this schema
        all_tables = mock_motherduck_manager.connection.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{test_config.schema_name}'"
        ).fetchall()
        print(f"Tables in {test_config.schema_name}: {all_tables}")

        assert len(all_tables) == 1
