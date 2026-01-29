"""Tests for DB dialect (FalkorDBDialect) in graphorm.drivers.dialects."""

import pytest

from graphorm.drivers.dialects import FalkorDBDialect


@pytest.fixture
def dialect():
    return FalkorDBDialect()


def test_create_index_sql(dialect):
    assert dialect.create_index_sql("User", "email") == "CREATE INDEX ON :User(email)"
    assert dialect.create_index_sql("Label", "prop") == "CREATE INDEX ON :Label(prop)"


def test_drop_index_sql(dialect):
    assert dialect.drop_index_sql("User", "email") == "DROP INDEX ON :User(email)"
    assert dialect.drop_index_sql("Label", "prop") == "DROP INDEX ON :Label(prop)"


def test_procedure_labels(dialect):
    assert dialect.procedure_labels() == "db.labels()"


def test_procedure_property_keys(dialect):
    assert dialect.procedure_property_keys() == "db.propertyKeys()"


def test_procedure_relationship_types(dialect):
    assert dialect.procedure_relationship_types() == "db.relationshipTypes()"


def test_procedure_indexes(dialect):
    # Full call with YIELD for predictable parsing (FalkorDB)
    assert dialect.procedure_indexes() == "db.indexes() YIELD label, properties"
