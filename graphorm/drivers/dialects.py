"""
Dialect abstraction for DB-specific Cypher syntax (indexes, procedure names).

Dialects provide only query fragments and procedure call strings; they do not
build full transport commands (e.g. GRAPH.QUERY). Transport stays in the driver.
"""

from typing import Protocol


class Dialect(Protocol):
    """Contract for a DB dialect: Cypher syntax only, no transport."""

    def create_index_sql(self, label: str, property_name: str) -> str:
        """Return the CREATE INDEX query string (no command prefix)."""
        ...

    def drop_index_sql(self, label: str, property_name: str) -> str:
        """Return the DROP INDEX query string."""
        ...

    def procedure_labels(self) -> str:
        """Return procedure name/call for listing labels."""
        ...

    def procedure_property_keys(self) -> str:
        """Return procedure name/call for listing property keys."""
        ...

    def procedure_relationship_types(self) -> str:
        """Return procedure name/call for listing relationship types."""
        ...

    def procedure_indexes(self) -> str:
        """Return full procedure call with YIELD for listing indexes (for predictable parsing)."""
        ...


class FalkorDBDialect:
    """Dialect for FalkorDB/RedisGraph."""

    def create_index_sql(self, label: str, property_name: str) -> str:
        return f"CREATE INDEX ON :{label}({property_name})"

    def drop_index_sql(self, label: str, property_name: str) -> str:
        return f"DROP INDEX ON :{label}({property_name})"

    def procedure_labels(self) -> str:
        return "db.labels()"

    def procedure_property_keys(self) -> str:
        return "db.propertyKeys()"

    def procedure_relationship_types(self) -> str:
        return "db.relationshipTypes()"

    def procedure_indexes(self) -> str:
        return "db.indexes() YIELD label, properties"
