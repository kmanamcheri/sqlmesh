from __future__ import annotations

import typing as t

from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.engine_adapter.shared import DataObject


class TrinoEngineAdapter(EngineAdapter):
    def _get_data_objects(self, schema_name: str, catalog_name: t.Optional[str] = None) -> t.List[DataObject]:
        catalog_clause = f"AND table_catalog = '{catalog_name}'" if catalog_name else ""
        query = f"""
            SELECT
                table_catalog AS catalog_name,
                table_schema AS schema_name,
                table_name AS name,
                CASE table_type
                    WHEN 'BASE TABLE' THEN 'table'
                    WHEN 'VIEW' then 'view'
                    ELSE 'unknown'
                END AS type
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}' {catalog_clause}
        """
        df = self.fetchdf(query)
        return [
            DataObject(
                catalog=row.catalog_name, schema=row.schema_name, name=row.name, type=DataObjectType.from_str(row.type)  # type: ignore
            )
            for row in df.itertuples()
        ]

    DIALECT = "trino"
    SUPPORTS_MATERIALIZED_VIEWS = True
    ESCAPE_JSON = True