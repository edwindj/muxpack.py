from ibis.expr.types import Table
from ibis import dtype

import logging

logger = logging.getLogger(__name__)


def check_edges(edges: Table, check_period=True) -> bool:
    """
    Check that the edges table has the required columns and types.

    Args:
        - edges: the edges table to check.
        - check_period: whether to require a ``period`` column.

    Returns:
        - ``True`` if the edges table is valid, ``False`` otherwise.
    """

    # the column types can be int32 or int64, but they must be integers, and the layer column must be a string
    expect_types = {
        "src": "integer",
        "dst": "integer",
        "period": "integer",
        "layer": "string",
        "relationtype": "integer",
    }

    if not check_period:
        expect_types.pop("period", None)

    if check_column_type(edges, expect_types):
        return True

    return False


def check_vertices(vertices: Table, check_period=True) -> bool:
    """
    Check that the vertices table has the required columns and types.

    Args:
        - vertices: the vertices table to check.
        - check_period: whether to require a ``period`` column.

    Returns:
        - ``True`` if the vertices table is valid, ``False`` otherwise.
    """
    required_columns = {"id", "period"} if check_period else {"id"}

    if not required_columns.issubset(set(vertices.columns)):
        logger.warning(f"Missing columns: {required_columns - set(vertices.columns)}")
        return False

    expect_types = {"id": "integer"}

    if check_period:
        expect_types["period"] = "integer"

    if not check_column_type(vertices, expect_types):
        return False

    return True


def check_column_type(t: Table, expected_types: dict[str, str]) -> bool:
    """
    Check that the columns in a table have the expected types.

    Args:
        - t: the table to check.
        - expected_types: dictionary mapping column names to expected type strings
          (e.g., ``"integer"``, ``"string"``).

    Returns:
        - ``True`` if all specified columns exist and have the expected types, ``False`` otherwise.
    """
    for column, expected_type in expected_types.items():
        col = t[column]
        if col is None:
            logger.warning(f"Column '{column}' is missing.")
            return False
        coltype = col.type()
        if expected_type == "integer" and coltype.is_integer():
            continue
        if expected_type == "string" and coltype.is_string():
            continue
        # most specific check, if the expected type is exactly the same as the column type, then it's valid
        if dtype(expected_type) == coltype:
            continue

        logger.warning(
            f"Incorrect type for column '{column}': '{coltype}', expected {expected_type}"
        )
        return False
    return True
