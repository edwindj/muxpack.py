from ibis.expr.types import Table

def check_edges(edges: Table) -> bool:
    """
    Check that the edges table has the required columns and types.
    
    :param edges: The edges table to check.
    :return: True if the edges table is valid, False otherwise.
    """

    expect_types = {
        "src": "int64",
        "dst": "int64",
        "year": "int64",
        "layer": "string"
    }

    for column, expected_type in expect_types.items():
        col = edges[column]
        if col is None:
            print(f"Column '{column}' is missing.")
            return False
        if (col.dtype != expected_type):
            print(f"Incorrect type for column '{column}': {col.dtype} (expected {expected_type})")
            return False
    return True

def check_vertices(vertices: Table) -> bool:
    """
    Check that the vertices table has the required columns and types.

    :param vertices: The vertices table to check.
    :return: True if the vertices table is valid, False otherwise.
    """
    required_columns = {"id"}

    if not required_columns.issubset(set(vertices.columns)):
        print(f"Missing columns: {required_columns - set(vertices.columns)}")
        return False
    
    expect_types = {
        "id": "int64"
    }

    for column, expected_type in expect_types.items():
        col = vertices[column]
        if col is None:
            continue
        if col.dtype != expected_type:
            print(f"Incorrect type for column '{column}': {col.dtype} (expected {expected_type})")
            return False
    return True