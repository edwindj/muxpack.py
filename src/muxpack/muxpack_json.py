from muxpack import MultiplexSeries
from .multiplex import Multiplex
from cmath import log
import ibis

from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)

def extract_muxpack_json(path: str | Path) -> tuple[str, list[str] | None, str]:
    logger.info("Loading file paths from {path}...")
    
    path = Path(path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    if not data["type"] == "muxpack":
        raise ValueError(f"Invalid multiplex JSON type: {data['type']}")

    dir = path.parent
    vertices = f"{dir / data['vertices']}"

    logger.info("Loading edges...")
    layers = data["layers"]
    if layers is not None:
        layers = [f"{dir / layer['path']}/*.parquet" for layer in layers]
    
    relationtypes = f"{dir / data['relationtype']}"
    return vertices, layers, relationtypes

def read_muxpack_json(path: str | Path, period: int | None = None):
    """
    Read a muxpack JSON file and return a Multiplex object.

    Args:
        path: Path to the muxpack JSON file.
        period: Optional period to set the period variable.

    Returns:
        A Multiplex object containing the loaded data.
    """
    vertices, layers, relationtypes = extract_muxpack_json(path)

    con = ibis.duckdb.connect()
    logger.info("Loading edges...{layers}", extra={"layers": layers})
    edges = con.read_parquet(layers, table_name="edges")

    logger.info("Loading vertices")
    try:
        vertices = ibis.read_parquet(f"{vertices}", table_name="vertices")
    except Exception as e:
        logger.info(f"No vertices found: {e}")
        vertices = None

    logger.info("Loading relationtypes from {relationtypes}", extra={"relationtypes": relationtypes})
    relationtypes = ibis.read_csv(relationtypes, table_name="relationtypes")

    # # if layers is not None:
    #     layers = {layer: dir / f"{layer}/edges/**/*.parquet" for layer in layers}
    # print({"dir": dir, "data": data, "vertices": vertices, "edges": edges, "relationtypes": relationtypes})
    m = Multiplex(
        edges=edges,
        vertices=vertices,
        period=period
    )
    return m

def read_muxpackseries_json(path: str | Path, periods: list[int] | None = None):
    """
    Read a muxpack series JSON file and return a MultiplexSeries object.
    A muxpack json file allows for specifying multiple periods, each containing its own multiplex data.

    Args:
        path: Path to the muxpack series JSON file.
        periods: Optional list of periods to filter the data.

    Returns:
        A MultiplexSeries object containing the loaded data.
    """
    path = Path(path)
    dir = path.parent
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    if not data["type"] == "muxpackseries":
        raise ValueError(f"Invalid multiplex series JSON type: {data['type']}")

    lperiods = data["periods"] if "periods" in data else []
    
    lvertices:list[str] = []
    ledges: list[str] = []
    lrelationtypes: list[str] = []
    for period in lperiods:
        p = period['period']
        if periods is not None and p not in periods:
            continue
        ppath = f"{dir / period['path'] / 'muxpack.json'}"
        # print(ppath)
        v, e, r = extract_muxpack_json(ppath)
        lvertices.append(v)
        ledges.extend(e)
        lrelationtypes.append(r)

    # print({"vertices": lvertices, "edges": ledges, "relationtype": lrelationtypes})
    con = ibis.duckdb.connect()

    logger.info("Loading vertices...{vertices}", extra={"vertices": lvertices})
    vertices = con.read_parquet(lvertices, table_name="vertices")

    logger.info("Loading edges...{edges}", extra={"edges": ledges})
    edges = con.read_parquet(ledges, table_name="edges")

    relationtypes = con.read_csv(lrelationtypes, table_name="relationtypes").distinct()

    m = MultiplexSeries(edges, vertices, relationtypes)
    return m