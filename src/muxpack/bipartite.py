from pathlib import Path
from ibis import Table
from . import io

class BiPartite:
    """
    Lazy Bipartite storage
    - sort on role_src, role_dst
    """
    edges: Table
    role_src: str 
    role_dst: str
    relationtype: str

    def __init__(self, edges: Table, role_src: str = "src" , role_dst: str = "dst", relationtype:str="relationtype"):
        self.edges = edges
        self.role_src = role_src
        self.role_dst = role_dst
        self.relationtype = relationtype

    def project_to_src(self):
        E = self.edges
        # TODO this is a explicit choice: relationtypes could be more complex, ie when two different role_src have
        # an other relation with the same role_dst. Simplifying that right now
        E_src = E.select(src = self.role_src, p = self.role_dst, relationtype = self.relationtype)
        E_dst = E.select(dst = self.role_src, p = self.role_dst)

        E = E_src.inner_join(E_dst, E_src.p == E_dst.p)
        E = E.filter(E.src != E.dst)
        E = E.drop("p")
        return E
    
    def project_to_dst(self):
        E = self.edges
        # TODO this is a explicit choice: relationtypes could be more complex, ie when two different role_src have
        # an other relation with the same role_dst. Simplifying that right now
        E_src = E.select(src = self.role_dst, p = self.role_src, relationtype = self.relationtype)
        E_dst = E.select(dst = self.role_dst, p = self.role_src)

        E = E_src.inner_join(E_dst, E_src.p == E_dst.p)
        E = E.filter(E.src != E.dst)
        E = E.drop("p")
        return E
    
    def save(self, dir: Path | str):
        """
        Save the bipartite graph to disk. This will save the edges as a parquet file and the metadata (role_src, role_dst, relationtype) as a json file.
        """
        io.save_bipartite(edges=self.edges, role_src=self.role_src, role_dst=self.role_dst, relationtype=self.relationtype, dir = "bipartite")
        bp = io.read_bipartite(dir = "bipartite")
        self.edges = bp.edges