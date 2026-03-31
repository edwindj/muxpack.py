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

    def __init__(
        self,
        edges: Table,
        role_src: str = "src",
        role_dst: str = "dst",
        relationtype: str = "relationtype",
    ):
        """
        Initialize a bipartite graph with the given edges table and role labels.

        Args:
            - edges: table containing the bipartite edges.
            - role_src: column name for the source role.
            - role_dst: column name for the destination role.
            - relationtype: column name for the relation type.
        """
        self.role_src = role_src
        self.role_dst = role_dst
        self.relationtype = relationtype

    def project_to_src(self) -> Table:
        """
        Project the bipartite graph onto the source role, producing a unipartite edge table.
        Two source nodes are connected if they share a common destination node.

        Returns:
            - Table with columns ``src``, ``dst``, and ``relationtype``.
        """
        E = self.edges
        # TODO this is a explicit choice: relationtypes could be more complex, ie when two different role_src have
        # an other relation with the same role_dst. Simplifying that right now
        E_src = E.select(
            src=self.role_src, p=self.role_dst, relationtype=self.relationtype
        )
        E_dst = E.select(dst=self.role_src, p=self.role_dst)

        E = E_src.inner_join(E_dst, E_src.p == E_dst.p)
        E = E.filter(E.src != E.dst)
        E = E.drop("p")
        return E

    def project_to_dst(self) -> Table:
        """
        Project the bipartite graph onto the destination role, producing a unipartite edge table.
        Two destination nodes are connected if they share a common source node.

        Returns:
            - Table with columns ``src``, ``dst``, and ``relationtype``.
        """
        E = self.edges
        # TODO this is a explicit choice: relationtypes could be more complex, ie when two different role_src have
        # an other relation with the same role_dst. Simplifying that right now
        # should we sort on role_src and role_dst or the other way around? For projection it does not matter, but for storage it does. We sort on role_src and role_dst for efficient projection, but that means that the projection to dst is less efficient. Maybe we should sort on role_dst and role_src instead?
        E_src = E.select(
            src=self.role_dst, p=self.role_src, relationtype=self.relationtype
        )
        E_dst = E.select(dst=self.role_dst, p=self.role_src)

        E = E_src.inner_join(E_dst, E_src.p == E_dst.p)
        E = E.filter(E.src != E.dst)
        E = E.drop("p")
        return E

    def save(self, dir: Path | str) -> None:
        """
        Save the bipartite graph to disk.
        Edges are saved as a Parquet file and metadata (``role_src``, ``role_dst``,
        ``relationtype``) as a JSON file. The ``edges`` property is updated to point
        at the saved file.

        Args:
            - dir: path to the directory where the BiPartite graph will be saved.
        """
        io.save_bipartite(
            edges=self.edges,
            role_src=self.role_src,
            role_dst=self.role_dst,
            relationtype=self.relationtype,
            dir=dir,
        )
        bp = io.read_bipartite(dir=dir)
        self.edges = bp.edges
