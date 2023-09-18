"""Graph containers - OSM-specific building strategies and manipulations"""
import json

import networkx as nx
import osmium
import pyproj
from shapely.geometry import LineString, Point, mapping, shape

from ..osw.osw_normalizer import OSWWayNormalizer, OSWNodeNormalizer, OSWPointNormalizer


class PointCounter(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.count = 0

    def point(self, n):
        self.count += 1

class NodeCounter(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.count = 0

    def node(self, n):
        self.count += 1


class WayCounter(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.count = 0

    def way(self, w):
        self.count += 1


class OSMWayParser(osmium.SimpleHandler):
    def __init__(self, way_filter, progressbar=None):
        osmium.SimpleHandler.__init__(self)
        self.G = nx.MultiDiGraph()
        if way_filter is None:
            self.way_filter = lambda w: True
        else:
            self.way_filter = way_filter
        self.progressbar = progressbar

    def way(self, w):
        if self.progressbar:
            self.progressbar.update(1)

        if not self.way_filter(w.tags):
            if self.progressbar:
                self.progressbar.update(1)
            return

        d = {"osm_id": int(w.id)}

        tags = dict(w.tags)
        tags["osm_id"] = str(int(w.id))

        d2 = {**d, **OSWWayNormalizer(tags).normalize()}

        for i in range(len(w.nodes) - 1):
            u = w.nodes[i]
            v = w.nodes[i + 1]

            # NOTE: why are the coordinates floats? Wouldn't fixed
            # precision be better?
            u_ref = int(u.ref)
            u_lon = float(u.lon)
            u_lat = float(u.lat)
            v_ref = int(v.ref)
            v_lon = float(v.lon)
            v_lat = float(v.lat)

            d3 = {**d2}
            d3["segment"] = i
            d3["ndref"] = [u_ref, v_ref]
            self.G.add_edges_from([(u_ref, v_ref, d3)])
            self.G.add_node(u_ref, lon=u_lon, lat=u_lat)
            self.G.add_node(v_ref, lon=v_lon, lat=v_lat)
            del u
            del v

        del w

class OSMNodeParser(osmium.SimpleHandler):
    def __init__(self, G, node_filter=None, progressbar=None):
        """

        :param G: MultiDiGraph that already has ways inserted as edges.
        :type G: nx.MultiDiGraph

        """
        osmium.SimpleHandler.__init__(self)
        self.G = G
        if node_filter is None:
            self.node_filter = lambda w: True
        else:
            self.node_filter = node_filter
        self.progressbar = progressbar

    def node(self, n):
        if self.progressbar:
            self.progressbar.update(1)

        if not self.node_filter(n.tags):
            return

        if n.id not in self.G.nodes:
            return

        d = {"osm_id": int(n.id)}

        tags = dict(n.tags)

        d2 = {**d, **OSWNodeNormalizer(tags).normalize()}

        self.G.add_node(n.id, **d2)

class OSMPointParser(osmium.SimpleHandler):
    def __init__(self, G, point_filter=None, progressbar=None):
        """

        :param G: MultiDiGraph that already has ways inserted as edges.
        :type G: nx.MultiDiGraph

        """
        osmium.SimpleHandler.__init__(self)
        self.G = G
        if point_filter is None:
            self.point_filter = lambda w: True
        else:
            self.point_filter = point_filter
        self.progressbar = progressbar

    def node(self, n):
        if self.progressbar:
            self.progressbar.update(1)

        if not self.point_filter(n.tags):
            return

        if n.id in self.G.nodes:
            return

        d = {"osm_id": int(n.id)}

        tags = dict(n.tags)

        d2 = {**d, **OSWPointNormalizer(tags).normalize()}

        self.G.add_node(n.id, lon=n.location.lon, lat=n.location.lat, **d2)


class OSMGraph:
    def __init__(self, G=None):
        if G is not None:
            self.G = G

        # Geodesic distance calculator. Assumes WGS84-like geometries.
        self.geod = pyproj.Geod(ellps="WGS84")

    @classmethod
    def from_pbf(
        self, pbf, way_filter=None, node_filter=None, point_filter=None, progressbar=None
    ):
        way_parser = OSMWayParser(way_filter, progressbar=progressbar)
        way_parser.apply_file(pbf, locations=True)
        G = way_parser.G
        del way_parser

        node_parser = OSMNodeParser(G, node_filter, progressbar=progressbar)
        node_parser.apply_file(pbf)
        G = node_parser.G
        del node_parser

        point_parser = OSMPointParser(G, point_filter, progressbar=progressbar)
        point_parser.apply_file(pbf)
        G = point_parser.G
        del point_parser

        return OSMGraph(G)

    def simplify(self):
        """Simplifies graph by merging way segments of degree 2 - i.e.
        continuations.

        """
        # Structure is way_id: (node, segment_number). This makes it easy to
        # sort on-the-fly.
        remove_nodes = {}

        for node, d in self.G.nodes(data=True):
            if OSWNodeNormalizer.osw_node_filter(d):
                # Skip if this is an node feature of interest, e.g. kerb ramp
                continue

            predecessors = list(self.G.predecessors(node))
            successors = list(self.G.successors(node))

            if (len(predecessors) == 1) and (len(successors) == 1):
                # Only one predecessor and one successor - ideal internal node
                # to remove from graph, merging its location data into other
                # edges.
                node_in = predecessors[0]
                node_out = successors[0]
                edge_in = self.G[node_in][node][0]
                edge_out = self.G[node][node_out][0]

                # Only one exception: we shouldn't remove a node that's shared
                # between two different ways: this is an important decision
                # point for some paths.
                if edge_in["osm_id"] != edge_out["osm_id"]:
                    continue

                node_data = (node_in, node, node_out, edge_in["segment"])

                # Group by way
                edge_id = edge_in["osm_id"]
                if edge_id in remove_nodes:
                    remove_nodes[edge_id].append(node_data)
                else:
                    remove_nodes[edge_id] = [node_data]

        # NOTE: an otherwise unconnected circular path would be removed, as all
        # nodes are degree 2 and on the same way. This path is pointless for a
        # network, but is something to keep in mind for any downstream
        # analysis.
        for way_id, node_data in remove_nodes.items():
            # Sort by segment number
            sorted_node_data = list(sorted(node_data, key=lambda x: x[3]))

            # Split into lists of neighboring nodes
            neighbors_list = []

            neighbors = [sorted_node_data.pop(0)]
            for node_in, node, node_out, segment_n in sorted_node_data:
                if (segment_n - neighbors[-1][3]) != 1:
                    # Not neighbors!
                    neighbors_list.append(neighbors)
                    neighbors = [(node_in, node, node_out, segment_n)]
                else:
                    # Neighbors!
                    neighbors.append((node_in, node, node_out, segment_n))
            neighbors_list.append(neighbors)

            # Remove internal nodes by group
            for neighbors in neighbors_list:
                u, v, w, segment_n = neighbors[0]
                # FIXME: this try/except is a hack to avert an uncommon and
                # unexplored edge case. Come back and fix!
                try:
                    edge_data = self.G[u][v][0]
                except KeyError:
                    continue
                ndref = edge_data["ndref"]
                self.G.remove_edge(u, v)
                for node_in, node, node_out, segment_n in neighbors:
                    ndref.append(node_out)
                    # Remove intervening edge
                    try:
                        self.G.remove_edge(node, node_out)
                    except nx.exception.NetworkXError:
                        pass
                self.G.add_edges_from([(u, node_out, edge_data)])

    def construct_geometries(self, progressbar=None):
        """Given the current list of node references per edge, construct
        geometry.

        """
        internal_nodes = []
        for u, v, d in self.G.edges(data=True):
            coords = []
            for ref in d["ndref"]:
                # FIXME: is this the best way to retrieve node attributes?
                node_d = self.G._node[ref]
                coords.append((node_d["lon"], node_d["lat"]))

            geometry = LineString(coords)
            d["geometry"] = geometry
            d["length"] = round(self.geod.geometry_length(geometry), 1)
            del d["ndref"]
            if progressbar:
                progressbar.update(1)

        for n, d in self.G.nodes(data=True):
            coords = []
            geometry = Point(d["lon"], d["lat"])
            d["geometry"] = geometry
            if progressbar:
                progressbar.update(1)

    def to_undirected(self):
        if self.G.is_multigraph():
            G = nx.MultiGraph(self.G)
        else:
            G = nx.Graph(self.G)
        return OSMGraph(G)

    def get_graph(self):
        return self.G

    def filter_edges(self, func):
        # TODO: put this in a "copy-like" function
        if self.G.is_multigraph():
            if self.G.is_directed():
                G = nx.MultiDiGraph()
            else:
                G = nx.MultiGraph()
        else:
            if self.G.is_directed():
                G = nx.DiGraph()
            else:
                G = nx.Graph()

        for u, v, d in self.G.edges(data=True):
            if func(u, v, d):
                G.add_edge(u, v, **d)

        # Copy in node data
        for node in G.nodes:
            d = self.G._node[node]
            G.add_node(node, **d)

        return OSMGraph(G)

    def is_multigraph(self):
        return self.G.is_multigraph()

    def is_directed(self):
        return self.G.is_directed()

    def to_geojson(self, *args):
        nodes_path = args[0]
        edges_path = args[1]
        edge_features = []
        for u, v, d in self.G.edges(data=True):
            d_copy = {**d}
            d_copy["_u_id"] = str(u)
            d_copy["_v_id"] = str(v)

            if "osm_id" in d_copy:
                d_copy.pop("osm_id")

            if "segment" in d_copy:
                d_copy.pop("segment")

            geometry = mapping(d_copy.pop("geometry"))

            edge_features.append(
                {"type": "Feature", "geometry": geometry, "properties": d_copy}
            )
        edges_fc = {"type": "FeatureCollection", "features": edge_features}

        node_features = []
        for n, d in self.G.nodes(data=True):
            d_copy = {**d}
            if "is_point" not in d_copy:
                d_copy["_id"] = str(n)

                if "osm_id" in d_copy:
                   d_copy.pop("osm_id")

                geometry = mapping(d_copy.pop("geometry"))

                if "lon" in d_copy:
                    d_copy.pop("lon")

                if "lat" in d_copy:
                    d_copy.pop("lat")

                node_features.append(
                    {"type": "Feature", "geometry": geometry, "properties": d_copy}
                )
        nodes_fc = {"type": "FeatureCollection", "features": node_features}

        with open(edges_path, "w") as f:
            json.dump(edges_fc, f)

        with open(nodes_path, "w") as f:
            json.dump(nodes_fc, f)

        if len(args) == 3:
            points_path = args[2]
            point_features = []
            for n, d in self.G.nodes(data=True):
                d_copy = {**d}
                if "is_point" in d_copy:
                    d_copy["_id"] = str(n)

                    if "osm_id" in d_copy:
                        d_copy.pop("osm_id")

                    geometry = mapping(d_copy.pop("geometry"))

                    d_copy.pop("is_point")

                    if "lon" in d_copy:
                        d_copy.pop("lon")

                    if "lat" in d_copy:
                        d_copy.pop("lat")

                    point_features.append(
                        {"type": "Feature", "geometry": geometry, "properties": d_copy}
                    )
            points_fc = {"type": "FeatureCollection", "features": point_features}

            with open(points_path, "w") as f:
                json.dump(points_fc, f)

    @classmethod
    def from_geojson(cls, nodes_path, edges_path):
        with open(nodes_path) as f:
            nodes_fc = json.load(f)

        with open(edges_path) as f:
            edges_fc = json.load(f)

        G = nx.MultiDiGraph()
        osm_graph = cls(G=G)

        for node_feature in nodes_fc["features"]:
            props = node_feature["properties"]
            n = props.pop("_id")
            props["geometry"] = shape(node_feature["geometry"])
            G.add_node(n, **props)

        for edge_feature in edges_fc["features"]:
            props = edge_feature["properties"]
            u = props.pop("_u_id")
            v = props.pop("_v_id")

            props["geometry"] = shape(edge_feature["geometry"])

            G.add_edge(u, v, **props)

        return osm_graph
