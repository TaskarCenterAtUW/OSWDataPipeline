import json

import numpy as np
import osmium
from osmium.geom import GeoJSONFactory
import rasterio
from shapely.geometry import LineString, MultiPolygon, Point, mapping, shape
import utm


def is_masked_area(tags):
    if "building" in tags:
        return True
    if "man_made" in tags and tags["man_made"] == "bridge":
        return True
    return False


def multipolygon_to_utm(geojson_geom):
    # Flatten array, then get UTM coords
    lons = []
    lats = []
    for polygon in geojson_geom["coordinates"]:
        for ring in polygon:
            for lon, lat in ring:
                lons.append(lon)
                lats.append(lat)
    xs, ys, zone_number, zone_letter = utm.from_latlon(
        np.array(lats), np.array(lons)
    )

    # Unflatten array so that it's a polygon again
    xs_rev = list(reversed(xs))
    ys_rev = list(reversed(ys))
    new_coords = []
    for polygon in geojson_geom["coordinates"]:
        p = []
        new_coords.append(p)
        for ring in polygon:
            r = []
            p.append(r)
            for lon, lat in ring:
                r.append([xs_rev.pop(), ys_rev.pop()])

    return (
        {"type": "MultiPolygon", "coordinates": new_coords},
        zone_number,
        zone_letter,
    )


def multipolygon_from_utm(geojson_geom, zone_number, zone_letter):
    # Reflatten and reproject to lon-lat
    xs_buff = []
    ys_buff = []
    for polygon in geojson_geom["coordinates"]:
        for ring in polygon:
            for x, y in ring:
                xs_buff.append(x)
                ys_buff.append(y)

    lats_buff, lons_buff = utm.to_latlon(
        np.array(xs_buff), np.array(ys_buff), zone_number, zone_letter
    )

    # Unflatten array so that it's a polygon again
    lons_buff_rev = list(reversed(lons_buff))
    lats_buff_rev = list(reversed(lats_buff))

    buff_coords = []
    for polygon in geojson_geom["coordinates"]:
        p = []
        buff_coords.append(p)
        for ring in polygon:
            r = []
            p.append(r)
            for lon, lat in ring:
                r.append([lons_buff_rev.pop(), lats_buff_rev.pop()])

    return {"type": "MultiPolygon", "coordinates": buff_coords}


def buffer_multipolygon(geojson_geom, buffer):
    utm_multipolygon, zone_number, zone_letter = multipolygon_to_utm(
        geojson_geom
    )
    multipolygon = shape(utm_multipolygon)
    buffered = multipolygon.buffer(buffer)

    if buffered.type == "Polygon":
        buffered = MultiPolygon([buffered])

    buffered_geojson = mapping(buffered)

    return multipolygon_from_utm(buffered_geojson, zone_number, zone_letter)


def buffer_linestring(geojson_linestring, buffer):
    lons, lats = zip(*geojson_linestring["coordinates"])
    xs, ys, zone_number, zone_letter = utm.from_latlon(
        np.array(lats), np.array(lons)
    )

    linestring = LineString(zip(xs, ys))

    polygon = linestring.buffer(buffer)

    buffered_xs, buffered_ys = zip(*polygon.exterior.coords)

    buffered_lats, buffered_lons = utm.to_latlon(
        np.array(buffered_xs), np.array(buffered_ys), zone_number, zone_letter
    )

    exterior_ring = list(zip(buffered_lons, buffered_lats))
    polygon = [exterior_ring]
    multipolygon_coords = [polygon]

    return {
        "type": "MultiPolygon",
        "coordinates": multipolygon_coords,
    }


class MaskedAreaCounter(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.count = 0

    def area(self, a):
        if is_masked_area(a.tags):
            self.count += 1


class MaskedAreaHandler(osmium.SimpleHandler):
    def __init__(self, buffer=None, progressbar=None):
        super().__init__()
        self.areas = []
        self.geojson_factory = GeoJSONFactory()
        self.buffer = buffer
        self.progressbar = progressbar

    def area(self, a):
        if is_masked_area(a.tags):
            try:
                geojson = self.geojson_factory.create_multipolygon(a)
                geojson_geom = json.loads(geojson)
                if self.buffer is not None:
                    geojson_geom = buffer_multipolygon(
                        geojson_geom, self.buffer
                    )

                self.areas.append(geojson_geom)
            except RuntimeError:
                # A RuntimeError is raised when the multipolygon cannot be
                # created. This is upstream behavior that we do not yet work
                # around, so instead we will simply skip the area
                pass

            if self.progressbar is not None:
                self.progressbar.update(1)


def bridge_filter(tags):
    if "bridge" in tags and tags["bridge"] == "yes":
        return True
    return False


class BridgeLineCounter(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.count = 0

    def way(self, w):
        if bridge_filter(w.tags):
            self.count += 1


class MaskedBridgeLineHandler(osmium.SimpleHandler):
    def __init__(self, buffer, progressbar=None):
        super().__init__()
        self.buffer = buffer
        self.bridges = []
        self.geojson_factory = GeoJSONFactory()
        self.progressbar = progressbar

    def way(self, w):
        if bridge_filter(w.tags):
            try:
                geojson = self.geojson_factory.create_linestring(w)
                geojson_geom = json.loads(geojson)
                buffered_geom = buffer_linestring(geojson_geom, self.buffer)

                self.bridges.append(buffered_geom)
            except RuntimeError:
                # A RuntimeError is raised when the linestring cannot be
                # created. This is upstream behavior that we do not yet work
                # around, so instead we will simply skip the line
                pass

            if self.progressbar is not None:
                self.progressbar.update(1)


def count_masked_areas(path):
    """Count the number of areas to mask in an OSM PBF file.

    :param path: Path to the .osm.pbf
    :type path: str

    """
    area_counter = MaskedAreaCounter()
    area_counter.apply_file(str(path))

    return area_counter.count


def extract_areas(path, buffer=None, progressbar=None):
    """Extract (multi)polygons of areas to mask from an OSM PBF file.

    :param path: Path to the .osm.pbf file.
    :type path: str
    :param progressbar: An (optional) click.progressbar object that will be
                        updated as areas are extracted.
    :type progressbar: click.progressbar

    """
    area_handler = MaskedAreaHandler(buffer=buffer, progressbar=progressbar)
    area_handler.apply_file(str(path))

    return area_handler.areas


def count_bridges(path):
    """Count the number of linear bridge features to mask in an OSM PBF file.

    :param path: Path to the .osm.pbf
    :type path: str

    """
    bridge_counter = BridgeLineCounter()
    bridge_counter.apply_file(str(path))

    return bridge_counter.count


def extract_bridges(path, buffer=30, progressbar=None):
    """Extract buffered polygons of bridge lines to mask from an OSM PBF file.

    :param path: Path to the .osm.pbf file.
    :type path: str
    :param progressbar: An (optional) click.progressbar object that will be
                        updated as areas are extracted.
    :type progressbar: click.progressbar

    """
    bridge_handler = MaskedBridgeLineHandler(
        buffer=buffer, progressbar=progressbar
    )
    bridge_handler.apply_file(str(path), locations=True)

    return bridge_handler.bridges


def mask_dem(dem_path, polygons, progressbar=False):
    """Insert a nodata mask into a DEM based on attributes of an OSM PBF file,
    namely masking out pixels near buildings and bridges.

    :param dem_path: Path to a DEM raster.
    :type dem_path: str
    :param polygons: Iterable of GeoJSON (multi)polygon geometries.
    :type polygons: Iterable of GeoJSON (multi)polygon geometries (dict).

    """
    with rasterio.open(dem_path, "r+") as rast:
        for polygon in polygons:
            mask_polygon(polygon, rast)
            if progressbar is not None:
                progressbar.update(1)


def to_raster_coords(geometry, raster):
    affine = ~raster.transform
    polygons = []
    for polygon in geometry["coordinates"]:
        new_polygon = []
        for ring in polygon:
            new_ring = []
            for coord in ring:
                x, y = affine * (coord[0], coord[1])
                new_ring.append([x, y])
            new_polygon.append(new_ring)
        polygons.append(new_polygon)

    new_geometry = {"type": "MultiPolygon", "coordinates": polygons}

    return new_geometry


def mask_polygon(polygon, raster):
    raster_coord_geojson = to_raster_coords(polygon, raster)
    geom = shape(raster_coord_geojson)
    bounds = geom.bounds
    minx = int(bounds[0])
    miny = int(bounds[1])
    maxx = int(bounds[2]) + 1
    maxy = int(bounds[3]) + 1

    if minx < 0 or miny < 0 or maxx > raster.width or maxy > raster.height:
        # Geometry falls outside of raster extent: do nothing
        return

    # Geometry bounds fall within raster extent, but could overlap boundary:
    # adjust window to be within boundary
    minx = max(0, minx)
    miny = max(0, miny)
    maxx = min(raster.width, maxx)
    maxy = min(raster.height, maxy)

    # Create windowed read indices
    dx = maxx - minx
    dy = maxy - miny

    # Object at boundary may not overlap properly - throw out
    if not dx or not dy:
        return

    # mask = np.ones((dy, dx)).astype(bool)
    xs = [p + 0.5 for p in range(minx, maxx)]
    ys = [p + 0.5 for p in range(miny, maxy)]

    window = rasterio.windows.Window(minx, miny, dx, dy)

    mask = raster.read_masks(indexes=1, window=window)

    for i, y in enumerate(ys):
        for j, x in enumerate(xs):
            point = Point(x, y)
            if geom.distance(point) == 0:
                mask[i, j] = False

    raster.write_mask(mask, window=rasterio.windows.Window(minx, miny, dx, dy))
