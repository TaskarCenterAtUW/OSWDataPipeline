import math
from pathlib import Path

import click
import numpy as np
from rasterio.windows import Window
import requests
from scipy.interpolate import RectBivariateSpline

from .constants import ned_13_index

AWS_BASE = "https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation"
TEMPLATE = AWS_BASE + "/13/TIFF/current/{e}/USGS_13_{e}.tif"


class InvalidNED13TileName(Exception):
    pass


class FailedWindowRead(ValueError):
    pass


def get_dem_dir(workdir):
    dem_path = Path(workdir, "dems")
    if not dem_path.exists():
        dem_path.mkdir()

    return dem_path


def list_ned13s(workdir):
    """List cached NED13 tilesets.

    :returns: List of strings

    """
    dem_dir = get_dem_dir(workdir)
    tifs = dem_dir.glob("*.tif")
    tiles = [Path(tif).stem for tif in tifs]
    matching = [tile for tile in tiles if tile in ned_13_index]

    return matching


def get_ned13_for_bounds(bounds, workdir, progressbar=False):
    """Retrieve the NED 1/3 arc-second tileset names based on a WGS84 (lon-lat)
    bounding box list: [w, s, e, n].

    :param bounds: Bounding box list
    :type bounds: List of float

    :returns: List of strings

    """
    # Calculate pairs of Northwest corners covered by the boundaries
    north_min = int(math.floor(bounds[1]))
    north_max = int(math.ceil(bounds[3]))
    west_min = int(math.floor(-1 * bounds[2]))
    west_max = int(math.ceil(-1 * bounds[0]))

    ned_13_tiles = []
    for n in range(north_min + 1, north_max + 1):
        # Added 1 to ranges because we need the top corner value whereas
        # range() defaults to lower
        for w in range(west_min + 1, west_max + 1):
            tile = f"n{n}w{w:03}"
            if tile in ned_13_index:
                ned_13_tiles.append(tile)
            else:
                # FIXME Outside range - issue warning? Log?
                pass

    # Check temporary dir for these tiles
    cached_tiles = list_ned13s(workdir)

    # TODO: use set operations so that the code is easier to understand
    fetch_tiles = [tile for tile in ned_13_tiles if tile not in cached_tiles]

    # FIXME: should split this function into two steps:
    # 1) Figure out which are missing, return these tileset names.
    # 2) CLI / GUI will display this info
    # 3) Downstream code will accept a list of these names as input for
    # fetching. Can happen async, etc.
    if fetch_tiles:
        print(f"Fetching DEM data for {fetch_tiles}...")
    else:
        print("No tiles need to be fetched.")

    # Any remaining tiles must be fetched and inserted into the database
    # TODO: make this fully async, use a queue to fetch and insert via separate
    # tasks
    for tilename in fetch_tiles:
        fetch_ned_tile(tilename, workdir, progressbar=progressbar)


def fetch_ned_tile(tilename, workdir, progressbar=False):
    if tilename not in ned_13_index:
        raise InvalidNED13TileName(f"Invalid tile name {tilename}")

    url = TEMPLATE.format(e=tilename)

    filename = f"{tilename}.tif"
    dem_dir = get_dem_dir(workdir)
    path = Path(dem_dir, filename)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        if progressbar:
            size = int(r.headers["Content-Length"].strip())
            pbar = click.progressbar(
                length=size, label=f"    downloading {filename}"
            )

        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                if progressbar:
                    pbar.update(len(chunk))


def bivariate_spline(dx, dy, arr):
    nrow, ncol = arr.shape

    ky = min(nrow - 1, 3)
    kx = min(nrow - 1, 3)

    spline = RectBivariateSpline(
        np.array(range(ncol)), np.array(range(nrow)), arr, kx=kx, ky=ky
    )
    return spline(dx, dy)[0][0]


def bilinear(dx, dy, arr):
    nrow, ncol = arr.shape
    if (nrow != 2) or (ncol != 2):
        raise ValueError("Shape of bilinear interpolation input must be 2x2")
    top = dx * arr[0, 0] + (1 - dx) * arr[0, 1]
    bottom = dx * arr[1, 0] + (1 - dx) * arr[1, 1]

    return dy * top + (1 - dy) * bottom


def idw(dx, dy, masked_array):
    if (masked_array.shape[0] != 3) or (masked_array.shape[1] != 3):
        # Received an array that isn't 3x3
        return None

    # Do not attempt interpolation if less than 25% of the data is unmasked.
    ncells = masked_array.shape[0] * masked_array.shape[1]
    if (masked_array.mask.sum() / ncells) >= 0.75:
        return None

    # TODO: save time by masking first
    # TODO: save time by precalculating squared values
    xs = np.array([[i - dx for i in range(masked_array.shape[0])]])
    ys = np.array([[i - dy for i in range(masked_array.shape[1])]])

    distances = np.sqrt((ys ** 2).T @ xs ** 2)

    distances_masked = distances[~masked_array.mask]
    values_masked = masked_array[~masked_array.mask]

    # FIXME: add distance weights? Should be distance squared or something,
    # right?
    inverse_distances = 1 / distances_masked
    weights = inverse_distances / inverse_distances.sum()
    weighted_values = np.multiply(values_masked, weights)

    value = weighted_values.sum()

    if np.isnan(value):
        return None
    return value


def interpolated_value(x, y, dem, method="idw", scaling_factor=1.0):
    """Given a point (x, y), find the interpolated value in the raster using
    bilinear interpolation.

    """
    methods = {"spline": bivariate_spline, "bilinear": bilinear, "idw": idw}

    # At this point, we assume that the input DEM is in the same crs as the
    # x y values.

    # The DEM's affine transformation: maps units along its indices to crs
    # coordinates. e.g. if the DEM is 1000x1000, maps xy values in the
    # 0-1000 range to the DEM's CRS, e.g. lon-lat
    aff = dem.transform
    # The inverse of the transform: maps values in the DEM's crs to indices.
    # Note: the output values are floats between the index integers.
    inv = ~aff

    # Get the in-DEM index coordinates
    _x, _y = inv * (x, y)

    # Extract a window of coordinates
    if method == "bilinear":
        # Get a 2x2 window of pixels surrounding the coordinates
        dim = 2
        offset_x = math.floor(_x)
        offset_y = math.floor(_y)
    elif method in ("spline", "idw"):
        # NOTE: 'idw' method can actually use any dim. Should allow dim to be
        # an input parameter.
        # Get a 5x5 window of pixels surrounding the coordinates
        dim = 3  # window size (should be odd)
        offset = math.floor(dim / 2.0)
        offset_x = int(math.floor(_x) - offset)
        offset_y = int(math.floor(_y) - offset)
    else:
        raise ValueError(
            "Invalid interpolation method {} selected".format(method)
        )
    # FIXME: create any necessary special handling for masked vs. unmasked data
    # FIXME: bilinear interp function doesn't work with masked data
    try:
        dem_arr = dem.read(
            1, window=Window(offset_x, offset_y, dim, dim), masked=True
        )
    except ValueError:
        raise FailedWindowRead

    dx = _x - offset_x
    dy = _y - offset_y

    interpolator = methods[method]

    interpolated = interpolator(dx, dy, dem_arr)

    if interpolated is None:
        return interpolated
    else:
        return scaling_factor * interpolated


def dem_interpolate(lon, lat, dem):
    try:
        # TODO: parallelize reads from DEMs.
        interpolated = interpolated_value(
            lon, lat, dem, method="idw", scaling_factor=1.0
        )
        if interpolated is not None:
            return interpolated
    except FailedWindowRead:
        # Ignore failed windows
        # TODO: log warnings?
        pass
    return None


def infer_incline(linestring, length, dem, precision=3):
    """Infer the incline value for a given linestring using NED 1/3 arc-second
    dataset(s). Does not checking to verify that NED(s) exist.

    """
    first_point = linestring.coords[0]
    last_point = linestring.coords[-1]

    first_elevation = dem_interpolate(first_point[0], first_point[1], dem)
    second_elevation = dem_interpolate(last_point[0], last_point[1], dem)

    if first_elevation is None or second_elevation is None:
        return None

    incline = (second_elevation - first_elevation) / length

    return round(incline, precision)
