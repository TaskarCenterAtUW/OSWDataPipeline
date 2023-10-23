"""osw_data CLI."""
import asyncio
import json
import subprocess
import jsonschema
from pathlib import Path
import geopandas as gpd

import click
import rasterio
from shapely.geometry import shape

from osw_data.osm.write import osm_write

from .constants import BUFFER_DIST, TMP_DIR
from .dems.transforms import get_ned13_for_bounds, infer_incline, list_ned13s
from .dems.mask_dem import (
    count_masked_areas,
    count_bridges,
    extract_areas,
    extract_bridges,
    mask_dem,
)
from .osm.osm_clip import osm_clip
from .osm.osm_graph import OSMGraph, PointCounter, NodeCounter, WayCounter
from .osm.fetch import osm_fetch
from .osw.osw_normalizer import OSWWayNormalizer, OSWNodeNormalizer, OSWPointNormalizer
from .schemas.config_schema import ConfigSchema

@click.group()
def osw_data() -> None:
    pass


@osw_data.command()
@click.argument("config", type=click.Path())
@click.option("--workdir", envvar="OSW_DATA_WORKDIR", default=TMP_DIR)
def fetch(config: str, workdir: str) -> None:
    config = ConfigSchema.dict_from_filepath(config)
    downloaded = set()

    for feature in config["features"]:
        extract_url = feature["properties"]["extract_url"]
        if not extract_url in downloaded:
            click.echo(f"Fetching osm.pbf for {feature['properties']['id']}...")
            download_path = osm_fetch(
                extract_url,
                workdir,
                progressbar=True,
            )
            downloaded.add(extract_url)
            click.echo(f"osm.pbf has been saved to {download_path}")
        else:
            click.echo(f"Already fetched osm.pbf for {feature['properties']['id']}")


@osw_data.command()
@click.argument("config", type=click.Path())
@click.option("--workdir", envvar="OSW_DATA_WORKDIR", default=TMP_DIR)
def clip(config: str, workdir: str) -> None:
    # FIXME: add option to configure number of simultaneous processes and/or
    # maximum memory usage.
    config = ConfigSchema.dict_from_filepath(config)

    regions = [region["properties"]["id"] for region in config["features"]]
    click.echo(f"Extracting clipped .osm.pbf regions for {', '.join(regions)}")

    osm_clips = []
    for region in config["features"]:
        extract_path = Path(
            workdir, Path(region["properties"]["extract_url"]).name
        )

        region_id = region["properties"]["id"]

        clipped_path = Path(workdir, f"{region_id}.osm.pbf")

        osm_clips.append(osm_clip(extract_path, clipped_path, region))

    async def run_all_osm_clips():
        await asyncio.gather(*osm_clips)

    asyncio.run(run_all_osm_clips())

    click.echo("Clipped OSM PBFs.")


def network(config: str, workdir: str) -> None:
    # FIXME: move at least some of the async functionality to upstream modules,
    # e.g. the 'asyncifying' run_in_executor-using async functions
    config = ConfigSchema.dict_from_filepath(config)

    # FIXME: define this at a module-level and make it the default behavior.
    # OpenSidewalks Schema-fication should be hardcoded (for now).
    def opensidewalks_way_filter(tags):
        normalizer = OSWWayNormalizer(tags)
        return normalizer.filter()

    def opensidewalks_node_filter(tags):
        normalizer = OSWNodeNormalizer(tags)
        return normalizer.filter()
    
    def opensidewalks_point_filter(tags):
        normalizer = OSWPointNormalizer(tags)
        return normalizer.filter()

    #
    # Get counts of all ways/nodes/points in each dataset
    #

    async def count_ways(pbf_path: str):
        loop = asyncio.get_event_loop()
        way_counter = WayCounter()
        await loop.run_in_executor(None, way_counter.apply_file, pbf_path)
        return way_counter.count

    async def count_nodes(pbf_path: str):
        loop = asyncio.get_event_loop()
        node_counter = NodeCounter()
        await loop.run_in_executor(None, node_counter.apply_file, pbf_path)
        return node_counter.count
    
    async def count_points(pbf_path: str):
        loop = asyncio.get_event_loop()
        point_counter = PointCounter()
        await loop.run_in_executor(None, point_counter.apply_file, pbf_path)
        return point_counter.count

    tasks = []
    for region in config["features"]:
        region_id = region["properties"]["id"]
        pbf_path = str(Path(workdir, f"{region_id}.osm.pbf"))
        tasks.append(count_ways(pbf_path))
        tasks.append(count_nodes(pbf_path))
        tasks.append(count_points(pbf_path))


    with click.progressbar(
        length=len(tasks),
        label="Estimating number of ways, nodes and points in datasets...",
    ) as pbar:

        async def count_main():
            results = []
            for future in asyncio.as_completed(tasks):
                pbar.update(1)
                results.append(await future)
            return results

        count_results = asyncio.run(count_main())
    graph_element_count = sum(count_results)

    #
    # Create an OSMGraph per region
    #
    async def get_osmgraph(region_id, pbf_path, way_filter, node_filter, point_filter, pbar):
        loop = asyncio.get_event_loop()
        OG = await loop.run_in_executor(
            None,
            OSMGraph.from_pbf,
            pbf_path,
            way_filter,
            node_filter,
            point_filter,
            pbar,
        )
        return region_id, OG

    with click.progressbar(
        length=graph_element_count,
        label="Creating networks from region extracts...",
    ) as pbar:

        tasks = []
        for region in config["features"]:
            region_id = region["properties"]["id"]
            pbf_path = str(Path(workdir, f"{region_id}.osm.pbf"))
            tasks.append(
                get_osmgraph(
                    region_id,
                    pbf_path,
                    opensidewalks_way_filter,
                    opensidewalks_node_filter,
                    opensidewalks_point_filter,
                    pbar,
                )
            )

        async def osm_graph_main():
            return await asyncio.gather(*tasks)

        osm_graph_results = asyncio.run(osm_graph_main())

    osm_graph_results = list(osm_graph_results)

    async def simplify_og(og):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, og.simplify)

    with click.progressbar(
        length=len(osm_graph_results),
        label="Simplifying ways...",
    ) as pbar:

        async def simplify_main():
            tasks = [
                simplify_og(OG) for region_id, OG in osm_graph_results
            ]
            for future in asyncio.as_completed(tasks):
                pbar.update(1)
                await future

        asyncio.run(simplify_main())

    #
    # Construct geometries
    #
    async def construct_geometries(og, pbar):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, og.construct_geometries, pbar)

    with click.progressbar(
        length=len(osm_graph_results),
        label="Constructing geometries...",
    ) as pbar:

        async def construct_geometries_main():
            tasks = [
                construct_geometries(OG, pbar)
                for region_id, OG in osm_graph_results
            ]
            await asyncio.gather(*tasks)

        asyncio.run(construct_geometries_main())

    #
    # Write to file
    #
    async def write_og(region_id: str, og):
        loop = asyncio.get_event_loop()
        points_path = Path(workdir, f"{region_id}.graph.points.geojson")
        nodes_path = Path(workdir, f"{region_id}.graph.nodes.geojson")
        edges_path = Path(workdir, f"{region_id}.graph.edges.geojson")
        await loop.run_in_executor(None, og.to_geojson, nodes_path, edges_path, points_path)

    with click.progressbar(
        length=len(osm_graph_results),
        label="Writing graph points, nodes and edges GeoJSONs to file...",
    ) as pbar:

        async def write_main():
            tasks = [
                write_og(region_id, OG) for region_id, OG in osm_graph_results
            ]
            for future in asyncio.as_completed(tasks):
                pbar.update(1)
                await future

        asyncio.run(write_main())

    regions = ", ".join(r for r, o in osm_graph_results)
    click.echo(f"Created networks from the clipped {regions} region(s).")


def mask(config: str, workdir: str) -> None:
    config = ConfigSchema.dict_from_filepath(config)

    tilesets = list_ned13s(workdir)
    tileset_paths = [
        Path(workdir, "dems", f"{tileset}.tif") for tileset in tilesets
    ]

    # Create blank mask by default - no pixels are masked.
    for path in tileset_paths:
        with rasterio.open(path, "r+") as rast:
            rast.write_mask(True)

    # Add masked regions
    for region in config["features"]:
        region_id = region["properties"]["id"]
        # Fetch DEMs if they aren't already cached
        get_ned13_for_bounds(
            shape(region["geometry"]).bounds, workdir, progressbar=True
        )

        clipped_extract_path = Path(workdir, f"{region_id}.osm.pbf")
        click.echo(f"Counting buildings and bridge areas in {region_id}...")
        area_count = count_masked_areas(clipped_extract_path)
        with click.progressbar(
            length=area_count,
            label=f"Extracting buildings and bridge areas from {region_id}: ",
        ) as pbar:
            area_geoms = extract_areas(
                clipped_extract_path, buffer=BUFFER_DIST, progressbar=pbar
            )

        for tileset, path in zip(tilesets, tileset_paths):
            with click.progressbar(
                length=area_count,
                label=f"Masking {tileset} with geometries from {region_id}",
            ) as pbar2:
                mask_dem(path, area_geoms, progressbar=pbar2)

        click.echo(f"Counting bridge lines in {region_id}...")
        bridge_count = count_bridges(clipped_extract_path)

        with click.progressbar(
            length=bridge_count,
            label=f"Extracting buffered bridge lines from {region_id}: ",
        ) as pbar:
            bridge_geoms = extract_bridges(
                clipped_extract_path, buffer=BUFFER_DIST, progressbar=pbar
            )

        for tileset, path in zip(tilesets, tileset_paths):
            with click.progressbar(
                length=bridge_count,
                label=f"Masking {tileset} with bridges from {region_id}",
            ) as pbar2:
                mask_dem(path, bridge_geoms, progressbar=pbar2)


def incline(config: str, workdir: str) -> None:
    config = ConfigSchema.dict_from_filepath(config)

    # Infer inclines
    for region in config["features"]:
        # Download region(s) if necessary
        get_ned13_for_bounds(
            shape(region["geometry"]).bounds, workdir, progressbar=True
        )
        region_id = region["properties"]["id"]

        graph_nodes_path = Path(workdir, f"{region_id}.graph.nodes.geojson")
        graph_edges_path = Path(workdir, f"{region_id}.graph.edges.geojson")

        # FIXME: using unweaver's geopackage might make many of these steps
        # easier
        OG = OSMGraph.from_geojson(graph_nodes_path, graph_edges_path)

        tilesets = list_ned13s(workdir)
        for tileset in tilesets:
            tileset_path = Path(workdir, "dems", f"{tileset}.tif")

            with rasterio.open(tileset_path) as dem:
                with click.progressbar(
                    length=len(OG.G.edges),
                    label=f"Estimating inclines for {region_id} for {tileset}",
                ) as bar:
                    for u, v, d in OG.G.edges(data=True):
                        incline = infer_incline(
                            d["geometry"], d["length"], dem, 3
                        )
                        if incline is not None:
                            d["incline"] = incline
                        bar.update(1)

        OG.to_geojson(graph_nodes_path, graph_edges_path)


def addOSWID(config: str, workdir: str) -> None:
    config = ConfigSchema.dict_from_filepath(config)

    for region in config["features"]:
        region_id = region["properties"]["id"]
        graph_geojson_path = Path(workdir, f"{region_id}.graph.edges.geojson")
        OSW_edges = gpd.read_file(graph_geojson_path)

        # Create _id for all edges
        OSW_edges.insert(loc=0, column = '_id', value = OSW_edges.index + 1)
        OSW_edges['_id'] = OSW_edges['_id'].astype('string')
        OSW_edges['step_count'] = OSW_edges['step_count'].astype('Int64')

        with open(Path(workdir, graph_geojson_path), "w") as f:
            f.write(OSW_edges.to_json(na='drop', drop_id=True))


@osw_data.command()
@click.argument("config", type=click.Path())
@click.option("--workdir", envvar="OSW_DATA_WORKDIR", default=TMP_DIR)
@click.pass_context
def create(ctx: click.Context, config: str, workdir: str) -> None:
    network(config, workdir)
    mask(config, workdir)
    incline(config, workdir)
    #FIXME: Update once ID generation completed
    addOSWID(config, workdir)


@osw_data.command()
@click.argument("config", type=click.Path())
@click.option("--workdir", envvar="OSW_DATA_WORKDIR", default=TMP_DIR)
def validate(config: str, workdir: str) -> None:
    def load_file(file_path):
        """Load Data"""
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data

    def validate_osw_errors(geojson_data, schema):
        """Validate OSW Data against the schema and process all errors"""
        validator = jsonschema.Draft7Validator(schema)
        errors = validator.iter_errors(geojson_data)

        error_count = 0
        for error in errors:
            error_count = error_count + 1
            #format and store in file for further review
            #click.echo(error)
        
        return error_count==0
    
    config = ConfigSchema.dict_from_filepath(config)
    schema = load_file("/schema/opensidewalks.schema.json")

    for region in config["features"]:
        region_id = region["properties"]["id"]
        graph_edges_geojson_path = Path(workdir, f"{region_id}.graph.edges.geojson")
        graph_nodes_geojson_path = Path(workdir, f"{region_id}.graph.nodes.geojson")
        graph_points_geojson_path = Path(workdir, f"{region_id}.graph.points.geojson")

        click.echo(f"Validating OSW dataset for {region_id}")
        # validate edges
        is_valid = validate_osw_errors(load_file(graph_edges_geojson_path), schema)
        if is_valid:
            click.echo(f"OSW edges are valid for {region_id}")
        else:
            click.echo(f"OSW edges failed validation for {region_id}")
            exit(1)

        # validate nodes
        is_valid = validate_osw_errors(load_file(graph_nodes_geojson_path), schema)
        if is_valid:
            click.echo(f"OSW nodes are valid for {region_id}")
        else:
            click.echo(f"OSW nodes failed validation for {region_id}")
            exit(1)

        # validate points
        if graph_points_geojson_path.exists():
            is_valid = validate_osw_errors(load_file(graph_points_geojson_path), schema)
            if is_valid:
                click.echo(f"OSW points are valid for {region_id}")
            else:
                click.echo(f"OSW points failed validation for {region_id}")
                exit(1)


@osw_data.command()
@click.argument("config", type=click.Path())
@click.option("--workdir", envvar="OSW_DATA_WORKDIR", default=TMP_DIR)
def toosmxml(config: str, workdir: str) -> None:
    config = ConfigSchema.dict_from_filepath(config)

    for region in config["features"]:
        region_id = region["properties"]["id"]

        click.echo(f"Converting OSW data to OSM.XML file for {region_id}...")
        osm_write(
            region_id,
            workdir
        )