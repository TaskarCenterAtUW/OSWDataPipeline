from pathlib import Path
import json
import ogr2osm
from osw_data.osm.osm_normalizer import OSMNormalizer

def osm_write(region_id, workdir):
    input_file = merge(region_id, workdir)
    output_file = Path(workdir, f"{region_id}.graph.osm.xml")

    # Create the translation object.
    translation_object = OSMNormalizer()

    # Create the ogr datasource
    datasource = ogr2osm.OgrDatasource(translation_object)
    datasource.open_datasource(input_file)

    # Instantiate the ogr to osm converter class ogr2osm.OsmData and start the
    #    conversion process
    osmdata = ogr2osm.OsmData(translation_object)
    osmdata.process(datasource)

    # Instantiate either ogr2osm.OsmDataWriter or ogr2osm.PbfDataWriter
    datawriter = ogr2osm.OsmDataWriter(output_file, suppress_empty_tags = True)
    osmdata.output(datawriter)

    # Delete merge file
    Path(input_file).unlink()

def merge(region_id, workdir)-> str:
    fc = {"type": "FeatureCollection", "features": []}

    # OSW Data
    graph_edges_geojson_path = Path(workdir, f"{region_id}.graph.edges.geojson")
    graph_nodes_geojson_path = Path(workdir, f"{region_id}.graph.nodes.geojson")
    graph_points_geojson_path = Path(workdir, f"{region_id}.graph.points.geojson")
    
    with open(graph_edges_geojson_path) as f:
        region_fc = json.load(f)
        fc["features"] = fc["features"] + region_fc["features"]

    with open(graph_nodes_geojson_path) as f:
        region_fc = json.load(f)
        fc["features"] = fc["features"] + region_fc["features"]

    if graph_points_geojson_path.exists():
        with open(graph_points_geojson_path) as f:
            region_fc = json.load(f)
            fc["features"] = fc["features"] + region_fc["features"]
    
    outputPath = Path(workdir, f"{region_id}.graph.all.geojson")
    with open(outputPath, "w") as f:
        json.dump(fc, f)
    
    return str(outputPath)