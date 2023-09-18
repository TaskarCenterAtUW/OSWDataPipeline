# OSW Data Pipeline
The OSW Data Pipeline is a full bottom-up data pipeline using OpenStreetMap and public DEM datasets. You must define a config.geojson file to configure the data process. Each region in this file include information on where to retrieve its OpenStreetMap data. This repo includes a sample config file at input/config.geojson.

## config.geojson
This configuration file is a GeoJSON FeatureCollection, which is essentially just an annotated list of MultiPolygon shapes representing an area intended to be extracted from OpenStreetMap and turned into routable OpenSidewalks data. An example is already present in input/config.geojson. Each "Feature" will have a polygon describing the area covered and a set of properties, all of which must be defined:

- `id`: A unique identifier string for this region. It can be anything so long
as it is unique, it is only used internally. We typically use `country.city`
or something along those lines.
- `extract_url`: A URL to a `.osm.pbf` file that includes this region. It will
be fetched automatically and used to extract path information.

## Building the docker image

    git clone --branch main https://github.com/TaskarCenterAtUW/OSWDataPipeline.git
    docker-compose build osw_data

## Running the data process

To fetch the source .OSM.PBF file, run this in the main directory of this repo:

    docker-compose run osw_data fetch /input/config.geojson

To clip the region out of the source file:

    docker-compose run osw_data clip /input/config.geojson

To build/process the data into an OpenSidewalks-compatible format, run this in the main directory of this repo:

    docker-compose run osw_data create /input/config.geojson

To validate the generated OpenSidewalks data against the schema run:

    docker-compose run osw_data validate /input/config.geojson
