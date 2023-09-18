"""Checks a region directory for valid"""

from marshmallow import Schema, fields

from .geojson import (
    MultiPolygonFeatureSchema,
    MultiPolygonFeatureCollectionSchema,
)


class RegionPropertiesSchema(Schema):
    id = fields.Str(required=True)
    bounds = fields.Tuple(
        (fields.Float(), fields.Float(), fields.Float(), fields.Float()),
        required=True,
    )


class RegionFeatureSchema(MultiPolygonFeatureSchema):
    properties = fields.Nested(RegionPropertiesSchema, required=True)


class RegionFeatureCollectionSchema(MultiPolygonFeatureCollectionSchema):
    features = fields.List(fields.Nested(RegionFeatureSchema))
