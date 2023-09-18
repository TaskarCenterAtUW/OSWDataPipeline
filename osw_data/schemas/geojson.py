"""Checks a region directory for valid"""

from marshmallow import Schema, fields


class MultiPolygonGeometrySchema(Schema):
    type = fields.Constant("MultiPolygon", required=True)
    coordinates = fields.List(
        fields.List(
            fields.List(fields.Tuple((fields.Float(), fields.Float()))),
            required=True,
        )
    )


class MultiPolygonFeatureSchema(Schema):
    type = fields.Constant("Feature", required=True)
    geometry = fields.Nested(MultiPolygonGeometrySchema, required=True)
    properties = fields.Dict(required=True)


class MultiPolygonFeatureCollectionSchema(Schema):
    type = fields.Constant("FeatureCollection", required=True)
    features = fields.List(fields.Nested(MultiPolygonFeatureSchema))
