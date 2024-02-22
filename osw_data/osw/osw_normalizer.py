import types

class OSWWayNormalizer:

    ROAD_HIGHWAY_VALUES = (
        "primary",
        "secondary",
        "tertiary",
        "residential",
        "service",
        "unclassified",
        "trunk", # Not traversible, just for conflation
        "primary_link",
        "secondary_link",
        "tertiary_link",
        "trunk_link"
    )

    def __init__(self, tags):
        self.tags = tags

    def filter(self):
        return (
            self.is_sidewalk()
            or self.is_crossing()
            or self.is_traffic_island()
            or self.is_footway()
            or self.is_stairs()
            or self.is_pedestrian()
            or self.is_living_street()
            or self.is_driveway()
            or self.is_alley()
            or self.is_parking_aisle()
            or self.is_road()
        )

    @staticmethod
    def osw_way_filter(tags):
        return OSWWayNormalizer(tags).filter()

    def normalize(self):
        if self.is_sidewalk():
            return self._normalize_sidewalk()
        elif self.is_crossing():
            return self._normalize_crossing()
        elif self.is_traffic_island():
            return self._normalize_traffic_island()
        elif self.is_footway():
            return self._normalize_footway()
        elif self.is_stairs():
            return self._normalize_stairs()
        elif self.is_pedestrian():
            return self._normalize_pedestrian()
        elif self.is_living_street():
            return self._normalize_living_street()
        elif self.is_driveway():
            return self._normalize_service_road()
        elif self.is_alley():
            return self._normalize_service_road()
        elif self.is_parking_aisle():
            return self._normalize_service_road()
        elif self.is_road():
            return self._normalize_road()
        else:
            raise ValueError("This is an invalid way")
    
    def _normalize_way(self, keep_keys={}, defaults = {}):
        generic_keep_keys = {"highway": str, "width": float, "surface": surface, "name": str, "description": str, "foot": foot}
        generic_defaults = {}
        
        new_tags = _normalize(self.tags, {**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags
    
    def _normalize_pedestrian(self, keep_keys = {}, defaults = {}):
        generic_keep_keys = {}
        generic_defaults = {"foot": "yes"}
        
        new_tags = self._normalize_way({**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags
    
    def _normalize_stairs(self, keep_keys = {}, defaults = {}):
        generic_keep_keys = {"step_count": int, "incline": ["climb", climb]}
        generic_defaults = {"foot": "yes"}
        
        new_tags = self._normalize_way({**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags
    
    def _normalize_footway(self, keep_keys = {}, defaults = {}):
        generic_keep_keys = {}
        generic_defaults = {"foot": "yes"}
        
        new_tags = self._normalize_way({**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags

    def _normalize_sidewalk(self, keep_keys = {}, defaults = {}):
        generic_keep_keys = {"footway": str}
        generic_defaults = {}
        
        new_tags = self._normalize_footway({**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags

    def _normalize_crossing(self, keep_keys = {}, defaults = {}):
        generic_keep_keys = {"footway": str, "crossing": ["crossing:markings", crossing_markings], "crossing:markings": crossing_markings}
        generic_defaults = {}
        
        new_tags = self._normalize_footway({**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags

    def _normalize_traffic_island(self, keep_keys = {}, defaults = {}):
        generic_keep_keys = {"footway": str}
        generic_defaults = {}
        
        new_tags = self._normalize_footway({**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags

    def _normalize_living_street(self, keep_keys = {}, defaults = {}):
        generic_keep_keys = {}
        generic_defaults = {"foot": "yes"}
        
        new_tags = self._normalize_way({**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags
    
    def _normalize_service_road(self, keep_keys = {}, defaults = {}):
        generic_keep_keys = {"service": str}
        generic_defaults = {}
        
        new_tags = self._normalize_road({**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags
    
    def _normalize_road(self, keep_keys = {}, defaults = {}):
        generic_keep_keys = {"highway": (lambda tag_value, tags: tag_value.partition("_")[0]), "maxspeed": ["ext:maxspeed", str]}
        generic_defaults = {}
        
        new_tags = self._normalize_way({**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags

    def is_sidewalk(self):
        return (self.tags.get("highway", "") == "footway") and (
            self.tags.get("footway", "") == "sidewalk"
        )

    def is_crossing(self):
        return (self.tags.get("highway", "") == "footway") and (
            self.tags.get("footway", "") == "crossing"
        )
    
    def is_traffic_island(self):
        return (self.tags.get("highway", "") == "footway") and (
            self.tags.get("footway", "") == "traffic_island"
        )

    def is_footway(self):
        return self.tags.get("highway", "") == "footway"
    
    def is_stairs(self):
        return self.tags.get("highway", "") == "steps"
    
    def is_pedestrian(self):
        return self.tags.get("highway", "") == "pedestrian"
    
    def is_living_street(self):
        return self.tags.get("highway", "") == "living_street"
    
    def is_driveway(self):
        return (self.tags.get("highway", "") == "service") and (
            self.tags.get("service", "") == "driveway"
        )

    def is_alley(self):
        return (self.tags.get("highway", "") == "service") and (
            self.tags.get("service", "") == "alley"
        )
    
    def is_parking_aisle(self):
        return (self.tags.get("highway", "") == "service") and (
            self.tags.get("service", "") == "parking_aisle"
        )
    
    def is_road(self):
        return self.tags.get("highway", "") in self.ROAD_HIGHWAY_VALUES

class OSWNodeNormalizer:
    KERB_VALUES = ("flush", "lowered", "rolled", "raised")

    def __init__(self, tags):
        self.tags = tags

    def filter(self):
        return self.is_kerb()

    @staticmethod
    def osw_node_filter(tags):
        return OSWNodeNormalizer(tags).filter()

    def normalize(self):
        if self.is_kerb():
            return self._normalize_kerb()
        else:
            raise ValueError("This is an invalid node")

    def _normalize_node(self, keep_keys={}, defaults = {}):
        generic_keep_keys = {}
        generic_defaults = {}
        
        new_tags = _normalize(self.tags, {**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags
    
    def _normalize_kerb(self, keep_keys = {}, defaults = {}):
        generic_keep_keys = {"barrier": "kerb", "kerb": kerb, "tactile_paving": tactile_paving}
        generic_defaults = {}
        
        new_tags = self._normalize_node({**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags

    def is_kerb(self):
        return (self.tags.get("kerb", "") in self.KERB_VALUES) or (
            self.tags.get("barrier", "") == "kerb" and ("kerb" not in self.tags or self.tags.get("kerb", "") == "yes")
        )
    
class OSWPointNormalizer:
    def __init__(self, tags):
        self.tags = tags

    def filter(self):
        return (self.is_powerpole()) or (
            self.is_firehydrant()) or (
            self.is_bench()) or (
            self.is_waste_basket()) or (
            self.is_manhole()) or (
            self.is_bollard()) or (
            self.is_street_lamp())
    
    @staticmethod
    def osw_point_filter(tags):
        return OSWPointNormalizer(tags).filter()

    def normalize(self):
        if self.is_powerpole():
            return self._normalize_point({"power": str})
        elif self.is_firehydrant():
            return self._normalize_point({"emergency": str})
        elif self.is_bench() or self.is_waste_basket():
            return self._normalize_point({"amenity": str})
        elif self.is_manhole():
            return self._normalize_point({"man_made": str})
        elif self.is_bollard():
            return self._normalize_point({"barrier": str})
        elif self.is_street_lamp():
            return self._normalize_point({"highway": str})
        else:
            raise ValueError("This is an invalid point")
    
    def _normalize_point(self, keep_keys={}, defaults = {}):
        generic_keep_keys = {}
        generic_defaults = {}
        
        new_tags = _normalize(self.tags, {**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags
    
    def is_powerpole(self):
        return self.tags.get("power", "") == "pole"
    
    def is_firehydrant(self):
        return self.tags.get("emergency", "") == "fire_hydrant"
    
    def is_bench(self):
        return self.tags.get("amenity", "") == "bench"
    
    def is_waste_basket(self):
        return self.tags.get("amenity", "") == "waste_basket"

    def is_manhole(self):
        return self.tags.get("man_made", "") == "manhole"
    
    def is_bollard(self):
        return self.tags.get("barrier", "") == "bollard"
    
    def is_street_lamp(self):
        return self.tags.get("highway", "") == "street_lamp"
    
class OSWLineNormalizer:
    def __init__(self, tags):
        self.tags = tags

    def filter(self):
        return (self.is_fence())
    
    @staticmethod
    def osw_line_filter(tags):
        return OSWLineNormalizer(tags).filter()

    def normalize(self):
        if self.is_fence():
            return self._normalize_line({"barrier": str})
        else:
            raise ValueError("This is an invalid line")
    
    def _normalize_line(self, keep_keys={}, defaults = {}):
        generic_keep_keys = {}
        generic_defaults = {}
        
        new_tags = _normalize(self.tags, {**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags
    
    def is_fence(self):
        return self.tags.get("barrier", "") == "fence"
    
class OSWPolygonNormalizer:
    # Will be fetched from schema soon
    BUILDING_VALUES = (
        "allotment_house",
        "apartments",
        "bakehouse",
        "barn",
        "barracks",
        "beach_hut",
        "boathouse",
        "bridge",
        "bungalow",
        "bunker",
        "cabin",
        "carport",
        "castle",
        "cathedral",
        "chapel",
        "church",
        "civic",
        "college",
        "commercial",
        "conservatory",
        "construction",
        "container",
        "cowshed",
        "detached",
        "digester",
        "dormitory",
        "farm",
        "farm_auxiliary",
        "fire_station",
        "garage",
        "garages",
        "gatehouse",
        "ger",
        "government",
        "grandstand",
        "greenhouse",
        "guardhouse",
        "hangar",
        "hospital",
        "hotel",
        "house",
        "houseboat",
        "hut",
        "industrial",
        "kindergarten",
        "kingdom_hall",
        "kiosk",
        "livestock",
        "military",
        "monastery",
        "mosque",
        "museum",
        "office",
        "outbuilding",
        "pagoda",
        "parking",
        "pavilion",
        "presbytery",
        "public",
        "quonset_hut",
        "religious",
        "residential",
        "retail",
        "riding_hall",
        "roof",
        "ruins",
        "school",
        "semidetached_house",
        "service",
        "shed",
        "shrine",
        "silo",
        "slurry_tank",
        "sports_centre",
        "sports_hall",
        "stable",
        "stadium",
        "static_caravan",
        "stilt_house",
        "storage_tank",
        "sty",
        "supermarket",
        "synagogue",
        "tech_cab",
        "temple",
        "tent",
        "terrace",
        "toilets",
        "tower",
        "train_station",
        "transformer_tower",
        "transportation",
        "tree_house",
        "trullo",
        "university",
        "warehouse",
        "water_tower",
        "windmill",
        "yes"
    )

    def __init__(self, tags):
        self.tags = tags

    def filter(self):
        return (self.is_building())
    
    @staticmethod
    def osw_polygon_filter(tags):
        return OSWPolygonNormalizer(tags).filter()

    def normalize(self):
        if self.is_building():
            return self._normalize_polygon({"building": str, "name": str, "opening_hours": str})
        else:
            raise ValueError("This is an invalid polygon")
    
    def _normalize_polygon(self, keep_keys={}, defaults = {}):
        generic_keep_keys = {}
        generic_defaults = {}
        
        new_tags = _normalize(self.tags, {**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags
    
    def is_building(self):
        return self.tags.get("building", "") in self.BUILDING_VALUES

class OSWZoneNormalizer:
    def __init__(self, tags):
        self.tags = tags

    def filter(self):
        return (self.is_pedestrian())
    
    @staticmethod
    def osw_zone_filter(tags):
        return OSWZoneNormalizer(tags).filter()

    def normalize(self):
        if self.is_pedestrian():
            return self._normalize_zone({"highway": str, "surface": surface, "name": str, "description": str})
        else:
            raise ValueError("This is an invalid zone")
    
    def _normalize_zone(self, keep_keys={}, defaults = {}):
        generic_keep_keys = {}
        generic_defaults = {"foot": "yes"}
        
        new_tags = _normalize(self.tags, {**generic_keep_keys, **keep_keys}, {**generic_defaults, **defaults})
        return new_tags
    
    def is_pedestrian(self):
        return self.tags.get("highway", "") == "pedestrian"
    
def _normalize(tags, keep_keys, defaults):
    new_tags = {}
    for tag, tag_type in keep_keys.items():
        try:
            if isinstance(tag_type, list):
                if isinstance(tag_type[1], (str, bool, int, float)):
                    new_tags[tag_type[0]] = tag_type[1]
                elif isinstance(tag_type[1], types.FunctionType):
                    temp = tag_type[1](tags[tag], tags)
                    if temp is not None:
                        new_tags[tag_type[0]] = temp
                    else:
                        raise ValueError
                else:
                    temp = tag_type[1](tags[tag])
                    if temp is not None:
                        new_tags[tag_type[0]] = temp
                    else:
                        raise ValueError
            else:
                if isinstance(tag_type, (str, bool, int, float)):
                    new_tags[tag] = tag_type
                elif isinstance(tag_type, types.FunctionType):
                    temp = tag_type(tags[tag], tags)
                    if temp is not None:
                        new_tags[tag] = temp
                    else:
                        raise ValueError
                else:
                    temp = tag_type(tags[tag])
                    if temp is not None:
                        new_tags[tag] = temp
                    else:
                        raise ValueError
        except ValueError:
            pass
        except KeyError:
            pass
    
    # Preserve order of keep_keys first followed by defaults
    return {**{**new_tags, **defaults}, **new_tags}
    
def tactile_paving(tag_value, tags):
    if tag_value.lower() not in (
                                "yes", 
                                "contrasted", 
                                "no",
                                "primitive",
                                ):
        return None
    else:
        return tag_value.lower()

def surface(tag_value, tags):
    if tag_value.lower() not in (
                                "asphalt", 
                                "concrete", 
                                "gravel", 
                                "grass", 
                                "paved", 
                                "paving_stones", 
                                "unpaved", 
                                "dirt", 
                                "grass_paver"
                                ):
        return None
    else:
        return tag_value.lower()
    
def crossing_markings(tag_value, tags):
    if tags.get("crossing:markings", "").lower() in (                        
                                "dashes",
                                "dots",
                                "ladder",
                                "ladder:paired",
                                "lines",
                                "lines:paired",
                                "no",
                                "skewed",
                                "surface",
                                "yes",
                                "zebra",
                                "zebra:bicolour",
                                "zebra:double",
                                "zebra:paired",
                                "rainbow",
                                "lines:rainbow",
                                "zebra:rainbow",
                                "ladder:skewed",
                                "pictograms"
                                ):
        return tags["crossing:markings"].lower()
    elif tags.get("crossing", "").lower() == "marked":
        return "yes"
    elif tags.get("crossing", "").lower() == "zebra":
        return "zebra"
    elif tags.get("crossing", "").lower() == "unmarked":
        return "no"
    else:
        return None
    
def climb(tag_value, tags):
    if tag_value.lower() not in (                        
                                "up",
                                "down"
                                ):
        return None
    else:
        return tag_value.lower()
    
def foot(tag_value, tags):
    if tag_value.lower() not in (
                                "yes", 
                                "no", 
                                "designated", 
                                "permissive", 
                                "use_sidepath", 
                                "private", 
                                "destination"
                                ):
        return None
    else:
        return tag_value.lower()
    
def kerb(tag_value, tags):
    if tag_value.lower() not in OSWNodeNormalizer.KERB_VALUES:
        return None
    else:
        return tag_value.lower()