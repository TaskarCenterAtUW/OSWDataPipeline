class OSWWayNormalizer:
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
        else:
            raise ValueError("This is an invalid way")
    
    def _normalize_way(self, keep_keys={}):
        generic_keep_keys={"highway": str, "width": float, "surface": surface, "name": str, "description": str}
        new_tags = {}

        new_tags = _normalize(self.tags, new_tags, generic_keep_keys)
        new_tags = _normalize(self.tags, new_tags, keep_keys)
        
        return new_tags
    
    def _normalize_path(self):
        new_tags = self._normalize_way()
        return new_tags
    
    def _normalize_living_street(self):
        new_tags = self._normalize_way()
        return new_tags
    
    def _normalize_cycleway(self):
        new_tags = self._normalize_way()
        return new_tags
    
    def _normalize_pedestrian(self):
        new_tags = self._normalize_way()
        return new_tags
    
    def _normalize_stairs(self):
        new_tags = self._normalize_way({"step_count": int, "incline": incline})
        if "incline" in new_tags:
            new_tags["climb"] = new_tags["incline"]
            del new_tags["incline"]
        return new_tags
    
    def _normalize_footway(self, keep_keys = {}):
        new_tags = self._normalize_way(keep_keys)

        return new_tags

    def _normalize_sidewalk(self):
        new_tags = self._normalize_footway({"footway": str})

        return new_tags

    def _normalize_crossing(self):
        new_tags = self._normalize_footway({"footway": str, "crossing": crossing, "crossing:markings": crossing_markings})

        return new_tags

    def _normalize_traffic_island(self):
        new_tags = self._normalize_footway({"footway": str})

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
    
    def is_cycleway(self):
        return self.tags.get("highway", "") == "cycleway"
    
    def is_living_street(self):
        return self.tags.get("highway", "") == "living_street"
    
    def is_path(self):
        return self.tags.get("highway", "") == "path"

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

    def _normalize_kerb(self):
        new_tags = {}
        generic_keep_keys = {}
        keep_keys = {"barrier": str, "kerb": str, "tactile_paving": tactile_paving}

        new_tags = _normalize(self.tags, new_tags, generic_keep_keys)
        new_tags = _normalize(self.tags, new_tags, keep_keys)

        new_tags["barrier"] = "kerb"

        return new_tags

    def is_kerb(self):
        return self.tags.get("kerb", "") in self.KERB_VALUES
    
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
    
    def _normalize_point(self, keep_keys):
        new_tags = {
            "is_point": True,
        }

        generic_keep_keys = {}

        new_tags = _normalize(self.tags, new_tags, generic_keep_keys)
        new_tags = _normalize(self.tags, new_tags, keep_keys)

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
    
def _normalize(tags, new_tags, keep_keys):
    for tag, tag_type in keep_keys.items():
        if tag in tags:
            try:
                temp = tag_type(tags[tag])
                if temp is not None:
                    new_tags[tag] = temp
                else:
                    raise ValueError
            except ValueError:
                pass
    
    return new_tags

def tactile_paving(tag_value):
    if tag_value.lower() not in (
                                "yes", 
                                "contrasted", 
                                "no"
                                ):
        return None
    else:
        return tag_value.lower() in (
                                    "yes", 
                                    "contrasted"
                                    )

def surface(tag_value):
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
    
def crossing(tag_value):
    if tag_value.lower() not in (
                                "marked", 
                                "uncontrolled", 
                                "traffic_signals", 
                                "zebra", 
                                "unmarked"
                                ):
        return None
    else:
        if tag_value.lower() in (
                                "marked", 
                                "uncontrolled", 
                                "traffic_signals", 
                                "zebra"
                                ):
            return "marked"
        elif tag_value.lower() == "unmarked":
            return "unmarked"
        
def crossing_markings(tag_value):
    if tag_value.lower() not in (                        
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
        return None
    else:
        return tag_value.lower()
    
def incline(tag_value):
    if tag_value.lower() not in (                        
                                "up",
                                "down"
                                ):
        return None
    else:
        return tag_value.lower()