import ogr2osm

class OSMNormalizer(ogr2osm.TranslationBase):  
    def filter_tags(self, tags):
        '''
        Override this method if you want to modify or add tags to the xml output
        '''
        # OSW fields with similar OSM field names
        tags['osw:incline'] = tags.pop('incline', '')
        tags['incline'] = tags.pop('climb', '')
        
        # Boolean fields (lossy, fix)
        if 'tactile_paving' in tags and tags['tactile_paving']:
            tags['tactile_paving'] = 'yes' if tags['tactile_paving'] == '1' else 'no'
        
        # OSW derived fields
        tags.pop('_u_id', '')
        tags.pop('_v_id', '')
        
        return tags

    def process_feature_post(self, osmgeometry, ogrfeature, ogrgeometry):
        '''
        This method is called after the creation of an OsmGeometry object. The
        ogr feature and ogr geometry used to create the object are passed as
        well. Note that any return values will be discarded by ogr2osm.
        '''
        osmgeometry.id = int(osmgeometry.tags.pop('_id')[0])