# -*- coding: utf-8 -*-
"""
Created on Fri Feb 23 14:01:37 2018

@author: Bill
"""
import xml.etree.cElementTree as ET
import json
import datetime
import codecs
from pymongo import MongoClient

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

def shape_element(element):
    node = {}
    if element.tag in ('node', 'way', 'relation'):
        node['id'] = get_attrib(element,'id')
        node['type'] = element.tag
        node['visible'] = get_attrib(element,'visible')
        node['created'] = get_created(element)
        node['pos'] = get_pos(element)
        
        if element.tag == "node":
            node['tags']=get_node_tags(element)
        
        if element.tag == "way":
            node['tags']=get_way_tags(element)
            
        if element.tag == "relation":
            node['tags']=get_relation_tags(element)
        
        return node
    else:
        return None

def get_relation_tags(element):
    tags={}
    tags['members']=[]
    for m in element.findall('member'):
        tags['members'].append({'type':m.attrib['type'],'ref':m.attrib['ref'],'role':m.attrib['role']})
    
    for a in element.findall('tag'):
        if a.attrib['k'] == 'from':
            tags['from'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'name':
            tags['name'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'network':
            tags['network'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'ref':
            tags['ref'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'route':
            tags['route'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'type':
            tags['type'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'wikidata':
            tags['wikidata'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'wikipedia':
            tags['wikipedia'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'code':
            tags['code'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'destination':
            tags['destination'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'waterway':
            tags['waterway'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'nsdi_code':
            tags['nsdi_code'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'zhb_code':
            tags['zhb_code'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'admin_level':
            tags['admin_level'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'boundary':
            tags['boundary'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'flag':
            tags['flag'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'is_in:continent':
            tags['continent'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'official_name':
            tags['official_name'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'distance':
            tags['distance'] = float(a.attrib['v'])
            continue
        if a.attrib['k'] == 'start_date':
            tags['start_date'] = datetime.datetime.strptime(a.attrib['v'],'%Y-%m-%d') 
            continue
        if a.attrib['k'] == 'loc_name':
            tags['loc_name'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'timezone':
            tags['timezone'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'landuse':
            tags['landuse'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'natural':
            tags['natural'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'description':
            tags['description'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'highway':
            tags['highway'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'toll':
            tags['toll'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'lanes':
            tags['lanes'] = int(a.attrib['v'])
            continue
        if a.attrib['k'] == 'building':
            tags['building'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'barrier':
            tags['barrier'] = a.attrib['v']
            continue
    
    return tags    
    

def get_way_tags(element):
    tags={}
    tags['refs'] = []
    for a in element.findall('nd'):
        tags['refs'].append(int(a.attrib['ref']))
    for a in element.findall('tag'):
        if a.attrib['k'] == 'electrified':
            tags['electrified'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'frequency':
            tags['frequency'] = int(a.attrib['v'])
            continue
        if a.attrib['k'] == 'gauge':
            tags['gauge'] = int(a.attrib['v'])
            continue
        if a.attrib['k'] == 'layer':
            tags['layer'] = int(a.attrib['v'])
            continue
        if a.attrib['k'] == 'name':
            tags['name'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'railway':
            tags['railway'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'usage':
            tags['usage'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'voltage':
            tags['voltage'] = int(a.attrib['v'])
            continue
        if a.attrib['k'] == 'man_made':
            tags['man_made'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'natural':
            tags['natural'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'created_by':
            tags['created_by'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'highway':
            tags['highway'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'int_ref':
            tags['int_ref'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'lanes':
            tags['lanes'] = int(a.attrib['v'])
            continue
        if a.attrib['k'] == 'oneway':
            tags['oneway'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'ref':
            tags['ref'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'source':
            tags['source'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'history':
            tags['history'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'bridge':
            tags['bridge'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'surface':
            tags['surface'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'surface':
            tags['surface'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'highspeed':
            tags['highspeed'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'maxspeed':
            tags['maxspeed'] =  int(a.attrib['v'])
            continue
        if a.attrib['k'] == 'usage':
            tags['usage'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'lit':
            tags['lit'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'code':
            tags['code'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'wikipedia':
            tags['wikipedia'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'wikidata':
            tags['wikidata'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'junction':
            tags['junction'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'leisure':
            tags['leisure'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'note':
            tags['note'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'landuse':
            tags['landuse'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'waterway':
            tags['waterway'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'power':
            tags['power'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'admin_level':
            tags['admin_level'] = int(a.attrib['v'])
            continue
        if a.attrib['k'] == 'boundary':
            tags['boundary'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'tunnel':
            tags['tunnel'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'service':
            tags['service'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'office':
            tags['office'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'addr:postcode':
            tags['postcode'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'tourism':
            tags['tourism'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'route_master':
            tags['route_master'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'destination':
            tags['destination'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'nsdi_code':
            tags['nsdi_code'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'zhb_code':
            tags['zhb_code'] =  a.attrib['v']
            continue
    return tags

def get_node_tags(element):
    tags={}
    for a in element.findall('tag'):
        if a.attrib['k'] == 'int_name':
            tags['int_name']=a.attrib['v']
            continue
        if a.attrib['k'] == 'alt_name':
            tags['alt_name']=a.attrib['v']
            continue
        if a.attrib['k'] == 'alt_name:en':
            tags['alt_name_en']=a.attrib['v']
            continue
        if a.attrib['k'] == 'is_in:continent':
            tags['continent'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'is_in:country':
            tags['country'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'is_in:country_code':
            tags['country_code'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'name':
            tags['name'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'website':
            tags['website'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'place':
            tags['place'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'population':
            tags['population'] = int(a.attrib['v'])
            continue
        if a.attrib['k'] == 'wikidata':
            tags['wikidata'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'wikipedia':
            tags['wikipedia'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'public_transport':
            tags['public_transport'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'railway':
            tags['railway'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'barrier':
            tags['barrier'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'highway':
            tags['highway'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'building':
            tags['building'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'lanes':
            tags['lanes'] = int(a.attrib['v'])
            continue
        if a.attrib['k'] == 'power':
            tags['power'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'source':
            tags['source'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'addr:housenumber':
            tags['housenumber'] = a.attrib['v']
            continue
        if a.attrib['k'] == 'addr:street':
            tags['street'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'addr:district':
            tags['district'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'addr:postcode':
            tags['postcode'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'amenity':
            tags['amenity'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'tourism':
            tags['tourism'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'shop':
            tags['shop'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'covered':
            tags['covered'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'sport':
            tags['sport'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'man_made':
            tags['man_made'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'tower:type':
            tags['tower_type'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'fixme':
            tags['fixme'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'historic':
            tags['historic'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'designation':
            tags['designation'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'religion':
            tags['religion'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'entrance':
            tags['entrance'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'operator':
            tags['operator'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'cuisine':
            tags['cuisine'] =  a.attrib['v']
            #<tag k="cuisine" v="chinese;savory_pancakes;deli"/>
            continue
        if a.attrib['k'] == 'addr:province':
            tags['province'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'addr:city':
            tags['city'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'healthcare':
            tags['healthcare'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'opening_hours':
            tags['opening_hours'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'office':
            tags['office'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'drive_in':
            tags['drive_in'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'internet_access':
            tags['internet_access'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'internet_access:fee':
            tags['internet_access_fee'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'microbrewery':
            tags['microbrewery'] =  True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'organic':
            tags['organic'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'outdoor_seating':
            tags['outdoor_seating'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'smoking':
            tags['smoking'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'takeaway':
            tags['takeaway'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'wheelchair':
            tags['wheelchair'] =  True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'station':
            tags['station'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'subway':
            tags['subway'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'leisure':
            tags['leisure'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'construction':
            tags['construction'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'construction_date':
            tags['construction_date'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'crossing':
            tags['crossing'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'phone':
            tags['phone'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'maxheight':
            tags['maxheight'] =  float(a.attrib['v'])
            continue
        if a.attrib['k'] == 'parking':
            tags['parking'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'old_name':
            tags['old_name'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'natural':
            tags['natural'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'memorial':
            tags['memorial'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'waterway':
            tags['waterway'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'generator_method':
            tags['generator_method'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'generator_source':
            tags['generator_source'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'oneway':
            tags['oneway'] = True if a.attrib['v'] == 'yes' else False
            continue
        if a.attrib['k'] == 'government':
            tags['government'] =  a.attrib['v']
            continue
        if a.attrib['k'] == 'surface':
            tags['surface'] =  a.attrib['v']
            continue
    return tags
        

def get_pos(element):
    if element.tag == 'node':
        return [float(element.attrib['lat']),float(element.attrib['lon'])]

def get_attrib(element,k):
    return element.get(k)

def get_created(element):
    
    created = {}    
    
    created["version"] = int(get_attrib(element,'version'))
    created["changeset"] = int(get_attrib(element,'changeset'))
    created["timestamp"] = datetime.datetime.strptime(get_attrib(element,'timestamp'),"%Y-%m-%dT%H:%M:%SZ")
    created["user"] = get_attrib(element,'user')
    created["uid"] = int(get_attrib(element,'uid'))
    
    return created

data_node=[]
data_way=[]
data_relation=[]

def process_map(file_in, pretty = False):
    for _, element in ET.iterparse(file_in):
        el = shape_element(element)
        if el:
            if el['type'] == 'node':
                data_node.append(el)
            elif el['type'] == 'way':
                data_way.append(el)
            else:
                data_relation.append(el)

def get_db():
    client = MongoClient('10.110.30.27',27017,serverSelectionTimeoutMS=10)
    db = client['zhengzhoumap']
    return db

class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)

def save_to_file(filename,data_source):
     with codecs.open(filename, encoding='utf-8', mode='w') as v:
        for d in data_source:
            v.write(json.dumps(d,cls=CJsonEncoder)+"\n")

def save_to_mongo(filename,db,tablename):
    for l in codecs.open(filename, encoding='utf-8', mode='r').readlines():
        data=json.loads(l)
        db[tablename].insert_one(data)

if __name__ == "__main__":

    process_map("zhengzhoumap.osm")
    
    #save_to_file("zhengzhoumap_node.json",data_node)
    save_to_file("zhengzhoumap_way.json",data_way)
    save_to_file("zhengzhoumap_relation.json",data_relation)
    
    db=get_db();
    #save_to_mongo("zhengzhoumap_node.json",db,'node')
    save_to_mongo("zhengzhoumap_way.json",db,'way')
    save_to_mongo("zhengzhoumap_relation.json",db,'relation')



