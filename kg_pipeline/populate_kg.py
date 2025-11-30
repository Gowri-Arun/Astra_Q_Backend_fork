"""
kg_pipeline/populate_kg_enhanced.py
Enhanced Knowledge Graph Population with All Relationships
"""

import os
import json
import re
from neo4j import GraphDatabase
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()


class EnhancedNeo4jPopulator:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
        # Satellite metadata from MOSDAC
        self.satellite_data = {
            'INSAT-3DR': {
                'id': 'insat-3dr',
                'launch_date': '2016-09-08',
                'orbit_type': 'geostationary',
                'status': 'active',
                'mission': {
                    'id': 'insat-3dr-mission',
                    'name': 'INSAT-3DR Mission',
                    'objectives': ['Weather monitoring', 'Disaster warning', 'Search and rescue'],
                    'agency': 'ISRO'
                },
                'payloads': [
                    {
                        'id': 'insat-3dr-imager',
                        'name': '6-channel Imager',
                        'type': 'imager',
                        'resolution': '1km to 8km',
                        'spectral_bands': ['VIS', 'SWIR', 'MIR', 'TIR1', 'TIR2', 'WV']
                    },
                    {
                        'id': 'insat-3dr-sounder',
                        'name': '19-channel Sounder',
                        'type': 'sounder',
                        'resolution': '10km',
                        'spectral_bands': ['LWIR', 'MWIR', 'SWIR', 'VIS']
                    }
                ],
                'parameters': ['sea_surface_temperature', 'rainfall', 'cloud_cover', 'water_vapor'],
                'region': 'Indian Ocean'
            },
            'INSAT-3D': {
                'id': 'insat-3d',
                'launch_date': '2013-07-26',
                'orbit_type': 'geostationary',
                'status': 'active',
                'mission': {
                    'id': 'insat-3d-mission',
                    'name': 'INSAT-3D Mission',
                    'objectives': ['Meteorological observation', 'Data relay'],
                    'agency': 'ISRO'
                },
                'payloads': [
                    {
                        'id': 'insat-3d-imager',
                        'name': '6-channel Imager',
                        'type': 'imager',
                        'resolution': '1km to 8km',
                        'spectral_bands': ['VIS', 'SWIR', 'MIR', 'TIR1', 'TIR2', 'WV']
                    },
                    {
                        'id': 'insat-3d-sounder',
                        'name': '19-channel Sounder',
                        'type': 'sounder',
                        'resolution': '10km',
                        'spectral_bands': ['LWIR', 'MWIR', 'SWIR', 'VIS']
                    }
                ],
                'parameters': ['sea_surface_temperature', 'rainfall', 'cloud_cover', 'outgoing_longwave_radiation'],
                'region': 'Indian Ocean'
            },
            'INSAT-3DS': {
                'id': 'insat-3ds',
                'launch_date': '2024-08-17',
                'orbit_type': 'geostationary',
                'status': 'active',
                'mission': {
                    'id': 'insat-3ds-mission',
                    'name': 'INSAT-3DS Mission',
                    'objectives': ['Enhanced meteorological observations', 'Improved imaging'],
                    'agency': 'ISRO'
                },
                'payloads': [
                    {
                        'id': 'insat-3ds-imager',
                        'name': 'Enhanced 6-channel Imager',
                        'type': 'imager',
                        'resolution': '1km to 4km',
                        'spectral_bands': ['VIS', 'SWIR', 'MIR', 'TIR1', 'TIR2', 'WV']
                    }
                ],
                'parameters': ['sea_surface_temperature', 'cloud_cover', 'water_vapor'],
                'region': 'Indian Ocean'
            },
            'OCEANSAT-2': {
                'id': 'oceansat-2',
                'launch_date': '2009-09-23',
                'orbit_type': 'polar',
                'status': 'active',
                'mission': {
                    'id': 'oceansat-2-mission',
                    'name': 'OCEANSAT-2 Mission',
                    'objectives': ['Ocean color monitoring', 'Wind vector measurements'],
                    'agency': 'ISRO'
                },
                'payloads': [
                    {
                        'id': 'oceansat-2-ocm',
                        'name': 'Ocean Colour Monitor (OCM)',
                        'type': 'radiometer',
                        'resolution': '360m',
                        'spectral_bands': ['8 visible to NIR bands']
                    },
                    {
                        'id': 'oceansat-2-oscat',
                        'name': 'Scatterometer (OSCAT)',
                        'type': 'scatterometer',
                        'resolution': '50km',
                        'spectral_bands': ['Ku-band']
                    }
                ],
                'parameters': ['chlorophyll_concentration', 'sea_surface_temperature', 'wind_speed', 'wind_direction'],
                'region': 'Global Ocean'
            },
            'OCEANSAT-3': {
                'id': 'oceansat-3',
                'launch_date': '2022-11-26',
                'orbit_type': 'polar',
                'status': 'active',
                'mission': {
                    'id': 'oceansat-3-mission',
                    'name': 'OCEANSAT-3 Mission',
                    'objectives': ['Ocean biology', 'Ocean surface studies'],
                    'agency': 'ISRO'
                },
                'payloads': [
                    {
                        'id': 'oceansat-3-ocm',
                        'name': 'Ocean Colour Monitor-3 (OCM-3)',
                        'type': 'radiometer',
                        'resolution': '360m',
                        'spectral_bands': ['13 bands from 400-1000nm']
                    }
                ],
                'parameters': ['chlorophyll_concentration', 'sea_surface_temperature', 'suspended_sediment'],
                'region': 'Global Ocean'
            },
            'SCATSAT-1': {
                'id': 'scatsat-1',
                'launch_date': '2016-09-26',
                'orbit_type': 'polar',
                'status': 'active',
                'mission': {
                    'id': 'scatsat-1-mission',
                    'name': 'SCATSAT-1 Mission',
                    'objectives': ['Ocean wind monitoring', 'Weather forecasting'],
                    'agency': 'ISRO'
                },
                'payloads': [
                    {
                        'id': 'scatsat-1-oscat',
                        'name': 'Ku-band Scatterometer',
                        'type': 'scatterometer',
                        'resolution': '50km',
                        'spectral_bands': ['Ku-band']
                    }
                ],
                'parameters': ['wind_speed', 'wind_direction'],
                'region': 'Global Ocean'
            },
            'MEGHA-TROPIQUES': {
                'id': 'megha-tropiques',
                'launch_date': '2011-10-12',
                'orbit_type': 'low inclination',
                'status': 'decommissioned',
                'mission': {
                    'id': 'megha-tropiques-mission',
                    'name': 'Megha-Tropiques Mission',
                    'objectives': ['Tropical water cycle', 'Atmospheric energy budget'],
                    'agency': 'ISRO-CNES'
                },
                'payloads': [
                    {
                        'id': 'megha-tropiques-madras',
                        'name': 'MADRAS',
                        'type': 'radiometer',
                        'resolution': '40km',
                        'spectral_bands': ['9 channels']
                    },
                    {
                        'id': 'megha-tropiques-saphir',
                        'name': 'SAPHIR',
                        'type': 'sounder',
                        'resolution': '10km',
                        'spectral_bands': ['6 channels near 183 GHz']
                    }
                ],
                'parameters': ['rainfall', 'water_vapor', 'cloud_liquid_water'],
                'region': 'Tropics'
            },
            'SARAL-ALTIKA': {
                'id': 'saral-altika',
                'launch_date': '2013-02-25',
                'orbit_type': 'polar',
                'status': 'active',
                'mission': {
                    'id': 'saral-mission',
                    'name': 'SARAL Mission',
                    'objectives': ['Ocean altimetry', 'Sea level monitoring'],
                    'agency': 'ISRO-CNES'
                },
                'payloads': [
                    {
                        'id': 'saral-altika',
                        'name': 'AltiKa Altimeter',
                        'type': 'altimeter',
                        'resolution': '2cm accuracy',
                        'spectral_bands': ['Ka-band']
                    }
                ],
                'parameters': ['sea_surface_height', 'wave_height', 'wind_speed'],
                'region': 'Global Ocean'
            }
        }
        
        # Parameter definitions
        self.parameters = {
            'sea_surface_temperature': {'category': 'ocean', 'unit': 'Celsius'},
            'rainfall': {'category': 'atmosphere', 'unit': 'mm/hr'},
            'cloud_cover': {'category': 'atmosphere', 'unit': 'percentage'},
            'water_vapor': {'category': 'atmosphere', 'unit': 'kg/m2'},
            'chlorophyll_concentration': {'category': 'ocean', 'unit': 'mg/m3'},
            'wind_speed': {'category': 'ocean', 'unit': 'm/s'},
            'wind_direction': {'category': 'ocean', 'unit': 'degrees'},
            'sea_surface_height': {'category': 'ocean', 'unit': 'meters'},
            'wave_height': {'category': 'ocean', 'unit': 'meters'},
            'suspended_sediment': {'category': 'ocean', 'unit': 'mg/l'},
            'cloud_liquid_water': {'category': 'atmosphere', 'unit': 'kg/m2'},
            'outgoing_longwave_radiation': {'category': 'atmosphere', 'unit': 'W/m2'}
        }
        
        # Region definitions
        self.regions = {
            'Indian Ocean': {
                'id': 'indian-ocean',
                'type': 'ocean',
                'bounds': {'lat_min': -60.0, 'lat_max': 30.0, 'lon_min': 30.0, 'lon_max': 120.0}
            },
            'Global Ocean': {
                'id': 'global-ocean',
                'type': 'ocean',
                'bounds': {'lat_min': -90.0, 'lat_max': 90.0, 'lon_min': -180.0, 'lon_max': 180.0}
            },
            'Tropics': {
                'id': 'tropics',
                'type': 'atmosphere',
                'bounds': {'lat_min': -23.5, 'lat_max': 23.5, 'lon_min': -180.0, 'lon_max': 180.0}
            }
        }
        
        # Product definitions from MOSDAC Open Data
        self.products = [
            {
                'id': 'bayesian-rainfall',
                'name': 'Bayesian based MT-SAPHIR rainfall',
                'description': 'Rainfall estimation using Bayesian algorithm from SAPHIR',
                'category': 'atmosphere',
                'product_type': 'L2',
                'format': 'HDF5',
                'spatial_resolution': '10km',
                'temporal_resolution': 'Daily',
                'satellite': 'MEGHA-TROPIQUES',
                'payload': 'megha-tropiques-saphir',
                'parameters': ['rainfall'],
                'region': 'Tropics'
            },
            {
                'id': 'gps-water-vapour',
                'name': 'GPS derived Integrated water vapour',
                'description': 'Water vapour measurements from GPS network',
                'category': 'atmosphere',
                'product_type': 'L2',
                'format': 'NetCDF',
                'spatial_resolution': 'Point',
                'temporal_resolution': 'Hourly',
                'parameters': ['water_vapor'],
                'region': 'Indian Ocean'
            },
            {
                'id': 'ocean-surface-current',
                'name': 'Global Ocean Surface Current',
                'description': 'Ocean surface current vectors derived from altimetry',
                'category': 'ocean',
                'product_type': 'L3',
                'format': 'NetCDF',
                'spatial_resolution': '0.25 degree',
                'temporal_resolution': 'Daily',
                'satellite': 'SARAL-ALTIKA',
                'payload': 'saral-altika',
                'parameters': ['wind_speed', 'wind_direction'],
                'region': 'Global Ocean'
            },
            {
                'id': 'sea-surface-salinity',
                'name': 'High Resolution Sea Surface Salinity',
                'description': 'High-resolution SSS measurements',
                'category': 'ocean',
                'product_type': 'L3',
                'format': 'NetCDF',
                'spatial_resolution': '25km',
                'temporal_resolution': 'Weekly',
                'satellite': 'OCEANSAT-3',
                'payload': 'oceansat-3-ocm',
                'parameters': ['sea_surface_temperature'],
                'region': 'Global Ocean'
            },
            {
                'id': 'soil-moisture',
                'name': 'Soil Moisture',
                'description': 'Surface soil moisture estimates',
                'category': 'land',
                'product_type': 'L2',
                'format': 'GeoTIFF',
                'spatial_resolution': '1km',
                'temporal_resolution': 'Daily',
                'parameters': [],
                'region': 'Indian Ocean'
            }
        ]
    
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        """Clear all nodes and relationships"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("✅ Database cleared")
    
    def create_constraints(self):
        """Create unique constraints and indexes"""
        constraints = [
            "CREATE CONSTRAINT satellite_id IF NOT EXISTS FOR (s:Satellite) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT parameter_id IF NOT EXISTS FOR (par:Parameter) REQUIRE par.id IS UNIQUE",
            "CREATE CONSTRAINT region_id IF NOT EXISTS FOR (r:Region) REQUIRE r.id IS UNIQUE",
            "CREATE CONSTRAINT mission_id IF NOT EXISTS FOR (m:Mission) REQUIRE m.id IS UNIQUE",
            "CREATE CONSTRAINT payload_id IF NOT EXISTS FOR (pl:Payload) REQUIRE pl.id IS UNIQUE",
            "CREATE INDEX satellite_name IF NOT EXISTS FOR (s:Satellite) ON (s.name)",
            "CREATE INDEX product_name IF NOT EXISTS FOR (p:Product) ON (p.name)",
        ]
        
        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception:
                    pass  # Already exists
        
        print("✅ Constraints and indexes created")
    
    def populate_all(self):
        """Main method to populate entire knowledge graph"""
        with self.driver.session() as session:
            print("\n🚀 Starting comprehensive population...\n")
            
            # Step 1: Create all parameters
            print("📊 Creating parameters...")
            for param_type, param_info in self.parameters.items():
                self._create_parameter(session, param_type, param_info)
            
            # Step 2: Create all regions
            print("\n🗺️  Creating regions...")
            for region_name, region_info in self.regions.items():
                self._create_region(session, region_name, region_info)
            
            # Step 3: Create satellites with all relationships
            print("\n🛰️  Creating satellites and relationships...")
            for sat_name, sat_data in self.satellite_data.items():
                print(f"\n  Processing {sat_name}...")
                
                # Create satellite
                self._create_satellite(session, sat_name, sat_data)
                
                # Create mission
                self._create_mission(session, sat_data['mission'])
                self._link_satellite_to_mission(session, sat_data['id'], sat_data['mission']['id'])
                
                # Create payloads
                for payload in sat_data['payloads']:
                    self._create_payload(session, payload)
                    self._link_satellite_to_payload(session, sat_data['id'], payload['id'])
                
                # Link to parameters
                for param_type in sat_data['parameters']:
                    self._link_satellite_to_parameter(session, sat_data['id'], param_type)
                
                # Link to region
                if sat_data['region'] in self.regions:
                    region_id = self.regions[sat_data['region']]['id']
                    self._link_satellite_to_region(session, sat_data['id'], region_id)
            
            # Step 4: Create products with relationships
            print("\n📦 Creating products and relationships...")
            for product in self.products:
                self._create_product(session, product)
                
                # Link to parameters
                for param_type in product['parameters']:
                    self._link_product_to_parameter(session, product['id'], param_type)
                
                # Link to satellite
                if 'satellite' in product:
                    sat_id = self.satellite_data[product['satellite']]['id']
                    self._link_satellite_to_product(session, sat_id, product['id'])
                
                # Link to payload
                if 'payload' in product:
                    self._link_product_to_payload(session, product['id'], product['payload'])
                
                # Link to region
                if product['region'] in self.regions:
                    region_id = self.regions[product['region']]['id']
                    self._link_product_to_region(session, product['id'], region_id)
            
            print("\n✅ Population complete!")
    
    def _create_satellite(self, session, name, data):
        query = """
        MERGE (s:Satellite {id: $id})
        SET s.name = $name,
            s.launch_date = date($launch_date),
            s.orbit_type = $orbit_type,
            s.status = $status
        """
        session.run(query, id=data['id'], name=name, 
                   launch_date=data['launch_date'],
                   orbit_type=data['orbit_type'],
                   status=data['status'])
    
    def _create_mission(self, session, mission):
        query = """
        MERGE (m:Mission {id: $id})
        SET m.name = $name,
            m.objectives = $objectives,
            m.agency = $agency
        """
        session.run(query, **mission)
    
    def _create_payload(self, session, payload):
        query = """
        MERGE (pl:Payload {id: $id})
        SET pl.name = $name,
            pl.type = $type,
            pl.resolution = $resolution,
            pl.spectral_bands = $spectral_bands
        """
        session.run(query, **payload)
    
    def _create_parameter(self, session, param_type, param_info):
        query = """
        MERGE (par:Parameter {id: $id})
        SET par.type = $type,
            par.category = $category,
            par.unit = $unit
        """
        session.run(query, id=param_type.replace('_', '-'), 
                   type=param_type, **param_info)
    
    def _create_region(self, session, name, region_info):
        query = """
        MERGE (r:Region {id: $id})
        SET r.name = $name,
            r.type = $type,
            r.bounds = $bounds
        """
        session.run(query, name=name, **region_info)
    
    def _create_product(self, session, product):
        query = """
        MERGE (p:Product {id: $id})
        SET p.name = $name,
            p.description = $description,
            p.category = $category,
            p.product_type = $product_type,
            p.format = $format,
            p.spatial_resolution = $spatial_resolution,
            p.temporal_resolution = $temporal_resolution
        """
        session.run(query, **{k: v for k, v in product.items() 
                             if k not in ['satellite', 'payload', 'parameters', 'region']})
    
    def _link_satellite_to_mission(self, session, sat_id, mission_id):
        query = """
        MATCH (s:Satellite {id: $sat_id})
        MATCH (m:Mission {id: $mission_id})
        MERGE (s)-[:PART_OF_MISSION]->(m)
        """
        session.run(query, sat_id=sat_id, mission_id=mission_id)
    
    def _link_satellite_to_payload(self, session, sat_id, payload_id):
        query = """
        MATCH (s:Satellite {id: $sat_id})
        MATCH (pl:Payload {id: $payload_id})
        MERGE (s)-[:CARRIES]->(pl)
        """
        session.run(query, sat_id=sat_id, payload_id=payload_id)
    
    def _link_satellite_to_parameter(self, session, sat_id, param_type):
        query = """
        MATCH (s:Satellite {id: $sat_id})
        MATCH (par:Parameter {id: $param_id})
        MERGE (s)-[:OBSERVES]->(par)
        """
        session.run(query, sat_id=sat_id, param_id=param_type.replace('_', '-'))
    
    def _link_satellite_to_region(self, session, sat_id, region_id):
        query = """
        MATCH (s:Satellite {id: $sat_id})
        MATCH (r:Region {id: $region_id})
        MERGE (s)-[:COVERS]->(r)
        """
        session.run(query, sat_id=sat_id, region_id=region_id)
    
    def _link_satellite_to_product(self, session, sat_id, product_id):
        query = """
        MATCH (s:Satellite {id: $sat_id})
        MATCH (p:Product {id: $product_id})
        MERGE (s)-[:PRODUCES]->(p)
        """
        session.run(query, sat_id=sat_id, product_id=product_id)
    
    def _link_product_to_parameter(self, session, product_id, param_type):
        query = """
        MATCH (p:Product {id: $product_id})
        MATCH (par:Parameter {id: $param_id})
        MERGE (p)-[:MEASURES]->(par)
        """
        session.run(query, product_id=product_id, param_id=param_type.replace('_', '-'))
    
    def _link_product_to_payload(self, session, product_id, payload_id):
        query = """
        MATCH (p:Product {id: $product_id})
        MATCH (pl:Payload {id: $payload_id})
        MERGE (p)-[:GENERATED_BY]->(pl)
        """
        session.run(query, product_id=product_id, payload_id=payload_id)
    
    def _link_product_to_region(self, session, product_id, region_id):
        query = """
        MATCH (p:Product {id: $product_id})
        MATCH (r:Region {id: $region_id})
        MERGE (p)-[:COVERS_REGION]->(r)
        """
        session.run(query, product_id=product_id, region_id=region_id)
    
    def verify_graph(self):
        """Print comprehensive statistics"""
        queries = {
            'Satellites': "MATCH (s:Satellite) RETURN count(s) as count",
            'Missions': "MATCH (m:Mission) RETURN count(m) as count",
            'Payloads': "MATCH (pl:Payload) RETURN count(pl) as count",
            'Products': "MATCH (p:Product) RETURN count(p) as count",
            'Parameters': "MATCH (par:Parameter) RETURN count(par) as count",
            'Regions': "MATCH (r:Region) RETURN count(r) as count",
            'Total Relationships': "MATCH ()-[r]->() RETURN count(r) as count",
        }
        
        relationship_queries = {
            'PART_OF_MISSION': "MATCH ()-[r:PART_OF_MISSION]->() RETURN count(r) as count",
            'CARRIES': "MATCH ()-[r:CARRIES]->() RETURN count(r) as count",
            'OBSERVES': "MATCH ()-[r:OBSERVES]->() RETURN count(r) as count",
            'PRODUCES': "MATCH ()-[r:PRODUCES]->() RETURN count(r) as count",
            'COVERS': "MATCH ()-[r:COVERS]->() RETURN count(r) as count",
            'MEASURES': "MATCH ()-[r:MEASURES]->() RETURN count(r) as count",
            'GENERATED_BY': "MATCH ()-[r:GENERATED_BY]->() RETURN count(r) as count",
            'COVERS_REGION': "MATCH ()-[r:COVERS_REGION]->() RETURN count(r) as count",
        }
        
        print("\n" + "="*60)
        print("KNOWLEDGE GRAPH STATISTICS")
        print("="*60)
        
        with self.driver.session() as session:
            print("\n📊 Node Counts:")
            for name, query in queries.items():
                result = session.run(query)
                count = result.single()['count']
                print(f"  {name:25s}: {count:5d}")
            
            print("\n🔗 Relationship Counts:")
            for name, query in relationship_queries.items():
                result = session.run(query)
                count = result.single()['count']
                print(f"  {name:25s}: {count:5d}")
        
        print("="*60 + "\n")


def main():
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
    
    populator = EnhancedNeo4jPopulator(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Optional: Clear existing data
        response = input("Clear existing database? (yes/no): ").strip().lower()
        if response == 'yes':
            populator.clear_database()
        
        # Create constraints
        populator.create_constraints()
        
        # Populate everything
        populator.populate_all()
        
        # Verify
        populator.verify_graph()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        populator.close()


if __name__ == "__main__":
    main()