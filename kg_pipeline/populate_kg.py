"""
kg_pipeline/populate_kg_enhanced.py
Enhanced Knowledge Graph Population from metadata_report-2.txt
"""

import os
import re
import ast
from collections import defaultdict
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# Path to your metadata_report-2.txt (adjust if needed)
METADATA_REPORT_PATH = os.path.join(
    os.path.dirname(__file__),
    "metadata_report-2.txt",
)

# Map raw parameter strings from metadata_report to normalized KG parameters
# Now includes a human-readable display_name as 4th element
PARAM_MAP = {
    "Rainfall": ("rainfall", "atmosphere", "mm/hr", "Rainfall"),
    "Ocean": ("ocean_variable", "ocean", None, "Ocean parameters"),
    "Water": ("water_variable", "hydrology", None, "Water-related parameters"),
    "Cloud": ("cloud", "atmosphere", None, "Cloud-related parameters"),
    "Soil Moisture": ("soil_moisture", "land", None, "Soil moisture"),
}


def make_product_display_name(product_id: str) -> str:
    """
    Turn a long JSON filename into a nicer display name.
    Example:
      Oceansat-3_Introduction_Meteorological_and_Oceanographic_Satellite_Data_Archival_Centre.json
      -> Oceansat-3 Introduction
    """
    base = product_id.replace(".json", "")
    base = base.replace(
        "_Meteorological_and_Oceanographic_Satellite_Data_Archival_Centre", ""
    )
    # Keep first 2–3 segments (satellite + short label), then join
    parts = base.split("_")
    if len(parts) > 3:
        parts = parts[:3]
    return " ".join(parts)


def parse_metadata_report(path: str):
    """
    Parse metadata_report-2.txt into:
      - satellites: dict name -> {id, name}
      - parameters: dict norm_name -> {id, type, category, unit, display_name}
      - regions: dict name -> {id, name, type}
      - products: list of {
            id, name, display_name,
            satellite, parameter, region,
            product_type, section, doc_section, keywords
        }

    Notes:
    - product_type can be: "data", "doc", "site_doc", "other".
    - section is a high-level bucket: "data", "doc_pages", "site_pages", "other".
    """
    satellites = {}
    parameters = {}
    regions = {}
    products = []

    if not os.path.exists(path):
        raise FileNotFoundError(f"metadata_report.txt not found at: {path}")

    current_section = None  # "data", "doc_pages", "site_pages", "other"

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            # Track which section we're in
            if line.startswith("=== DATA PRODUCTS"):
                current_section = "data"
                continue
            if line.startswith("=== SATELLITE / PRODUCT DOC PAGES"):
                current_section = "doc_pages"
                continue
            if line.startswith("=== GENERIC SITE PAGES"):
                current_section = "site_pages"
                continue
            if line.startswith("=== OTHERS / UNCLASSIFIED"):
                current_section = "other"
                continue

            # Lines of interest look like: "<filename>.json: FOUND -> { ... }"
            if "FOUND ->" not in line:
                continue

            left, right = line.split("FOUND ->", 1)
            product_id = left.split(".json")[0].strip() + ".json"

            # Extract the dict on the right safely
            m = re.search(r"\{.*\}", right)
            if not m:
                continue
            meta_str = m.group(0)
            try:
                meta = ast.literal_eval(meta_str)
            except Exception:
                continue

            sat_name = meta.get("satellite")
            param_raw = meta.get("parameter")
            region_name = meta.get("region")
            product_type = meta.get("product_type")  # "data", "doc", "site_doc", "other"
            doc_section = meta.get("doc_section")    # e.g., "Introduction", "Payloads"
            keywords = meta.get("keywords")          # list of strings

            # Build product record
            products.append(
                {
                    "id": product_id,
                    "name": product_id,  # technical id
                    "display_name": make_product_display_name(product_id),
                    "satellite": sat_name,
                    "parameter": param_raw,
                    "region": region_name,
                    "product_type": product_type,
                    "section": current_section,
                    "doc_section": doc_section,
                    "keywords": keywords or [],
                }
            )

            # Satellites
            if sat_name:
                if sat_name not in satellites:
                    satellites[sat_name] = {
                        "id": sat_name.lower().replace(" ", "-"),
                        "name": sat_name,
                    }

            # Parameters (normalized + display_name)
            if param_raw:
                norm, cat, unit, disp = PARAM_MAP.get(
                    param_raw,
                    (
                        param_raw.lower().replace(" ", "_"),
                        None,
                        None,
                        param_raw,
                    ),
                )
                if norm not in parameters:
                    parameters[norm] = {
                        "id": norm.replace("_", "-"),
                        "type": norm,
                        "category": cat,
                        "unit": unit,
                        "display_name": disp,
                    }

            # Regions
            if region_name:
                if region_name not in regions:
                    regions[region_name] = {
                        "id": region_name.lower().replace(" ", "-"),
                        "name": region_name,
                        "type": "country",  # simple default
                    }

    return satellites, parameters, regions, products


class EnhancedNeo4jPopulator:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

        # Load satellites, parameters, regions, products from metadata_report-2.txt
        satellites, parameters, regions, products = parse_metadata_report(
            METADATA_REPORT_PATH
        )

        # Store in instance attributes
        self.satellites = satellites
        self.parameters = parameters
        self.regions = regions
        self.products = products

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
        ]

        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception:
                    pass

        print("✅ Constraints and indexes created")

    def populate_all(self):
        """Main method to populate entire knowledge graph"""
        with self.driver.session() as session:
            print("\n🚀 Starting population from metadata_report-2.txt...\n")

            # 1) Parameters
            print("📊 Creating parameters...")
            for _, param_info in self.parameters.items():
                self._create_parameter(session, param_info)

            # 2) Regions
            print("\n🗺️  Creating regions...")
            for _, region_info in self.regions.items():
                self._create_region(session, region_info)

            # 3) Satellites
            print("\n🛰️  Creating satellites...")
            for _, sat_data in self.satellites.items():
                self._create_satellite(session, sat_data)

            # 4) Products + relationships
            print("\n📦 Creating products and relationships...")
            for product in self.products:
                # If you want to only treat "real" data products as Product nodes for KG queries,
                # you can use this filter:
                # if product.get("product_type") != "data":
                #     continue
                self._create_product(session, product)

                # Product -> Parameter
                param_raw = product.get("parameter")
                if param_raw:
                    norm, _, _, _ = PARAM_MAP.get(
                        param_raw,
                        (param_raw.lower().replace(" ", "_"), None, None, param_raw),
                    )
                    if norm in self.parameters:
                        self._link_product_to_parameter(
                            session,
                            product_id=product["id"],
                            param_id=self.parameters[norm]["id"],
                        )

                # Satellite -> Product
                sat_name = product.get("satellite")
                if sat_name and sat_name in self.satellites:
                    self._link_satellite_to_product(
                        session,
                        sat_id=self.satellites[sat_name]["id"],
                        product_id=product["id"],
                    )

                # Product -> Region
                region_name = product.get("region")
                if region_name and region_name in self.regions:
                    self._link_product_to_region(
                        session,
                        product_id=product["id"],
                        region_id=self.regions[region_name]["id"],
                    )

            print("\n✅ Population complete!")

    # === CREATE HELPERS ===

    def _create_satellite(self, session, sat_data):
        query = """
        MERGE (s:Satellite {id: $id})
        SET s.name = $name
        """
        session.run(query, id=sat_data["id"], name=sat_data["name"])

    def _create_parameter(self, session, param_info):
        query = """
        MERGE (par:Parameter {id: $id})
        SET par.type = $type,
            par.category = $category,
            par.unit = $unit,
            par.display_name = $display_name
        """
        session.run(
            query,
            id=param_info["id"],
            type=param_info["type"],
            category=param_info["category"],
            unit=param_info["unit"],
            display_name=param_info.get("display_name", param_info["type"]),
        )

    def _create_region(self, session, region_info):
        query = """
        MERGE (r:Region {id: $id})
        SET r.name = $name,
            r.type = $type
        """
        session.run(
            query,
            id=region_info["id"],
            name=region_info["name"],
            type=region_info["type"],
        )

    def _create_product(self, session, product):
        query = """
        MERGE (p:Product {id: $id})
        SET p.name = $name,
            p.display_name = $display_name,
            p.product_type = $product_type,
            p.section = $section,
            p.doc_section = $doc_section,
            p.keywords = $keywords
        """
        session.run(
            query,
            id=product["id"],
            name=product["name"],
            display_name=product.get("display_name", product["name"]),
            product_type=product.get("product_type"),
            section=product.get("section"),
            doc_section=product.get("doc_section"),
            keywords=product.get("keywords", []),
        )

    # === RELATION HELPERS (ontology-specific) ===

    def _link_satellite_to_product(self, session, sat_id, product_id):
        query = """
        MATCH (s:Satellite {id: $sat_id})
        MATCH (p:Product {id: $product_id})
        MERGE (s)-[:PRODUCES]->(p)
        """
        session.run(query, sat_id=sat_id, product_id=product_id)

    def _link_product_to_parameter(self, session, product_id, param_id):
        query = """
        MATCH (p:Product {id: $product_id})
        MATCH (par:Parameter {id: $param_id})
        MERGE (p)-[:OBSERVES]->(par)
        """
        session.run(query, product_id=product_id, param_id=param_id)

    def _link_product_to_region(self, session, product_id, region_id):
        query = """
        MATCH (p:Product {id: $product_id})
        MATCH (r:Region {id: $region_id})
        MERGE (p)-[:COVERS]->(r)
        """
        session.run(query, product_id=product_id, region_id=region_id)

    def verify_graph(self):
        """Print basic statistics"""
        queries = {
            "Satellites": "MATCH (s:Satellite) RETURN count(s) as count",
            "Products": "MATCH (p:Product) RETURN count(p) as count",
            "Parameters": "MATCH (par:Parameter) RETURN count(par) as count",
            "Regions": "MATCH (r:Region) RETURN count(r) as count",
            "Total Relationships": "MATCH ()-[r]->() RETURN count(r) as count",
        }

        relationship_queries = {
            "PRODUCES": "MATCH ()-[r:PRODUCES]->() RETURN count(r) as count",
            "OBSERVES": "MATCH ()-[r:OBSERVES]->() RETURN count(r) as count",
            "COVERS": "MATCH ()-[r:COVERS]->() RETURN count(r) as count",
        }

        print("\n" + "=" * 60)
        print("KNOWLEDGE GRAPH STATISTICS")
        print("=" * 60)

        with self.driver.session() as session:
            print("\n📊 Node Counts:")
            for name, query in queries.items():
                result = session.run(query)
                count = result.single()["count"]
                print(f"  {name:25s}: {count:5d}")

            print("\n🔗 Relationship Counts:")
            for name, query in relationship_queries.items():
                result = session.run(query)
                count = result.single()["count"]
                print(f"  {name:25s}: {count:5d}")

        print("=" * 60 + "\n")


def main():
    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USER")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

    populator = EnhancedNeo4jPopulator(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    try:
        response = input("Clear existing database? (yes/no): ").strip().lower()
        if response == "yes":
            populator.clear_database()

        populator.create_constraints()
        populator.populate_all()
        populator.verify_graph()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
    finally:
        populator.close()


if __name__ == "__main__":
    main()
