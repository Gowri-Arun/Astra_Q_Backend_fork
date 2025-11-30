"""
kg_pipeline/queries.py
Centralized Cypher query templates for Neo4j Knowledge Graph
"""

# ==========================================
# SATELLITE QUERIES
# ==========================================

GET_SATELLITE_BY_NAME = """
MATCH (s:Satellite {name: $name})
RETURN s
"""

SEARCH_SATELLITES = """
MATCH (s:Satellite)
WHERE toLower(s.name) CONTAINS toLower($query)
RETURN s.id as id, s.name as name, s as properties
LIMIT $limit
"""

GET_SATELLITE_CAPABILITIES = """
MATCH (s:Satellite {name: $name})
OPTIONAL MATCH (s)-[:CARRIES]->(pl:Payload)
OPTIONAL MATCH (s)-[:OBSERVES]->(par:Parameter)
OPTIONAL MATCH (s)-[:PRODUCES]->(prod:Product)
OPTIONAL MATCH (s)-[:COVERS]->(r:Region)
RETURN s,
       collect(DISTINCT pl) as payloads,
       collect(DISTINCT par) as parameters,
       collect(DISTINCT prod) as products,
       collect(DISTINCT r) as regions
"""

LIST_ALL_SATELLITES = """
MATCH (s:Satellite)
RETURN s.id as id, s.name as name, s.status as status, s.orbit_type as orbit_type
ORDER BY s.name
"""

# ==========================================
# PRODUCT QUERIES
# ==========================================

SEARCH_PRODUCTS = """
MATCH (p:Product)
WHERE toLower(p.name) CONTAINS toLower($query) 
   OR toLower(p.description) CONTAINS toLower($query)
RETURN p.id as id, p.name as name, p as properties
LIMIT $limit
"""

GET_PRODUCTS_BY_CATEGORY = """
MATCH (p:Product)
WHERE p.category = $category
RETURN p
ORDER BY p.name
"""

GET_PRODUCT_DETAILS = """
MATCH (p:Product {id: $product_id})
OPTIONAL MATCH (p)-[:MEASURES]->(par:Parameter)
OPTIONAL MATCH (p)-[:GENERATED_BY]->(pl:Payload)
OPTIONAL MATCH (p)-[:COVERS_REGION]->(r:Region)
OPTIONAL MATCH (p)-[:USES_ALGORITHM]->(alg:Algorithm)
OPTIONAL MATCH (p)-[:DOCUMENTED_IN]->(doc:Document)
RETURN p,
       collect(DISTINCT par) as parameters,
       collect(DISTINCT pl) as payloads,
       collect(DISTINCT r) as regions,
       collect(DISTINCT alg) as algorithms,
       collect(DISTINCT doc) as documents
"""

# ==========================================
# PARAMETER QUERIES
# ==========================================

SEARCH_PARAMETERS = """
MATCH (par:Parameter)
WHERE toLower(par.type) CONTAINS toLower($query)
   OR toLower(par.category) CONTAINS toLower($query)
RETURN par.id as id, par.type as name, par as properties
LIMIT $limit
"""

GET_PARAMETERS_BY_CATEGORY = """
MATCH (par:Parameter {category: $category})
RETURN par
ORDER BY par.type
"""

FIND_SATELLITES_OBSERVING_PARAMETER = """
MATCH (s:Satellite)-[:OBSERVES]->(par:Parameter {type: $param_type})
RETURN s.name as satellite, s.id as satellite_id
"""

# ==========================================
# RELATIONSHIP QUERIES
# ==========================================

GET_RELATED_ENTITIES = """
MATCH (entity {id: $entity_id})-[r]-(related)
RETURN related.id as id, 
       related.name as name, 
       labels(related)[0] as type,
       related as properties,
       type(r) as relationship
LIMIT $limit
"""

GET_SATELLITE_TO_PRODUCTS_PATH = """
MATCH path = (s:Satellite {name: $satellite_name})-[:PRODUCES|CARRIES*1..2]->(p:Product)
RETURN path
LIMIT 10
"""

FIND_PARAMETER_RELATIONSHIPS = """
MATCH (p1:Parameter {type: $param_type})-[:RELATED_TO]-(p2:Parameter)
RETURN p1.type as parameter1, p2.type as parameter2
"""

# ==========================================
# DOCUMENT QUERIES
# ==========================================

GET_DOCUMENTS_FOR_SATELLITE = """
MATCH (s:Satellite {name: $satellite_name})<-[:DESCRIBES]-(doc:Document)
RETURN doc.title as title, doc.url as url, doc.type as type
"""

SEARCH_DOCUMENTS = """
MATCH (doc:Document)
WHERE toLower(doc.title) CONTAINS toLower($query)
RETURN doc
LIMIT $limit
"""

# ==========================================
# ALGORITHM QUERIES
# ==========================================

GET_ALGORITHMS_FOR_PARAMETER = """
MATCH (par:Parameter {type: $param_type})<-[:APPLIES_TO]-(alg:Algorithm)
RETURN alg.name as algorithm, alg.description as description, alg.version as version
"""

# ==========================================
# REGION QUERIES
# ==========================================

GET_SATELLITES_COVERING_REGION = """
MATCH (s:Satellite)-[:COVERS]->(r:Region {name: $region_name})
RETURN s.name as satellite, s.id as satellite_id
"""

# ==========================================
# STATISTICS QUERIES
# ==========================================

GET_GRAPH_STATS = """
MATCH (s:Satellite) WITH count(s) as satellites
MATCH (p:Product) WITH satellites, count(p) as products
MATCH (par:Parameter) WITH satellites, products, count(par) as parameters
MATCH (d:Document) WITH satellites, products, parameters, count(d) as documents
MATCH ()-[r]->() WITH satellites, products, parameters, documents, count(r) as relationships
RETURN satellites, products, parameters, documents, relationships
"""

COUNT_BY_LABEL = """
MATCH (n)
RETURN labels(n)[0] as label, count(n) as count
ORDER BY count DESC
"""

# ==========================================
# COMPLEX ANALYTICAL QUERIES
# ==========================================

GET_MOST_CONNECTED_SATELLITES = """
MATCH (s:Satellite)-[r]-()
RETURN s.name as satellite, count(r) as connections
ORDER BY connections DESC
LIMIT 10
"""

GET_PRODUCT_LINEAGE = """
MATCH path = (p:Product {name: $product_name})-[:DERIVED_FROM*]->(ancestor:Product)
RETURN path
"""

FIND_COMMON_PARAMETERS = """
MATCH (s1:Satellite {name: $satellite1})-[:OBSERVES]->(par:Parameter)<-[:OBSERVES]-(s2:Satellite {name: $satellite2})
RETURN par.type as common_parameter
"""

# ==========================================
# HELPER: Query Dictionary for Easy Access
# ==========================================

COMMON_QUERIES = {
    # Satellites
    'get_satellite_by_name': GET_SATELLITE_BY_NAME,
    'search_satellites': SEARCH_SATELLITES,
    'get_satellite_capabilities': GET_SATELLITE_CAPABILITIES,
    'list_all_satellites': LIST_ALL_SATELLITES,
    
    # Products
    'search_products': SEARCH_PRODUCTS,
    'get_products_by_category': GET_PRODUCTS_BY_CATEGORY,
    'get_product_details': GET_PRODUCT_DETAILS,
    
    # Parameters
    'search_parameters': SEARCH_PARAMETERS,
    'get_parameters_by_category': GET_PARAMETERS_BY_CATEGORY,
    'find_satellites_observing_parameter': FIND_SATELLITES_OBSERVING_PARAMETER,
    
    # Relationships
    'get_related_entities': GET_RELATED_ENTITIES,
    'get_satellite_to_products_path': GET_SATELLITE_TO_PRODUCTS_PATH,
    
    # Documents
    'get_documents_for_satellite': GET_DOCUMENTS_FOR_SATELLITE,
    'search_documents': SEARCH_DOCUMENTS,
    
    # Algorithms
    'get_algorithms_for_parameter': GET_ALGORITHMS_FOR_PARAMETER,
    
    # Regions
    'get_satellites_covering_region': GET_SATELLITES_COVERING_REGION,
    
    # Statistics
    'get_graph_stats': GET_GRAPH_STATS,
    'count_by_label': COUNT_BY_LABEL,
    
    # Analytics
    'get_most_connected_satellites': GET_MOST_CONNECTED_SATELLITES,
    'get_product_lineage': GET_PRODUCT_LINEAGE,
    'find_common_parameters': FIND_COMMON_PARAMETERS,
}


# ==========================================
# HELPER: Execute Query with Parameters
# ==========================================

def execute_query(session, query_name: str, params: dict = None):
    """
    Execute a query by name from COMMON_QUERIES
    
    Usage:
        from kg_pipeline.queries import execute_query, COMMON_QUERIES
        
        with driver.session() as session:
            results = execute_query(session, 'search_satellites', {'query': 'INSAT', 'limit': 5})
    """
    if query_name not in COMMON_QUERIES:
        raise ValueError(f"Query '{query_name}' not found in COMMON_QUERIES")
    
    query = COMMON_QUERIES[query_name]
    result = session.run(query, params or {})
    return [record.data() for record in result]


# ==========================================
# USAGE EXAMPLE
# ==========================================

if __name__ == "__main__":
    """
    Example: How to use these queries
    """
    from neo4j import GraphDatabase
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))
    )
    
    with driver.session() as session:
        # Example 1: Search satellites
        print("=== Searching for INSAT satellites ===")
        results = execute_query(session, 'search_satellites', {'query': 'INSAT', 'limit': 5})
        for result in results:
            print(f"  - {result['name']}")
        
        # Example 2: Get statistics
        print("\n=== Graph Statistics ===")
        stats = execute_query(session, 'get_graph_stats')
        if stats:
            print(f"  Satellites: {stats[0]['satellites']}")
            print(f"  Products: {stats[0]['products']}")
            print(f"  Parameters: {stats[0]['parameters']}")
            print(f"  Documents: {stats[0]['documents']}")
            print(f"  Relationships: {stats[0]['relationships']}")
    
    driver.close()