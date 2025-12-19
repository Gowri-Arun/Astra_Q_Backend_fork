# Cypher Query Examples for MOSDAC Knowledge Graph

These queries assume the current KG schema:

- Nodes:
  - `Satellite {id, name}`
  - `Product {id, name}`
  - `Parameter {id, type, category, unit}`
  - `Region {id, name, type}`
- Relationships:
  - `(:Satellite)-[:PRODUCES]->(:Product)`
  - `(:Product)-[:OBSERVES]->(:Parameter)`
  - `(:Product)-[:COVERS]->(:Region)`

## 1. List all satellites
MATCH (s:Satellite)
RETURN s.name AS satellite
ORDER BY s.name;

## 2. List products for each satellite
MATCH (s:Satellite)-[:PRODUCES]->(p:Product)
RETURN s.name AS satellite, collect(p.id) AS products
ORDER BY s.name;

## 3. Rainfall-related products
MATCH (p:Product)-[:OBSERVES]->(par:Parameter {type: "rainfall"})
RETURN p.id AS product_id, p.name AS product_name
ORDER BY product_id;

## 4. Ocean-related products by satellite
MATCH (s:Satellite)-[:PRODUCES]->(p:Product)-[:OBSERVES]->(par:Parameter)
WHERE par.category = "ocean"
RETURN s.name AS satellite, collect(DISTINCT p.id) AS ocean_products
ORDER BY s.name;


## 5. Products covering India
MATCH (p:Product)-[:COVERS]->(r:Region {name: "India"})
RETURN p.id AS product_id, p.name AS product_name
ORDER BY product_id;


## 6. Parameters observed by a specific satellite
Example for INSAT-3DR:
MATCH (s:Satellite {name: "INSAT-3DR"})-[:PRODUCES]->(p:Product)-[:OBSERVES]->(par:Parameter)
RETURN DISTINCT par.type AS parameter
ORDER BY parameter;

## 7. Satellites associated with each parameter
MATCH (s:Satellite)-[:PRODUCES]->(p:Product)-[:OBSERVES]->(par:Parameter)
RETURN par.type AS parameter, collect(DISTINCT s.name) AS satellites
ORDER BY parameter;

## 8. Products grouped by satellite and parameter
MATCH (s:Satellite)-[:PRODUCES]->(p:Product)-[:OBSERVES]->(par:Parameter)
RETURN s.name AS satellite, par.type AS parameter, collect(p.id) AS products
ORDER BY s.name, parameter;






