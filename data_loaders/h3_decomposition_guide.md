# H3 Grid Cell Decomposition Guide

## 🌐 **Overview**

Our pipeline uses H3 (Hexagonal Hierarchical Spatial Index) to decompose both PV installations and country geometries into hexagonal grid cells. This enables efficient spatial queries, multi-scale analysis, and optimized data processing.

## 📊 **H3 Implementation Strategy**

### **PV Installations H3 Indexing**
- **Resolution 10** (~15m hexagons): Installation-level precision
- **Resolution 7** (~1.2km hexagons): Neighborhood-level analysis
- **Resolution 5** (~8.5km hexagons): City/regional-level analysis

### **Country Geometries H3 Decomposition**
- **Resolution 5** (~8.5km hexagons): Country-level grid cells
- Each country geometry is broken down into constituent H3 cells
- Enables efficient spatial joins and aggregations

## 🔧 **Technical Implementation**

### **Python H3 Library (h3-py)**
Used in staging/prepared models for:
```python
import h3

# Get H3 cells covering a polygon
h3_cells = h3.polygon_to_cells(polygon_coords, resolution=5)

# Get H3 cell boundary
cell_boundary = h3.h3_to_geo_boundary(h3_cell, geo_json=True)
```

### **DuckDB H3 Extension**
Used for SQL-based operations:
```sql
-- Install H3 extension
INSTALL h3 FROM community;
LOAD h3;

-- Decompose country geometry into H3 cells (correct function name)
SELECT h3_polygon_wkt_to_cells(ST_AsText(geometry), 5) AS h3_cells_list
FROM country_geoms;

-- Get H3 cell geometry
SELECT h3_cell_to_boundary_wkt(h3_cell) AS cell_geometry
FROM h3_cells;

-- Get H3 cell center point (correct function name)
SELECT h3_cell_to_lat_lng(h3_cell) AS cell_center
FROM h3_cells;
```

## 📋 **Data Tables Created**

### **1. PV Installations with H3 Indices**
Table: `prepared.prep_pv_datasets_unified`
```sql
SELECT 
    installation_id,
    h3_index_res10,  -- Installation-level
    h3_index_res7,   -- Neighborhood-level  
    h3_index_res5,   -- City-level
    geometry_wkt,
    area_m2
FROM prepared.prep_pv_datasets_unified;
```

### **2. Country Geometries with H3 Decomposition**
Table: `prepared.country_geoms_with_h3`
```sql
SELECT 
    division_id,
    country_iso,
    division_name,
    country_pv_count,
    country_pv_area_m2,
    h3_cells_list,      -- Array of H3 cells covering country
    h3_cell_count,      -- Number of H3 cells per country
    h3_resolution       -- H3 resolution used (5)
FROM prepared.country_geoms_with_h3;
```

### **3. Normalized Country H3 Cells**
Table: `prepared.country_h3_cells`
```sql
SELECT 
    division_id,
    country_iso,
    h3_cell,                    -- Individual H3 cell ID
    h3_cell_geometry_wkt,       -- H3 cell boundary as WKT
    h3_cell_center,             -- H3 cell center point
    country_pv_count,           -- PV count for entire country
    country_pv_area_m2          -- PV area for entire country
FROM prepared.country_h3_cells;
```

## 🚀 **Use Cases Enabled**

### **1. Multi-Scale Spatial Analysis**
```sql
-- Analyze PV density at different scales
SELECT 
    h3_index_res5 as city_level_cell,
    COUNT(*) as installations,
    SUM(area_m2) as total_area
FROM prepared.prep_pv_datasets_unified
GROUP BY h3_index_res5
ORDER BY total_area DESC;
```

### **2. Country-Level Aggregations**
```sql
-- PV installations per country using H3 spatial join
SELECT 
    c.country_iso,
    c.division_name,
    COUNT(DISTINCT pv.installation_id) as pv_count,
    SUM(pv.area_m2) as total_pv_area
FROM prepared.country_h3_cells c
JOIN prepared.prep_pv_datasets_unified pv 
    ON pv.h3_index_res5 = c.h3_cell
GROUP BY c.country_iso, c.division_name;
```

### **3. Efficient Spatial Filtering**
```sql
-- Find all PV installations within specific H3 cells
SELECT pv.*
FROM prepared.prep_pv_datasets_unified pv
WHERE pv.h3_index_res7 IN (
    SELECT h3_cell 
    FROM prepared.country_h3_cells 
    WHERE country_iso = 'USA'
);
```

### **4. Cross-Resolution Analysis**
```sql
-- Analyze PV distribution across H3 hierarchy
WITH h3_hierarchy AS (
    SELECT 
        h3_index_res5 as parent_cell,
        h3_index_res7 as child_cell,
        h3_index_res10 as grandchild_cell,
        COUNT(*) as installations
    FROM prepared.prep_pv_datasets_unified
    GROUP BY h3_index_res5, h3_index_res7, h3_index_res10
)
SELECT 
    parent_cell,
    COUNT(DISTINCT child_cell) as child_cells,
    SUM(installations) as total_installations
FROM h3_hierarchy
GROUP BY parent_cell;
```

## 📈 **Performance Benefits**

### **1. Spatial Index Efficiency**
- H3 cells provide natural spatial indexing
- Hexagonal grid eliminates edge effects of square grids
- Hierarchical structure enables multi-resolution queries

### **2. Query Optimization**
- Join on H3 cell IDs instead of expensive spatial operations
- Pre-computed cell boundaries reduce geometry calculations
- Efficient aggregation using H3 hierarchy

### **3. Scalability**
- H3 cells enable distributed processing
- Consistent cell sizes across globe
- Natural partitioning for cloud-scale analytics

## 🔍 **H3 Resolution Reference**

| Resolution | Avg Hexagon Area | Avg Edge Length | Use Case |
|------------|------------------|-----------------|----------|
| 0 | 4,250,546.8 km² | 1,107.7 km | Continental |
| 1 | 607,220.9 km² | 418.7 km | Country |
| 2 | 86,745.9 km² | 158.2 km | State/Province |
| 3 | 12,392.3 km² | 59.8 km | Metropolitan |
| 4 | 1,770.3 km² | 22.6 km | County |
| **5** | **252.9 km²** | **8.5 km** | **City (Our Choice)** |
| 6 | 36.1 km² | 3.2 km | Municipality |
| **7** | **5.2 km²** | **1.2 km** | **Neighborhood** |
| 8 | 0.7 km² | 461.4 m | District |
| 9 | 0.1 km² | 174.4 m | Block |
| **10** | **15,047.5 m²** | **65.9 m** | **Installation** |

## 🛠️ **Implementation Notes**

### **DuckDB H3 Extension Functions Used**
- `h3_polygon_wkt_to_cells(wkt_string, resolution)` - Decompose WKT polygon into H3 cells
- `h3_cell_to_boundary_wkt(h3_cell)` - Get H3 cell boundary as WKT
- `h3_cell_to_lat_lng(h3_cell)` - Get H3 cell center coordinates
- `h3_cell_to_parent(h3_cell, parent_resolution)` - Get parent cell
- `h3_cell_to_children(h3_cell, child_resolution)` - Get child cells

### **Error Handling**
- Graceful fallback if H3 extension unavailable
- Validation of geometry before H3 decomposition
- Logging of H3 processing statistics

### **Future Enhancements**
- Add H3 decomposition for other administrative levels (states, cities)
- Implement H3-based STAC query optimization
- Create H3-based spatial clustering algorithms

This H3 decomposition strategy provides the foundation for efficient, scalable geospatial analysis of PV installations across multiple resolutions and administrative boundaries!
