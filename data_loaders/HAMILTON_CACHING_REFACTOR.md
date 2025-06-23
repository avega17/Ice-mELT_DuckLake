# Hamilton Built-in Caching Refactoring

## 🎯 **Overview**

This document describes the refactoring of our custom caching system to use Hamilton's built-in caching capabilities. Hamilton's native caching provides superior functionality, better integration, and more robust cache management compared to our previous custom implementation.

## 📊 **Before vs After Comparison**

| Feature | Custom Cache (Before) | Hamilton Cache (After) |
|---------|----------------------|------------------------|
| **Cache Key Generation** | Manual dataset names | Automatic: code + data versions |
| **Cache Invalidation** | Manual cache clearing | Automatic on code/data changes |
| **Storage Formats** | Pickle only | Multiple: pickle, parquet, json, csv |
| **Cache Behaviors** | Basic enable/disable | DEFAULT, RECOMPUTE, DISABLE, IGNORE |
| **Introspection** | Basic file listing | Rich visualization + structured logs |
| **Integration** | External utility functions | Native Hamilton nodes |
| **Dependency Tracking** | None | Automatic dependency-based invalidation |
| **Visualization** | None | Built-in DAG visualization with cache hits |

## 🔧 **Key Changes Made**

### 1. **Hamilton Module Updates**

#### **data_sources.py**
```python
# Added Hamilton caching decorators
from hamilton.function_modifiers import tag, cache

@cache(behavior="recompute")  # Always reload manifest to catch updates
def dataset_metadata(manifest_path: str = "doi_manifest.json") -> Dict[str, Dict[str, Any]]:

@cache(behavior="default", format="json")  # Cache download paths as JSON
def dataset_download_path(target_datasets: str, dataset_metadata: Dict[str, Dict[str, Any]], ...):
```

#### **transformations.py**
```python
# Added caching to expensive operations
@cache(behavior="default", format="parquet")  # Cache expensive geospatial processing
def processed_geodataframe(...):

@cache(behavior="default", format="parquet")  # Cache Arrow table conversion
def arrow_table_with_geometry(...):
```

### 2. **Driver Configuration Updates**

#### **Enhanced Driver Creation**
```python
def create_hamilton_driver(
    config: Dict[str, Any],
    use_parallel: bool = True,
    enable_caching: bool = True  # New parameter
) -> driver.Driver:
    
    # Hamilton caching configuration
    if enable_caching:
        cache_config = {
            "default_behavior": "default",
            "recompute": config.get("force_download", False),
            "path": config.get("cache_path", "./.hamilton_cache")
        }
        builder = builder.with_cache(**cache_config)
```

### 3. **Download Function Refactoring**

#### **New Hamilton-Compatible Download Function**
```python
def download_doi_dataset_files_hamilton(
    dataset_name: str,
    dataset_metadata: Dict[str, Dict[str, Any]],
    max_mb: int = 250,
    force_download: bool = False  # Removed use_cache - Hamilton handles this
) -> str:
    """
    Download function designed for Hamilton's caching system.
    No custom cache logic - Hamilton handles all caching automatically.
    """
```

### 4. **Command Line Interface Updates**

```bash
# New cache-related options
--no-cache              # Disable Hamilton's built-in caching system
--force-download        # Force re-download (triggers Hamilton cache recomputation)
--cache-path PATH       # Path to Hamilton cache directory (default: ./.hamilton_cache)
```

## 🎨 **Cache Behaviors Explained**

### **Node-Level Cache Behaviors**

1. **`DEFAULT`** - Standard caching behavior (cache results, retrieve when possible)
2. **`RECOMPUTE`** - Always execute the node, but store results in cache
3. **`DISABLE`** - Don't cache this node at all
4. **`IGNORE`** - Don't cache, but don't affect downstream cache decisions

### **Applied Cache Strategies**

```python
# Always reload manifest to catch updates
@cache(behavior="recompute")
def dataset_metadata(...):

# Cache expensive downloads with JSON format
@cache(behavior="default", format="json")
def dataset_download_path(...):

# Cache expensive geospatial processing with Parquet format
@cache(behavior="default", format="parquet")
def processed_geodataframe(...):
```

## 🚀 **Usage Examples**

### **Basic Usage with Caching**
```bash
# Run with default Hamilton caching
python refactored_pv_doi_pipeline.py

# Run with custom cache directory
python refactored_pv_doi_pipeline.py --cache-path ./my_cache

# Force recomputation of all cached results
python refactored_pv_doi_pipeline.py --force-download
```

### **Programmatic Usage**
```python
from refactored_pv_doi_pipeline import run_doi_pipeline

# Run with Hamilton caching enabled
result = run_doi_pipeline(
    enable_caching=True,
    cache_path="./project_cache"
)

# Run without caching
result = run_doi_pipeline(enable_caching=False)
```

## 📈 **Benefits of Hamilton Caching**

### **1. Automatic Cache Management**
- **Smart Invalidation**: Cache automatically invalidates when code or input data changes
- **Dependency Tracking**: Changes upstream automatically invalidate downstream cache
- **Version Awareness**: Code changes trigger automatic cache updates

### **2. Rich Introspection**
```python
# View cache execution with visualization
dr.cache.view_run()

# Access structured logs
logs = dr.cache.logs(level="info")

# Inspect cache keys and data versions
data_versions = dr.cache.data_versions[dr.cache.last_run_id]
```

### **3. Multiple Storage Formats**
- **Parquet**: For DataFrames and structured data
- **JSON**: For metadata and configuration
- **Pickle**: For complex Python objects (default)
- **CSV**: For simple tabular data

### **4. Production-Ready Features**
- **Concurrent Access**: Safe for multi-process execution
- **Storage Management**: Built-in cleanup and eviction policies
- **Monitoring**: Comprehensive logging and metrics

## 🧪 **Testing**

### **Run Hamilton Caching Tests**
```bash
cd data_loaders
python test_hamilton_caching.py
```

### **Test Cache Visualization**
```bash
# Run pipeline and view cache visualization
python refactored_pv_doi_pipeline.py
# Then in Python:
# dr.cache.view_run()  # Shows cache hits/misses
```

## 🔍 **Cache Introspection**

### **View Cache Status**
```python
# Check cache directory
ls -la .hamilton_cache/

# View cache logs
dr.cache.logs(level="info")

# Inspect data versions
dr.cache.data_versions
```

### **Cache Debugging**
```python
# View cache keys for debugging
from hamilton.caching.cache_key import decode_key
cache_key = dr.cache.cache_keys[run_id]["node_name"]
decoded = decode_key(cache_key)
print(decoded)
```

## 🎯 **Migration Benefits**

1. **🚀 Performance**: Smarter caching with automatic invalidation
2. **🔧 Maintainability**: No custom cache code to maintain
3. **📊 Observability**: Rich visualization and logging
4. **🛡️ Reliability**: Production-tested caching system
5. **🎨 Flexibility**: Multiple cache behaviors and formats
6. **📈 Scalability**: Built for distributed and concurrent execution

## 🔮 **Future Enhancements**

With Hamilton's caching foundation, we can easily add:
- **Remote caching** for distributed execution
- **Cache eviction policies** for storage management
- **Integration with Hamilton UI** for visual cache monitoring
- **Custom cache stores** (Redis, S3, etc.)

---

**Result**: A more robust, feature-rich, and maintainable caching system that integrates seamlessly with Hamilton's execution model! 🎉
