#!/usr/bin/env python3
"""
Debug script to isolate the Hamilton caching configuration issue.
"""

def test_basic_hamilton_cache():
    """Test basic Hamilton cache configuration."""
    print("🔍 Testing basic Hamilton cache configuration...")
    
    try:
        from hamilton import driver
        print("   ✅ Hamilton driver import successful")
        
        # Test 1: Basic cache configuration
        print("   🧪 Test 1: Basic cache with path only")
        builder1 = driver.Builder().with_cache(path="./test_cache")
        print("   ✅ Basic cache configuration successful")
        
        # Test 2: Cache with recompute=True
        print("   🧪 Test 2: Cache with recompute=True")
        builder2 = driver.Builder().with_cache(path="./test_cache", recompute=True)
        print("   ✅ Cache with recompute=True successful")
        
        # Test 3: Cache with recompute as list
        print("   🧪 Test 3: Cache with recompute as list")
        builder3 = driver.Builder().with_cache(path="./test_cache", recompute=["some_node"])
        print("   ✅ Cache with recompute as list successful")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Hamilton cache test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_cache_decorators():
    """Test cache decorators on simple functions."""
    print("\n🔍 Testing cache decorators...")
    
    try:
        from hamilton.function_modifiers import cache, tag
        print("   ✅ Cache decorator import successful")
        
        # Test cache decorator
        @cache(behavior="default")
        @tag(test="true")
        def test_function() -> str:
            return "test"
        
        print("   ✅ Cache decorator application successful")
        return True
        
    except Exception as e:
        print(f"   ❌ Cache decorator test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_driver_creation():
    """Test creating a driver with our configuration."""
    print("\n🔍 Testing driver creation with our config...")
    
    try:
        from hamilton import driver
        from hamilton.function_modifiers import cache, tag
        
        # Create a simple test module
        @cache(behavior="default")
        @tag(test="true")
        def simple_function() -> str:
            return "hello"
        
        # Create module object
        import types
        test_module = types.ModuleType("test_module")
        test_module.simple_function = simple_function
        
        # Test driver creation
        print("   🧪 Creating driver with cache...")
        config = {"path": "./test_cache"}
        
        builder = (
            driver.Builder()
            .with_modules(test_module)
            .with_cache(**config)
        )
        
        dr = builder.build()
        print("   ✅ Driver creation successful")
        
        # Test execution
        print("   🧪 Testing execution...")
        result = dr.execute(["simple_function"])
        print(f"   ✅ Execution successful: {result}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Driver creation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_our_config():
    """Test the exact configuration we're using."""
    print("\n🔍 Testing our exact configuration...")
    
    try:
        from hamilton import driver
        
        # Simulate our exact config
        config = {
            "database_path": "../db/eo_pv_data.duckdb",
            "force_download": False,
            "max_mb": 250,
            "cache_path": "./.hamilton_cache"
        }
        
        # Test cache config creation
        cache_config = {
            "path": config.get("cache_path", "./.hamilton_cache")
        }
        
        if config.get("force_download", False):
            cache_config["recompute"] = True
        
        print(f"   📋 Cache config: {cache_config}")
        
        # Test builder creation
        builder = driver.Builder().with_cache(**cache_config)
        print("   ✅ Our cache configuration successful")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Our config test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all debug tests."""
    print("🐛 Hamilton Cache Configuration Debug")
    print("=" * 50)
    
    tests = [
        ("Basic Hamilton Cache", test_basic_hamilton_cache),
        ("Cache Decorators", test_cache_decorators),
        ("Driver Creation", test_driver_creation),
        ("Our Configuration", test_our_config)
    ]
    
    passed = 0
    for test_name, test_func in tests:
        print(f"\n📋 Running: {test_name}")
        if test_func():
            passed += 1
            print(f"✅ {test_name} PASSED")
        else:
            print(f"❌ {test_name} FAILED")
    
    print(f"\n📊 Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All debug tests passed!")
    else:
        print("⚠️  Some tests failed - check Hamilton installation and version")


if __name__ == "__main__":
    main()
