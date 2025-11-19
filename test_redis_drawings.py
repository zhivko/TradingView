#!/usr/bin/env python3
"""
Test script to verify Redis drawings functionality.
This script directly tests the Redis operations to debug the shape deletion issue.
"""

import redis
import json

def test_redis_drawings():
    # Connect to Redis
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✓ Redis connection successful")
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        return

    # Test data - simulate a user with some shapes
    test_user = "test_user@example.com"
    test_symbol = "BTCUSDT"
    
    # Initial shapes with 2 shapes
    initial_shapes = [
        {
            "type": "line",
            "x0": "2023-01-01",
            "y0": 20000,
            "x1": "2023-01-02", 
            "y1": 21000,
            "line": {"color": "blue", "width": 2}
        },
        {
            "type": "line", 
            "x0": "2023-01-03",
            "y0": 22000,
            "x1": "2023-01-04",
            "y1": 23000,
            "line": {"color": "red", "width": 3}
        }
    ]
    
    # After deletion - only 1 shape
    updated_shapes = [initial_shapes[1]]  # Remove first shape
    
    key = f'drawings:{test_user}:{test_symbol}'
    
    print(f"\n🧪 Testing Redis drawings for key: {key}")
    
    # Step 1: Clear any existing data
    try:
        r.delete(key)
        print("✓ Cleared existing data")
    except Exception as e:
        print(f"✗ Failed to clear data: {e}")
        return
    
    # Step 2: Save initial shapes (2 shapes)
    try:
        initial_json = json.dumps(initial_shapes)
        result = r.set(key, initial_json)
        print(f"✓ Saved initial shapes (count: {len(initial_shapes)}) - Redis result: {result}")
        
        # Verify
        verify = r.get(key)
        verify_data = json.loads(verify) if verify else []
        print(f"✓ Verification - stored count: {len(verify_data)}")
        
    except Exception as e:
        print(f"✗ Failed to save initial shapes: {e}")
        return
    
    # Step 3: Update to reduced shapes (1 shape) - simulating deletion
    try:
        updated_json = json.dumps(updated_shapes)
        result = r.set(key, updated_json)
        print(f"✓ Saved updated shapes (count: {len(updated_shapes)}) - Redis result: {result}")
        
        # Verify
        verify = r.get(key)
        verify_data = json.loads(verify) if verify else []
        print(f"✓ Verification - stored count after update: {len(verify_data)}")
        
        # Check if deletion was successful
        if len(verify_data) == 1:
            print("✓ SUCCESS: Shape deletion worked correctly in Redis")
        else:
            print(f"✗ FAILED: Expected 1 shape, got {len(verify_data)} shapes")
            
    except Exception as e:
        print(f"✗ Failed to save updated shapes: {e}")
        return
    
    # Step 4: Test complete deletion (0 shapes)
    try:
        empty_json = json.dumps([])
        result = r.set(key, empty_json)
        print(f"✓ Saved empty shapes - Redis result: {result}")
        
        # Verify
        verify = r.get(key)
        verify_data = json.loads(verify) if verify else []
        print(f"✓ Verification - stored count after empty: {len(verify_data)}")
        
        if len(verify_data) == 0:
            print("✓ SUCCESS: Complete shape deletion worked correctly in Redis")
        else:
            print(f"✗ FAILED: Expected 0 shapes, got {len(verify_data)} shapes")
            
    except Exception as e:
        print(f"✗ Failed to save empty shapes: {e}")
        return
    
    # Step 5: Test data that might be causing issues
    print(f"\n🔍 Testing edge cases:")
    
    # Test None/null shapes
    try:
        null_json = json.dumps(None)
        result = r.set(key + "_null", null_json)
        print(f"✓ Null data test - Redis result: {result}")
    except Exception as e:
        print(f"✗ Null data test failed: {e}")
    
    # Clean up test data
    try:
        r.delete(key)
        r.delete(key + "_null")
        print("✓ Test cleanup completed")
    except Exception as e:
        print(f"✗ Test cleanup failed: {e}")

if __name__ == "__main__":
    test_redis_drawings()