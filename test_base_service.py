#!/usr/bin/env python3

import sys
import os
sys.path.append('app')

from app.services.base_service import BaseService
import pandas as pd

def test_base_service():
    print("=== Testing BaseService ===")

    try:
        # Initialize BaseService
        print("1. Initializing BaseService...")
        base_service = BaseService()

        print(f"2. Environment folder: {base_service.folder}")

        # Test clinic data
        print(f"3. Clinic data loaded: {len(base_service.clinic)} rows")
        print(f"   Clinic columns: {list(base_service.clinic.columns)}")

        # Test queries
        print(f"4. Queries loaded: {len(base_service.queries)} rows")
        print(f"   Query columns: {list(base_service.queries.columns)}")

        # Test specific queries
        print("5. Testing individual query values:")
        print(f"   email_spec_query type: {type(base_service.email_spec_query)}")
        print(f"   email_spec_query value: {str(base_service.email_spec_query)[:100]}...")

        print(f"   update_clinic_email_query type: {type(base_service.update_clinic_email_query)}")
        print(f"   update_clinic_email_query value: {str(base_service.update_clinic_email_query)[:100]}...")

        # Check if queries are nan
        if pd.isna(base_service.update_clinic_email_query):
            print("   ⚠️  update_clinic_email_query is NaN!")

            # Check all values in updateClinicEmail column
            print("   Checking all updateClinicEmail values:")
            update_col = base_service.queries['updateClinicEmail']
            for i, val in enumerate(update_col):
                if pd.notna(val):
                    print(f"     Row {i}: {str(val)[:100]}...")
        else:
            print("   ✅ update_clinic_email_query is valid")

        # Test other tables
        print(f"6. Other tables:")
        print(f"   fb: {len(base_service.fb)} rows")
        print(f"   clinic_list: {len(base_service.clinic_list)} rows")
        print(f"   stop_words: {len(base_service.stop_words)} items")

        print("✅ BaseService test completed successfully")
        return True

    except Exception as e:
        print(f"❌ BaseService test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_base_service()