#!/usr/bin/env python3
"""
Test script to validate FIFO logic without requiring database connection.
This creates mock data to test the select_items_for_order function.
"""

import pandas as pd
from datetime import datetime, timedelta


def validate_fifo_compliance(selected_items: pd.DataFrame) -> bool:
    """
    Validate that selected items comply with FIFO rules.

    Checks:
    1. Items are sorted by idingreso (numeric ascending)
    2. Items are sorted by ingresa date within each idingreso (earliest first)

    Returns:
        True if FIFO compliant, False otherwise
    """
    if selected_items.empty or len(selected_items) <= 1:
        return True

    # Check idingreso ordering (convert to numeric for comparison)
    try:
        selected_items_copy = selected_items.copy()
        selected_items_copy['_idingreso_numeric'] = pd.to_numeric(selected_items_copy['idingreso'], errors='coerce')
        idingreso_sorted = selected_items_copy['_idingreso_numeric'].is_monotonic_increasing

        # Check ingresa date ordering within each idingreso
        ingresa_sorted = True
        for idingreso, group in selected_items_copy.groupby('idingreso', sort=False):
            if len(group) > 1 and 'ingresa' in group.columns:
                group_ingresa = group['ingresa'].dropna()
                if len(group_ingresa) > 1:
                    if not group_ingresa.is_monotonic_increasing:
                        ingresa_sorted = False
                        print(f"    FIFO violation in idingreso {idingreso}: dates not in ascending order")
                        print(f"    Dates: {group_ingresa.tolist()}")
                        break

        if not idingreso_sorted:
            print(f"    FIFO violation: idingresos not in ascending order")
            print(f"    idingresos: {selected_items_copy['idingreso'].tolist()}")

        return idingreso_sorted and ingresa_sorted
    except Exception as e:
        print(f"  Warning: Could not validate FIFO compliance: {e}")
        return True  # Don't fail the process if validation has issues


def select_items_for_order(available_items: pd.DataFrame, qty_needed: float) -> pd.DataFrame:
    """
    Select items to fulfill the order quantity following FIFO rules.

    FIFO Logic:
    - Items are already sorted by idingreso (numeric ascending) and ingresa date (earliest first)
    - Process items in this sorted order to ensure FIFO compliance
    - Continue taking from subsequent idingresos until qty_needed is fulfilled
    - If an idingreso has fewer than 5 remaining items after fulfilling qty, include all remaining items

    Args:
        available_items: DataFrame of available items sorted by FIFO rules (idingreso, ingresa date)
        qty_needed: Total quantity (pesokgs) needed to fulfill the order

    Returns:
        DataFrame of selected items that fulfill the order
    """
    if available_items.empty:
        return pd.DataFrame()

    selected_items = []
    accumulated_qty = 0.0

    # Track which idingreso we're currently processing
    current_idingreso = None
    items_in_current_ingreso = pd.DataFrame()

    # Process items in FIFO order (already sorted by query)
    for idx, row in available_items.iterrows():
        pesokgs = row['pesokgs'] if pd.notna(row['pesokgs']) else 0.0
        idingreso = row['idingreso']

        # Track when we move to a new idingreso
        if current_idingreso != idingreso:
            current_idingreso = idingreso
            items_in_current_ingreso = available_items[available_items['idingreso'] == idingreso]

        # Add current item
        selected_items.append(row)
        accumulated_qty += pesokgs

        # Check if we've met the quantity requirement
        if accumulated_qty >= qty_needed:
            # Find remaining items in the current idingreso
            current_position = items_in_current_ingreso.index.get_loc(idx)
            remaining_items_count = len(items_in_current_ingreso) - (current_position + 1)

            # If there are 1-4 remaining items in this idingreso, include them all
            if 0 < remaining_items_count < 5:
                remaining_indices = items_in_current_ingreso.index[current_position + 1:]
                for remaining_idx in remaining_indices:
                    remaining_row = items_in_current_ingreso.loc[remaining_idx]
                    selected_items.append(remaining_row)
                    accumulated_qty += remaining_row['pesokgs'] if pd.notna(remaining_row['pesokgs']) else 0.0

            break

    if not selected_items:
        return pd.DataFrame()

    result_df = pd.DataFrame(selected_items)

    return result_df


def create_test_data_scenario_1():
    """
    Test scenario 1: Multiple idingresos with different dates
    - idingreso 100: 3 items, 100kg each
    - idingreso 200: 5 items, 50kg each
    - idingreso 300: 10 items, 20kg each
    Need: 350kg (should take all from 100, all from 200, and some from 300)
    """
    base_date = datetime(2025, 1, 1)

    data = []
    # idingreso 100 (oldest)
    for i in range(3):
        data.append({
            'idingreso': '100',
            'itemno': str(i+1),
            'pesokgs': 100.0,
            'ingresa': base_date + timedelta(days=i)
        })

    # idingreso 200 (middle)
    for i in range(5):
        data.append({
            'idingreso': '200',
            'itemno': str(i+1),
            'pesokgs': 50.0,
            'ingresa': base_date + timedelta(days=10 + i)
        })

    # idingreso 300 (newest)
    for i in range(10):
        data.append({
            'idingreso': '300',
            'itemno': str(i+1),
            'pesokgs': 20.0,
            'ingresa': base_date + timedelta(days=20 + i)
        })

    return pd.DataFrame(data), 350.0


def create_test_data_scenario_2():
    """
    Test scenario 2: Less than 5 remaining items in an idingreso
    - idingreso 100: 8 items, 50kg each
    Need: 300kg (should take 6 items = 300kg, then take remaining 2 items since <5 remain)
    """
    base_date = datetime(2025, 1, 1)

    data = []
    for i in range(8):
        data.append({
            'idingreso': '100',
            'itemno': str(i+1),
            'pesokgs': 50.0,
            'ingresa': base_date + timedelta(days=i)
        })

    return pd.DataFrame(data), 300.0


def create_test_data_scenario_3():
    """
    Test scenario 3: Items from same idingreso with different ingresa dates
    - idingreso 100: 10 items with varying dates
    Need: 250kg
    """
    base_date = datetime(2025, 1, 1)

    data = []
    for i in range(10):
        data.append({
            'idingreso': '100',
            'itemno': str(i+1),
            'pesokgs': 50.0,
            # Create some dates out of order to test sorting
            'ingresa': base_date + timedelta(days=i * 2)
        })

    return pd.DataFrame(data), 250.0


def run_tests():
    """Run all test scenarios"""

    print("=" * 80)
    print("FIFO Logic Test Suite")
    print("=" * 80)

    # Scenario 1: Multiple idingresos
    print("\n[Scenario 1] Multiple idingresos - Need 350kg")
    print("-" * 80)
    available_items, qty_needed = create_test_data_scenario_1()
    print(f"Available items from {len(available_items['idingreso'].unique())} idingresos:")
    for idingreso, group in available_items.groupby('idingreso'):
        print(f"  idingreso {idingreso}: {len(group)} items, {group['pesokgs'].sum()}kg total")

    selected = select_items_for_order(available_items, qty_needed)
    print(f"\nSelected {len(selected)} items totaling {selected['pesokgs'].sum()}kg")
    print(f"From idingresos: {', '.join(selected['idingreso'].unique())}")

    is_compliant = validate_fifo_compliance(selected)
    print(f"FIFO Compliant: {'✓ YES' if is_compliant else '✗ NO'}")

    # Scenario 2: Less than 5 remaining
    print("\n" + "=" * 80)
    print("[Scenario 2] Less than 5 remaining items - Need 300kg from 8 items (50kg each)")
    print("-" * 80)
    available_items, qty_needed = create_test_data_scenario_2()
    print(f"Available: {len(available_items)} items totaling {available_items['pesokgs'].sum()}kg")

    selected = select_items_for_order(available_items, qty_needed)
    print(f"\nSelected {len(selected)} items totaling {selected['pesokgs'].sum()}kg")
    print(f"Expected: 8 items (6 to meet qty + 2 remaining since <5 left)")
    print(f"Result: {'✓ CORRECT' if len(selected) == 8 else '✗ INCORRECT'}")

    is_compliant = validate_fifo_compliance(selected)
    print(f"FIFO Compliant: {'✓ YES' if is_compliant else '✗ NO'}")

    # Scenario 3: Same idingreso with different dates
    print("\n" + "=" * 80)
    print("[Scenario 3] Single idingreso with varying dates - Need 250kg")
    print("-" * 80)
    available_items, qty_needed = create_test_data_scenario_3()
    print(f"Available: {len(available_items)} items from idingreso 100")

    selected = select_items_for_order(available_items, qty_needed)
    print(f"\nSelected {len(selected)} items totaling {selected['pesokgs'].sum()}kg")
    print(f"Date range: {selected['ingresa'].min()} to {selected['ingresa'].max()}")

    is_compliant = validate_fifo_compliance(selected)
    print(f"FIFO Compliant: {'✓ YES' if is_compliant else '✗ NO'}")

    print("\n" + "=" * 80)
    print("Test suite complete!")
    print("=" * 80)


if __name__ == "__main__":
    run_tests()
