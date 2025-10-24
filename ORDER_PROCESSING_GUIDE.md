# Import Order Processing Guide

## Overview

This document explains the order processing logic implemented in `import_order_builder.py`, which automates the creation of import orders by selecting inventory items based on FIFO (First-In-First-Out) principles and matching incoterms.

## Table of Contents

1. [Process Flow](#process-flow)
2. [Input Requirements](#input-requirements)
3. [FIFO Logic](#fifo-logic)
4. [Incoterm Matching](#incoterm-matching)
5. [Client Code Resolution](#client-code-resolution)
6. [Item Selection Rules](#item-selection-rules)
7. [Output Generation](#output-generation)
8. [Database Dependencies](#database-dependencies)
9. [Error Handling](#error-handling)

---

## Process Flow

The order processing follows these steps:

```
1. Order Received via Excel File
   - File contains 4 columns: Supplier, Model, Color, Quantity
   - Columns are automatically renamed and standardized
   - Whitespace is stripped from all string columns
   ↓
2. User Selects File (via file picker dialog)
   - import_order_builder.py prompts for file selection
   ↓
3. Process All Products
   - For Each Order Line:
     a. Strip whitespace from model and color fields
     b. Match Incoterm (idcoflete)
     c. Find Available Items (idstatus='00')
     d. Select Items Using FIFO
     e. Validate FIFO Compliance
     f. Add to Result Set
   - Products with discrepancies are tracked separately
   - Processing continues for all products (doesn't stop on errors)
   ↓
4. Report Failed/Inconsistent Products
   - Display summary of any products that couldn't be processed
   - Show error details for each failed product
   ↓
5. Generate Excel Verification Table
   - Grouped summary by model, color, and incoterm
   - Shows total weight and item count
   - Saved as {filename}_output.xlsx
   ↓
6. User Review and Confirmation
   - User reviews the Excel verification table
   - System prompts: "Do you want to proceed with DBF generation?"
   - User must explicitly confirm (yes/no)
   ↓
7. Special Case: Client 0002 (ITMA) - Client Inventory Review
   - **Only triggers for special case client '0002'**
   - System automatically generates complete client inventory Excel
   - Inventory grouped by incoterm with all available products
   - Includes: idingreso, model, variant, quantity, units, entry date, tax-free days
   - User shares this inventory file with the client
   - Client reviews to determine if additional items should be added to order
   - System prompts: "Has the client confirmed the order is complete?"
   - If client needs to add items: cancel and rerun after updating order Excel
   - If client confirms: proceed to DBF generation
   ↓
8. Generate DBF Tables (only after all confirmations)
   - One DBF file per incoterm
   - Individual item records (not grouped)
   - Includes all order details and FIFO-selected items
```

---

## Input Requirements

### Excel File Format

The input Excel file must contain exactly **4 columns** (in this order):

| Column | Name (Internal) | Description | Format |
|--------|----------------|-------------|---------|
| 1 | `sup` | Supplier/Contact ID | Integer (padded to 6 digits with leading zeros) |
| 2 | `model` | Model/Product ID | String (20 chars max) |
| 3 | `coldes` | Color/Description | String (20 chars max) |
| 4 | `atrersemfix` | Quantity Needed (kg) | Float/Integer |

**Example:**

```
| Supplier | Model | Color | Quantity |
|----------|-------|-------|----------|
| 44       | ABC123| RED   | 1500.50  |
| 353      | XYZ789| BLUE  | 2000     |
```

### Data Transformations

1. **Supplier ID Padding**: The supplier ID is automatically padded with leading zeros to 6 digits
   - Input: `44` → Output: `000044`
   - Input: `353` → Output: `000353`

2. **Whitespace Trimming**: All string columns are automatically stripped of leading/trailing whitespace
   - This prevents lookup failures caused by Excel formatting
   - Applied to `model` and `coldes` columns
   - Example: `"WHITE "` → `"WHITE"`
   - Example: `" ABC123 "` → `"ABC123"`

3. **Missing Value Removal**: Rows with any missing values are automatically removed (e.g., total rows)

---

## FIFO Logic

### Core FIFO Principles

The system implements strict FIFO (First-In-First-Out) inventory management:

1. **Primary Sort: `idingreso` (Intake Number)**
   - Items are sorted numerically by `idingreso` (earliest intake first)
   - The `idingreso` field is cast to INTEGER for proper numeric sorting
   - Example: idingreso `001` comes before `002`, which comes before `010`

2. **Secondary Sort: `ingresa` (Entry Date)**
   - Within each `idingreso`, items are sorted by entry date (`ingresa`)
   - Earliest dates are processed first
   - NULL dates are treated as "last" (ASC NULLS LAST)

3. **Tertiary Sort: `itemno` (Item Number)**
   - For consistency, items are sorted by `itemno` within the same date

### FIFO Query Example

```sql
SELECT i.*
FROM mobu_opl_insaldo i
JOIN mobu_opl_cohd c ON i.retnum = c.retnum
WHERE i.idcontacto = '000044'
  AND i.idmodelo = 'ABC123'
  AND i.idcoldis = 'RED'
  AND i.idstatus = '00'
  AND c.idcoflete = 'FOB'
ORDER BY
  CAST(i.idingreso AS INTEGER) ASC,  -- Oldest intake first
  i.ingresa ASC NULLS LAST,           -- Oldest entry date first
  i.itemno ASC                        -- Consistent ordering
```

### FIFO Validation

The system validates FIFO compliance after item selection:

```python
def validate_fifo_compliance(selected_items: pd.DataFrame) -> bool:
    """
    Checks:
    1. Items are sorted by idingreso (numeric ascending)
    2. Items are sorted by ingresa date within each idingreso (earliest first)

    Returns:
        True if FIFO compliant, False otherwise
    """
```

If FIFO validation fails, a warning is logged but the process continues.

---

## Incoterm Matching

### What are Incoterms?

Incoterms (International Commercial Terms) define shipping responsibilities. In this system, they're identified by `idcoflete` (e.g., "FOB", "CIF", "EXW").

### Incoterm Resolution Process

1. **Find Oldest Intake's Incoterm**
   ```sql
   SELECT c.idcoflete, i.idingreso
   FROM mobu_opl_insaldo i
   JOIN mobu_opl_cohd c ON i.retnum = c.retnum
   WHERE i.idcontacto = :sup
     AND i.idmodelo = :model
     AND i.idcoldis = :coldes
     AND i.idstatus = '00'
   ORDER BY
     CAST(i.idingreso AS INTEGER) ASC,
     i.ingresa ASC NULLS LAST
   LIMIT 1
   ```

2. **Use Same Incoterm for All Selected Items**
   - All items selected for an order must share the same incoterm
   - This ensures shipping terms remain consistent

3. **Separate Orders by Incoterm**
   - If items have different incoterms, they're placed in separate orders
   - Each incoterm generates its own DBF file

### Fuzzy Matching

If an exact match for `coldes` (color/description) is not found:

1. **Calculate Similarity**
   - Uses character-based similarity scoring
   - Removes accents and normalizes strings for comparison
   - Threshold: 80% similarity

2. **User Confirmation**
   - If close matches (≥80% similarity) are found, the user is prompted:
   ```
   >> Use this match? (y/n/a for 'yes to all similar'):
   ```

3. **Match Acceptance**
   - `y` or `yes`: Use the matched value
   - `n` or `no`: Reject and raise error
   - `a` or `all`: Accept this and similar future matches

---

## Client Code Resolution

### Purpose

The client code (`idcentro`) is embedded in the order number and used for routing/identification.

### Resolution Logic

```
1. Check Special Cases
   ↓
   If idcontacto IN ('000044', '000353', '000355', '000205', '000151', '000108', '000396'):
      Return '0002'

2. Query mobu_opl_incontac
   ↓
   Get idcontacto value (e.g., "AL0002")

3. Remove Leading Zeros
   ↓
   Transform: "AL0002" → "AL02"
   Logic: Remove up to 2 leading zeros from the numeric part

4. Query mobu_opl_ctcentro
   ↓
   Match against idcentro field

5. Return Matched Value
   ↓
   Use in order numero generation
```

### Special Cases

These supplier IDs always map to client code `0002`:
- `000044`
- `000353`
- `000355`
- `000205`
- `000151`
- `000108`
- `000396`

### Zero Removal Example

| Original | Letter Part | Number Part | Zeros Removed | Result |
|----------|-------------|-------------|---------------|--------|
| AL0002   | AL          | 0002        | 2             | AL02   |
| AB00123  | AB          | 00123       | 2             | AB123  |
| XY000456 | XY          | 000456      | 2             | XY0456 |

---

## Item Selection Rules

### Quantity Fulfillment

1. **Process Items in FIFO Order**
   - Iterate through sorted items
   - Accumulate quantity (`pesokgs`) until need is met

2. **Cross-Intake Selection**
   - If one `idingreso` doesn't have enough inventory, continue to the next
   - Example:
     ```
     Need: 1000 kg

     idingreso 001: 600 kg (select all)
     idingreso 002: 800 kg (select 400 kg to reach 1000 kg total)
     ```

### Remaining Items Rule

**If an `idingreso` has 1-4 remaining items after fulfilling quantity, include them all.**

**Rationale**: Avoid leaving small quantities that are difficult to manage.

**Example**:
```
Need: 1000 kg
Available in idingreso 002:
  - Item 1: 200 kg
  - Item 2: 300 kg
  - Item 3: 250 kg  ← Fulfills requirement (750 kg total)
  - Item 4: 100 kg  ← Included (only 1 item remaining)

Total Selected: 850 kg (slight overage acceptable)
```

### Status Filtering

**Only items with `idstatus = '00'` are considered available.**

Other status codes indicate:
- Reserved items
- Damaged items
- Items in transit
- Other unavailable states

---

## Output Generation

### 1. Excel Summary File

**Filename**: `{original_filename}_output.xlsx`

**Columns**:
- `idmodelo`: Model ID
- `idcoldis`: Color/Description
- `incoterm`: Incoterm code (idcoflete)
- `total_pesokgs`: Total weight selected (kg)
- `item_count`: Number of individual items

**Purpose**: High-level summary for review and validation

### 2. DBF Files

**Filename Format**: `{YYYYMMDD-HHMMSS-client}_{incoterm}.dbf`

**Example**: `20250124-143025-0002_FOB.dbf`

**Structure** (19 fields):

| Field | Type | Width | Description |
|-------|------|-------|-------------|
| numero | C | 25 | Order number (YYYYMMDD-HHMMSS-client) |
| itemline | C | 5 | Item line number (blank) |
| idproducto | C | 15 | Product ID (idingreso + itemno) |
| idcontacto | C | 6 | Supplier ID |
| idmodelo | C | 20 | Model ID |
| idcoldis | C | 20 | Color/Description |
| idubica | C | 6 | Location ID |
| idreceiver | C | 4 | Receiver ID (empty) |
| cantidad | N | 10,2 | Quantity (kg) |
| fecrec | D | 8 | Reception date (empty) |
| scanned | L | 1 | Scanned flag (False) |
| available | L | 1 | Available flag (True) |
| ingresa | D | 8 | Entry date (current date) |
| modifica | D | 8 | Modification date (current date) |
| usuario | C | 4 | User ID ('1000') |
| equipo | C | 4 | Equipment ID ('0001') |
| idubica1 | C | 6 | Secondary location |
| client | C | 10 | Client code (idcentro) |
| idcoflete | C | 6 | Incoterm code |

**Purpose**: Individual item details for import into legacy systems

---

## Database Dependencies

### Required Tables

1. **`ds_vfp.mobu_opl_insaldo`** (Inventory Balance)
   - **Key Fields**:
     - `idingreso`: Intake number (FIFO primary key)
     - `ingresa`: Entry date (FIFO secondary key)
     - `itemno`: Item number
     - `idcontacto`: Supplier/Contact ID
     - `idmodelo`: Model/Product ID
     - `idcoldis`: Color/Description
     - `pesokgs`: Weight in kilograms
     - `idstatus`: Status code ('00' = available)
     - `retnum`: Receipt number (FK to cohd)
     - `idubica`: Location ID
     - `idubica1`: Secondary location ID

2. **`ds_vfp.mobu_opl_cohd`** (Receipt Header)
   - **Key Fields**:
     - `retnum`: Receipt number (PK)
     - `idcoflete`: Incoterm code

3. **`ds_vfp.mobu_opl_incontac`** (Contacts)
   - **Key Fields**:
     - `idcontacto`: Contact ID (both PK and string representation)

4. **`ds_vfp.mobu_opl_ctcentro`** (Centers/Clients)
   - **Key Fields**:
     - `idcentro`: Center/Client ID

### Database Connection

```python
DB_USER = "jm"
DB_HOST = "127.0.0.1"
DB_PORT = "9700"
DB_NAME = "datax"

# Connection string:
postgresql+psycopg2://jm:@127.0.0.1:9700/datax
```

### Key Relationships

```
mobu_opl_insaldo.retnum → mobu_opl_cohd.retnum
mobu_opl_insaldo.idcontacto → mobu_opl_incontac.idcontacto
mobu_opl_incontac.idcontacto → mobu_opl_ctcentro.idcentro (after transformation)
```

---

## Error Handling

### Error Processing Strategy

The system uses a **continue-on-error** approach:

1. **Non-Blocking Processing**: When an error occurs for one product, processing continues for remaining products
2. **Error Collection**: Failed orders are tracked in a separate list with full error details
3. **End-of-Process Reporting**: All failed orders are displayed in a summary after processing completes
4. **Partial Success**: Successfully processed products generate output even if some products fail

**Example Output:**
```
================================================================================
FAILED ORDERS SUMMARY: 2 order(s) could not be processed
================================================================================
  Order #3: sup=000123, model=XYZ456, coldes=GREEN, qty=500
    Error: No incoterm found for sup=000123, model=XYZ456, coldes=GREEN
  Order #5: sup=000044, model=ABC789, coldes=BLUE , qty=1000
    Error: No available items found
================================================================================
```

This approach allows users to:
- Review all errors at once
- Process valid orders without interruption
- Investigate and correct failed orders separately
- Reprocess only the failed items in a subsequent run

### Common Errors and Solutions

#### 1. **Contact Not Found**
```
ERROR: idcontacto '000123' not found in mobu_opl_incontac table.
```
**Solution**: Verify the supplier exists in the database

#### 2. **No Matching idcentro**
```
ERROR: No matching idcentro found in mobu_opl_ctcentro for 'AL02'
```
**Solution**: Check if the transformed value exists in ctcentro table

#### 3. **No Incoterm Found**
```
ERROR: No incoterm found for sup=000044, model=ABC123, coldes=RED
```
**Solutions**:
- Verify product exists in insaldo with idstatus='00'
- Check if coldes matches exactly (or use fuzzy matching)
- Ensure items have associated retnum in cohd

#### 4. **No Available Items**
```
WARNING: No available items found for this combination
```
**Possible Causes**:
- All items have idstatus != '00' (reserved/unavailable)
- No items match the incoterm
- Inventory depleted for this product

#### 5. **FIFO Compliance Warning**
```
WARNING: Selected items may not be FIFO compliant!
```
**Note**: This is a validation warning. The order is still created but should be reviewed.

### Debug Output

The system provides detailed debug output for troubleshooting:

```
DEBUG: Query returned 45 items from 3 idingreso(s)
  idingreso 001: 15 items, total 1234.56kg, retnum=RET001
    Date range: 2025-01-15 to 2025-01-20
  idingreso 002: 20 items, total 2345.67kg, retnum=RET002
    Date range: 2025-01-22 to 2025-01-25
  idingreso 003: 10 items, total 987.65kg, retnum=RET003
    Date range: 2025-01-28 to 2025-01-30
```

---

## Usage Example

### Input Excel File (`orders.xlsx`)

```
| Supplier | Model  | Color | Quantity |
|----------|--------|-------|----------|
| 44       | ABC123 | RED   | 1500     |
| 353      | XYZ789 | BLUE  | 2000     |
```

### Execution

```bash
python import_order_builder.py
```

### Console Output

```
Please select the Excel order file...
Selected file: /path/to/orders.xlsx
Read Excel file with 2 rows and 4 columns
Original columns: ['Supplier', 'Model', 'Color', 'Quantity']
Renamed columns to: ['sup', 'model', 'coldes', 'atrersemfix']
Padded 'sup' column with leading zeros to 6 digits

Processing order 1/2: sup=000044, model=ABC123, coldes=RED, qty=1500
  Using special case idcentro: 0002 for idcontacto: 000044
  Found incoterm: FOB from oldest idingreso: 001
  DEBUG: Query returned 25 items from 2 idingreso(s)
    idingreso 001: 15 items, total 800.00kg, retnum=RET001
      Date range: 2025-01-15 to 2025-01-20
    idingreso 002: 10 items, total 900.00kg, retnum=RET002
      Date range: 2025-01-22 to 2025-01-25
  Selected 20 items from 2 idingreso(s): 001, 002
  Total weight: 1550.00 kg (needed: 1500 kg)

Processing order 2/2: sup=000353, model=XYZ789, coldes=BLUE, qty=2000
  Using special case idcentro: 0002 for idcontacto: 000353
  Found incoterm: CIF from oldest idingreso: 003
  DEBUG: Query returned 30 items from 1 idingreso(s)
    idingreso 003: 30 items, total 2100.00kg, retnum=RET003
      Date range: 2025-01-10 to 2025-01-15
  Selected 25 items from 1 idingreso(s): 003
  Total weight: 2050.00 kg (needed: 2000 kg)

Excel results saved to: /path/to/orders_output.xlsx

Generating DBF for incoterm FOB
  Numero: 20250124-143025-0002
  Client code: 0002
DBF file saved: /path/to/20250124-143025-0002_FOB.dbf (20 items)

Generating DBF for incoterm CIF
  Numero: 20250124-143026-0002
  Client code: 0002
DBF file saved: /path/to/20250124-143026-0002_CIF.dbf (25 items)

Total items selected: 45
Total weight: 3600.00 kg

✓ Processing complete!
  - Excel output: /path/to/orders_output.xlsx
  - DBF files: /path/to/{filename}_{incoterm}.dbf
```

---

## Best Practices

1. **Review Fuzzy Matches**: Always carefully review fuzzy color/description matches
2. **Validate FIFO**: Check for FIFO compliance warnings
3. **Verify Quantities**: Compare selected vs. needed quantities
4. **Check Incoterms**: Ensure incoterms are as expected
5. **Backup Data**: Always backup before processing large orders
6. **Test with Small Files**: Test with 2-3 orders before processing bulk

---

## Maintenance Notes

### Updating Special Cases

To add/remove special case suppliers that map to idcentro '0002':

```python
# In get_client_code() function (import_order_builder.py:158)
special_cases = {'000044', '000353', '000355', '000205', '000151', '000108', '000396'}
```

### Database Configuration

Update database credentials in `import_order_builder.py`:

```python
DB_USER = "jm"
DB_HOST = "127.0.0.1"
DB_PORT = "9700"
DB_NAME = "datax"
```

### DBF Field Structure

To modify DBF structure, update the `table_structure` list in `generate_dbf_order()` function (line 564).

---

## Glossary

- **FIFO**: First-In-First-Out - inventory management method
- **Incoterm**: International Commercial Terms - shipping/delivery terms
- **idingreso**: Intake/receipt number - when inventory was received
- **ingresa**: Entry date - when item was entered into system
- **idcontacto**: Contact/Supplier ID
- **idcentro**: Center/Client ID
- **idcoflete**: Freight/Incoterm code
- **idstatus**: Item status code ('00' = available)
- **pesokgs**: Weight in kilograms
- **retnum**: Receipt number linking insaldo to cohd
- **DBF**: dBASE file format - legacy database file

---

## Revision History

| Version | Date | Author | Description |
|---------|------|--------|-------------|
| 1.0 | 2025-01-24 | JM | Initial documentation |

---

## Support

For issues or questions:
1. Check error messages carefully
2. Review debug output
3. Verify database connectivity
4. Confirm data exists in all required tables
5. Check special cases and fuzzy match thresholds