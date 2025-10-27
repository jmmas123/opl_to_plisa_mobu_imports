import pandas as pd
from sqlalchemy import create_engine, text
import os
from typing import List, Dict, Any, Optional, Tuple
import unicodedata
from datetime import datetime
import dbf
import subprocess
import sys

# Try a import tkinter, but make it optional
try:
    from tkinter import Tk, filedialog
    TKINTER_AVAILABLE = True
except ImportError:

    TKINTER_AVAILABLE = False
    print("Nota: tkinter no disponible. El selector de archivos estará deshabilitado.")

# Database configuration - modify these as needed
DB_USER = "jm"
DB_HOST = "127.0.0.1"
DB_PORT = "9700"
DB_NAME = "datax"


def get_db_connection():
    """Create and return database engine connection."""
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    return engine


def select_excel_file() -> Optional[str]:
    """
    Open a file picker dialog for the user a select an Excel file.
    Returns the selected file path or None if cancelled.
    """
    root = Tk()
    root.withdraw()  # Hide the main window
    root.attributes('-topmost', True)  # Bring dialog a front

    file_path = filedialog.askopenfilename(
        title="Seleccionar Archivo de Pedido Excel",
        filetypes=[
            ("Archivos Excel", "*.xlsx *.xls"),
            ("Todos los archivos", "*.*")
        ]
    )

    root.destroy()

    return file_path if file_path else None


def read_excel_orders(file_path: str) -> pd.DataFrame:
    """
    Read the Excel file containing order requirements.
    Accepts any four columnas and renames them a the required column names.
    The order of columnas is preserved: first column -> sup, second -> model,
    third -> coldes, fourth -> atrersemfix
    The first column (sup) will be padded with leading zeros a make it 6 digits.
    """
    df = pd.read_excel(file_path)
    print(f"Archivo Excel leído con {len(df)} filas y {len(df.columns)} columnas")

    required_columns = ['sup', 'model', 'coldes', 'atrersemfix']

    # Check that we have exactly 4 columnas
    if len(df.columns) != 4:
        raise ValueError(f"Se esperaban exactamente 4 columnas, pero se encontraron {len(df.columns)} columnas")

    # Get the original column names for logging
    original_columns = df.columns.tolist()
    print(f"Columnas originales: {original_columns}")

    # Rename columnas a required names in order
    df.columns = required_columns
    print(f"Columnas renombradas a: {required_columns}")

    # Remove rows with any missing values (e.g., total rows, incomplete data)
    original_count = len(df)
    df = df.dropna(how='any')
    removed_count = original_count - len(df)
    if removed_count > 0:
        print(f"Eliminadas {removed_count} fila(s) con valores faltantes (ej., filas totales)")

    # Pad the first column (sup/idcontacto) with leading zeros a make it 6 digits
    # Convert a int first a remove any decimal points (Excel reads numbers as floats)
    df['sup'] = df['sup'].astype(int).astype(str).str.zfill(6)
    print(f"Columna 'sup' rellenada con ceros a la izquierda hasta 6 dígitos")

    # Strip whitespace from string columnas a avoid lookup issues
    # (Excel data often has trailing spaces like "WHITE " that don't match DB values)
    df['model'] = df['model'].astype(str).str.strip()
    df['coldes'] = df['coldes'].astype(str).str.strip()
    print(f"Espacios en blanco eliminados de las columnas 'model' y 'coldes'")

    return df


def normalize_string(s: str) -> str:
    """
    Normalize a string for comparison by:
    - Removing accents (é -> e, á -> a, etc.)
    - Converting a uppercase
    - Stripping whitespace
    """
    # Decompose unicode characters and filter out combining marks (accents)
    nfd = unicodedata.normalize('NFD', s)
    without_accents = ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')
    return without_accents.upper().strip()


def calculate_similarity(s1: str, s2: str) -> float:
    """
    Calculate similarity ratio between two strings (0.0 a 1.0).
    Uses a simple character-based approach similar a Levenshtein distance.
    """
    if s1 == s2:
        return 1.0

    # Normalize both strings for comparison
    norm_s1 = normalize_string(s1)
    norm_s2 = normalize_string(s2)

    if norm_s1 == norm_s2:
        return 0.95  # Very high similarity if only accents differ

    # Simple character matching ratio
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0

    # Count matching characters
    matches = sum(1 for a, b in zip(s1, s2) if a == b)
    max_len = max(len1, len2)

    return matches / max_len


def find_similar_products(engine, sup: str, model: str, coldes: str) -> pd.DataFrame:
    """
    Find similar products when exact match is not found or insufficient.

    Search strategy:
    1. Same supplier, similar model (≥80% similarity), exact color
    2. Same supplier, exact model, similar color (≥80% similarity)
    3. Same supplier, similar model and color (≥80% similarity each)

    Returns DataFrame with columnas: idcontacto, idmodelo, idcoldis, idcoflete,
                                     total_available_kg, model_similarity, color_similarity
    """
    # Search for products with same supplier and available status
    query = text("""
        SELECT DISTINCT
            i.idcontacto,
            i.idmodelo,
            i.idcoldis,
            c.idcoflete,
            SUM(i.pesokgs) as total_available_kg,
            COUNT(*) as item_count
        FROM ds_vfp.mobu_opl_insaldo i
        JOIN ds_vfp.mobu_opl_cohd c ON i.retnum = c.retnum
        WHERE i.idcontacto = :sup
            AND i.idstatus = '00'
        GROUP BY i.idcontacto, i.idmodelo, i.idcoldis, c.idcoflete
        HAVING SUM(i.pesokgs) > 0
        ORDER BY i.idmodelo, i.idcoldis
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'sup': sup})

    if df.empty:
        return pd.DataFrame()

    # Calculate similarity scores for each product
    df['model_similarity'] = df['idmodelo'].apply(lambda x: calculate_similarity(model, x))
    df['color_similarity'] = df['idcoldis'].apply(lambda x: calculate_similarity(coldes, x))

    # Filter for similar products (≥80% similarity threshold)
    # Exclude exact match if it exists (will be handled separately)
    similar_products = df[
        ~((df['idmodelo'] == model) & (df['idcoldis'] == coldes)) &
        ((df['model_similarity'] >= 0.80) | (df['color_similarity'] >= 0.80))
    ].copy()

    if similar_products.empty:
        return pd.DataFrame()

    # Sort by overall similarity (average of both scores)
    similar_products['overall_similarity'] = (
        similar_products['model_similarity'] + similar_products['color_similarity']
    ) / 2
    similar_products = similar_products.sort_values('overall_similarity', ascending=False)

    return similar_products


def get_client_code(engine, idcontacto: str) -> str:
    """
    Get the client code (idcentro) for a given supplier (idcontacto).

    Logic:
    1. For specific idcontacto values (000044, 000353, 000355, 000205, 000151, 000108, 000396),
       return '0002' as the idcentro (special cases)
    2. For other cases:
       - Query mobu_opl_incontac a get idcontacto value (e.g., "AL0002")
       - Remove two leading zeros a get "AL02"
       - Query mobu_opl_ctcentro a find matching idcentro
       - Return the matched idcentro value
       - If not found at any step, raise an error for user a investigate

    Args:
        engine: Database engine connection
        idcontacto: Contact/Supplier ID (6 digits with leading zeros)

    Returns:
        idcentro value (client code)

    Raises:
        ValueError: If idcontacto not found in mobu_opl_incontac or no matching idcentro found
    """
    # Special cases - hardcoded mapping
    special_cases = {'000044', '000353', '000355', '000205', '000151', '000108', '000396'}

    if idcontacto in special_cases:
        print(f"  Usando caso especial idcentro: 0002 para idcontacto: {idcontacto}")
        return '0002'

    # Query mobu_opl_incontac a get the idcontacto string representation
    query = text("""
        SELECT idcontacto
        FROM ds_vfp.mobu_opl_incontac
        WHERE idcontacto = :idcontacto
        LIMIT 1
    """)

    with engine.connect() as conn:
        query_result = conn.execute(query, {'idcontacto': idcontacto})
        row = query_result.fetchone()

        if not row:
            error_msg = f"ERROR: idcontacto '{idcontacto}' no encontrado en tabla mobu_opl_incontac. Por favor verifique que este contacto existe en la base de datos."
            print(f"\n{'='*80}")
            print(f"  {error_msg}")
            print(f"{'='*80}\n")
            raise ValueError(error_msg)

        contacto_value = row[0].strip() if row[0] else idcontacto

        # Remove two leading zeros from the idcontacto
        # Example: "AL0002" -> "AL02"
        if len(contacto_value) >= 2:
            # Find the position of the first two zeros after any letters
            modified_value = contacto_value
            # If it starts with letters, keep them and remove zeros from the numeric part
            letter_part = ''
            number_part = contacto_value

            for i, char in enumerate(contacto_value):
                if not char.isdigit():
                    letter_part = contacto_value[:i+1]
                    number_part = contacto_value[i+1:]
                else:
                    break

            # Remove leading zeros (up a 2) from the number part
            number_stripped = number_part.lstrip('0')
            # If we removed more than 2 zeros, add some back
            zeros_removed = len(number_part) - len(number_stripped)
            if zeros_removed > 2:
                number_part = '0' * (zeros_removed - 2) + number_stripped
            else:
                number_part = number_stripped

            modified_value = letter_part + number_part
        else:
            modified_value = contacto_value

        print(f"  Buscando idcentro con valor modificado: '{modified_value}' (de '{contacto_value}')")

        # Query mobu_opl_ctcentro a find matching idcentro
        centro_query = text("""
            SELECT idcentro
            FROM ds_vfp.mobu_opl_ctcentro
            WHERE idcentro = :modified_value
            LIMIT 1
        """)

        centro_result = conn.execute(centro_query, {'modified_value': modified_value})
        centro_row = centro_result.fetchone()

        if centro_row:
            idcentro = centro_row[0].strip() if centro_row[0] else None
            if idcentro:
                print(f"  idcentro encontrado: {idcentro}")
                return idcentro
            else:
                error_msg = f"ERROR: Registro encontrado pero idcentro está vacío para valor modificado '{modified_value}' (de idcontacto '{idcontacto}'). Por favor verifique los datos en mobu_opl_ctcentro."
                print(f"\n{'='*80}")
                print(f"  {error_msg}")
                print(f"{'='*80}\n")
                raise ValueError(error_msg)
        else:
            error_msg = f"ERROR: No se encontró idcentro coincidente en mobu_opl_ctcentro para '{modified_value}' (de idcontacto '{idcontacto}', valor original '{contacto_value}'). Por favor verifique que este centro existe en la base de datos."
            print(f"\n{'='*80}")
            print(f"  {error_msg}")
            print(f"{'='*80}\n")
            raise ValueError(error_msg)


def get_all_available_inventory_for_product(engine, sup: str, model: str, coldes: str) -> pd.DataFrame:
    """
    Get ALL available inventory for a product across ALL incoterms.
    This helps identify if there's inventory under different incoterms.

    Returns DataFrame with columnas: idcoflete, total_available_kg, oldest_idingreso
    sorted by total_available_kg descending (most inventory first)
    """
    query = text("""
        SELECT
            c.idcoflete,
            SUM(i.pesokgs) as total_available_kg,
            MIN(CAST(i.idingreso AS INTEGER)) as oldest_idingreso_num,
            MIN(i.idingreso) as oldest_idingreso
        FROM ds_vfp.mobu_opl_insaldo i
        JOIN ds_vfp.mobu_opl_cohd c ON i.retnum = c.retnum
        WHERE i.idcontacto = :sup
            AND i.idmodelo = :model
            AND i.idcoldis = :coldes
            AND i.idstatus = '00'
        GROUP BY c.idcoflete
        ORDER BY oldest_idingreso_num ASC
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'sup': sup, 'model': model, 'coldes': coldes})

    return df


def get_incoterm_for_product(engine, sup: str, model: str, coldes: str) -> Tuple[str, str]:
    """
    Query the incoterm (idcoflete) for a specific product combination.

    Steps:
    1. Query insaldo table filtered by sup (idcontacto), model (idmodelo),
       coldes (idcoldis), and idstatus='00'
    2. Get retnum values from insaldo
    3. Query cohd table using retnum a get idcoflete

    Returns:
        Tuple of (idcoflete, matched_coldes) where matched_coldes is the actual
        database value used (may differ from input if fuzzy matching was used)
    """
    query = text("""
        SELECT c.idcoflete, i.idingreso
        FROM ds_vfp.mobu_opl_insaldo i
        JOIN ds_vfp.mobu_opl_cohd c ON i.retnum = c.retnum
        WHERE i.idcontacto = :sup
            AND i.idmodelo = :model
            AND i.idcoldis = :coldes
            AND i.idstatus = '00'
        ORDER BY
            CAST(i.idingreso AS INTEGER) ASC,
            i.ingresa ASC NULLS LAST
        LIMIT 1
    """)

    with engine.connect() as conn:
        query_result = conn.execute(query, {'sup': sup, 'model': model, 'coldes': coldes})
        row = query_result.fetchone()
        if row:
            idcoflete = row[0]
            oldest_idingreso = row[1]
            print(f"  Incoterm encontrado: {idcoflete} del idingreso más antiguo: {oldest_idingreso}")
            return idcoflete, coldes  # Return idcoflete and the exact coldes used
        else:
            # DEBUG: Show what values actually exist in the database for this product
            debug_query = text("""
                SELECT DISTINCT
                    i.idcontacto,
                    i.idmodelo,
                    i.idcoldis,
                    i.idstatus,
                    LENGTH(i.idcoldis) as coldes_length,
                    c.idcoflete
                FROM ds_vfp.mobu_opl_insaldo i
                JOIN ds_vfp.mobu_opl_cohd c ON i.retnum = c.retnum
                WHERE i.idcontacto = :sup
                    AND i.idmodelo = :model
                    AND i.idstatus = '00'
                LIMIT 5
            """)

            debug_result = conn.execute(debug_query, {'sup': sup, 'model': model})
            debug_rows = debug_result.fetchall()

            print(f"  DEBUG: No se encontró coincidencia exacta. Se buscó:")
            print(f"    sup (idcontacto): '{sup}' (longitud: {len(sup)})")
            print(f"    model (idmodelo): '{model}' (longitud: {len(model)})")
            print(f"    coldes (idcoldis): '{coldes}' (longitud: {len(coldes)}, repr: {repr(coldes)})")
            print(f"    idstatus: '00'")

            if debug_rows:
                print(f"  DEBUG: Encontrados {len(debug_rows)} registro(s) DISPONIBLES (idstatus='00') con mismo sup/model:")

                # Find best matches based on similarity
                matches_with_similarity = []
                for i, row in enumerate(debug_rows, 1):
                    db_coldes = row[2] if row[2] else ""
                    similarity = calculate_similarity(coldes, db_coldes)
                    matches_with_similarity.append((i, row, similarity))

                    print(f"    [{i}] idcontacto: '{row[0]}', idmodelo: '{row[1]}', idcoldis: '{row[2]}' (longitud: {row[4]}, repr: {repr(row[2])}), idstatus: '{row[3]}', idcoflete: '{row[5]}'")
                    if row[2]:  # If idcoldis is not None
                        # Show character-by-character comparison
                        if row[2] != coldes:
                            print(f"        Diferencia: Esperado '{coldes}' vs Base de datos '{row[2]}'")
                            print(f"        Comparación de bytes: Esperado {coldes.encode('utf-8')} vs Base de datos {row[2].encode('utf-8')}")
                            print(f"        Puntuación de similitud: {similarity:.2%}")

                # Sort by similarity (descending)
                matches_with_similarity.sort(key=lambda x: x[2], reverse=True)

                # Find close matches (similarity >= 80%)
                # Todos los debug_rows already have idstatus='00', so no need a filter again
                close_matches = [(idx, row, sim) for idx, row, sim in matches_with_similarity if sim >= 0.80]

                if close_matches:
                    print(f"\n  COINCIDENCIA DIFUSA: Encontradas {len(close_matches)} coincidencia(s) cercana(s) con idstatus='00':")
                    for idx, row, sim in close_matches:
                        print(f"    [{idx}] '{row[2]}' (similitud: {sim:.2%}, idcoflete: '{row[5]}')")

                    print(f"\n  Buscando: '{coldes}'")
                    best_match_idx, best_match_row, best_match_sim = close_matches[0]
                    print(f"  Mejor coincidencia: [{best_match_idx}] '{best_match_row[2]}' (similitud: {best_match_sim:.2%})")

                    # Ask user for confirmation
                    response = input(f"\n  >> ¿Usar esta coincidencia? (s/n/t para 'sí a todas las similares'): ").strip().lower()

                    if response in ['y', 'yes', 'a', 'all']:
                        print(f"  ✓ Usando valor de base de datos: '{best_match_row[2]}' con idcoflete: '{best_match_row[5]}'")
                        return best_match_row[5], best_match_row[2]  # Return (idcoflete, matched_coldes)
                    else:
                        print(f"  ✗ Coincidencia rechazada por usuario")
                        raise ValueError(f"No se encontró incoterm para sup={sup}, model={model}, coldes={coldes} (usuario rechazó coincidencia difusa)")
                else:
                    print(f"  No se encontraron coincidencias cercanas (umbral de similitud: 80%)")
            else:
                print(f"  DEBUG: No se encontraron registros con sup={sup} y model={model}")

            raise ValueError(f"No se encontró incoterm para sup={sup}, model={model}, coldes={coldes}")


def get_available_items(engine, sup: str, model: str, coldes: str, idcoflete: str) -> pd.DataFrame:
    """
    Get all available ítems de insaldo that match the product and incoterm criteria.
    Returns full rows with all columnas, sorted by FIFO rules:
    1. First by idingreso (converted a numeric for proper sorting)
    2. Then by ingresa date (earliest first)
    3. Finally by itemno for consistency

    Includes c.idcoflete in the result so we can track which incoterm each item came from.
    """
    query = text("""
        SELECT i.*, c.idcoflete as source_idcoflete
        FROM ds_vfp.mobu_opl_insaldo i
        JOIN ds_vfp.mobu_opl_cohd c ON i.retnum = c.retnum
        WHERE i.idcontacto = :sup
            AND i.idmodelo = :model
            AND i.idcoldis = :coldes
            AND i.idstatus = '00'
            AND c.idcoflete = :idcoflete
        ORDER BY
            CAST(i.idingreso AS INTEGER) ASC,
            i.ingresa ASC NULLS LAST,
            i.itemno ASC
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'sup': sup, 'model': model, 'coldes': coldes, 'idcoflete': idcoflete})

    # DEBUG: Show what was returned from the query
    if not df.empty:
        print(f"  DEBUG: Consulta devolvió {len(df)} ítems de {df['idingreso'].nunique()} idingreso(s)")
        for idingreso, group in df.groupby('idingreso', sort=False):
            print(f"    idingreso {idingreso}: {len(group)} ítems, total {group['pesokgs'].sum():.2f}kg, retnum={group['retnum'].iloc[0]}")
            print(f"      Rango de fechas: {group['ingresa'].min()} a {group['ingresa'].max()}")

    return df


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

    # Check idingreso ordering (convert a numeric for comparison)
    try:
        selected_items['_idingreso_numeric'] = pd.to_numeric(selected_items['idingreso'], errors='coerce')
        idingreso_sorted = selected_items['_idingreso_numeric'].is_monotonic_increasing

        # Check ingresa date ordering within each idingreso
        ingresa_sorted = True
        for idingreso, group in selected_items.groupby('idingreso', sort=False):
            if len(group) > 1 and 'ingresa' in group.columns:
                group_ingresa = group['ingresa'].dropna()
                if len(group_ingresa) > 1:
                    if not group_ingresa.is_monotonic_increasing:
                        ingresa_sorted = False
                        break

        return idingreso_sorted and ingresa_sorted
    except Exception as e:
        print(f"  Advertencia: No se pudo validar cumplimiento FIFO: {e}")
        return True  # Don't fail the process if validation has issues


def select_items_for_order(available_items: pd.DataFrame, qty_needed: float) -> pd.DataFrame:
    """
    Select items a fulfill the order quantity following FIFO rules.

    FIFO Logic:
    - Items are already sorted by idingreso (numeric ascending) and ingresa date (earliest first)
    - Process items in this sorted order a ensure FIFO compliance
    - Continue taking from subsequent idingresos until qty_needed is fulfilled
    - If an idingreso has fewer than 5 remaining items after fulfilling qty, include all remaining items

    Args:
        available_items: DataFrame of available items sorted by FIFO rules (idingreso, ingresa date)
        qty_needed: Total quantity (pesokgs) needed a fulfill the order

    Returns:
        DataFrame of selected items that fulfill the order
    """
    if available_items.empty:
        return pd.DataFrame()

    selected_items = []
    accumulated_qty = 0.0

    # Track which idingreso we're currently processing
    current_idingreso = None
    items_in_current_ingreso = []

    # Process items in FIFO order (already sorted by query)
    for idx, row in available_items.iterrows():
        pesokgs = row['pesokgs'] if pd.notna(row['pesokgs']) else 0.0
        idingreso = row['idingreso']

        # Track when we move a a new idingreso
        if current_idingreso != idingreso:
            current_idingreso = idingreso
            items_in_current_ingreso = available_items[available_items['idingreso'] == idingreso]

        # Add current item
        selected_items.append(row)
        accumulated_qty += pesokgs

        # Check if we've met the quantity requirement
        if accumulated_qty >= qty_needed:
            # Find remaining items in the current idingreso
            idx_pos = items_in_current_ingreso.index.get_loc(idx)
            # get_loc can return an int or slice; handle int case
            current_position = idx_pos if isinstance(idx_pos, int) else idx_pos.start
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


def generate_client_inventory_for_confirmation(engine, output_dir: str, client_code: str) -> Optional[str]:
    """
    Generate client inventory Excel for special case client '0002' (ITMA).
    This allows the client a review their complete inventory and confirm
    if they want a add anything else a the order.

    Args:
        engine: Database engine connection
        output_dir: Directory a save the inventory Excel file
        client_code: Client code (idcentro)

    Returns:
        Path a the generated inventory Excel file, or None if not applicable
    """
    # Only generate for special case client '0002' (ITMA)
    if client_code != '0002':
        return None

    print(f"\n{'='*80}")
    print("GENERACIÓN DE INVENTARIO DEL CLIENTE (Caso Especial Cliente 0002)")
    print(f"{'='*80}")
    print("Generando inventario completo para revisión del cliente...")
    print("Esto permite al cliente verificar y agregar cualquier ítem adicional al pedido.")

    # Get all idcontacto values that start with '0' (ITMA suppliers)
    query = text("""
        SELECT DISTINCT i.idcontacto
        FROM ds_vfp.mobu_opl_insaldo i
        WHERE i.idcontacto LIKE '0%'
            AND i.idstatus = '00'
        ORDER BY i.idcontacto
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        idcontacto_list = [row[0] for row in result.fetchall()]

    if not idcontacto_list:
        print("No se encontró inventario para cliente 0002 (ITMA).")
        return None

    print(f"Encontrados {len(idcontacto_list)} contactos de proveedor para cliente ITMA")

    # Get inventory by incoterm (reuse logic from inventory_viewer.py)
    idcontacto_params = ','.join([f"'{contact}'" for contact in idcontacto_list])

    inventory_query = text(f"""
        SELECT
            c.idcoflete,
            i.idingreso,
            i.idmodelo,
            i.idcoldis,
            i.pesokgs,
            m.idum,
            i.ingresa
        FROM ds_vfp.mobu_opl_insaldo i
        JOIN ds_vfp.mobu_opl_cohd c ON i.retnum = c.retnum
        LEFT JOIN ds_vfp.mobu_opl_inmodelo m ON i.idmodelo = m.idmodelo
        WHERE i.idcontacto IN ({idcontacto_params})
            AND i.idstatus = '00'
        ORDER BY c.idcoflete, i.idingreso, i.idmodelo, i.idcoldis
    """)

    with engine.connect() as conn:
        inventory_df = pd.read_sql(inventory_query, conn)

    if inventory_df.empty:
        print("No se encontró inventario disponible.")
        return None

    # Calculate days remaining in 2-year tax period
    current_date = pd.Timestamp.now().tz_localize(None)
    inventory_df['ingresa'] = pd.to_datetime(inventory_df['ingresa'], errors='coerce').dt.tz_localize(None)
    inventory_df['two_year_deadline'] = inventory_df['ingresa'] + pd.DateOffset(years=2)
    inventory_df['days_remaining'] = (inventory_df['two_year_deadline'] - current_date).dt.days

    # Generate timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"inventory_ITMA_CLIENT_{timestamp}.xlsx"
    output_path = os.path.join(output_dir, filename)

    # Create Excel writer with one sheet per incoterm
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for idcoflete, incoterm_group in inventory_df.groupby('idcoflete'):
            # Group by idingreso, idmodelo, idcoldis and aggregate
            grouped = incoterm_group.groupby(['idingreso', 'idmodelo', 'idcoldis'], as_index=False).agg({
                'pesokgs': 'sum',
                'idum': 'first',
                'ingresa': 'first',
                'days_remaining': 'first'
            })

            # Rename columnas for readability in Spanish
            grouped.rename(columns={
                'pesokgs': 'unidades',
                'idcoldis': 'variante',
                'idum': 'u/m',
                'ingresa': 'fecha ingreso',
                'days_remaining': 'dias libres'
            }, inplace=True)

            # Reorder columnas
            grouped = grouped[['idingreso', 'idmodelo', 'variante', 'unidades', 'u/m', 'fecha ingreso', 'dias libres']]

            # Clean up incoterm name for sheet name (Excel has 31 char limit)
            sheet_name = str(idcoflete)[:31] if idcoflete else "NO_INCOTERM"

            # Write DataFrame a sheet
            grouped.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"  Hoja agregada '{sheet_name}': {len(grouped)} ítems, {grouped['unidades'].sum():.2f} unidades totales")

    print(f"\nClient inventory Excel saved to: {output_path}")
    print(f"{'='*80}\n")

    return output_path


def generate_inventory_by_incoterm(engine, output_dir: str, client_code: str, selected_incoterms: List[str] = None) -> Optional[str]:
    """
    Generate inventory Excel filtered by specific incoterm(s).
    This allows users a view available inventory for specific incoterms a add more products.

    Args:
        engine: Database engine connection
        output_dir: Directory a save the inventory Excel file
        client_code: Client code (idcentro)
        selected_incoterms: List of incoterm codes a include (None = all)

    Returns:
        Path a the generated inventory Excel file, or None if no inventory found
    """
    print(f"\n{'='*80}")
    print("GENERACIÓN DE INVENTARIO POR INCOTERM")
    print(f"{'='*80}")
    print("Generando inventario filtrado por incoterm(s) seleccionado(s)...")
    print("Esto le permite revisar productos disponibles y agregar más al pedido.")

    # Get all idcontacto values for this client
    query = text("""
        SELECT DISTINCT i.idcontacto
        FROM ds_vfp.mobu_opl_insaldo i
        WHERE i.idstatus = '00'
        ORDER BY i.idcontacto
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        idcontacto_list = [row[0] for row in result.fetchall()]

    if not idcontacto_list:
        print("No se encontró inventario.")
        return None

    print(f"Encontrados {len(idcontacto_list)} contactos de proveedor")

    # Build incoterm filter if needed
    incoterm_filter = ""
    if selected_incoterms:
        incoterm_params = ','.join([f"'{inc}'" for inc in selected_incoterms])
        incoterm_filter = f"AND c.idcoflete IN ({incoterm_params})"
        print(f"Filtrando por incoterm(s): {', '.join(selected_incoterms)}")

    # Get inventory by incoterm
    idcontacto_params = ','.join([f"'{contact}'" for contact in idcontacto_list])

    inventory_query = text(f"""
        SELECT
            c.idcoflete,
            i.idingreso,
            i.idmodelo,
            i.idcoldis,
            i.pesokgs,
            m.idum,
            i.ingresa
        FROM ds_vfp.mobu_opl_insaldo i
        JOIN ds_vfp.mobu_opl_cohd c ON i.retnum = c.retnum
        LEFT JOIN ds_vfp.mobu_opl_inmodelo m ON i.idmodelo = m.idmodelo
        WHERE i.idcontacto IN ({idcontacto_params})
            AND i.idstatus = '00'
            {incoterm_filter}
        ORDER BY c.idcoflete, i.idingreso, i.idmodelo, i.idcoldis
    """)

    with engine.connect() as conn:
        inventory_df = pd.read_sql(inventory_query, conn)

    if inventory_df.empty:
        print("No se encontró inventario disponible para incoterm(s) seleccionado(s).")
        return None

    # Calculate days remaining in 2-year tax period
    current_date = pd.Timestamp.now().tz_localize(None)
    inventory_df['ingresa'] = pd.to_datetime(inventory_df['ingresa'], errors='coerce').dt.tz_localize(None)
    inventory_df['two_year_deadline'] = inventory_df['ingresa'] + pd.DateOffset(years=2)
    inventory_df['days_remaining'] = (inventory_df['two_year_deadline'] - current_date).dt.days

    # Generate timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    incoterm_suffix = '_'.join(selected_incoterms) if selected_incoterms else 'ALL'
    filename = f"inventory_{incoterm_suffix}_{timestamp}.xlsx"
    output_path = os.path.join(output_dir, filename)

    # Create Excel writer with one sheet per incoterm
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for idcoflete, incoterm_group in inventory_df.groupby('idcoflete'):
            # Group by idingreso, idmodelo, idcoldis and aggregate
            grouped = incoterm_group.groupby(['idingreso', 'idmodelo', 'idcoldis'], as_index=False).agg({
                'pesokgs': 'sum',
                'idum': 'first',
                'ingresa': 'first',
                'days_remaining': 'first'
            })

            # Rename columnas for readability in Spanish
            grouped.rename(columns={
                'pesokgs': 'unidades',
                'idcoldis': 'variante',
                'idum': 'u/m',
                'ingresa': 'fecha ingreso',
                'days_remaining': 'dias libres'
            }, inplace=True)

            # Reorder columnas
            grouped = grouped[['idingreso', 'idmodelo', 'variante', 'unidades', 'u/m', 'fecha ingreso', 'dias libres']]

            # Clean up incoterm name for sheet name (Excel has 31 char limit)
            sheet_name = str(idcoflete)[:31] if idcoflete else "NO_INCOTERM"

            # Write DataFrame a sheet
            grouped.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"  Hoja agregada '{sheet_name}': {len(grouped)} ítems, {grouped['unidades'].sum():.2f} unidades totales")

    print(f"\nInventory Excel saved to: {output_path}")
    print(f"{'='*80}\n")

    return output_path


def generate_dbf_order(result_df: pd.DataFrame, source_filename: str, dbf_output_dir: str = None, engine=None, selected_incoterms: List[str] = None) -> List[str]:
    """
    Generate DBF file from the results, with individual items (not grouped).

    New DBF Structure:
    1. numero - C(25): Order number (YYYYMMDD-HHMMSS-client)
    2. itemline - C(5): Item line number (left blank)
    3. idproducto - C(15): Producto ID (idingreso + itemno)
    4. idcontacto - C(6): Contact/Supplier ID
    5. idmodelo - C(20): Model ID
    6. idcoldis - C(20): Color/Description ID
    7. idubica - C(6): Location ID
    8. idreceiver - C(4): Receiver ID (empty)
    9. cantidad - N(10,2): Quantity in kg
    10. fecrec - D: Reception date (empty)
    11. scanned - L: Scanned flag (empty)
    12. available - L: Available flag
    13. ingresa - D: Entry date
    14. modifica - D: Modification date
    15. usuario - C(4): User ID
    16. equipo - C(4): Equipment ID
    17. idubica1 - C(6): Secondary location ID (from insaldo)
    18. client - C(10): Client code (idcentro)
    19. idcoflete - C(6): Incoterm code

    Args:
        result_df: DataFrame with selected items
        source_filename: Original Excel filename (without extension) for Numero field
        dbf_output_dir: Directory a save DBF files (defaults a same as Excel output)
        engine: Database engine connection (needed a fetch client codes)
        selected_incoterms: List of incoterm codes a generate DBF for (None = all)

    Returns:
        List of generated incoterm codes
    """
    if result_df.empty:
        print("No hay datos para generar archivos DBF")
        return []

    generated_incoterms = []

    # Get current datetime for order creation timestamps
    current_datetime = datetime.now()

    # Group by incoterm a create separate orders
    for idcoflete, incoterm_group in result_df.groupby('order_idcoflete'):
        # Skip if this incoterm is not in the selected list
        if selected_incoterms is not None and idcoflete not in selected_incoterms:
            print(f"Omitiendo incoterm {idcoflete} (no seleccionado)")
            continue
        # Get the first idcontacto from this group a fetch the client code
        first_idcontacto = incoterm_group['idcontacto'].iloc[0]

        # Fetch client code (idcentro)
        if engine:
            client_code = get_client_code(engine, first_idcontacto)
        else:
            print("  ADVERTENCIA: No se proporcionó motor de base de datos, usando código de cliente predeterminado '0002'")
            client_code = '0002'

        # Generate numero: YYYYMMDD-HHMMSS-client
        # Format: 20250123-143025-0002
        numero = current_datetime.strftime(f"%Y%m%d-%H%M%S-{client_code}")

        print(f"\nGenerating DBF for incoterm {idcoflete}")
        print(f"  Numero: {numero}")
        print(f"  Código de cliente: {client_code}")

        # Create DBF filename: numero_idcoflete.dbf
        dbf_filename = f"{numero}_{idcoflete}.dbf"
        dbf_path = os.path.join(dbf_output_dir, dbf_filename) if dbf_output_dir else dbf_filename

        # Define DBF structure with explicit field types, widths, and decimal places
        # Format: 'FIELDNAME C(width)' for Character, 'FIELDNAME N(width,decimals)' for Numeric,
        #         'FIELDNAME L' for Logical, 'FIELDNAME D' for Date
        # Note: DateTime fields use D (Date) type in standard DBF
        table_structure = [
            'numero C(25)',      # Order number (YYYYMMDD-HHMMSS-client)
            'itemline C(5)',     # Item line number
            'idproducto C(15)',  # Producto ID
            'idcontacto C(6)',   # Contact/Supplier ID
            'idmodelo C(20)',    # Model ID
            'idcoldis C(20)',    # Color/Description ID
            'idubica C(6)',      # Location ID
            'idreceiver C(4)',   # Receiver ID
            'cantidad N(10,2)',  # Quantity (Numeric with 2 decimals)
            'fecrec D',          # Reception date (Date)
            'scanned L',         # Scanned flag (Logical)
            'available L',       # Available flag (Logical)
            'ingresa D',         # Entry date
            'modifica D',        # Modification date
            'usuario C(4)',      # User ID
            'equipo C(4)',       # Equipment ID
            'idubica1 C(6)',     # Secondary location ID
            'client C(10)',      # Client code (idcentro)
            'idcoflete C(6)'     # Incoterm code
        ]

        # Create the DBF table with the defined structure
        table = dbf.Table(dbf_path, '; '.join(table_structure))

        # Open the table for writing
        table.open(mode=dbf.READ_WRITE)

        # Process each individual item (no grouping)
        for _, row in incoterm_group.iterrows():
            # Combine idingreso + itemno for idproducto (max 15 chars)
            idproducto = f"{row['idingreso']}{row['itemno']}"[:15]

            # Get idubica1 from the row (should be in insaldo data)
            idubica1 = str(row.get('idubica1', ''))[:6] if pd.notna(row.get('idubica1')) else ''

            # Debug: print individual row data
            print(f"    Ítem DBF: numero={numero}, idproducto={idproducto}, cantidad={row['pesokgs']:.2f}, client={client_code}, idcoflete={idcoflete}")

            # Add record a DBF table
            # Empty D (Date) fields should be None, empty L (Logical) should be False or None
            table.append({
                'numero': numero[:25],  # YYYYMMDD-HHMMSS-client format
                'itemline': '',
                'idproducto': idproducto[:15],
                'idcontacto': str(row['idcontacto'])[:6],
                'idmodelo': str(row['idmodelo'])[:20],
                'idcoldis': str(row['idcoldis'])[:20],
                'idubica': str(row['idubica'])[:6],
                'idreceiver': '',
                'cantidad': float(row['pesokgs']),
                'fecrec': None,  # Empty date
                'scanned': False,  # Empty logical
                'available': True,  # True logical value
                'ingresa': current_datetime.date(),  # Date only
                'modifica': current_datetime.date(),  # Date only
                'usuario': '1000',
                'equipo': '0001',
                'idubica1': idubica1[:6],
                'client': client_code[:10],  # Client code (idcentro)
                'idcoflete': str(idcoflete)[:6]  # Incoterm code
            })

        # Close the table a save changes
        table.close()

        print(f"Archivo DBF guardado: {dbf_path} ({len(incoterm_group)} items)")
        generated_incoterms.append(idcoflete)

    return generated_incoterms


def build_import_order(excel_path: str, output_path: str = None, generate_dbf: bool = True) -> Tuple[pd.DataFrame, bool]:
    """
    Main function a build the import order.

    Args:
        excel_path: Path a the Excel file with order requirements
        output_path: Optional path a save the output Excel file
        generate_dbf: Whether a generate DBF files (default: True)

    Returns:
        Tuple of (DataFrame with all selected ítems, bool indicating if DBF files were generated)
    """
    dbf_generated = False  # Track whether DBF generation actually happened
    # Extract filename from excel_path for DBF generation
    source_filename = os.path.splitext(os.path.basename(excel_path))[0]
    # Read order requirements
    orders_df = read_excel_orders(excel_path)

    # Get database connection
    engine = get_db_connection()

    all_selected_items = []
    failed_orders = []  # Track orders with discrepancies/errors
    products_needing_review = []  # Track products that need user review (deferred prompts)

    # Process each order line
    for row_idx, order_row in orders_df.iterrows():
        sup = order_row['sup']
        model = order_row['model']
        coldes = order_row['coldes']
        qty_needed = order_row['atrersemfix']

        # Convert index a int for display (iterrows returns Hashable index)
        order_num = int(row_idx) + 1 if isinstance(row_idx, (int, float)) else row_idx + 1

        print(f"Procesando pedido {order_num}/{len(orders_df)}: sup={sup}, model={model}, coldes={coldes}, qty={qty_needed}")

        try:
            # First, check ALL available inventory across ALL incoterms for this exact product
            all_inventory = get_all_available_inventory_for_product(engine, sup, model, coldes)

            if not all_inventory.empty:
                # Producto exists! Show all incoterm options
                print(f"  ✓ Producto encontrado en {len(all_inventory)} incoterm(s):")
                for idx, inv_row in all_inventory.iterrows():
                    print(f"    - {inv_row['idcoflete']}: {inv_row['total_available_kg']:.2f} kg disponibles (idingreso más antiguo: {inv_row['oldest_idingreso']})")

                # FIFO PRINCIPLE: Use OLDEST idingreso first, regardless of incoterm
                # Sort by oldest idingreso a respect FIFO
                all_inventory_sorted = all_inventory.sort_values('oldest_idingreso_num')

                # Get ítems de ALL incoterms, sorted by idingreso (FIFO)
                # We'll select across incoterms if needed
                all_available_items = []
                incoterms_used = []

                for _, inv_row in all_inventory_sorted.iterrows():
                    incoterm = inv_row['idcoflete']
                    items = get_available_items(engine, sup, model, coldes, incoterm)
                    if not items.empty:
                        all_available_items.append(items)
                        incoterms_used.append(incoterm)

                if all_available_items:
                    # Combine all items and sort by FIFO (idingreso, ingresa)
                    available_items = pd.concat(all_available_items, ignore_index=True)
                    available_items = available_items.sort_values(
                        by=['idingreso', 'ingresa', 'itemno'],
                        key=lambda x: pd.to_numeric(x, errors='coerce') if x.name == 'idingreso' else x
                    )

                    total_available = available_items['pesokgs'].sum()
                    matched_coldes = coldes
                    product_found = True
                    quantity_sufficient = total_available >= qty_needed

                    # Determine which incoterm(s) will be used
                    oldest_idingreso = available_items.iloc[0]['idingreso']
                    oldest_incoterm = available_items.iloc[0]['order_idcoflete'] if 'order_idcoflete' in available_items.columns else None

                    # Get the incoterm from cohd for the oldest item
                    oldest_retnum = available_items.iloc[0]['retnum']
                    query_incoterm = text("SELECT idcoflete FROM ds_vfp.mobu_opl_cohd WHERE retnum = :retnum")
                    with engine.connect() as conn:
                        result = conn.execute(query_incoterm, {'retnum': oldest_retnum})
                        row = result.fetchone()
                        idcoflete = row[0] if row else incoterms_used[0]

                    print(f"  → Siguiendo FIFO: Comenzando desde idingreso más antiguo {oldest_idingreso} (incoterm: {idcoflete})")

                    if len(set(incoterms_used)) > 1 and not quantity_sufficient:
                        print(f"  ℹ Múltiples incoterms disponibles - se seleccionará del más antiguo primero, complementando si es necesario")

                    if quantity_sufficient:
                        print(f"  → Total disponible: {total_available:.2f} kg (necesarios: {qty_needed:.2f} kg)")
                    else:
                        shortage = qty_needed - total_available
                        print(f"  ⚠ Cantidad total insuficiente: {total_available:.2f} kg (necesarios: {qty_needed:.2f} kg, faltante: {shortage:.2f} kg)")
                else:
                    # No items found (shouldn't happen if all_inventory not empty)
                    print(f"  ERROR: El inventario existe pero no se pudieron recuperar ítems")
                    available_items = pd.DataFrame()
                    matched_coldes = coldes
                    product_found = False
                    quantity_sufficient = False
                    total_available = 0.0
                    idcoflete = all_inventory.iloc[0]['idcoflete']

            else:
                # No exact match - try fuzzy matching on coldes
                print(f"  No se encontró coincidencia exacta, intentando coincidencia difusa de color...")
                idcoflete, matched_coldes = get_incoterm_for_product(engine, sup, model, coldes)

                if matched_coldes != coldes:
                    print(f"  Incoterm encontrado: {idcoflete} (usando coldes con coincidencia difusa: '{matched_coldes}')")
                else:
                    print(f"  Incoterm encontrado: {idcoflete}")

                # Get available items with matching incoterm (use the matched coldes value)
                available_items = get_available_items(engine, sup, model, matched_coldes, idcoflete)
                product_found = not available_items.empty
                total_available = available_items['pesokgs'].sum() if product_found else 0.0
                quantity_sufficient = total_available >= qty_needed if product_found else False

            # Handle cases: product not found or insufficient quantity
            if not product_found or not quantity_sufficient:
                # Determine error type
                if not product_found:
                    error_msg = 'No se encontraron ítems disponibles'
                    print(f"  ADVERTENCIA: {error_msg}")
                else:
                    shortage = qty_needed - total_available
                    error_msg = f'Cantidad insuficiente (disponible: {total_available:.2f} kg, necesarios: {qty_needed:.2f} kg, faltante: {shortage:.2f} kg)'
                    print(f"  ADVERTENCIA: {error_msg}")

                # Search for similar products
                print(f"  Buscando productos similares...")
                similar_products = find_similar_products(engine, sup, model, coldes)

                if not similar_products.empty:
                    print(f"  → Encontrados {len(similar_products)} producto(s) similar(es) - se solicitará selección al final")

                    # DEFER PROMPT: Add a products_needing_review instead of prompting now
                    products_needing_review.append({
                        'order_num': order_num,
                        'sup': sup,
                        'model': model,
                        'coldes': coldes,
                        'qty_needed': qty_needed,
                        'available_qty': total_available if product_found else 0.0,
                        'error_type': 'insufficient' if product_found else 'not_found',
                        'error_msg': error_msg,
                        'similar_products': similar_products,
                        'available_items': available_items if product_found else pd.DataFrame()
                    })
                    continue
                else:
                    print(f"  ✗ No se encontraron productos similares")
                    failed_orders.append({
                        'order_num': order_num,
                        'sup': sup,
                        'model': model,
                        'coldes': coldes,
                        'qty_needed': qty_needed,
                        'error': error_msg,
                        'alternatives': 0
                    })
                    continue

            # Select items a fulfill the order (or partial if insufficient)
            selected_items = select_items_for_order(available_items, qty_needed)

            if not selected_items.empty:
                # Validate FIFO compliance
                is_fifo_compliant = validate_fifo_compliance(selected_items)
                if not is_fifo_compliant:
                    print(f"  ADVERTENCIA: ¡Los ítems seleccionados pueden no cumplir con FIFO!")

                # Add order reference columnas
                selected_items['order_sup'] = sup
                selected_items['order_model'] = model
                selected_items['order_coldes'] = coldes
                selected_items['order_qty_needed'] = qty_needed
                # Use the ACTUAL incoterm from source_idcoflete instead of overwriting
                selected_items['order_idcoflete'] = selected_items['source_idcoflete']

                # Log FIFO details with greater visibility
                unique_ingresos = selected_items['idingreso'].unique()
                unique_incoterms = selected_items['source_idcoflete'].unique()
                actual_qty = selected_items['pesokgs'].sum()
                num_items = len(selected_items)

                print(f"\n  {'─'*76}")
                print(f"  ✓ PEDIDO CUMPLIDO")
                print(f"  {'─'*76}")
                print(f"    Ítems agregados: {num_items} items")
                print(f"    Peso agregado: {actual_qty:.2f} kg (necesarios: {qty_needed:.2f} kg)")
                print(f"    De {len(unique_ingresos)} idingreso(s): {', '.join(map(str, unique_ingresos))}")

                # Show incoterm breakdown if multiple incoterms used
                if len(unique_incoterms) > 1:
                    print(f"    ⚠ CUMPLIMIENTO MULTI-INCOTERM: Usando {len(unique_incoterms)} incoterm(s)")
                    for inc in unique_incoterms:
                        inc_items = selected_items[selected_items['source_idcoflete'] == inc]
                        inc_qty = inc_items['pesokgs'].sum()
                        inc_ingresos = inc_items['idingreso'].unique()
                        print(f"      - {inc}: {inc_qty:.2f} kg de idingreso(s) {', '.join(map(str, inc_ingresos))}")
                else:
                    print(f"    Incoterm: {unique_incoterms[0]}")

                print(f"  {'─'*76}\n")

                # Warn if partial fulfillment
                if actual_qty < qty_needed:
                    shortage = qty_needed - actual_qty
                    print(f"  ⚠ CUMPLIMIENTO PARCIAL: Faltante de {shortage:.2f} kg")

                all_selected_items.append(selected_items)
            else:
                print(f"  ADVERTENCIA: No se pudieron seleccionar ítems para este pedido")
                failed_orders.append({
                    'order_num': order_num,
                    'sup': sup,
                    'model': model,
                    'coldes': coldes,
                    'qty_needed': qty_needed,
                    'error': 'No se pudieron seleccionar ítems',
                    'alternatives': 0
                })

        except Exception as e:
            print(f"  ERROR procesando pedido: {str(e)}")
            failed_orders.append({
                'order_num': order_num,
                'sup': sup,
                'model': model,
                'coldes': coldes,
                'qty_needed': qty_needed,
                'error': str(e),
                'alternatives': 0
            })
            continue

    # DEFERRED PROMPTS: Handle products needing review (after all orders processed)
    if products_needing_review:
        print(f"\n{'='*80}")
        print(f"PRODUCTOS QUE REQUIEREN REVISIÓN: {len(products_needing_review)} producto(s) requieren su atención")
        print(f"{'='*80}")
        print("Los siguientes productos tuvieron problemas durante el procesamiento. Por favor revise las")
        print("alternativas similares y seleccione cuáles usar.\n")

        for idx, product_info in enumerate(products_needing_review, 1):
            order_num = product_info['order_num']
            sup = product_info['sup']
            model = product_info['model']
            coldes = product_info['coldes']
            qty_needed = product_info['qty_needed']
            available_qty = product_info['available_qty']
            error_type = product_info['error_type']
            error_msg = product_info['error_msg']
            similar_products = product_info['similar_products']

            print(f"\n{'─'*76}")
            print(f"Producto {idx}/{len(products_needing_review)}: Pedido #{order_num}")
            print(f"{'─'*76}")
            print(f"  Original: sup={sup}, model={model}, coldes={coldes}, cant_necesaria={qty_needed} kg")
            print(f"  Problema: {error_msg}")
            print(f"\n  PRODUCTOS SIMILARES ENCONTRADOS: {len(similar_products)} alternativa(s)")
            print(f"  {'─'*76}")

            # Display top 5 similar products
            for alt_idx, (_, alt_product) in enumerate(similar_products.head(5).iterrows(), 1):
                print(f"  [{alt_idx}] Modelo: {alt_product['idmodelo']} | Color: {alt_product['idcoldis']} | "
                      f"Incoterm: {alt_product['idcoflete']}")
                print(f"      Disponible: {alt_product['total_available_kg']:.2f} kg | "
                      f"Model similarity: {alt_product['model_similarity']:.1%} | "
                      f"Color similarity: {alt_product['color_similarity']:.1%}")

            # Display available options
            print(f"\n  {'─'*76}")
            print(f"  OPCIONES DISPONIBLES:")
            print(f"  {'─'*76}")
            if available_qty > 0:
                print(f"  [p] PARCIAL - Importar solo los {available_qty:.2f} kg disponibles (cumplimiento parcial)")
            print(f"  [1-{min(5, len(similar_products))}] COMPLEMENTAR - Importar {available_qty:.2f} kg disponibles + alternativa seleccionada")
            print(f"  [n] CANCELAR - Omitir este producto completamente (no importar nada)")
            print(f"  {'─'*76}")

            # Prompt user a select option
            response = input(f"\n  >> Su elección: ").strip().lower()

            # Handle PARTIAL import option
            if response == 'p' and available_qty > 0:
                print(f"  ✓ Importando cantidad parcial: {available_qty:.2f} kg")

                # Get available items for the original product
                available_items = product_info['available_items']

                if not available_items.empty:
                    # Select items for partial fulfillment
                    selected_items = select_items_for_order(available_items, available_qty)

                    if not selected_items.empty:
                        # Validate FIFO compliance
                        is_fifo_compliant = validate_fifo_compliance(selected_items)
                        if not is_fifo_compliant:
                            print(f"  ADVERTENCIA: ¡Los ítems seleccionados pueden no cumplir con FIFO!")

                        # Add order reference columnas
                        selected_items['order_sup'] = sup
                        selected_items['order_model'] = model
                        selected_items['order_coldes'] = coldes
                        selected_items['order_qty_needed'] = qty_needed
                        selected_items['order_idcoflete'] = selected_items['source_idcoflete']

                        # Log details
                        unique_ingresos = selected_items['idingreso'].unique()
                        actual_qty = selected_items['pesokgs'].sum()
                        num_items = len(selected_items)

                        print(f"\n  {'─'*76}")
                        print(f"  ✓ PEDIDO PARCIAL IMPORTADO")
                        print(f"  {'─'*76}")
                        print(f"    Ítems agregados: {num_items} items")
                        print(f"    Peso agregado: {actual_qty:.2f} kg (necesarios: {qty_needed:.2f} kg)")
                        print(f"    Faltante: {qty_needed - actual_qty:.2f} kg")
                        print(f"    De {len(unique_ingresos)} idingreso(s): {', '.join(map(str, unique_ingresos))}")
                        print(f"  {'─'*76}\n")

                        all_selected_items.append(selected_items)
                    else:
                        print(f"  ✗ No se pudieron seleccionar ítems para importación parcial")
                        failed_orders.append({
                            'order_num': order_num,
                            'sup': sup,
                            'model': model,
                            'coldes': coldes,
                            'qty_needed': qty_needed,
                            'error': f'{error_msg} (importación parcial falló)',
                            'alternatives': len(similar_products)
                        })
                else:
                    print(f"  ✗ No hay ítems disponibles para importar")
                    failed_orders.append({
                        'order_num': order_num,
                        'sup': sup,
                        'model': model,
                        'coldes': coldes,
                        'qty_needed': qty_needed,
                        'error': error_msg,
                        'alternatives': len(similar_products)
                    })

            # Handle COMPLEMENT with alternative option
            elif response.isdigit() and 1 <= int(response) <= min(5, len(similar_products)):
                selected_idx = int(response) - 1
                alt_product = similar_products.iloc[selected_idx]

                # Use the alternative product a complement
                alt_model = alt_product['idmodelo']
                alt_coldes = alt_product['idcoldis']
                alt_idcoflete = alt_product['idcoflete']

                print(f"  ✓ Complementando con alternativa: model={alt_model}, coldes={alt_coldes}, incoterm={alt_idcoflete}")

                complement_items = []
                total_imported_qty = 0.0

                # Step 1: Import available quantity from original product (if any)
                if available_qty > 0:
                    print(f"  → Importando {available_qty:.2f} kg disponibles del producto original...")
                    available_items = product_info['available_items']

                    if not available_items.empty:
                        original_selected = select_items_for_order(available_items, available_qty)

                        if not original_selected.empty:
                            # Add order reference columnas
                            original_selected['order_sup'] = sup
                            original_selected['order_model'] = model
                            original_selected['order_coldes'] = coldes
                            original_selected['order_qty_needed'] = qty_needed
                            original_selected['order_idcoflete'] = original_selected['source_idcoflete']

                            original_qty = original_selected['pesokgs'].sum()
                            total_imported_qty += original_qty
                            complement_items.append(original_selected)
                            print(f"    ✓ Agregados {len(original_selected)} items ({original_qty:.2f} kg) del original")

                # Step 2: Calculate remaining quantity needed
                remaining_qty = qty_needed - total_imported_qty
                print(f"  → Cantidad restante necesaria: {remaining_qty:.2f} kg")

                # Step 3: Import from alternative a complement
                if remaining_qty > 0:
                    print(f"  → Importando {remaining_qty:.2f} kg de la alternativa...")
                    alt_available_items = get_available_items(engine, sup, alt_model, alt_coldes, alt_idcoflete)
                    alt_total_available = alt_available_items['pesokgs'].sum()

                    if alt_total_available < remaining_qty:
                        print(f"    ⚠ La alternativa tiene cantidad insuficiente ({alt_total_available:.2f} kg < {remaining_qty:.2f} kg)")
                        print(f"    Se usarán todos los {alt_total_available:.2f} kg disponibles de la alternativa")

                    # Select ítems de alternative product
                    alt_selected = select_items_for_order(alt_available_items, remaining_qty)

                    if not alt_selected.empty:
                        # Add order reference columnas
                        alt_selected['order_sup'] = sup
                        alt_selected['order_model'] = model
                        alt_selected['order_coldes'] = coldes
                        alt_selected['order_qty_needed'] = qty_needed
                        alt_selected['order_idcoflete'] = alt_selected['source_idcoflete']

                        alt_qty = alt_selected['pesokgs'].sum()
                        total_imported_qty += alt_qty
                        complement_items.append(alt_selected)
                        print(f"    ✓ Agregados {len(alt_selected)} items ({alt_qty:.2f} kg) de la alternativa")

                # Combine all complement items
                if complement_items:
                    combined_items = pd.concat(complement_items, ignore_index=True)

                    # Validate FIFO compliance
                    is_fifo_compliant = validate_fifo_compliance(combined_items)
                    if not is_fifo_compliant:
                        print(f"  ADVERTENCIA: ¡Los ítems seleccionados pueden no cumplir con FIFO!")

                    # Log summary
                    unique_ingresos = combined_items['idingreso'].unique()
                    num_items = len(combined_items)

                    print(f"\n  {'─'*76}")
                    print(f"  ✓ PEDIDO COMPLEMENTADO")
                    print(f"  {'─'*76}")
                    print(f"    Ítems agregados: {num_items} items")
                    print(f"    Peso agregado: {total_imported_qty:.2f} kg (necesarios: {qty_needed:.2f} kg)")
                    print(f"    De {len(unique_ingresos)} idingreso(s): {', '.join(map(str, unique_ingresos))}")
                    print(f"  {'─'*76}\n")

                    # Warn if still short
                    if total_imported_qty < qty_needed:
                        shortage = qty_needed - total_imported_qty
                        print(f"  ⚠ CUMPLIMIENTO PARCIAL: Faltante de {shortage:.2f} kg")

                    all_selected_items.append(combined_items)
                else:
                    print(f"  ✗ No se pudieron seleccionar ítems from alternative")
                    failed_orders.append({
                        'order_num': order_num,
                        'sup': sup,
                        'model': model,
                        'coldes': coldes,
                        'qty_needed': qty_needed,
                        'error': f'{error_msg} (complemento falló)',
                        'alternatives': len(similar_products)
                    })

            # Handle CANCEL option
            elif response == 'n':
                print(f"  ✗ Producto cancelado por usuario")
                failed_orders.append({
                    'order_num': order_num,
                    'sup': sup,
                    'model': model,
                    'coldes': coldes,
                    'qty_needed': qty_needed,
                    'error': f'{error_msg} (cancelado por usuario)',
                    'alternatives': len(similar_products)
                })

            # Handle invalid input
            else:
                print(f"  ✗ Opción inválida seleccionada")
                failed_orders.append({
                    'order_num': order_num,
                    'sup': sup,
                    'model': model,
                    'coldes': coldes,
                    'qty_needed': qty_needed,
                    'error': f'{error_msg} (opción inválida)',
                    'alternatives': len(similar_products)
                })

        print(f"\n{'='*80}")
        print(f"REVISIÓN COMPLETA: Procesados {len(products_needing_review)} producto(s)")
        print(f"{'='*80}\n")

        # Re-combine all selected items if any were added during review
        if all_selected_items:
            # This will be used later when generating the result_df
            pass

    # Report on failed orders if any
    if failed_orders:
        print(f"\n{'='*80}")
        print(f"RESUMEN DE PEDIDOS FALLIDOS: {len(failed_orders)} pedido(s) no pudieron ser procesados")
        print(f"{'='*80}")
        for failed in failed_orders:
            print(f"  Pedido #{failed['order_num']}: sup={failed['sup']}, model={failed['model']}, "
                  f"coldes={failed['coldes']}, qty={failed['qty_needed']} kg")
            print(f"    Error: {failed['error']}")
            if 'alternatives' in failed and failed['alternatives'] > 0:
                print(f"    Nota: {failed['alternatives']} producto(s) similar(es) fueron encontrados pero no seleccionados")
        print(f"{'='*80}\n")

    # Combine all selected items
    if all_selected_items:
        result_df = pd.concat(all_selected_items, ignore_index=True)

        # Save filtered Excel output
        if output_path:
            # Group by incoterm, idmodelo, idcoldis, and idingreso for easier review
            grouped_df = result_df.groupby(['order_idcoflete', 'idmodelo', 'idcoldis', 'idingreso']).agg({
                'pesokgs': 'sum',  # Total weight
                'itemno': 'count'  # Count of items
            }).reset_index()

            # Rename columnas for clarity
            grouped_df.columns = ['incoterm', 'idmodelo', 'idcoldis', 'idingreso', 'total_pesokgs', 'item_count']

            # Sort by incoterm, idmodelo, idcoldis, and idingreso
            grouped_df = grouped_df.sort_values(
                by=['incoterm', 'idmodelo', 'idcoldis', 'idingreso'],
                ascending=True
            )

            grouped_df.to_excel(output_path, index=False)
            print(f"\nExcel de verificacion guardado en: {output_path}")
            print(f"Por favor revise los detalles del pedido antes de continuar.")

            # Generate DBF files if requested
            if generate_dbf:
                # Display order summary and incoterms
                print(f"\n{'='*80}")
                print("VERIFICACIÓN DE PEDIDO REQUERIDA")
                print(f"{'='*80}")
                print(f"Procesados exitosamente: {len(all_selected_items)} producto(s)")
                if failed_orders:
                    print(f"Falló el procesamiento: {len(failed_orders)} producto(s) (ver resumen arriba)")
                print(f"\nTotal items a ser importados: {len(result_df)}")
                print(f"Peso total: {result_df['pesokgs'].sum():.2f} kg")
                print(f"\nExcel de verificacion: {output_path}")

                # Display incoterms in the order
                available_incoterms = sorted(result_df['order_idcoflete'].unique().tolist())
                print(f"\n{'='*80}")
                print(f"DESGLOSE POR INCOTERM ({len(available_incoterms)} incoterm(s) en este pedido)")
                print(f"{'='*80}")
                for i, incoterm in enumerate(available_incoterms, 1):
                    incoterm_data = result_df[result_df['order_idcoflete'] == incoterm]
                    item_count = len(incoterm_data)
                    total_weight = incoterm_data['pesokgs'].sum()
                    print(f"  [{i}] {incoterm}: {item_count} ítems, {total_weight:.2f} kg")

                print(f"\n{'='*80}")
                print("OPCIONES DE GENERACIÓN DBF")
                print(f"{'='*80}")
                print("  [1] Importar TODOS los incoterms")
                print("  [2] Importar ALGUNOS incoterms (seleccionará cuáles)")
                print("  [3] DESCARGAR inventario para incoterm(s) específico(s) (para agregar más productos)")
                print("  [4] CANCELAR (no generar archivos DBF)")
                print(f"{'='*80}\n")

                dbf_choice = input(">> Su elección (1/2/3/4): ").strip()

                if dbf_choice in ['1', '2']:
                    output_dir = os.path.dirname(output_path)

                    # Handle user's choice
                    if dbf_choice == '1':
                        # Import ALL incoterms
                        print(f"\n✓ Generating DBF files for ALL {len(available_incoterms)} incoterm(s)...")
                        generated = generate_dbf_order(result_df, source_filename, output_dir, engine, selected_incoterms=None)
                        if generated:
                            dbf_generated = True
                            print(f"\n✓ Successfully generated DBF files for {len(generated)} incoterm(s): {', '.join(generated)}")

                    elif dbf_choice == '2':
                        # Import SOME incoterms - ask which ones
                        print(f"\n{'='*80}")
                        print("SELECCIONAR INCOTERMS A IMPORTAR")
                        print(f"{'='*80}")

                        # Re-display the incoterm list for easy reference
                        for i, incoterm in enumerate(available_incoterms, 1):
                            incoterm_data = result_df[result_df['order_idcoflete'] == incoterm]
                            item_count = len(incoterm_data)
                            total_weight = incoterm_data['pesokgs'].sum()
                            print(f"  [{i}] {incoterm}: {item_count} ítems, {total_weight:.2f} kg")

                        # Add "import all" option if there are more than 2 incoterms
                        if len(available_incoterms) == 2:
                            print(f"  [3] Importar TODOS (ambos incoterms)")
                            print(f"\nEnter your choice (1, 2, or 3):\n")
                        else:
                            print(f"\nEnter the number(s) of the incoterm(s) you want a import")
                            print(f"(comma-separated for multiple, e.g., '1,2,3')\n")

                        incoterm_response = input(">> Su selección: ").strip()

                        # Handle special case: option 3 for "import all" when there are exactly 2 incoterms
                        if len(available_incoterms) == 2 and incoterm_response == '3':
                            print(f"\n✓ Generating DBF files for ALL incoterms...")
                            generated = generate_dbf_order(result_df, source_filename, output_dir, engine, selected_incoterms=None)
                            if generated:
                                dbf_generated = True
                                print(f"\n✓ Successfully generated DBF files for {len(generated)} incoterm(s): {', '.join(generated)}")
                        else:
                            # Parse comma-separated numbers
                            try:
                                selected_indices = [int(x.strip()) for x in incoterm_response.split(',')]
                                selected_incoterms = []

                                for idx in selected_indices:
                                    if 1 <= idx <= len(available_incoterms):
                                        selected_incoterms.append(available_incoterms[idx - 1])
                                    else:
                                        print(f"Advertencia: Índice inválido {idx}, omitiendo...")

                                if selected_incoterms:
                                    print(f"\n✓ Generating DBF files for {len(selected_incoterms)} incoterm(s) seleccionado(s): {', '.join(selected_incoterms)}")
                                    generated = generate_dbf_order(result_df, source_filename, output_dir, engine, selected_incoterms=selected_incoterms)
                                    if generated:
                                        dbf_generated = True
                                        print(f"\n✓ Successfully generated DBF files for {len(generated)} incoterm(s): {', '.join(generated)}")

                                        # Show which incoterms were NOT generated
                                        skipped_incoterms = [inc for inc in available_incoterms if inc not in generated]
                                        if skipped_incoterms:
                                            print(f"  Nota: {len(skipped_incoterms)} incoterm(s) no generados: {', '.join(skipped_incoterms)}")
                                else:
                                    print("\nNo valid incoterms selected. Generación DBF cancelada.")

                            except ValueError:
                                print(f"\nInvalid input format: '{incoterm_response}'")
                                print("Por favor use números separados por comas (ej., '1,2,3')")
                                print("Generación DBF cancelada.")

                elif dbf_choice == '3':
                    # Download inventory for specific incoterm(s)
                    output_dir = os.path.dirname(output_path)

                    # Determine client code
                    first_idcontacto = result_df['idcontacto'].iloc[0]
                    client_code = get_client_code(engine, first_idcontacto)

                    print(f"\n{'='*80}")
                    print("SELECCIONAR INCOTERM(S) PARA DESCARGA DE INVENTARIO")
                    print(f"{'='*80}")
                    print("¿Para cuál(es) incoterm(s) desea descargar inventario?")
                    print(f"{'─'*80}")

                    # Display the incoterm list
                    for i, incoterm in enumerate(available_incoterms, 1):
                        incoterm_data = result_df[result_df['order_idcoflete'] == incoterm]
                        item_count = len(incoterm_data)
                        total_weight = incoterm_data['pesokgs'].sum()
                        print(f"  [{i}] {incoterm}: {item_count} ítems, {total_weight:.2f} kg (actualmente en pedido)")

                    if len(available_incoterms) > 1:
                        print(f"  [{len(available_incoterms) + 1}] TODOS los incoterms (descargar todos)")

                    print(f"\nEnter the number(s) of the incoterm(s) you want inventory for")
                    print(f"(comma-separated for multiple, e.g., '1,2')\n")

                    inventory_response = input(">> Su selección: ").strip()

                    # Handle "download all" option
                    if inventory_response == str(len(available_incoterms) + 1):
                        print(f"\n✓ Downloading inventory for ALL {len(available_incoterms)} incoterm(s)...")
                        inventory_path = generate_inventory_by_incoterm(engine, output_dir, client_code, selected_incoterms=None)

                        if inventory_path:
                            print(f"\n{'='*80}")
                            print(f"INVENTARIO DESCARGADO")
                            print(f"{'='*80}")
                            print(f"Archivo de inventario: {inventory_path}")
                            print(f"Ahora puede revisar este archivo y agregar más productos a su pedido.")
                            print(f"Después de actualizar el pedido, vuelva a ejecutar este script para generar archivos DBF.")
                            print(f"{'='*80}\n")
                    else:
                        # Parse comma-separated numbers
                        try:
                            selected_indices = [int(x.strip()) for x in inventory_response.split(',')]
                            selected_inventory_incoterms = []

                            for idx in selected_indices:
                                if 1 <= idx <= len(available_incoterms):
                                    selected_inventory_incoterms.append(available_incoterms[idx - 1])
                                else:
                                    print(f"Advertencia: Índice inválido {idx}, omitiendo...")

                            if selected_inventory_incoterms:
                                print(f"\n✓ Downloading inventory for {len(selected_inventory_incoterms)} incoterm(s): {', '.join(selected_inventory_incoterms)}...")
                                inventory_path = generate_inventory_by_incoterm(engine, output_dir, client_code, selected_incoterms=selected_inventory_incoterms)

                                if inventory_path:
                                    print(f"\n{'='*80}")
                                    print(f"INVENTARIO DESCARGADO")
                                    print(f"{'='*80}")
                                    print(f"Archivo de inventario: {inventory_path}")
                                    print(f"Ahora puede revisar este archivo y agregar más productos a su pedido.")
                                    print(f"Después de actualizar el pedido, vuelva a ejecutar este script para generar archivos DBF.")
                                    print(f"{'='*80}\n")
                            else:
                                print("\nNo valid incoterms selected. Descarga de inventario cancelada.")

                        except ValueError:
                            print(f"\nInvalid input format: '{inventory_response}'")
                            print("Please use comma-separated numbers (e.g., '1,2')")
                            print("Descarga de inventario cancelada.")

                elif dbf_choice == '4':
                    print("\nCreacion de tablas ,DBF cancelada por el usuario.")
                    print("Puede revisar el archivo Excel y ejecutar el script nuevamente cuando esté listo.")
                else:
                    print("\nOpcion invalida seleccionada.")
                    print("Puede revisar el archivo Excel y ejecutar el script nuevamente cuando esté listo.")

        return result_df, dbf_generated
    else:
        print("\nNo items were selected")
        if failed_orders:
            print(f"Todos los {len(failed_orders)} pedido(s) fallaron en el procesamiento. Ver errores arriba.")
        return pd.DataFrame(), dbf_generated


if __name__ == "__main__":
    # Use file picker a select Excel file
    print("Por favor seleccione el archivo de pedido Excel...")
    excel_file = select_excel_file()

    if not excel_file:
        print("No se seleccionó archivo. Saliendo.")
        exit(0)

    print(f"Archivo seleccionado: {excel_file}")

    # Generate output path in same directory as input file
    output_dir = os.path.dirname(excel_file)
    filename_base = os.path.splitext(os.path.basename(excel_file))[0]
    output_excel_path = os.path.join(output_dir, f"{filename_base}_output.xlsx")

    # Build the import order
    result, dbf_generated = build_import_order(
        excel_path=excel_file,
        output_path=output_excel_path,
        generate_dbf=True
    )

    print(f"\nTotal de ítems seleccionados: {len(result)}")
    if not result.empty:
        print(f"Peso total: {result['pesokgs'].sum():.2f} kg")
        print(f"\n✓ ¡Procesamiento completo!")
        print(f"  - Salida Excel: {output_excel_path}")
        if dbf_generated:
            print(f"  - Archivos DBF: {output_dir}/{{filename}}_{{incoterm}}.dbf")
        else:
            print(f"  - Archivos DBF: No generados (usuario canceló o confirmación de cliente pendiente)")
    else:
        print("\n✗ No se seleccionaron ítems")