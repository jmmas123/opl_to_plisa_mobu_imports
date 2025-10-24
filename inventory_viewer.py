import pandas as pd
from sqlalchemy import create_engine, text
import os
from typing import List, Dict, Any, Optional
from datetime import datetime

# Try to import tkinter, but make it optional
try:
    from tkinter import Tk, filedialog
    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False
    print("Note: tkinter not available. Directory picker dialog will be disabled.")

# Database configuration - same as import_order_builder.py
DB_USER = "jm"
DB_HOST = "127.0.0.1"
DB_PORT = "9700"
DB_NAME = "datax"


def get_db_connection():
    """Create and return database engine connection."""
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    return engine


def get_available_clients(engine) -> pd.DataFrame:
    """
    Get list of unique clients from the inventory.

    For ITMA (idcontacto starting with '0'), group all under 'INVERSIONES TEXTILES MAS, S.A. DE C.V.'
    For all other clients, use the actual descrip value from mobu_opl_incontac.

    Returns:
        DataFrame with columns: client_name, idcontacto_list (for ITMA, this will be a list of all 0-starting contacts)
    """
    query = text("""
        SELECT DISTINCT
            CASE
                WHEN i.idcontacto LIKE '0%' THEN 'INVERSIONES TEXTILES MAS, S.A. DE C.V.'
                ELSE COALESCE(c.descrip, i.idcontacto)
            END as client_name,
            i.idcontacto
        FROM ds_vfp.mobu_opl_insaldo i
        LEFT JOIN ds_vfp.mobu_opl_incontac c ON i.idcontacto = c.idcontacto
        WHERE i.idstatus = '00'
        ORDER BY client_name
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    # Group by client_name and collect all idcontacto values
    result = df.groupby('client_name')['idcontacto'].apply(list).reset_index()
    result.columns = ['client_name', 'idcontacto_list']

    return result


def select_client(clients_df: pd.DataFrame) -> tuple[str, List[str]]:
    """
    Display available clients and let user select one.

    Returns:
        Tuple of (client_name, list of idcontacto values for that client)
    """
    print("\nAvailable Clients:")
    print("=" * 80)

    for idx, row in clients_df.iterrows():
        client_name = row['client_name']
        idcontacto_count = len(row['idcontacto_list'])

        if client_name == 'INVERSIONES TEXTILES MAS, S.A. DE C.V.':
            print(f"{idx + 1}. {client_name} ({idcontacto_count} supplier contacts)")
        else:
            print(f"{idx + 1}. {client_name}")

    print("=" * 80)

    while True:
        try:
            choice = input("\nSelect a client (enter number): ").strip()
            choice_idx = int(choice) - 1

            if 0 <= choice_idx < len(clients_df):
                selected_row = clients_df.iloc[choice_idx]
                client_name = selected_row['client_name']
                idcontacto_list = selected_row['idcontacto_list']

                print(f"\nSelected: {client_name}")
                if client_name == 'INVERSIONES TEXTILES MAS, S.A. DE C.V.':
                    print(f"  (Including {len(idcontacto_list)} supplier contacts: {', '.join(idcontacto_list[:5])}...)")

                return (client_name, idcontacto_list)
            else:
                print(f"Invalid selection. Please enter a number between 1 and {len(clients_df)}")
        except ValueError:
            print("Invalid input. Please enter a number.")


def get_inventory_by_incoterm(engine, idcontacto_list: List[str]) -> Dict[str, pd.DataFrame]:
    """
    Get inventory grouped by incoterm (idcoflete) for the selected client.

    Args:
        engine: Database connection engine
        idcontacto_list: List of idcontacto values to query (multiple for ITMA, single for others)

    Returns:
        Dictionary mapping incoterm (idcoflete) to DataFrame with columns:
        idingreso, idmodelo, idcoldis, units, idum, ingresa, days_remaining
    """
    # Convert idcontacto_list to SQL-friendly format
    idcontacto_params = ','.join([f"'{contact}'" for contact in idcontacto_list])

    query = text(f"""
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
        df = pd.read_sql(query, conn)

    if df.empty:
        print("No inventory found for selected client.")
        return {}

    # Calculate days remaining in 2-year tax period
    current_date = pd.Timestamp.now().tz_localize(None)  # Make timezone-naive
    df['ingresa'] = pd.to_datetime(df['ingresa'], errors='coerce').dt.tz_localize(None)  # Make timezone-naive
    df['two_year_deadline'] = df['ingresa'] + pd.DateOffset(years=2)
    df['days_remaining'] = (df['two_year_deadline'] - current_date).dt.days

    # Group by incoterm
    result = {}
    for idcoflete, incoterm_group in df.groupby('idcoflete'):
        # Group by idingreso, idmodelo, idcoldis and aggregate
        grouped = incoterm_group.groupby(['idingreso', 'idmodelo', 'idcoldis'], as_index=False).agg({
            'pesokgs': 'sum',  # Sum up the weight/quantity
            'idum': 'first',  # Take first (should be same for all rows with same idmodelo)
            'ingresa': 'first',  # Take first ingresa date
            'days_remaining': 'first'  # Take first days_remaining
        })

        # Rename columns for better readability in Spanish
        grouped.rename(columns={
            'pesokgs': 'unidades',
            'idcoldis': 'variante',
            'idum': 'u/m',
            'ingresa': 'fecha ingreso',
            'days_remaining': 'dias libres'
        }, inplace=True)

        # Reorder columns for better readability
        grouped = grouped[['idingreso', 'idmodelo', 'variante', 'unidades', 'u/m', 'fecha ingreso', 'dias libres']]

        result[idcoflete] = grouped

    return result


def select_output_directory() -> Optional[str]:
    """
    Open a directory picker dialog for the user to select output location.
    Returns the selected directory path or None if cancelled.
    """
    if not TKINTER_AVAILABLE:
        print("Warning: tkinter not available. Using current directory.")
        return os.getcwd()

    root = Tk()
    root.withdraw()  # Hide the main window
    root.attributes('-topmost', True)  # Bring dialog to front

    directory = filedialog.askdirectory(
        title="Select Output Directory for Inventory Excel File"
    )

    root.destroy()

    return directory if directory else None


def generate_inventory_excel(client_name: str, inventory_by_incoterm: Dict[str, pd.DataFrame], output_dir: str = None):
    """
    Generate Excel file with one sheet per incoterm.

    Args:
        client_name: Name of the selected client
        inventory_by_incoterm: Dictionary mapping incoterm to inventory DataFrame
        output_dir: Directory to save the Excel file (defaults to current directory)
    """
    if not inventory_by_incoterm:
        print("No inventory data to generate Excel file.")
        return None

    # Create a safe filename from client name
    safe_client_name = "".join(c if c.isalnum() or c in (' ', '_') else '_' for c in client_name)
    safe_client_name = safe_client_name.replace(' ', '_')

    # Generate timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Determine output directory
    if output_dir is None:
        output_dir = os.getcwd()

    filename = f"inventory_{safe_client_name}_{timestamp}.xlsx"
    output_path = os.path.join(output_dir, filename)

    # Create Excel writer
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for incoterm, df in inventory_by_incoterm.items():
            # Clean up incoterm name for sheet name (Excel has 31 char limit)
            sheet_name = str(incoterm)[:31] if incoterm else "NO_INCOTERM"

            # Write DataFrame to sheet
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"  Added sheet '{sheet_name}': {len(df)} items, {df['unidades'].sum():.2f} total units")

    print(f"\nInventory Excel file saved to: {output_path}")
    return output_path


def main():
    """
    Main function to run the inventory viewer.
    """
    print("=" * 80)
    print("INVENTORY VIEWER")
    print("=" * 80)

    # Get database connection
    engine = get_db_connection()

    # Step 1: Get available clients
    print("\nStep 1: Loading available clients...")
    clients_df = get_available_clients(engine)
    print(f"Found {len(clients_df)} unique clients")

    # Step 2: Let user select a client
    print("\nStep 2: Select a client")
    client_name, idcontacto_list = select_client(clients_df)

    # Step 3: Get inventory by incoterm
    print(f"\nStep 3: Loading inventory for {client_name}...")
    inventory_by_incoterm = get_inventory_by_incoterm(engine, idcontacto_list)

    if not inventory_by_incoterm:
        print("No inventory found for this client.")
        return

    print(f"Found inventory across {len(inventory_by_incoterm)} incoterm(s):")
    for incoterm, df in inventory_by_incoterm.items():
        print(f"  {incoterm}: {len(df)} items, {df['unidades'].sum():.2f} total units")

    # Step 4: Select output directory
    print(f"\nStep 4: Select output directory...")
    output_dir = select_output_directory()

    if not output_dir:
        print("No directory selected. Exiting.")
        return

    print(f"Selected directory: {output_dir}")

    # Step 5: Generate Excel file
    print(f"\nStep 5: Generating Excel file...")
    output_path = generate_inventory_excel(client_name, inventory_by_incoterm, output_dir)

    if output_path:
        print("\n" + "=" * 80)
        print("COMPLETE")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("ERROR: Failed to generate Excel file")
        print("=" * 80)


if __name__ == "__main__":
    main()