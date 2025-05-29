import pandas as pd
import os

def group_postcodes_by_state(input_path, output_xlsx_path=None):
    """
    Groups postcodes by state from a CSV and writes each state's postcodes to a separate sheet in an Excel file.
    :param input_csv_path: Path to the input CSV file (must have 'state' and 'postcode' columns)
    :param output_xlsx_path: Path to the output Excel file. If None, saves in same dir as input with default name.
    :return: The path to the created Excel file.
    """
    # Load the data
    df = pd.read_csv(input_path, dtype={'postcode': str})
    df["postcode"] = df["postcode"].str.zfill(4)

    # Output path (same directory)
    output_path = os.path.join(os.path.dirname(input_path), "grouped_postcodes_by_state.xlsx")

    # Group by state and save only 'postcode' column (with duplicates)
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for state, group in df.groupby("state"):
            postcodes = group[["postcode"]].sort_values(by="postcode")
            postcodes.to_excel(writer, sheet_name=state, index=False)

    print(f"Excel file saved to: {output_xlsx_path}")


def get_postcodes_by_state(file_path, state_abbr):
    """
    Reads the specified sheet (state abbreviation) from the given Excel file
    and returns a list of postcodes.
    :param file_path: Path to the xlsx file
    :param state_abbr: State abbreviation string (e.g., 'ACT', 'NSW')
    :return: List of postcodes (as integers)
    """
    try:
        df = pd.read_excel(file_path, sheet_name=state_abbr)
        # Assumes the postcode column is named 'postcode' (case-insensitive match)
        postcode_col = [col for col in df.columns if col.lower() == "postcode"]
        if not postcode_col:
            raise ValueError("No 'postcode' column found in the sheet.")
        postcodes = df[postcode_col[0]].dropna().astype(int).tolist()
        return postcodes
    except Exception as e:
        print(f"Error reading postcodes for state '{state_abbr}': {e}")
        return []
