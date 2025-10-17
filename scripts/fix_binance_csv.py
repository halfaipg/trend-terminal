"""
Fix corrupted Binance CSV timestamps
Early rows show 1970 because timestamps are in milliseconds but parsed as seconds
"""
import pandas as pd
from datetime import datetime
import sys

def fix_binance_csv(input_file, output_file=None):
    """Fix timestamps in Binance CSV"""
    
    if output_file is None:
        output_file = input_file.replace('.csv', '_fixed.csv')
    
    print(f"Loading {input_file}...")
    df = pd.read_csv(input_file)
    
    print(f"✓ Loaded {len(df):,} rows")
    print(f"\nFirst 3 dates (before fix):")
    print(df['Date'].head(3))
    
    # Parse dates with mixed format to handle inconsistent timestamps
    df['Date'] = pd.to_datetime(df['Date'], format='mixed', utc=True)
    
    # Check if we have 1970 dates (corrupted timestamps)
    min_year = df['Date'].dt.year.min()
    max_year = df['Date'].dt.year.max()
    
    print(f"\nDate range: {min_year} to {max_year}")
    
    if min_year == 1970:
        print("\n⚠️  Found 1970 dates - timestamps need fixing")
        
        # Find rows with 1970 dates
        corrupted = df['Date'].dt.year == 1970
        num_corrupted = corrupted.sum()
        
        print(f"   Corrupted rows: {num_corrupted:,}")
        
        # For 1970 dates, the timestamp is likely in milliseconds
        # We need to re-parse those specific rows
        # Get the original date strings again
        df_orig = pd.read_csv(input_file)
        
        for idx in df[corrupted].index:
            date_str = df_orig.loc[idx, 'Date']
            # Extract just the timestamp part before the timezone
            if '+' in date_str:
                timestamp_part = date_str.split('+')[0].strip()
                # Try parsing as milliseconds timestamp
                try:
                    # If it looks like "1970-01-18 09:28:48", it's actually a Unix timestamp in ms
                    # Convert "1970-01-18 09:28:48" -> extract seconds from epoch
                    dt_temp = pd.to_datetime(timestamp_part)
                    seconds_from_epoch = (dt_temp - pd.Timestamp("1970-01-01")).total_seconds()
                    # This is actually milliseconds, so treat it as such
                    correct_dt = pd.to_datetime(seconds_from_epoch * 1000, unit='ms', utc=True)
                    df.loc[idx, 'Date'] = correct_dt
                except:
                    pass
        
        print(f"\n✓ Fixed {num_corrupted:,} timestamps")
        print(f"\nFirst 3 dates (after fix):")
        print(df['Date'].head(3))
        print(f"\nNew date range: {df['Date'].min()} to {df['Date'].max()}")
    
    # Save fixed CSV
    print(f"\nSaving to {output_file}...")
    df.to_csv(output_file, index=False)
    print(f"✅ Saved {len(df):,} rows")
    
    return df

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python fix_binance_csv.py <input_file.csv> [output_file.csv]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    fix_binance_csv(input_file, output_file)

