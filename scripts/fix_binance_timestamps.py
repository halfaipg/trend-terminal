"""
Fix Binance CSV where Unix timestamps are formatted as 1970 dates
The timestamp 1500000000000 (ms) is displayed as "1970-01-18 09:28:48"
"""
import pandas as pd
import sys

def fix_timestamps(input_file, output_file=None):
    """Fix corrupted timestamps in Binance CSV"""
    
    if output_file is None:
        output_file = input_file.replace('.csv', '_fixed.csv')
    
    print(f"Loading {input_file}...")
    df = pd.read_csv(input_file)
    
    print(f"✓ Loaded {len(df):,} rows")
    print(f"\nFirst 3 dates (raw):")
    print(df['Date'].head(3))
    
    # Parse the dates
    df['Date'] = pd.to_datetime(df['Date'], format='mixed', utc=True)
    
    # Check for 1970 dates (corrupted)
    mask_1970 = df['Date'].dt.year == 1970
    num_corrupted = mask_1970.sum()
    
    if num_corrupted > 0:
        print(f"\n⚠️  Found {num_corrupted:,} rows with 1970 dates")
        print("Converting from Unix timestamp milliseconds...")
        
        # For 1970 dates, extract the timestamp and convert from milliseconds
        # "1970-01-18 09:28:48" means (18-1)*86400 + 9*3600 + 28*60 + 48 seconds from epoch
        # This is actually the timestamp in MILLISECONDS stored as a date
        for idx in df[mask_1970].index:
            dt_1970 = df.loc[idx, 'Date']
            # Calculate seconds from 1970-01-01
            seconds_from_epoch = (dt_1970 - pd.Timestamp("1970-01-01", tz='UTC')).total_seconds()
            # This value is actually milliseconds, so convert
            correct_timestamp_ms = int(seconds_from_epoch * 1000)
            # Now convert back to proper datetime
            df.loc[idx, 'Date'] = pd.to_datetime(correct_timestamp_ms, unit='ms', utc=True)
        
        print(f"✓ Fixed {num_corrupted:,} timestamps")
    
    print(f"\nDate range: {df['Date'].min()} to {df['Date'].max()}")
    print(f"\nFirst 5 dates (fixed):")
    print(df['Date'].head())
    print(f"\nLast 5 dates:")
    print(df['Date'].tail())
    
    # Save
    print(f"\nSaving to {output_file}...")
    df.to_csv(output_file, index=False)
    print(f"✅ Done! Saved {len(df):,} rows")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python fix_binance_timestamps.py <input.csv> [output.csv]")
        sys.exit(1)
    
    fix_timestamps(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)


