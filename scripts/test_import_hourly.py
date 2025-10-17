"""
Test importing hourly BTC data from CSV before DB import
"""
import pandas as pd
from datetime import datetime

# Load CSV
csv_path = '/tmp/btc_hourly.csv'
print(f"Loading {csv_path}...")

df = pd.read_csv(csv_path)
print(f"\n✓ Loaded {len(df):,} rows")
print(f"\nColumns: {list(df.columns)}")
print(f"\nFirst 5 rows:")
print(df.head())
print(f"\nLast 5 rows:")
print(df.tail())

# Check for required columns
required = ['TIME_UNIX', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'VOLUME_FROM']
missing = [col for col in required if col not in df.columns]
if missing:
    print(f"\n❌ Missing columns: {missing}")
    exit(1)

# Convert timestamp
df['timestamp'] = pd.to_datetime(df['TIME_UNIX'], unit='s', utc=True)
print(f"\n✓ Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

# Validate prices
print(f"\n✓ Price range: ${df['CLOSE_PRICE'].min():,.2f} to ${df['CLOSE_PRICE'].max():,.2f}")
print(f"✓ Total volume: {df['VOLUME_FROM'].sum():,.2f} BTC")

# Check for duplicates
dupes = df.duplicated(subset=['TIME_UNIX']).sum()
print(f"\n✓ Duplicates: {dupes}")

# Show sample data for DB insert
print(f"\n✓ Sample row for DB insert:")
sample = df.iloc[0]
print(f"  Time: {sample['timestamp']}")
print(f"  Open: {sample['OPEN_PRICE']}")
print(f"  High: {sample['HIGH_PRICE']}")
print(f"  Low: {sample['LOW_PRICE']}")
print(f"  Close: {sample['CLOSE_PRICE']}")
print(f"  Volume: {sample['VOLUME_FROM']}")

# Test resampling to 4h
print(f"\n✓ Testing 4h resample...")
df_4h = df.set_index('timestamp').resample('4h').agg({
    'OPEN_PRICE': 'first',
    'HIGH_PRICE': 'max',
    'LOW_PRICE': 'min',
    'CLOSE_PRICE': 'last',
    'VOLUME_FROM': 'sum'
}).dropna()
print(f"  4h bars: {len(df_4h):,}")
print(f"  4h date range: {df_4h.index.min()} to {df_4h.index.max()}")

print("\n✅ CSV looks good! Ready to import to DB")


