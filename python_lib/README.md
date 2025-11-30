# data-to-parquet-bin

A Python wrapper for the high-performance Rust `data-to-parquet` tool.

## Installation

You can install this package using pip or uv:

```bash
pip install .
# or
uv pip install .
```

## Usage

```python
import asyncio
from data_to_parquet_bin import convert_to_parquet

async def main():
    try:
        await convert_to_parquet(
            input_path="data.xlsx",
            output_path="data.parquet",
            sheet_index=0,
            batch_size=10000
        )
        print("Conversion successful!")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
```
