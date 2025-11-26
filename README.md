# Data to Parquet Converter

A high-performance Rust tool for converting Excel files (`.xlsx` and `.xlsb`) to Parquet format.

## Features

-   **Format Support**: Handles both `.xlsx` (Excel Open XML) and `.xlsb` (Excel Binary) formats.
-   **High Performance**: Utilizes multi-threading for parallel processing of cell data.
-   **Memory Efficient**: Streaming reader implementation to handle large files without loading everything into memory.
-   **Parquet Compression**: Uses ZSTD compression for efficient storage.
-   **Flexible Configuration**: CLI options for batch size, row skipping, and sheet selection.

## Installation

Ensure you have Rust installed. Then build the project:

```bash
cargo build --release
```

The executable will be located in `target/release/data-to-parquet`.

## Usage

Run the tool using the command line interface:

```bash
# Basic usage
cargo run --release -- -i <input_file> -o <output_file>

# Example for XLSX
cargo run --release -- -i ./data/sample.xlsx -o ./data/output.parquet

# Example for XLSB
cargo run --release -- -i ./data/sample.xlsb -o ./data/output.parquet
```

### Options

| Option          | Short | Description                                   | Default      |
| --------------- | ----- | --------------------------------------------- | ------------ |
| `--input`       | `-i`  | Input Excel file path (`.xlsx` or `.xlsb`)    | **Required** |
| `--output`      | `-o`  | Output Parquet file path                      | **Required** |
| `--sheet-name`  |       | Specific sheet name to process                | First sheet  |
| `--sheet-index` |       | Specific sheet index to process (starts at 0) | 0            |
| `--skip-rows`   |       | Number of rows to skip at the beginning       | 0            |
| `--batch-size`  |       | Number of rows per batch for processing       | 5000         |

### Examples

**Convert a specific sheet by name:**

```bash
cargo run --release -- -i data.xlsx -o data.parquet --sheet-name "Sheet2"
```

**Convert the second sheet (index 1):**

```bash
cargo run --release -- -i data.xlsx -o data.parquet --sheet-index 1
```

**Skip the first header row:**

```bash
cargo run --release -- -i data.xlsx -o data.parquet --skip-rows 1
```

## Performance Notes

-   The tool automatically detects the number of logical cores and spawns worker threads accordingly.
-   Batch size can be tuned for performance. Larger batches may increase memory usage but reduce I/O overhead.
-   ZSTD compression is enabled by default for the output Parquet file.

## License

MIT
