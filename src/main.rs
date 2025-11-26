use clap::Parser;
use data_to_parquet::{
    ConvertExcelToParquetOptions, convert_xlsb_to_parquet, convert_xlsx_to_parquet,
};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input Excel file path (.xlsx or .xlsb)
    #[arg(short, long)]
    input: PathBuf,

    /// Output Parquet file path
    #[arg(short, long)]
    output: PathBuf,

    /// Sheet name to process (optional, defaults to first sheet)
    #[arg(long)]
    sheet_name: Option<String>,

    /// Sheet index to process (optional, starts from 0)
    #[arg(long)]
    sheet_index: Option<usize>,

    /// Number of rows to skip
    #[arg(long, default_value_t = 0)]
    skip_rows: usize,

    /// Batch size for processing
    #[arg(long, default_value_t = 5000)]
    batch_size: usize,
}

fn main() {
    let args = Args::parse();

    let input_path = args.input.as_path();
    let output_path = args.output.as_path();

    let options = ConvertExcelToParquetOptions {
        excel_file: input_path,
        output_path,
        skip_rows: args.skip_rows,
        batch_size: args.batch_size,
        sheet_name: args.sheet_name,
        sheet_index: args.sheet_index,
    };

    let extension = input_path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_lowercase());

    let result = match extension.as_deref() {
        Some("xlsx") => convert_xlsx_to_parquet(options),
        Some("xlsb") => convert_xlsb_to_parquet(options),
        _ => {
            eprintln!("Error: Unsupported file extension. Please use .xlsx or .xlsb");
            std::process::exit(1);
        }
    };

    if let Err(e) = result {
        eprintln!("Error converting file: {}", e);
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_convert_xlsb() {
        let input = Path::new("./data/sample.xlsb");
        let output = Path::new("./data/data.parquet");

        // 仅在文件存在时运行测试，避免 CI 报错
        if !input.exists() {
            return;
        }

        let options = ConvertExcelToParquetOptions {
            excel_file: input,
            output_path: output,
            skip_rows: 0,
            batch_size: 5000,
            sheet_name: None,
            sheet_index: None,
        };

        let result = convert_xlsb_to_parquet(options);
        assert!(result.is_ok(), "Failed to convert xlsb: {:?}", result.err());
    }

    #[test]
    fn test_convert_xlsx() {
        let input = Path::new("./data/sample.xlsx");
        let output = Path::new("./data/data_xlsx.parquet");

        // 仅在文件存在时运行测试，避免 CI 报错
        if !input.exists() {
            return;
        }

        let options = ConvertExcelToParquetOptions {
            excel_file: input,
            output_path: output,
            skip_rows: 0,
            batch_size: 5000,
            sheet_name: None,
            sheet_index: None,
        };

        let result = convert_xlsx_to_parquet(options);
        assert!(result.is_ok(), "Failed to convert xlsx: {:?}", result.err());
    }
}
