use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use calamine::{Reader, Xlsb, open_workbook};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

struct ConvertExcelToParquetOptions<'a> {
    excel_file: &'a Path,
    output_path: &'a Path,
    skip_rows: usize,
    batch_size: usize,
    sheet_name: Option<String>,
}

/// 将单元格值转为字符串
fn cell_to_string(cell: &calamine::DataRef) -> String {
    match cell {
        calamine::DataRef::Int(i) => i.to_string(),
        calamine::DataRef::Float(f) => f.to_string(),
        calamine::DataRef::String(s) => s.clone(),
        calamine::DataRef::SharedString(s) => s.to_string(),
        calamine::DataRef::Bool(b) => b.to_string(),
        calamine::DataRef::DateTime(dt) => dt.to_string(),
        calamine::DataRef::DurationIso(d) => d.to_string(),
        calamine::DataRef::DateTimeIso(dt) => dt.to_string(),
        calamine::DataRef::Error(e) => format!("{:?}", e),
        calamine::DataRef::Empty => String::new(),
    }
}

fn convert_excel_to_parquet(options: ConvertExcelToParquetOptions) -> Result<()> {
    println!("Starting conversion for: {}", options.excel_file.display());

    // Open Excel file (使用 Xlsb 类型打开 xlsb 文件)
    let mut workbook: Xlsb<_> =
        open_workbook(options.excel_file).context("Failed to open Excel file")?;

    // Get the first worksheet name
    let sheet_name = if let Some(sheet_name) = options.sheet_name {
        sheet_name
    } else {
        workbook
            .sheet_names()
            .first()
            .context("No worksheets found")?
            .clone()
    };
    println!("Processing sheet: {}", sheet_name);

    // 使用流式读取器
    let mut cells_reader = workbook
        .worksheet_cells_reader(&sheet_name)
        .context("Failed to get worksheet cells reader")?;

    // 获取维度信息
    let dimensions = cells_reader.dimensions();
    let num_cols = (dimensions.end.1 - dimensions.start.1 + 1) as usize;
    let start_col = dimensions.start.1;
    let header_row_idx = dimensions.start.0 + options.skip_rows as u32;
    println!(
        "Sheet dimensions: rows {}-{}, cols {}-{}",
        dimensions.start.0, dimensions.end.0, dimensions.start.1, dimensions.end.1
    );

    // 状态变量
    let mut current_row_cells: HashMap<u32, String> = HashMap::new();
    let mut current_row: Option<u32> = None;
    let mut batch_buffer: Vec<Vec<Option<String>>> = Vec::with_capacity(options.batch_size);
    let mut writer: Option<ArrowWriter<File>> = None;
    let mut schema: Option<Arc<Schema>> = None;
    let mut headers: Vec<String> = Vec::new();
    let mut total_rows: usize = 0;

    // 写入一个批次的辅助闭包
    let write_batch = |writer: &mut ArrowWriter<File>,
                       schema: &Arc<Schema>,
                       headers: &[String],
                       batch: &[Vec<Option<String>>]|
     -> Result<()> {
        let mut columns: Vec<ArrayRef> = Vec::new();
        for col_idx in 0..headers.len() {
            let values: Vec<Option<String>> = batch
                .iter()
                .map(|row| row.get(col_idx).and_then(|v| v.clone()))
                .collect();
            let string_array = StringArray::from(values);
            columns.push(Arc::new(string_array));
        }
        let record_batch = RecordBatch::try_new(schema.clone(), columns)
            .context("Failed to create record batch")?;
        writer
            .write(&record_batch)
            .context("Failed to write record batch")?;
        Ok(())
    };

    // 流式读取单元格，边读边写
    while let Some(cell) = cells_reader.next_cell().context("Failed to read cell")? {
        let (row, col) = cell.get_position();
        let value = cell_to_string(cell.get_value());

        // 检查是否进入新行
        if current_row.is_none() {
            current_row = Some(row);
        } else if current_row != Some(row) {
            let prev_row = current_row.unwrap();

            if prev_row == header_row_idx {
                // 处理表头行：构建 headers 并创建 writer
                headers = build_headers(&current_row_cells, num_cols, start_col);
                println!("Found headers: {} columns", headers.len());

                // Create Arrow schema
                let s = Arc::new(Schema::new(
                    headers
                        .iter()
                        .map(|name| Field::new(name, DataType::Utf8, true))
                        .collect::<Vec<Field>>(),
                ));
                schema = Some(s.clone());

                // Set up Parquet writer
                let props = WriterProperties::builder()
                    .set_compression(Compression::BROTLI(BrotliLevel::default()))
                    .set_max_row_group_size(options.batch_size) // 关键：设置 RowGroup 大小，每满 batch_size 就刷盘
                    .build();
                let file =
                    File::create(options.output_path).context("Failed to create output file")?;
                writer = Some(
                    ArrowWriter::try_new(file, s, Some(props))
                        .context("Failed to create parquet writer")?,
                );
            } else if prev_row > header_row_idx {
                // 数据行：添加到批次缓冲区
                let row_vec = build_row_from_cells(&current_row_cells, num_cols, start_col);
                batch_buffer.push(row_vec);
                total_rows += 1;

                // 如果批次满了，立即写入
                if batch_buffer.len() >= options.batch_size {
                    if let (Some(w), Some(s)) = (writer.as_mut(), schema.as_ref()) {
                        write_batch(w, s, &headers, &batch_buffer)?;
                        println!(
                            "Written batch of {} rows (total: {})",
                            batch_buffer.len(),
                            total_rows
                        );
                        batch_buffer.clear();
                    }
                }
            }

            current_row_cells.clear();
            current_row = Some(row);
        }

        current_row_cells.insert(col, value);
    }

    // 处理最后一行
    if let Some(row) = current_row {
        if row == header_row_idx {
            // 表头是最后一行（只有表头没有数据）
            headers = build_headers(&current_row_cells, num_cols, start_col);
            let s = Arc::new(Schema::new(
                headers
                    .iter()
                    .map(|name| Field::new(name, DataType::Utf8, true))
                    .collect::<Vec<Field>>(),
            ));
            schema = Some(s.clone());
            let props = WriterProperties::builder()
                .set_compression(Compression::BROTLI(BrotliLevel::default()))
                .set_max_row_group_size(options.batch_size) // 关键：设置 RowGroup 大小
                .build();
            let file = File::create(options.output_path).context("Failed to create output file")?;
            writer = Some(
                ArrowWriter::try_new(file, s, Some(props))
                    .context("Failed to create parquet writer")?,
            );
        } else if row > header_row_idx {
            let row_vec = build_row_from_cells(&current_row_cells, num_cols, start_col);
            batch_buffer.push(row_vec);
            total_rows += 1;
        }
    }

    // 写入剩余的数据
    if !batch_buffer.is_empty() {
        if let (Some(w), Some(s)) = (writer.as_mut(), schema.as_ref()) {
            write_batch(w, s, &headers, &batch_buffer)?;
            println!("Written final batch of {} rows", batch_buffer.len());
        }
    }

    // Close the writer
    if let Some(w) = writer {
        w.close().context("Failed to close writer")?;
    }

    println!(
        "Successfully converted {} to {} ({} rows)",
        options.excel_file.to_string_lossy(),
        options.output_path.to_string_lossy(),
        total_rows
    );

    Ok(())
}

/// 构建表头，处理空表头和重复表头
fn build_headers(cells: &HashMap<u32, String>, num_cols: usize, start_col: u32) -> Vec<String> {
    let mut headers: Vec<String> = (0..num_cols)
        .map(|i| {
            let col = start_col + i as u32;
            cells.get(&col).cloned().unwrap_or_default()
        })
        .collect();

    // Handle empty headers
    for (i, header) in headers.iter_mut().enumerate() {
        if header.is_empty() {
            *header = format!("Field_{}", i);
        }
    }

    // Handle duplicate headers
    let mut seen: HashMap<String, i32> = HashMap::new();
    for i in 0..headers.len() {
        let header = &headers[i];
        let count = seen.entry(header.clone()).or_insert(0);
        *count += 1;
        if *count > 1 {
            headers[i] = format!("{}_{}", header, count);
        }
    }

    headers
}

/// 从单元格映射构建行数据
fn build_row_from_cells(
    cells: &HashMap<u32, String>,
    num_cols: usize,
    start_col: u32,
) -> Vec<Option<String>> {
    (0..num_cols)
        .map(|i| {
            let col = start_col + i as u32;
            cells.get(&col).map(|s| s.clone())
        })
        .collect()
}

fn main() {
    if let Err(e) = convert_excel_to_parquet(ConvertExcelToParquetOptions {
        excel_file: Path::new("./data/sample.xlsb"),
        output_path: Path::new("./data/data.parquet"),
        skip_rows: 0,
        batch_size: 10000,
        sheet_name: None,
    }) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
