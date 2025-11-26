use anyhow::{Context, Result};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use calamine::{Reader, Xlsb, Xlsx, open_workbook};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;

pub struct ConvertExcelToParquetOptions<'a> {
    pub excel_file: &'a Path,
    pub output_path: &'a Path,
    pub skip_rows: usize,
    pub batch_size: usize,
    pub sheet_name: Option<String>,
    pub sheet_index: Option<usize>,
}

// 类型定义
type RawCell = (u32, u32, String);
type RawBatch = (usize, Vec<RawCell>);
type ProcessedBatch = (usize, RecordBatch);

/// 将 Xlsx 文件转换为 Parquet
pub fn convert_xlsx_to_parquet(options: ConvertExcelToParquetOptions) -> Result<()> {
    println!(
        "Starting conversion for (XLSX): {}",
        options.excel_file.display()
    );
    let mut workbook: Xlsx<_> =
        open_workbook(options.excel_file).context("Failed to open Excel file")?;

    // Get sheet name using Reader trait
    let sheet_name = get_sheet_name(&workbook, &options)?;
    println!("Processing sheet: {}", sheet_name);

    let mut cells_reader = workbook
        .worksheet_cells_reader(&sheet_name)
        .context("Failed to get worksheet cells reader")?;

    let dimensions = cells_reader.dimensions();
    let mut context = ConversionContext::new(&options, dimensions)?;

    while let Some(cell) = cells_reader.next_cell().context("Failed to read cell")? {
        let (row, col) = cell.get_position();
        let value = cell_to_string(cell.get_value());
        context.process_cell(row, col, value)?;
    }

    context.finish()?;

    println!(
        "Successfully converted {} to {} (processed ~{} rows)",
        options.excel_file.to_string_lossy(),
        options.output_path.to_string_lossy(),
        context.total_rows
    );

    Ok(())
}

/// 将 Xlsb 文件转换为 Parquet
pub fn convert_xlsb_to_parquet(options: ConvertExcelToParquetOptions) -> Result<()> {
    println!(
        "Starting conversion for (XLSB): {}",
        options.excel_file.display()
    );
    let mut workbook: Xlsb<_> =
        open_workbook(options.excel_file).context("Failed to open Excel file")?;

    let sheet_name = get_sheet_name(&workbook, &options)?;
    println!("Processing sheet: {}", sheet_name);

    let mut cells_reader = workbook
        .worksheet_cells_reader(&sheet_name)
        .context("Failed to get worksheet cells reader")?;

    let dimensions = cells_reader.dimensions();
    let mut context = ConversionContext::new(&options, dimensions)?;

    while let Some(cell) = cells_reader.next_cell().context("Failed to read cell")? {
        let (row, col) = cell.get_position();
        let value = cell_to_string(cell.get_value());
        context.process_cell(row, col, value)?;
    }

    context.finish()?;

    println!(
        "Successfully converted {} to {} (processed ~{} rows)",
        options.excel_file.to_string_lossy(),
        options.output_path.to_string_lossy(),
        context.total_rows
    );

    Ok(())
}

// 辅助函数：获取 Sheet Name
fn get_sheet_name<R>(workbook: &R, options: &ConvertExcelToParquetOptions) -> Result<String>
where
    R: Reader<std::io::BufReader<File>>,
{
    if let Some(sheet_name) = &options.sheet_name {
        Ok(sheet_name.clone())
    } else if let Some(index) = options.sheet_index {
        workbook
            .sheet_names()
            .get(index)
            .context(format!("Sheet index {} out of bounds", index))
            .map(|s| s.clone())
    } else {
        workbook
            .sheet_names()
            .first()
            .context("No worksheets found")
            .map(|s| s.clone())
    }
}

/// 转换上下文，管理状态和线程
struct ConversionContext {
    // Config
    header_row_idx: u32,
    num_cols: usize,
    start_col: u32,
    batch_size: usize,

    // State
    current_row: Option<u32>,
    current_row_cells: HashMap<u32, String>, // Header building
    raw_cells_buffer: Vec<RawCell>,
    current_batch_rows: usize,
    batch_counter: usize,
    workers_started: bool,
    pub total_rows: usize,

    // Channels & Threads
    work_tx: Option<mpsc::SyncSender<RawBatch>>, // Option allows dropping to signal EOF
    result_tx: Option<mpsc::SyncSender<ProcessedBatch>>, // Option allows dropping
    worker_threads: Vec<thread::JoinHandle<Result<()>>>,
    writer_thread: Option<thread::JoinHandle<Result<()>>>,

    // Shared for init
    work_rx: Option<Arc<std::sync::Mutex<mpsc::Receiver<RawBatch>>>>,
    result_rx: Option<mpsc::Receiver<ProcessedBatch>>,
    output_path: PathBuf,
}

impl ConversionContext {
    fn new(
        options: &ConvertExcelToParquetOptions,
        dimensions: calamine::Dimensions,
    ) -> Result<Self> {
        let num_cols = (dimensions.end.1 - dimensions.start.1 + 1) as usize;
        let start_col = dimensions.start.1;
        let header_row_idx = dimensions.start.0 + options.skip_rows as u32;

        println!(
            "Sheet dimensions: rows {}-{}, cols {}-{}",
            dimensions.start.0, dimensions.end.0, dimensions.start.1, dimensions.end.1
        );

        let num_workers = 8;
        let (work_tx, work_rx) = mpsc::sync_channel::<RawBatch>(num_workers * 2);
        let (result_tx, result_rx) = mpsc::sync_channel::<ProcessedBatch>(num_workers * 2);

        Ok(Self {
            header_row_idx,
            num_cols,
            start_col,
            batch_size: options.batch_size,

            current_row: None,
            current_row_cells: HashMap::new(),
            raw_cells_buffer: Vec::with_capacity(options.batch_size * num_cols),
            current_batch_rows: 0,
            batch_counter: 0,
            workers_started: false,
            total_rows: 0,

            work_tx: Some(work_tx),
            result_tx: Some(result_tx),
            worker_threads: Vec::new(),
            writer_thread: None,

            work_rx: Some(Arc::new(std::sync::Mutex::new(work_rx))),
            result_rx: Some(result_rx),
            output_path: options.output_path.to_path_buf(),
        })
    }

    fn process_cell(&mut self, row: u32, col: u32, value: String) -> Result<()> {
        if !self.workers_started {
            self.handle_header_phase(row, col, value)
        } else {
            self.handle_worker_phase(row, col, value)
        }
    }

    fn handle_header_phase(&mut self, row: u32, col: u32, value: String) -> Result<()> {
        if self.current_row.is_none() {
            self.current_row = Some(row);
        } else if self.current_row != Some(row) {
            // 换行了，检查上一行是不是 header
            let prev_row = self.current_row.unwrap();

            if prev_row == self.header_row_idx {
                self.start_workers_and_writer()?;
                self.workers_started = true;
            }

            self.current_row_cells.clear();
            self.current_row = Some(row);
        }
        self.current_row_cells.insert(col, value);
        Ok(())
    }

    fn start_workers_and_writer(&mut self) -> Result<()> {
        let headers = build_headers(&self.current_row_cells, self.num_cols, self.start_col);
        println!("Found headers: {} columns", headers.len());

        let schema = Arc::new(Schema::new(
            headers
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect::<Vec<Field>>(),
        ));

        // Start Workers
        let num_workers = 8;
        let work_rx = self.work_rx.take().unwrap(); // Take the rx to share

        for _ in 0..num_workers {
            let work_rx_clone = work_rx.clone();
            let result_tx_clone = self.result_tx.as_ref().unwrap().clone();
            let schema_clone = schema.clone();
            let headers_len = headers.len();
            let start_col_val = self.start_col;

            let handle = thread::spawn(move || -> Result<()> {
                loop {
                    let msg = {
                        let lock = work_rx_clone.lock().unwrap();
                        match lock.recv() {
                            Ok(m) => m,
                            Err(_) => break,
                        }
                    };
                    let (id, cells) = msg;
                    let record_batch = create_record_batch_from_cells(
                        &schema_clone,
                        headers_len,
                        &cells,
                        start_col_val,
                    )?;
                    if result_tx_clone.send((id, record_batch)).is_err() {
                        break;
                    }
                }
                Ok(())
            });
            self.worker_threads.push(handle);
        }

        // Start Writer
        let output_path = self.output_path.clone();
        let batch_size = self.batch_size;
        let schema_clone = schema.clone();
        let result_rx = self.result_rx.take().unwrap();

        self.writer_thread = Some(thread::spawn(move || -> Result<()> {
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .set_max_row_group_size(batch_size)
                .build();

            let file = File::create(output_path).context("Failed to create output file")?;
            let mut writer = ArrowWriter::try_new(file, schema_clone, Some(props))
                .context("Failed to create parquet writer")?;

            let mut buffer: HashMap<usize, RecordBatch> = HashMap::new();
            let mut next_expected_id = 0;
            let mut total_written_rows = 0;

            while let Ok((id, batch)) = result_rx.recv() {
                if id == next_expected_id {
                    let batch_rows = batch.num_rows();
                    writer
                        .write(&batch)
                        .context("Failed to write record batch")?;
                    total_written_rows += batch_rows;
                    println!(
                        "Writer: wrote batch {} ({} rows). Total written: {}",
                        id, batch_rows, total_written_rows
                    );
                    next_expected_id += 1;

                    while let Some(next_batch) = buffer.remove(&next_expected_id) {
                        let next_rows = next_batch.num_rows();
                        writer
                            .write(&next_batch)
                            .context("Failed to write buffered batch")?;
                        total_written_rows += next_rows;
                        println!(
                            "Writer: wrote buffered batch {} ({} rows). Total written: {}",
                            next_expected_id, next_rows, total_written_rows
                        );
                        next_expected_id += 1;
                    }
                } else {
                    buffer.insert(id, batch);
                }
            }

            if !buffer.is_empty() {
                eprintln!("Warning: Writer finished with buffered batches remaining!");
            }
            writer.close()?;
            println!("Writer thread: finished.");
            Ok(())
        }));

        Ok(())
    }

    fn handle_worker_phase(&mut self, row: u32, col: u32, value: String) -> Result<()> {
        if self.current_row != Some(row) {
            self.current_batch_rows += 1;
            self.current_row = Some(row);
            self.total_rows += 1;

            if self.current_batch_rows >= self.batch_size {
                self.send_batch()?;
            }
        }
        self.raw_cells_buffer.push((row, col, value));
        Ok(())
    }

    fn send_batch(&mut self) -> Result<()> {
        if let Some(tx) = &self.work_tx {
            let buffer = std::mem::replace(
                &mut self.raw_cells_buffer,
                Vec::with_capacity(self.batch_size * self.num_cols),
            );
            if tx.send((self.batch_counter, buffer)).is_err() {
                return Err(anyhow::anyhow!("Worker channel closed"));
            }
            self.batch_counter += 1;
            self.current_batch_rows = 0;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        // Send remaining
        if !self.raw_cells_buffer.is_empty() {
            self.send_batch()?;
        }

        // Drop work_tx to signal workers to stop
        self.work_tx = None;

        // Wait for workers
        for handle in self.worker_threads.drain(..) {
            let _ = handle.join();
        }

        // Drop our result_tx copy so the writer knows when all workers are done
        self.result_tx = None;

        if let Some(handle) = self.writer_thread.take() {
            handle.join().unwrap()?;
        }
        Ok(())
    }
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

// 新的 Worker 函数：从 RawCell 构建 RecordBatch
fn create_record_batch_from_cells(
    schema: &Arc<Schema>,
    num_header_cols: usize,
    cells: &[(u32, u32, String)],
    start_col: u32,
) -> Result<RecordBatch> {
    let mut row_map: HashMap<u32, HashMap<u32, String>> = HashMap::new();
    let mut row_indices: Vec<u32> = Vec::new();

    for (r, c, v) in cells {
        let row_entry = row_map.entry(*r).or_insert_with(|| {
            row_indices.push(*r);
            HashMap::new()
        });
        row_entry.insert(*c, v.clone());
    }

    row_indices.sort_unstable();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_header_cols);

    for i in 0..num_header_cols {
        let target_col_idx = start_col + i as u32;
        let mut col_values: Vec<Option<String>> = Vec::with_capacity(row_indices.len());

        for row_idx in &row_indices {
            let val = row_map
                .get(row_idx)
                .and_then(|cols| cols.get(&target_col_idx).cloned());
            col_values.push(val);
        }

        columns.push(Arc::new(StringArray::from(col_values)));
    }

    RecordBatch::try_new(schema.clone(), columns).context("Failed to create record batch")
}

fn build_headers(cells: &HashMap<u32, String>, num_cols: usize, start_col: u32) -> Vec<String> {
    let mut headers: Vec<String> = (0..num_cols)
        .map(|i| {
            let col = start_col + i as u32;
            cells.get(&col).cloned().unwrap_or_default()
        })
        .collect();

    for (i, header) in headers.iter_mut().enumerate() {
        if header.is_empty() {
            *header = format!("Field_{}", i);
        }
    }

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
