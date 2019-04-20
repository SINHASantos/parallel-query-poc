use std::fs;
use std::fs::File;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use datafusion::datasource::datasource::RecordBatchIterator;
use datafusion::datasource::parquet::ParquetFile;
use datafusion::error::{ExecutionError, Result};

use crate::execution::{ExecutionPlan, ThreadSafeRecordBatchIterator};

/// Execution plan for reading partitioned parquet files
pub struct ParquetExec {
    /// Directory containing parquet files with identical schemas (one file = one partition)
    dir: String,
}

impl ParquetExec {
    pub fn new(filename: &str) -> Self {
        Self {
            dir: filename.to_string(),
        }
    }
}

impl ExecutionPlan for ParquetExec {
    fn execute(&self) -> Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>> {
        let mut parquet_partitions: Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>> = vec![];
        for entry in fs::read_dir(&self.dir).unwrap() {
            let entry = entry.unwrap();
            let filename = format!("{}/{}", &self.dir, entry.file_name().to_str().unwrap());
            println!("{}", filename);
            let parquet_channel = ParquetChannel::open(&filename);
            parquet_partitions.push(Arc::new(Mutex::new(parquet_channel)));
        }
        parquet_partitions
    }
}

/// Because we can't send ParquetFile between threads currently, we need to create the ParquetFile
/// in it's own thread and use channels to communicate with it
pub struct ParquetChannel {
    request_tx: Sender<()>,
    response_rx: Receiver<Option<RecordBatch>>,
}

impl ParquetChannel {
    pub fn open(filename: &str) -> Self {
        let (request_tx, request_rx): (Sender<()>, Receiver<()>) = mpsc::channel();
        let (response_tx, response_rx): (
            Sender<Option<RecordBatch>>,
            Receiver<Option<RecordBatch>>,
        ) = mpsc::channel();

        let filename = filename.to_string();

        thread::spawn(move || {
            let file = File::open(filename).unwrap();
            let mut parquet_file = ParquetFile::open(file, None, 1024).unwrap();
            loop {
                match request_rx.recv() {
                    Ok(_) => match parquet_file.next() {
                        Ok(option) => response_tx.send(option).unwrap(),
                        Err(e) => {
                            println!("{:?}", e);
                            response_tx.send(None).unwrap();
                        }
                    },
                    _ => {
                        break;
                    }
                }
            }
        });

        ParquetChannel {
            request_tx,
            response_rx,
        }
    }
}

impl ThreadSafeRecordBatchIterator for ParquetChannel {
    fn schema(&self) -> &Arc<Schema> {
        unimplemented!()
    }

    fn next(&self) -> Result<Option<RecordBatch>> {
        self.request_tx.send(()).unwrap();
        match self.response_rx.recv() {
            Ok(option) => Ok(option),
            Err(_) => Err(ExecutionError::General(format!("TBD"))),
        }
    }
}
