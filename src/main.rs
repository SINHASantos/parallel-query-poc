use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use std::fs::{self, File};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use datafusion::datasource::datasource::RecordBatchIterator;
use datafusion::datasource::parquet::ParquetFile;
use datafusion::error::Result;

fn main() {
    // create execution plan to read parquet partitions
    let parquet_exec = ParquetExec::new("data");
    let parquet_partitions = parquet_exec.execute();

    // create excution plan to apply a selection
    let filter_exec = FilterExec::new(parquet_partitions, "id > 123".to_string());
    let filter_partitions = filter_exec.execute();

    // execute the top level plan with one thread per partition
    let mut handles = vec![];
    for partition in &filter_partitions {
        let partition = partition.clone();
        handles.push(thread::spawn(move || {
            println!("Starting thread");
            let partition = partition.lock().unwrap();
            let batch = partition.next().unwrap().unwrap();
            println!("rows = {}", batch.num_rows());
        }));
    }

    // wait for threads to finish
    for handle in handles {
        handle.join().unwrap();
    }
}

trait ExecutionPlan {
    fn execute(&self) -> Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>>;
}

struct ParquetExec {
    filename: String,
}

impl ParquetExec {
    pub fn new(filename: &str) -> Self {
        Self {
            filename: filename.to_string(),
        }
    }
}

impl ExecutionPlan for ParquetExec {
    fn execute(&self) -> Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>> {
        let mut parquet_partitions: Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>> = vec![];
        for entry in fs::read_dir(&self.filename).unwrap() {
            let entry = entry.unwrap();
            let filename = format!("{}/{}", &self.filename, entry.file_name().to_str().unwrap());
            println!("{}", filename);
            let parquet_channel = ParquetChannel::open(&filename);
            parquet_partitions.push(Arc::new(Mutex::new(parquet_channel)));
        }
        parquet_partitions
    }
}

/// Selection e.g. apply predicate to filter rows from the record batches
struct FilterExec {
    input: Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>>,
    predicate: String,
}

impl FilterExec {
    pub fn new(input: Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>>, predicate: String) -> Self {
        Self { input, predicate }
    }
}

impl ExecutionPlan for FilterExec {
    fn execute(&self) -> Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>> {
        self.input
            .iter()
            .map(|p| {
                Arc::new(Mutex::new(FilterPartition { input: p.clone() }))
                    as Arc<Mutex<ThreadSafeRecordBatchIterator>>
            })
            .collect::<Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>>>()
    }
}

struct FilterPartition {
    input: Arc<Mutex<ThreadSafeRecordBatchIterator>>,
}

impl ThreadSafeRecordBatchIterator for FilterPartition {
    fn schema(&self) -> &Arc<Schema> {
        unimplemented!()
    }

    fn next(&self) -> Result<Option<RecordBatch>> {
        let batch = self.input.lock().unwrap().next();
        println!("Filtering batch");
        batch
    }
}

/// Because we can't send ParquetFile between threads currently, we need to create the ParquetFile
/// in it's own thread and use channels to communicate with it
struct ParquetChannel {
    request_tx: Sender<()>,
    response_rx: Receiver<RecordBatch>,
}

impl ParquetChannel {
    pub fn open(filename: &str) -> Self {
        let (request_tx, request_rx): (Sender<()>, Receiver<()>) = mpsc::channel();
        let (response_tx, response_rx): (Sender<RecordBatch>, Receiver<RecordBatch>) =
            mpsc::channel();

        let filename = filename.to_string();

        thread::spawn(move || {
            let file = File::open(filename).unwrap();
            let mut parquet_file = ParquetFile::open(file, None, 1024).unwrap();
            loop {
                request_rx.recv().unwrap();

                let batch = parquet_file.next().unwrap().unwrap();

                response_tx.send(batch).unwrap();
            }
        });

        ParquetChannel {
            request_tx,
            response_rx,
        }
    }
}

/// Iterator for reading a series of record batches with a known schema
pub trait ThreadSafeRecordBatchIterator: Send {
    /// Get the schema of this iterator
    fn schema(&self) -> &Arc<Schema>;

    /// Get the next batch in this iterator
    fn next(&self) -> Result<Option<RecordBatch>>;
}

impl ThreadSafeRecordBatchIterator for ParquetChannel {
    fn schema(&self) -> &Arc<Schema> {
        unimplemented!()
    }

    fn next(&self) -> Result<Option<RecordBatch>> {
        self.request_tx.send(()).unwrap();
        Ok(Some(self.response_rx.recv().unwrap()))
    }
}
