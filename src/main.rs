use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use std::fs::{self, File};

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use datafusion::datasource::datasource::RecordBatchIterator;
use datafusion::datasource::parquet::ParquetFile;
use datafusion::error::Result;
use arrow::builder::Int32Builder;

trait Func: Send + Sync {
    fn execute(&self, batch: &RecordBatch) -> Result<ArrayRef>;
}

struct FilterFunc {
}
impl Func for FilterFunc {
    fn execute(&self, _batch: &RecordBatch) -> Result<ArrayRef> {
        //TODO implement
        Ok(Arc::new(Int32Builder::new(0).finish()) as ArrayRef)
    }
}

fn main() {
    // create execution plan to read parquet partitions
    let parquet_exec = ParquetExec::new("data");

    // create execution plan to apply a selection
    let predicate = FilterFunc {};
    let filter_exec = FilterExec::new(Arc::new(parquet_exec), Arc::new(predicate));

    // execute the top level plan with one thread per partition
    let filter_partitions = filter_exec.execute();
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

/// An execution plan produces one iterator over record batches per partition
trait ExecutionPlan {
    fn execute(&self) -> Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>>;
}

/// Iterator for reading a series of record batches with a known schema
pub trait ThreadSafeRecordBatchIterator: Send {
    /// Get the schema of this iterator
    fn schema(&self) -> &Arc<Schema>;

    /// Get the next batch in this iterator
    fn next(&self) -> Result<Option<RecordBatch>>;
}

/// Execution plan for reading partitioned parquet files
struct ParquetExec {
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

/// Selection e.g. apply predicate to filter rows from the record batches
struct FilterExec {
    input: Arc<ExecutionPlan>,
    predicate: Arc<Func>,
}

impl FilterExec {
    pub fn new(input: Arc<ExecutionPlan>, predicate: Arc<Func>) -> Self {
        Self { input, predicate }
    }
}

impl ExecutionPlan for FilterExec {
    fn execute(&self) -> Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>> {
        let predicate = self.predicate.clone();
        self.input
            .execute()
            .iter()
            .map(move |p| {
                Arc::new(Mutex::new(FilterPartition { input: p.clone(), predicate: predicate.clone() }))
                    as Arc<Mutex<ThreadSafeRecordBatchIterator>>
            })
            .collect::<Vec<Arc<Mutex<ThreadSafeRecordBatchIterator>>>>()
    }
}

struct FilterPartition {
    input: Arc<Mutex<ThreadSafeRecordBatchIterator>>,
    predicate: Arc<Func>,
}

impl ThreadSafeRecordBatchIterator for FilterPartition {
    fn schema(&self) -> &Arc<Schema> {
        unimplemented!()
    }

    fn next(&self) -> Result<Option<RecordBatch>> {
        match self.input.lock().unwrap().next()? {
            Some(batch) => {
                println!("Filtering batch");
                self.predicate.execute(&batch)?;
                Ok(Some(batch))
            }
            None => Ok(None)
        }
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

impl ThreadSafeRecordBatchIterator for ParquetChannel {
    fn schema(&self) -> &Arc<Schema> {
        unimplemented!()
    }

    fn next(&self) -> Result<Option<RecordBatch>> {
        self.request_tx.send(()).unwrap();
        Ok(Some(self.response_rx.recv().unwrap()))
    }
}
