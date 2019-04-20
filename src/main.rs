
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;

use std::fs::{self, File};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use datafusion::datasource::parquet::ParquetFile;
use datafusion::datasource::datasource::RecordBatchIterator;
use datafusion::error::Result;

fn main() {

    let dir = "data";

    let mut parquet_partitions : Vec<Arc<Mutex<ParquetChannel>>> = vec![];
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let filename = format!("{}/{}", dir, entry.file_name().to_str().unwrap());
        println!("{}", filename);
        let parquet_channel = ParquetChannel::open(&filename);
        parquet_partitions.push(Arc::new(Mutex::new(parquet_channel)));
    }

    let mut part0 = parquet_partitions[0].lock().unwrap();
    let batch = part0.next().unwrap().unwrap();
    println!("rows = {}", batch.num_rows());

}

/// Because we can't send ParquetFile between threads currently, we need to create the ParquetFile
/// in it's own thread and use channels to communicate with it
struct ParquetChannel {
    request_tx: Sender<()>,
    response_rx: Receiver<RecordBatch>
}

impl ParquetChannel {

    pub fn open(filename: &str) -> Self {

        let (request_tx, request_rx): (Sender<()>, Receiver<()>) = mpsc::channel();
        let (response_tx, response_rx): (Sender<RecordBatch>, Receiver<RecordBatch>) = mpsc::channel();

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
            request_tx, response_rx
        }

    }
}

impl RecordBatchIterator for ParquetChannel {

    fn schema(&self) -> &Arc<Schema> {
        unimplemented!()
    }

    fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.request_tx.send(()).unwrap();
        Ok(Some(self.response_rx.recv().unwrap()))
    }
}
