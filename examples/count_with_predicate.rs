use std::sync::Arc;
use std::thread;

use parallel_query_poc::execution::*;
use parallel_query_poc::filter_exec::*;
use parallel_query_poc::parquet_exec::*;

/// This example is simulating a SELECT COUNT(*) FROM data WHERE condition
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
            let mut count = 0;
            loop {
                match partition.next() {
                    Ok(Some(batch)) => {
                        count += batch.num_rows();
                    }
                    Ok(None) => {
                        println!("Thread fetched {} rows", count);
                        return;
                    }
                    Err(e) => {
                        println!("Thread terminated due to error: {:?}", e);
                        return;
                    }
                }
            }
        }));
    }

    // wait for threads to finish
    for handle in handles {
        handle.join().unwrap();
    }
}
