use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::builder::Int32Builder;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use datafusion::error::Result;

use crate::execution::{ExecutionPlan, Func, ThreadSafeRecordBatchIterator};

/// Selection e.g. apply predicate to filter rows from the record batches
pub struct FilterExec {
    input: Arc<ExecutionPlan>,
    predicate: Arc<Func>,
}

impl FilterExec {
    pub fn new(input: Arc<ExecutionPlan>, predicate: Arc<Func>) -> Self {
        Self { input, predicate }
    }
}

impl ExecutionPlan for FilterExec {
    fn execute(&self) -> Vec<Arc<ThreadSafeRecordBatchIterator>> {
        let predicate = self.predicate.clone();
        self.input
            .execute()
            .iter()
            .map(move |p| {
                Arc::new(FilterPartition {
                    input: p.clone(),
                    predicate: predicate.clone(),
                }) as Arc<ThreadSafeRecordBatchIterator>
            })
            .collect::<Vec<Arc<ThreadSafeRecordBatchIterator>>>()
    }
}

pub struct FilterPartition {
    input: Arc<ThreadSafeRecordBatchIterator>,
    predicate: Arc<Func>,
}

impl ThreadSafeRecordBatchIterator for FilterPartition {
    fn schema(&self) -> &Arc<Schema> {
        unimplemented!()
    }

    fn next(&self) -> Result<Option<RecordBatch>> {
        match self.input.next()? {
            Some(batch) => {
                println!("Filtering batch");
                self.predicate.execute(&batch)?;
                Ok(Some(batch))
            }
            None => Ok(None),
        }
    }
}

pub struct FilterFunc {}

impl Func for FilterFunc {
    fn execute(&self, _batch: &RecordBatch) -> Result<ArrayRef> {
        //TODO implement
        Ok(Arc::new(Int32Builder::new(0).finish()) as ArrayRef)
    }
}
