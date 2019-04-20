use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use datafusion::error::Result;

/// An execution plan produces one iterator over record batches per partition
pub trait ExecutionPlan {
    fn execute(&self) -> Vec<Arc<ThreadSafeRecordBatchIterator>>;
}

/// Iterator for reading a series of record batches with a known schema
pub trait ThreadSafeRecordBatchIterator: Send + Sync {
    /// Get the schema of this iterator
    fn schema(&self) -> &Arc<Schema>;

    /// Get the next batch in this iterator
    fn next(&self) -> Result<Option<RecordBatch>>;
}

pub trait Func: Send + Sync {
    fn execute(&self, batch: &RecordBatch) -> Result<ArrayRef>;
}
