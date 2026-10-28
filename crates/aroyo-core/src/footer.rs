//! A Parquet footer for the row groups flushed so far, built without closing the writer.

use std::io::Write;
use std::sync::Arc;

use parquet::file::metadata::{FileMetaData, ParquetMetaData, ParquetMetaDataWriter};
use parquet::file::writer::SerializedFileWriter;

use crate::error::Result;

/// The footer bytes (metadata, its length and the magic) that make the bytes
/// written so far a complete Parquet file.
///
/// Column chunk offsets in the flushed row-group metadata are absolute file
/// offsets, so the footer is valid at the writer's current `bytes_written()`.
/// Page indexes are left out: the writer assigns their offsets only when it
/// writes the final footer, so a checkpoint footer cannot reference them.
pub fn checkpoint_footer<W: Write + Send>(file: &SerializedFileWriter<W>) -> Result<Vec<u8>> {
    let props = file.properties();
    let row_groups = file.flushed_row_groups().to_vec();
    let num_rows = row_groups.iter().map(|group| group.num_rows()).sum();
    let file_metadata = FileMetaData::new(
        props.writer_version().as_num(),
        num_rows,
        Some(props.created_by().to_string()),
        props.key_value_metadata().cloned(),
        Arc::new(file.schema_descr().clone()),
        None,
    );
    let metadata = ParquetMetaData::new(file_metadata, row_groups);
    let mut footer = Vec::new();
    ParquetMetaDataWriter::new(&mut footer, &metadata)
        .with_write_path_in_schema(props.write_path_in_schema())
        .finish()?;
    Ok(footer)
}
