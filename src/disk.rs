//! Disk partition implementation.

use crate::encoding::GorillaDecoder;
use crate::label::marshal_metric_name;
use crate::{DataPoint, Label, Result, Row, TsinkError};
use memmap2::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

pub const DATA_FILE_NAME: &str = "data";
pub const META_FILE_NAME: &str = "meta.json";

/// Metadata for a disk partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionMeta {
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub num_data_points: usize,
    pub metrics: HashMap<String, DiskMetric>,
    pub created_at: SystemTime,
}

/// Metadata for a metric in a disk partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct DiskMetric {
    pub name: String,
    pub offset: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub num_data_points: usize,
}

/// A disk partition stores time-series data on disk using memory-mapped files.
pub struct DiskPartition {
    dir_path: PathBuf,
    meta: PartitionMeta,
    mapped_file: Mmap,
    retention: Duration,
}

impl DiskPartition {
    /// Opens an existing disk partition.
    pub fn open(dir_path: impl AsRef<Path>, retention: Duration) -> Result<Self> {
        let dir_path = dir_path.as_ref();

        // Read metadata
        let meta_path = dir_path.join(META_FILE_NAME);
        if !meta_path.exists() {
            return Err(TsinkError::InvalidPartition {
                id: dir_path.to_string_lossy().to_string(),
            });
        }

        let meta_file = File::open(&meta_path)?;
        let meta: PartitionMeta = serde_json::from_reader(meta_file)?;

        // Memory-map the data file
        let data_path = dir_path.join(DATA_FILE_NAME);
        let data_file = File::open(&data_path)?;

        if data_file.metadata()?.len() == 0 {
            return Err(TsinkError::NoDataPoints {
                metric: "unknown".to_string(),
                start: 0,
                end: 0,
            });
        }

        let mapped_file = unsafe { MmapOptions::new().map(&data_file)? };

        Ok(Self {
            dir_path: dir_path.to_path_buf(),
            meta,
            mapped_file,
            retention,
        })
    }

    /// Creates a new disk partition from memory partition data.
    pub fn create(
        dir_path: impl AsRef<Path>,
        meta: PartitionMeta,
        data: Vec<u8>,
        retention: Duration,
    ) -> Result<Self> {
        let dir_path = dir_path.as_ref();

        // Create directory
        fs::create_dir_all(dir_path)?;

        // Write data file
        let data_path = dir_path.join(DATA_FILE_NAME);
        fs::write(&data_path, &data)?;

        // Write metadata file (write last to indicate valid partition)
        let meta_path = dir_path.join(META_FILE_NAME);
        let meta_file = File::create(&meta_path)?;
        serde_json::to_writer_pretty(meta_file, &meta)?;

        // Open the created partition
        Self::open(dir_path, retention)
    }
}

impl crate::partition::Partition for DiskPartition {
    fn insert_rows(&self, _rows: &[Row]) -> Result<Vec<Row>> {
        Err(TsinkError::ReadOnlyPartition {
            path: self.dir_path.clone(),
        })
    }

    fn select_data_points(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        if self.expired() {
            return Err(TsinkError::NoDataPoints {
                metric: "unknown".to_string(),
                start: 0,
                end: 0,
            });
        }

        let metric_name = marshal_metric_name(metric, labels);

        let disk_metric = match self.meta.metrics.get(&metric_name) {
            Some(m) => m,
            None => return Ok(Vec::new()),
        };

        // Create a cursor at the metric's offset
        let data_slice = &self.mapped_file[disk_metric.offset as usize..];
        let cursor = Cursor::new(data_slice.to_vec());

        // Decode points
        let mut decoder = GorillaDecoder::new(cursor.into_inner());
        let mut points = Vec::with_capacity(disk_metric.num_data_points);

        for _ in 0..disk_metric.num_data_points {
            let point = decoder.decode_point()?;

            if point.timestamp < start {
                continue;
            }
            if point.timestamp >= end {
                break;
            }

            points.push(point);
        }

        Ok(points)
    }

    fn min_timestamp(&self) -> i64 {
        self.meta.min_timestamp
    }

    fn max_timestamp(&self) -> i64 {
        self.meta.max_timestamp
    }

    fn size(&self) -> usize {
        self.meta.num_data_points
    }

    fn active(&self) -> bool {
        false // Disk partitions are always read-only
    }

    fn expired(&self) -> bool {
        if let Ok(elapsed) = self.meta.created_at.elapsed() {
            elapsed > self.retention
        } else {
            false
        }
    }

    fn clean(&self) -> Result<()> {
        fs::remove_dir_all(&self.dir_path)?;
        Ok(())
    }

    fn flush_to_disk(&self) -> Result<Option<(Vec<u8>, PartitionMeta)>> {
        // DiskPartition is already on disk, so return None
        Ok(None)
    }
}
