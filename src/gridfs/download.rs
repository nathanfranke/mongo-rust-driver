use std::{
    ops::Range,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{
    future::{BoxFuture, FutureExt},
    io::AsyncRead,
};

use super::{Chunk, FilesCollectionDocument};
use crate::{
    action::gridfs::download::DownloadRange,
    bson::doc,
    error::{ErrorKind, GridFsErrorKind, Result},
    options::FindOptions,
    Collection,
    Cursor,
};

/// A stream from which a file stored in a GridFS bucket can be downloaded.
///
/// # Downloading from the Stream
/// The `GridFsDownloadStream` type implements [`futures_io::AsyncRead`]. It is recommended that
/// users call the utility methods in [`AsyncReadExt`](futures_util::io::AsyncReadExt) to interact
/// with the stream.
///
/// ```rust
/// # use mongodb::{bson::Bson, error::Result, gridfs::{GridFsBucket, GridFsDownloadStream}};
/// # async fn download_example(bucket: GridFsBucket, id: Bson) -> Result<()> {
/// use futures_util::io::AsyncReadExt;
///
/// let mut buf = Vec::new();
/// let mut download_stream = bucket.open_download_stream(id).await?;
/// download_stream.read_to_end(&mut buf).await?;
/// # Ok(())
/// # }
/// ```
///
/// If the destination is a local file (or other `AsyncWrite` byte sink), the contents of the stream
/// can be efficiently written to it with [`futures_util::io::copy`].
///
/// # Using [`tokio::io::AsyncRead`]
/// Users who prefer to use tokio's `AsyncRead` trait can use the [`tokio_util::compat`] module.
///
/// ```rust
/// # use mongodb::{bson::Bson, error::Result, gridfs::{GridFsBucket, GridFsUploadStream}};
/// # async fn compat_example(bucket: GridFsBucket, id: Bson) -> Result<()> {
/// use tokio_util::compat::FuturesAsyncReadCompatExt;
///
/// let futures_upload_stream = bucket.open_download_stream(id).await?;
/// let tokio_upload_stream = futures_upload_stream.compat();
/// # Ok(())
/// # }
/// ```
pub struct GridFsDownloadStream {
    state: State,
    current_n: u32,
    total_n: u32,
    file: FilesCollectionDocument,
    to_skip: u64,
    to_take: u64,
}

type GetBytesFuture = BoxFuture<'static, Result<(Vec<u8>, Box<Cursor<Chunk<'static>>>)>>;

enum State {
    // Idle is stored as an option so that its fields can be moved into a GetBytesFuture
    // without requiring ownership of the state. It can always be unwrapped safely.
    Idle(Option<Idle>),
    Busy(GetBytesFuture),
    Done,
}

struct Idle {
    buffer: Vec<u8>,
    cursor: Box<Cursor<Chunk<'static>>>,
}

impl State {
    fn set_busy(&mut self, new_future: GetBytesFuture) -> &mut GetBytesFuture {
        *self = State::Busy(new_future);
        match self {
            Self::Busy(ref mut future) => future,
            _ => unreachable!(),
        }
    }
}

fn validate_range_value(range_value: Option<u64>, file_length: u64) -> Result<()> {
    if let Some(range) = range_value {
        if range > file_length {
            return Err(
                ErrorKind::GridFs(GridFsErrorKind::PartialDownloadRangeOutOfBounds {
                    file_length,
                    out_of_bounds_value: range,
                })
                .into(),
            );
        }
    }

    Ok(())
}

impl GridFsDownloadStream {
    pub(crate) async fn new(
        file: FilesCollectionDocument,
        chunks: &Collection<Chunk<'static>>,
        range: DownloadRange,
    ) -> Result<Self> {
        validate_range_value(range.0, file.length)?;
        validate_range_value(range.1, file.length)?;

        let is_empty_range = match range {
            DownloadRange(Some(start), Some(end)) => start == end,
            _ => false,
        };

        let to_skip = range.0.unwrap_or(0);
        let to_take = range.1.unwrap_or(file.length) - to_skip;
        let chunk_size = file.chunk_size_bytes as u64;
        let chunks_to_skip = to_skip / chunk_size;
        let total_chunks = range
            .1
            .map(|end| end / chunk_size + u64::from(end % chunk_size != 0));

        let initial_state = if file.length == 0 || is_empty_range {
            State::Done
        } else {
            let options = FindOptions::builder()
                .sort(doc! { "n": 1 })
                .limit(total_chunks.map(|end| (end - chunks_to_skip) as i64))
                .build();
            let cursor = chunks
                .find(doc! { "files_id": &file.id, "n": { "$gte": chunks_to_skip as i64 } })
                .with_options(options)
                .await?;

            State::Idle(Some(Idle {
                buffer: Vec::new(),
                cursor: Box::new(cursor),
            }))
        };

        Ok(Self {
            state: initial_state,
            current_n: chunks_to_skip as u32,
            total_n: total_chunks.map(|value| value as u32).unwrap_or(file.n()),
            file,
            to_skip: to_skip % chunk_size,
            to_take,
        })
    }
}

impl AsyncRead for GridFsDownloadStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::result::Result<usize, futures_util::io::Error>> {
        let stream = self.get_mut();

        let result = match &mut stream.state {
            State::Idle(idle) => {
                let Idle { buffer, cursor } = idle.take().unwrap();

                if !buffer.is_empty() {
                    Ok((buffer, cursor))
                } else {
                    let chunks_in_buf = FilesCollectionDocument::n_from_vals(
                        stream.to_skip + buf.len() as u64,
                        stream.file.chunk_size_bytes,
                    );
                    // We should read from current_n to chunks_in_buf + current_n, or, if that would
                    // exceed the total number of chunks in the file, to the last chunk in the file.
                    let final_n = std::cmp::min(chunks_in_buf + stream.current_n, stream.total_n);
                    let n_range = stream.current_n..final_n;

                    stream.current_n = final_n;

                    let new_future = stream.state.set_busy(
                        get_bytes(
                            cursor,
                            buffer,
                            n_range,
                            stream.file.chunk_size_bytes,
                            stream.file.length,
                            stream.to_skip,
                        )
                        .boxed(),
                    );

                    stream.to_skip = 0;

                    match new_future.poll_unpin(cx) {
                        Poll::Ready(result) => result,
                        Poll::Pending => return Poll::Pending,
                    }
                }
            }

            State::Busy(future) => match future.poll_unpin(cx) {
                Poll::Ready(result) => result,
                Poll::Pending => return Poll::Pending,
            },
            State::Done => return Poll::Ready(Ok(0)),
        };

        match result {
            Ok((mut buffer, cursor)) => {
                let mut bytes_to_write = std::cmp::min(buffer.len(), buf.len());

                if bytes_to_write as u64 > stream.to_take {
                    bytes_to_write = stream.to_take as usize;
                }

                buf[..bytes_to_write].copy_from_slice(buffer.drain(0..bytes_to_write).as_slice());
                stream.to_take -= bytes_to_write as u64;

                stream.state = if stream.to_take == 0 {
                    State::Done
                } else {
                    State::Idle(Some(Idle { buffer, cursor }))
                };

                Poll::Ready(Ok(bytes_to_write))
            }
            Err(error) => {
                stream.state = State::Done;
                Poll::Ready(Err(error.into_futures_io_error()))
            }
        }
    }
}

async fn get_bytes(
    mut cursor: Box<Cursor<Chunk<'static>>>,
    mut buffer: Vec<u8>,
    n_range: Range<u32>,
    chunk_size_bytes: u32,
    file_len: u64,
    mut to_skip: u64,
) -> Result<(Vec<u8>, Box<Cursor<Chunk<'static>>>)> {
    for n in n_range {
        if !cursor.advance().await? {
            return Err(ErrorKind::GridFs(GridFsErrorKind::MissingChunk { n }).into());
        }

        let chunk = cursor.deserialize_current()?;
        let chunk_bytes = chunk.data.bytes;

        if chunk.n != n {
            return Err(ErrorKind::GridFs(GridFsErrorKind::MissingChunk { n }).into());
        }

        let expected_len =
            FilesCollectionDocument::expected_chunk_length_from_vals(file_len, chunk_size_bytes, n);
        if chunk_bytes.len() != (expected_len as usize) {
            return Err(ErrorKind::GridFs(GridFsErrorKind::WrongSizeChunk {
                actual_size: chunk_bytes.len(),
                expected_size: expected_len,
                n,
            })
            .into());
        }

        if to_skip >= chunk_bytes.len() as u64 {
            to_skip -= chunk_bytes.len() as u64;
        } else if to_skip > 0 {
            buffer.extend_from_slice(&chunk_bytes[to_skip as usize..]);
            to_skip = 0;
        } else {
            buffer.extend_from_slice(chunk_bytes);
        }
    }

    Ok((buffer, cursor))
}
