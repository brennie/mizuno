use std::collections::HashSet;
use std::fmt;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::str::{self, Utf8Error};

use byteorder::{ByteOrder, BigEndian};
use failure::Fail;
use tokio::codec::{LengthDelimitedCodec, FramedRead};
use tokio::io::read_exact;
use tokio::prelude::*;
use tokio_process::{Child, ChildStdin, ChildStdout, CommandExt};

use crate::capabilities::Capability;

#[derive(Debug, Fail)]
pub enum ConnectionError {
    #[fail(display = "IO error: {}", _0)]
    Io(#[cause] io::Error),

    #[fail(display = "No stdin handle")]
    NoStdin,

    #[fail(display = "No stdout handle")]
    NoStdout,

    #[fail(display = "Error parsing hello: {}", _0)]
    Hello(HelloError),
}

#[derive(Debug, Fail)]
pub enum HelloError {
    #[fail(display = "Failed to read hello chunk: {}", _0)]
    ReadChunk(#[cause] ReadChunkError),

    #[fail(
        display = "hello from Mercurial on invalid channel; got {} but expected output",
        _0
    )]
    InvalidChannel(Channel),

    #[fail(display = "Could not decode hello from Mercurial: {}", _0)]
    DecodeError(#[cause] Utf8Error),

    #[fail(display = "hello from Mercurial missing encoding")]
    NoEncoding,

    #[fail(display = "hello from Mercurial missing capabilities")]
    NoCapabilities,

    #[fail(display = "Mercurial lacks runcommand capability")]
    MissingRunCommand,
}

#[derive(Debug, Fail)]
pub enum ReadChunkError {
    #[fail(display = "Could not read channel: {}", _0)]
    ReadChannel(#[cause] io::Error),

    #[fail(display = "Could not read chunk length: {}", _0)]
    ReadLength(#[cause] io::Error),

    #[fail(display = "Could not read chunk data: {}", _0)]
    ReadData(#[cause] io::Error),

    #[fail(display = "{}", _0)]
    InvalidChannel(InvalidChannelError),
}

#[derive(Clone, Debug, Default)]
pub struct ConnectionBuilder {
    pub(self) hg: Option<PathBuf>,
    pub(self) pwd: Option<PathBuf>,
}

impl ConnectionBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_hg(&mut self, path: &Path) -> &mut Self {
        self.hg = Some(path.into());
        self
    }

    pub fn with_pwd(&mut self, path: &Path) -> &mut Self {
        self.pwd = Some(path.into());
        self
    }

    pub fn connect(&self) -> impl Future<Item = Connection, Error = ConnectionError> {
        Connection::from_builder(&self)
    }
}

#[derive(Debug, Fail)]
#[fail(display = "Unknown channel \"{}\"", channel)]
pub struct InvalidChannelError {
    pub channel: char,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Channel {
    Output,
    Error,
    Debug,
    Result,
    Input,
    LineInput,
}

impl Channel {
    fn try_from(c: u8) -> Result<Self, InvalidChannelError> {
        Ok(match c {
            b'o' => Channel::Output,
            b'e' => Channel::Error,
            b'd' => Channel::Debug,
            b'r' => Channel::Result,
            b'I' => Channel::Input,
            b'L' => Channel::LineInput,
            _ => return Err(InvalidChannelError { channel: c as char }),
        })
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Channel::Output => "output",
                Channel::Error => "error",
                Channel::Debug => "debug",
                Channel::Result => "result",
                Channel::Input => "input",
                Channel::LineInput => "line input",
            }
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Chunk {
    Output(Vec<u8>),
    Error(Vec<u8>),
    Debug(Vec<u8>),
    Result(Vec<u8>),
    Input(u32),
    LineInput(u32),
}

impl Chunk {
    pub fn channel(&self) -> Channel {
        match self {
            Chunk::Output(..) => Channel::Output,
            Chunk::Error(..) => Channel::Error,
            Chunk::Debug(..) => Channel::Debug,
            Chunk::Result(..) => Channel::Result,
            Chunk::Input(..) => Channel::Input,
            Chunk::LineInput(..) => Channel::LineInput,
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    hg: Child,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    encoding: String,
    capabilities: HashSet<Capability>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.hg.kill().unwrap_or(())
    }
}

impl Connection {
    pub fn new() -> impl Future<Item = Self, Error = ConnectionError> {
        Self::from_builder(&Default::default())
    }

    pub(self) fn from_builder(builder: &ConnectionBuilder) -> impl Future<Item = Self, Error = ConnectionError> {
        let hg_cmd = builder
            .hg
            .as_ref()
            .map(AsRef::as_ref)
            .unwrap_or_else(|| Path::new("hg"));

        let mut command = Command::new(hg_cmd);

        command
            .arg("serve")
            .arg("--cmdserver")
            .arg("pipe")
            .env("HGPLAIN", "True")
            .env("HGENCODING", "UTF-8")
            .env("HGENCODINGMODE", "strict")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());

        if let Some(ref pwd) = builder.pwd {
            command.current_dir(pwd);
        }

        let mut hg = match command.spawn_async() {
            Ok(hg) => hg,
            err @ Err(..) => return err.map_err(ConnectionError::Io).into_future(),
        };

        let stdin = hg.stdin().take().ok_or(ConnectionError::NoStdin)?;
        let mut stdout = hg.stdout().take().ok_or(ConnectionError::NoStdout)?;

        Self::read_chunk_from(&mut stdout)
            .map_err(|e| ConnectionError::Hello(HelloError::ReadChunk(e)))
            .map(move |(stdout, hello_chunk)| {
                let (encoding, capabilities) = match Self::parse_hello(hello_chunk) {
                    Ok(ok) => ok,
                    err @ Err(..) => return err.map_err(ConnectionError::Hello).into_future();
                }

                futures::ok(Connection {
                    hg,
                    stdin: Some(stdin),
                    stdout: Some(stdout),
                    encoding,
                    capabilities,
                })
            })
    }

    pub fn capabilities(&self) -> &HashSet<Capability> {
        &self.capabilities
    }

    pub fn encoding(&self) -> &str {
        &self.encoding
    }

    pub fn read_chunk(&mut self) -> impl Future<Item = (ChildStdout, Chunk), Error = ReadChunkError> {
        Self::read_chunk_from(&mut self.stdout)
    }

    pub fn run_command(&'_ mut self, command: &[&str]) -> Result<CommandIterator<'_>, io::Error> {
        let len: usize = command.iter().map(|s| s.len()).sum::<usize>() + command.len() - 1;

        // TODO: Any communication errors should bring down the connection.
        write!(&mut self.stdin, "runcommand\n")?;

        self.stdin.write_u32::<BigEndian>(len as u32)?;

        for (i, part) in command.iter().enumerate() {
            write!(&mut self.stdin, "{}", part)?;

            if i + 1 != command.len() {
                write!(&mut self.stdin, "{}", 0 as char)?;
            }
        }

        Ok(CommandIterator::new(self))
    }

    fn read_chunk_from<R>(r: &mut R) -> impl Future<Item = (R, Chunk), Error = ReadChunkError>
    where
        R: AsyncRead,
    {
        read_exact(r, [0u8])
            .map_err(ReadChunkError::ReadChannel)
            .and_then(|(r, buf)| {
                Channel::try_from(buf[0])
                    .map_err(ReadChunkError::InvalidChannel)
                    .map(move |c| (r, c))
                    .into_future()
            })
            .and_then(|(r, channel)| match channel {
                Channel::Output | Channel::Error | Channel::Debug | Channel::Result => {
                    future::Either::A(
                    read_exact(r, [0u8; 4])
                        .map_err(ReadChunkError::ReadLength)
                        .and_then(|(r, bytes)| {
                            let len = BigEndian::read_u32(&bytes) as usize;
                            let buf = vec![0; len];

                            read_exact(r, buf)
                                .map_err(ReadChunkError::ReadData)
                        })
                        .map(|(r, buf)| {
                            let chunk = match channel {
                                Channel::Output => Chunk::Output(buf),
                                Channel::Error => Chunk::Error(buf),
                                Channel::Debug => Chunk::Debug(buf),
                                Channel::Result => Chunk::Result(buf),
                                _ => unreachable!(),
                            };

                            (r, chunk)
                        }))
                }

                Channel::Input | Channel::LineInput => {
                    future::Either::B(
                        read_exact(r, [0u8; 4])
                            .map_err(ReadChunkError::ReadData)
                            .map(|(r, bytes)| {
                                let val = BigEndian::read_u32(&bytes);
                                let chunk = match channel {
                                    Channel::Input => Chunk::Input(val),
                                    Channel::LineInput => Chunk::LineInput(val),
                                    _ => unreachable!(),
                                }

                                (r, val)
                        })
                    )
                }
            })
    }

    fn parse_hello(chunk: Chunk) -> Result<(String, HashSet<Capability>), HelloError> {
        let mut encoding = None;
        let mut capabilities = None;

        let hello_bytes = match chunk {
            Chunk::Output(bytes) => bytes,
            _ => return Err(HelloError::InvalidChannel(chunk.channel())),
        };

        let hello = str::from_utf8(&hello_bytes).map_err(HelloError::DecodeError)?;

        for line in hello.lines() {
            if let Some(split_at) = line.find(": ") {
                let (key, value) = line.split_at(split_at);
                let value = &value[2..];

                match key {
                    "capabilities" => {
                        capabilities =
                            Some(value.split(" ").map(Into::into).collect::<HashSet<_>>())
                    }
                    "encoding" => encoding = Some(value.into()),
                    _ => {}
                }
            }
        }

        let encoding = encoding.ok_or(HelloError::NoEncoding)?;
        let capabilities = capabilities.ok_or(HelloError::NoCapabilities)?;

        if !capabilities.contains(&Capability::RunCommand) {
            Err(HelloError::MissingRunCommand)
        } else {
            Ok((encoding, capabilities))
        }
    }
}

pub struct CommandIterator<'a> {
    conn: &'a mut Connection,
    finished: bool,
}

impl<'a> CommandIterator<'a> {
    pub(self) fn new(conn: &'a mut Connection) -> Self {
        CommandIterator {
            conn,
            finished: false,
        }
    }
}

impl<'a> Iterator for CommandIterator<'a> {
    type Item = Result<Chunk, ReadChunkError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        let chunk = match self.conn.read_chunk() {
            Ok(chunk) => chunk,
            err @ Err(..) => return Some(err),
        };

        if chunk.channel() == Channel::Result {
            self.finished = true;
        }

        Some(Ok(chunk))
    }
}

#[cfg(test)]
mod test {
    use std::env::set_current_dir;
    use std::path::Path;

    use tempdir::TempDir;

    use super::{Capability, Chunk, Connection, ConnectionBuilder};

    #[test]
    fn hello() -> Result<(), failure::Error> {
        let conn = Connection::new()?;
        assert_eq!(conn.encoding(), "UTF-8");
        assert!(conn.capabilities().contains(&Capability::RunCommand));

        Ok(())
    }

    #[test]
    fn runcommand_init() -> Result<(), failure::Error> {
        let tmpdir = TempDir::new("hg-repo")?;
        set_current_dir(&tmpdir)?;

        let mut conn = Connection::new()?;

        let chunk = conn
            .run_command(&["init"])?
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(chunk, vec![Chunk::Result(vec![0, 0, 0, 0])]);

        Ok(())
    }

    #[test]
    fn runcommand_status() -> Result<(), failure::Error> {
        let path = if cfg!(windows) {
            Path::new("C:\\")
        } else {
            Path::new("/")
        };

        let mut conn = ConnectionBuilder::new().with_pwd(&path).connect()?;

        let chunks = conn
            .run_command(&["status"])?
            .collect::<Result<Vec<_>, _>>()?;

        let err_msg = format!(
            "abort: no repository found in '{}' (.hg not found)!\n",
            path.display()
        )
        .bytes()
        .collect::<Vec<_>>();

        assert_eq!(
            chunks,
            &[Chunk::Error(err_msg), Chunk::Result(vec![0, 0, 0, 255])]
        );

        Ok(())
    }
}
