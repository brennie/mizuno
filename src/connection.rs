use std::collections::HashSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::str;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use failure::{err_msg, format_err, Error};

use crate::capabilities::Capability;

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

    pub fn connect(&self) -> Result<Connection, failure::Error> {
        Connection::from_builder(&self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Channel {
    Output,
    Error,
    Debug,
    Result,
    Input,
    LineInput,
}

impl Channel {
    fn try_from(c: u8) -> Result<Self, Error> {
        Ok(match c {
            b'o' => Channel::Output,
            b'e' => Channel::Error,
            b'd' => Channel::Debug,
            b'r' => Channel::Result,
            b'I' => Channel::Input,
            b'L' => Channel::LineInput,
            _ => return Err(format_err!("Unknown channel: {}", c)),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Chunk {
    Output(Vec<u8>),
    Error(Vec<u8>),
    Debug(Vec<u8>),
    Result(u32),
    Input(u32),
    LineInput(u32),
}

#[derive(Debug)]
pub struct Connection {
    hg: Child,
    encoding: String,
    capabilities: HashSet<Capability>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.hg.kill().unwrap_or(())
    }
}

impl Connection {
    pub fn new() -> Result<Self, failure::Error> {
        Self::from_builder(&Default::default())
    }

    pub(self) fn from_builder(builder: &ConnectionBuilder) -> Result<Self, Error> {
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

        let mut hg = command.spawn()?;

        let hello_chunk = Self::read_chunk_from(hg.stdout.as_mut().expect("no stdout handle"))?;

        let (encoding, capabilities) = Self::parse_hello(hello_chunk)?;

        Ok(Connection {
            hg,
            encoding,
            capabilities,
        })
    }

    pub fn capabilities(&self) -> &HashSet<Capability> {
        &self.capabilities
    }

    pub fn encoding(&self) -> &str {
        &self.encoding
    }

    pub fn read_chunk(&mut self) -> Result<Chunk, Error> {
        Self::read_chunk_from(self.hg.stdout.as_mut().expect("no stdout handle"))
    }

    pub fn run_command(&mut self, command: &[&str]) -> Result<Chunk, Error> {
        let len: usize = command.iter().map(|s| s.len()).sum::<usize>() + command.len() - 1;

        let mut stdin = self.hg.stdin.as_mut().expect("no stdin handle");

        write!(&mut stdin, "runcommand\n")?;

        stdin.write_u32::<BigEndian>(len as u32)?;

        for (i, part) in command.iter().enumerate() {
            write!(&mut stdin, "{}", part)?;

            if i + 1 != command.len() {
                write!(&mut stdin, "{}", 0 as char)?;
            }
        }

        self.read_chunk()
    }

    fn read_chunk_from<R>(r: &mut R) -> Result<Chunk, Error>
    where
        R: Read,
    {
        let channel = {
            let mut buf = [0u8];
            r.read_exact(&mut buf)?;

            Channel::try_from(buf[0]).unwrap()
        };

        Ok(match channel {
            Channel::Output | Channel::Error | Channel::Debug => {
                let len = r.read_u32::<BigEndian>()? as usize;
                let mut buf = vec![0; len];

                r.read_exact(&mut buf)?;

                match channel {
                    Channel::Output => Chunk::Output(buf),
                    Channel::Error => Chunk::Error(buf),
                    Channel::Debug => Chunk::Debug(buf),
                    _ => unreachable!(),
                }
            }

            Channel::Result | Channel::Input | Channel::LineInput => {
                let val = r.read_u32::<BigEndian>()?;

                match channel {
                    Channel::Result => Chunk::Result(val),
                    Channel::Input => Chunk::Input(val),
                    Channel::LineInput => Chunk::LineInput(val),
                    _ => unreachable!(),
                }
            }
        })
    }

    fn parse_hello(chunk: Chunk) -> Result<(String, HashSet<Capability>), Error> {
        let mut encoding = None;
        let mut capabilities = None;

        let hello_bytes = match chunk {
            Chunk::Output(bytes) => bytes,
            _ => return Err(err_msg("invalid chunk kind in hello")),
        };

        let hello = str::from_utf8(&hello_bytes)?;

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

        let encoding = encoding.ok_or_else(|| err_msg("no encoding in hello"))?;
        let capabilities = capabilities.ok_or_else(|| err_msg("no capabilities in hello"))?;

        if !capabilities.contains(&Capability::RunCommand) {
            Err(err_msg("no runcommand capability"))
        } else {
            Ok((encoding, capabilities))
        }
    }
}

#[cfg(test)]
mod test {
    use std::env::set_current_dir;

    use tempdir::TempDir;

    use super::{Capability, Chunk, Connection};

    #[test]
    fn hello() -> Result<(), failure::Error> {
        let conn = Connection::new()?;
        assert_eq!(conn.encoding(), "UTF-8");
        assert!(conn.capabilities().contains(&Capability::RunCommand));

        Ok(())
    }

    #[test]
    fn runcommand_status() -> Result<(), failure::Error> {
        let tmpdir = TempDir::new("hg-repo")?;
        set_current_dir(&tmpdir)?;

        let mut conn = Connection::new()?;

        let chunk = conn.run_command(&["init"])?;

        assert_eq!(chunk, Chunk::Result(4));

        Ok(())
    }
}
