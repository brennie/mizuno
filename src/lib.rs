use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Stdio};

use byteorder::{BigEndian, ReadBytesExt};
use failure::Error;
use lazy_static::lazy_static;

#[derive(Clone, Debug, Default)]
pub struct ConnectionBuilder<'a> {
    path: Option<&'a Path>,
}

impl<'a> ConnectionBuilder<'a> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_path(&'a mut self, path: &'a Path) -> &'a mut Self {
        self.path = Some(path);
        self
    }

    pub fn connect(&self) -> Result<Connection, Error> {
        lazy_static! {
            static ref HG: &'static Path = Path::new("hg");
        }

        let hg = Command::new(self.path.unwrap_or(*HG))
            .arg("serve")
            .arg("--cmdserver")
            .arg("pipe")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;

        Ok(Connection { hg })
    }
}

#[derive(Debug)]
pub struct Connection {
    hg: Child,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.hg.kill().unwrap_or(())
    }
}

impl Connection {
    pub fn new() -> Result<Connection, Error> {
        ConnectionBuilder::new().connect()
    }

    pub fn read_chunk(&mut self) -> Result<Chunk, Error> {
        let stdout = self.hg.stdout.as_mut().expect("no stdout handle?");

        let channel = {
            let mut buf = [0u8];
            stdout.read_exact(&mut buf)?;

            buf[0]
        };

        let chunk = match channel {
            b'o' => {
                let len = stdout.read_u32::<BigEndian>()? as usize;
                let mut buf = vec![0; len];

                stdout.read_exact(&mut buf)?;

                Chunk::Output(buf)
            }

            b'e' => unimplemented!(),
            b'r' => unimplemented!(),
            b'd' => unimplemented!(),
            b'I' => unimplemented!(),
            b'L' => unimplemented!(),

            c => panic!("Unknown channel: {:?}", c),
        };

        Ok(chunk)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Chunk {
    Output(Vec<u8>),
}

#[cfg(test)]
mod test {
    use crate::{Chunk, Connection};
    use std::str;

    #[test]
    fn read_chunk() -> Result<(), failure::Error> {
        let mut conn = Connection::new()?;
        let chunk = conn.read_chunk()?;

        let Chunk::Output(bytes) = chunk;
        let hello = str::from_utf8(&bytes)?.split('\n').collect::<Vec<_>>();

        assert!(hello.len() > 2);
        assert_eq!(hello[0], "capabilities: getencoding runcommand");
        assert_eq!(hello[1], "encoding: UTF-8");

        Ok(())
    }
}
