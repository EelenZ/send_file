use std::{collections::HashMap, ffi::OsStr, path::{Path, PathBuf}, sync::Arc, sync::Mutex};
use async_std::{fs::File, task};
use async_std::net::{TcpStream, TcpListener};
use async_std::prelude::*;
use futures::channel::mpsc;
use uuid::Uuid;

type Sender<T> = mpsc::UnboundedSender<T>; 
type Receiver<T> = mpsc::UnboundedReceiver<T>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const LOCAL: &str = "127.0.0.1:6000";

#[derive(Debug)]
pub struct Chunk {
    token: Uuid,
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct Master {
    file_chunk_map : Arc<Mutex<HashMap<String, Vec<Uuid>>>>,
    chunk_sender: Sender<Chunk>,
    chunk_reciver: Receiver<Chunk>,
}

impl Master {
    pub fn new() -> Self {
        let mut file_chunk_map: HashMap<String, Vec<Uuid>> = HashMap::new();
        let (mut chunk_sender, chunk_reciver) = mpsc::unbounded::<Chunk>();



        Self {
            file_chunk_map: Arc::new(Mutex::new(file_chunk_map)),
            chunk_sender: chunk_sender,
            chunk_reciver: chunk_reciver,
        }
    }


    async fn split_file(path: &Path, mut chunk_sender: Sender<Chunk>, file_chunk_map: Arc<Mutex<HashMap<String, Vec<Uuid>>>>)-> Result<()> {
        let filename = path.file_name().and_then(OsStr::to_str).unwrap();
        let mut file = File::open(path).await?;
        loop {
            let mut buf = vec![0u8; 64 * 1024 * 1024];
            let n = file.read(&mut buf[..]).await?;
            match n {
                0 => break,
                n => {
                    let token = Uuid::new_v4();
                    let chunk = Chunk{token, data: buf};

                    file_chunk_map.lock().unwrap().entry(filename.to_string())
                                .or_insert_with(Vec::new)
                                .push(token);
                   
                    &chunk_sender.start_send(chunk);
                }
            }
        }

        Ok(())
    }

    pub async fn run(&mut self, file_path: &'static Path) -> Result<()>{
        let server = TcpListener::bind(LOCAL).await?;

        let mut file_chunk_map = self.file_chunk_map.clone();
        let mut chunk_sender = self.chunk_sender.clone();
        //Master::split_file(file_path, chunk_sender, file_chunk_map).await?;
        let handle = task::spawn(async move {
            Master::split_file(file_path, chunk_sender, file_chunk_map).await
        });
        handle.await?;

    /*     let mut incoming = server.incoming();
        while let Some(stream) = incoming.next().await {
            let stream = stream?;
            println!("Accepting from: {}", stream.peer_addr()?);
            while let Ok(Some(chunk)) = self.chunk_reciver.try_next() {
                println!("{}", chunk.token);
            }
        } */

        while let Ok(Some(chunk)) = self.chunk_reciver.try_next() {
            println!("{}", chunk.token);
        }

        Ok(())
    }

   
}


#[async_std::main]
async fn main() -> Result<()>{
    let path = Path::new(r"D:\Software\manjaro\manjaro-kde-20.0.3-200606-linux56.iso");

    let mut master = Master::new();
    master.run(path).await;
    //println!("{:?}", master.file_chunk_map);
    
    Ok(())

}
