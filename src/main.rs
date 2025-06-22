mod page;
mod storage;
mod journal;

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Error, ErrorKind, Read, Write};
use std::ops::Add;
use std::{collections::HashMap, hash::Hash, io, path::PathBuf};
use bincode::{Encode, Decode};
use journal::WAL;

use libc::printf;
use storage::SegmentStore;
use unicode_segmentation::UnicodeSegmentation;
use std::path::Path;
use serde::{Serialize, Deserialize};
type DocumentId = usize;

#[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug)]
struct WritableDocs { 
    docs : HashMap<DocumentId, PathBuf>,
    current_doc_id : usize
}

#[derive(Debug)]
struct InvertedIndex { 
    pub index : HashMap<String, Vec<DocumentId>>,
    pub docs : WritableDocs,
    pub weights : HashMap<String, usize>,
    pub last_used : i32,
    pub docs_count: usize,
    pub cap: usize,
    pub segment_store : SegmentStore
}

impl InvertedIndex { 
    pub fn new() -> Self { 
        let dir_path = Path::new("./segments");
        let path = Path::new("./segments/index.seg");
        let _ = std::fs::create_dir_all(dir_path);
        let docs = Self::load_docs_from_disk().unwrap();
        let inverted_index = Self { 
            index : HashMap::new(),
            docs: docs.clone(),
            weights: HashMap::new(),
            docs_count: docs.docs.len(),
            last_used: 0,
            cap: 5,
            segment_store: SegmentStore::new(path, 4096, 16).unwrap()
        };
        println!("inverted index : {inverted_index:?}");
        inverted_index        
    }
    
        
    
    pub fn load_docs_from_disk() -> io::Result<WritableDocs> { 
        let file_path = "./docs.bin";
        if !std::fs::exists(file_path)? {
            return Ok(WritableDocs { docs: HashMap::new(), current_doc_id: 0 });
        } 
        let mut file = File::open(file_path)?;
        let config = bincode::config::standard();
        let mut buff = vec![0u8; 2048];
        let written = file.read(&mut buff)?;
        match bincode::decode_from_slice::<WritableDocs, bincode::config::Configuration>(&buff[..written], config)
        .map_err(|err| std::io::Error::new(ErrorKind::BrokenPipe, "can not decode")) { 
            Ok((docs, size)) => { 
                return Ok(docs)
            },
            Err(err) => { 
                println!("error while loading docs from disk: {err:?}");
                return Ok(WritableDocs { docs: HashMap::new(), current_doc_id : 0})
            }
        }
    }

    pub fn evict(&mut self) { 
        println!("evicting");
        if let Some((term, _)) = self.weights.iter().min_by_key(|(_, used)| *used)
        .map(|(term,used)| (term.to_string(), *used)) { 
            self.weights.remove(&term);
            println!("evicting {term:?}");
            self.index.remove(&term);
        }
        println!("after evicting the index is {:?}", self.index)
    }

    pub fn write_docs_to_disk(&self) -> io::Result<()>{ 
        let docs_filepath = "./docs.bin";
        let config = bincode::config::standard();
        let mut file = OpenOptions::new().write(true).read(true).create(true).append(false).open(docs_filepath)?;
        let mut bytes = vec![0u8; 2048];
        match bincode::encode_into_slice(self.docs.clone(), &mut bytes, config) { 
            Ok(written) => { 
                let _ = file.write_all(&bytes[..written])?;
                return Ok(())
            },
            Err(_) => {}
        }
        return Ok(())
    }

    

    pub fn add_document(&mut self,  content: String, path: PathBuf) -> io::Result<()> { 
        let doc_id = self.docs.current_doc_id;
        println!("content : {content:?}, doc_id :{doc_id} and path: {path:?}");
        println!("docs mapping : {:?}", self.docs.docs);
        for (_,val) in self.docs.docs.iter() { 
            if val == &path { 
                println!("already exists");
                return Err(Error::new(ErrorKind::AlreadyExists, format!("document of path : {:?} already exists", path)))
            }
        }
        self.docs.docs.insert(doc_id, path);
        self.docs.current_doc_id += 1;
        let _ = self.write_docs_to_disk()?;

        let lowercase_tokens = content.to_lowercase();
        let contents = lowercase_tokens.unicode_words().map(str::to_string);
        let mut tokens = Vec::new();
        let mut local_index: HashMap<String, Vec<DocumentId>> = HashMap::new();
        let mut freqs = HashMap::new();
        for content in contents {
            tokens.push(content.clone()); 
            *freqs.entry(content.clone()).or_insert(0) += 1;
        }
        let unique_words : Vec<&String>= freqs.keys().collect();
        if self.index.len() + unique_words.len() > self.cap { 
            self.evict();
        }
        for word in unique_words  { 
            local_index.entry(word.to_string()).or_default().push(doc_id);  
            
            self.index.entry(word.to_string()).or_default().push(doc_id);  
       

        }
        println!("index entry is {:?}", self.index);
        
        self.docs_count += 1;
        let config = bincode::config::standard();
        for (term, docs) in local_index { 
            let mut buf = vec![0u8; 1024];
            if let Ok(size) = bincode::encode_into_slice(docs, &mut buf, config){ 
                let _ = self.segment_store.write(term, &buf[..size]);
            } 
        }
        Ok(())
    }

    pub fn search(&mut self, term: String) -> Vec<&PathBuf>{ 
        println!("index: {:?}", self.index.clone());
        
        self.last_used += 1;
        let last_used = self.last_used;
        self.weights.insert(term.clone(), last_used as usize);
        if let Some(docs) = self.index.get(&term) {
            println!("docs in index : {:?}", docs);
            return docs.iter().filter_map(|doc| self.docs.docs.get(doc)).collect();
        }
        let config = bincode::config::standard();
        if let Ok(bytes_vec) = self.segment_store.read_bytes(term.clone()) {
            let mut file_paths = Vec::new(); 
            if self.index.len()  +  1 > self.cap && !self.index.contains_key(&term){ 
                self.evict();
                
            }
            for bytes in bytes_vec { 
                match bincode::decode_from_slice::<Vec<usize>, _>(&bytes, config) { 
                    Ok((docs,_)) => { 
                        println!("docs comming from disk ; {:?}", docs);
                        self.index.entry(term.clone()).or_default().extend(docs.clone());
                        
                        let path: Vec<&PathBuf> = docs.iter().filter_map(|doc| self.docs.docs.get(doc)).collect();
                        file_paths.extend(path);
                    },
                    Err(_) => return vec![]
                }
            } 
            println!("index: {:?}", self.index.clone());
            return file_paths;
        }
        vec![]
    }
}

async fn read_files() -> io::Result<Vec<(String, PathBuf)>>{ 
    let mut entries = tokio::fs::read_dir("./src/documents").await.unwrap();
    let mut files = Vec::new();
    
    while let Some(entry) = entries.next_entry().await? { 
        let path = entry.path();
        println!("{path:?}");
        if path.is_file() && path.extension().map(|ext | ext == "txt").unwrap_or(false) { 
            let content = tokio::fs::read_to_string(path.clone()).await?;
            files.push((content, path.clone()));
        }
    }
    Ok(files)
}

#[tokio::main]
async fn main() -> io::Result<()>{
    println!("here");
    let file_contents = read_files().await.unwrap();
    
    println!("file is not there");
    let mut inverted_index = InvertedIndex::new();
    for (index, (content, path)) in file_contents.iter().enumerate() { 
        let _ = inverted_index.add_document( content.to_string(), path.to_path_buf());
    }
    
    
    

    let search_keys = vec!["system", "language", "system", "is", "rust", "a", "mountain", "awesome", "world", "here"];
     
    for key in search_keys { 
        println!("result {:?}", inverted_index.search(key.to_string()));
    }
    println!("index size: {}", inverted_index.index.len());
     
    Ok(())
}
//Optionally wrap in impl Iterator<Item = usize> for lazy decoding of doc IDs (e.g., delta-decoded blocks).
// pub fn load_from_disk(path: &Path) -> Result<Self, DecodeError>{ 
    //     println!("reading from disk");
    //     let mut file = File::open(path).unwrap();
    //     let mut buffer = Vec::new();
    //     let size = file.read_to_end(&mut buffer).unwrap();
    //     match bincode::decode_from_slice(&buffer, bincode::config::standard()) { 
    //         Ok((index, _)) => { 
    //             return Ok(index)
    //         } , 
    //         Err(err) => return Err(err)
    //     }
        
    // }
    // pub fn save_to_disk(&mut self) -> io::Result<()> {
    //     let config = bincode::config::standard();
    //     for (term, data) in self.index.clone() {  
    //         let mut buf = vec![0u8; 1024];
    //         if let Ok(size) = bincode::encode_into_slice(data, &mut buf, config){ 
    //             self.segment_store.write(term, &buf[..size]);
    //         } 
    //     }
    //     Ok(())

    // }
