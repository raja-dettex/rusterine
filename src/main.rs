mod page;
mod storage;

use std::io::{Read, Write};
use std::ops::Add;
use std::{collections::HashMap, hash::Hash, io, path::PathBuf};

use bincode::error::DecodeError;
use bincode::Decode;
use serde::{Deserialize, Serialize};
use storage::SegmentStore;
use unicode_segmentation::UnicodeSegmentation;
use std::path::Path;
use std::fs::{File, OpenOptions};
type DocumentId = usize;
type WeightedDocumentIds = (usize, Vec<DocumentId>);
#[derive(Debug)]
struct InvertedIndex { 
    pub index : HashMap<String, Vec<DocumentId>>,
    pub weights : HashMap<String, usize>,
    pub docs : HashMap<DocumentId, PathBuf>,
    pub term_doc_freqs: HashMap<String, usize>,
    pub term_freqs: HashMap<DocumentId, HashMap<String, usize>>,
    pub docs_length: HashMap<DocumentId, usize>,
    pub docs_count: usize,
    pub cap: usize,
    pub segment_store : SegmentStore
}

impl InvertedIndex { 
    pub fn new() -> Self { 
        let dir_path = Path::new("./segments");
        let path = Path::new("./segments/index.seg");
        std::fs::create_dir_all(dir_path);
        Self { index : HashMap::new(),
            weights: HashMap::new(), 
            docs: HashMap::new(),
            term_doc_freqs: HashMap::new(),
            term_freqs: HashMap::new(),
            docs_length: HashMap::new(),
            docs_count: 0,
            cap: 30,
            segment_store: SegmentStore::new(path, 4096, 16).unwrap()
        }
    }
    

    pub fn evict(&mut self) { 
        println!("evicting");
        if let Some((term, used)) = self.weights.iter().min_by_key(|(term, used)| *used)
        .map(|(term,used)| (term.to_string(), *used)) { 
            self.weights.remove(&term);
            self.index.remove(&term);
        }
    }

    

    pub fn add_document(&mut self, doc_id: DocumentId, content: String, path: PathBuf) -> io::Result<()> { 
        self.docs.insert(doc_id, path);
        let lowercase_tokens = content.to_lowercase();
        let contents = lowercase_tokens.unicode_words().map(str::to_string);
        let mut tokens = Vec::new();
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
            *self.term_doc_freqs.entry(word.clone()).or_insert(0) += 1;
            self.index.entry(word.to_string()).or_default().push(doc_id);  
            let _ = self.weights.entry(word.clone()).or_insert(0).add(1);

        }
        self.term_freqs.insert(doc_id, freqs);
        self.docs_length.insert(doc_id, tokens.len());
        self.docs_count += 1;
        let config = bincode::config::standard();
        for (term, docs) in self.index.clone() { 
            let mut buf = vec![0u8; 1024];
            if let Ok(size) = bincode::encode_into_slice(docs, &mut buf, config){ 
                self.segment_store.write(term, &buf[..size]);
            } 
        }
        Ok(())
    }

    pub fn search(&mut self, term: String) -> Vec<&PathBuf>{ 
        if let Some(docs) = self.index.get(&term) {
            let _ = self.weights.entry(term.clone()).or_insert(0).add(1);
            return docs.iter().filter_map(|doc| self.docs.get(doc)).collect();
        }
        let config = bincode::config::standard();
        if let Ok(bytes) = self.segment_store.read_bytes(term.clone()) { 
            match bincode::decode_from_slice::<Vec<usize>, _>(&bytes, config) { 
                Ok((docs,_)) => { 
                    if self.index.len()  +  1 > self.cap { 
                        self.evict();
                        
                    }
                    self.index.insert(term.clone(), docs.clone());
                    let _ = self.weights.entry(term.clone()).or_insert(0).add(1);
                    return docs.iter().filter_map(|doc| self.docs.get(doc)).collect();
                },
                Err(_) => return vec![]
            }
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
        let _ = inverted_index.add_document(index, content.to_string(), path.to_path_buf());
    }
    
    
    

    let search_keys = vec!["rust", "awesome", "mountain", "lorem"];
     
    for key in search_keys { 
        println!("{:?}", inverted_index.search(key.to_string()));
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
