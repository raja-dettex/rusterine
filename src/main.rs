use std::io::{Read, Write};
use std::{collections::HashMap, hash::Hash, io, path::PathBuf};

use bincode::error::DecodeError;
use bincode::Decode;
use serde::{Deserialize, Serialize};
use unicode_segmentation::UnicodeSegmentation;
use std::path::Path;
use std::fs::{File, OpenOptions};
type DocumentId = usize;

#[derive(Debug, Serialize, Deserialize, Decode)]
struct InvertedIndex { 
    pub index : HashMap<String, Vec<DocumentId>>,
    pub docs : HashMap<DocumentId, PathBuf>,
    pub term_doc_freqs: HashMap<String, usize>,
    pub term_freqs: HashMap<DocumentId, HashMap<String, usize>>,
    pub docs_length: HashMap<DocumentId, usize>,
    pub docs_count: usize
}

impl InvertedIndex { 
    pub fn new() -> Self { 
        Self { index : HashMap::new(), 
            docs: HashMap::new(),
            term_doc_freqs: HashMap::new(),
            term_freqs: HashMap::new(),
            docs_length: HashMap::new(),
            docs_count: 0
        }
    }
    pub fn save_to_disk(&mut self, path: &Path) -> io::Result<()> { 
        println!("saving to disk");
        let config = bincode::config::standard();
        if !std::path::Path::exists(path) { 
            std::fs::File::create(path).unwrap();
        }
        let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        match bincode::serde::encode_to_vec(self, config) { 
            Ok(encoded) => { 
                println!("encoded bytes are here: {:?}", encoded);
                let _ = file.write_all(&encoded).unwrap();
                println!("{} bytes are written to disk", encoded.len());
                file.flush().unwrap();
            },
            Err(err) => { println!("error serializing indexes : {:?}", err); } 
        }
        Ok(())

    }

    pub fn load_from_disk(path: &Path) -> Result<Self, DecodeError>{ 
        println!("reading from disk");
        let mut file = File::open(path).unwrap();
        let mut buffer = Vec::new();
        let size = file.read_to_end(&mut buffer).unwrap();
        match bincode::decode_from_slice(&buffer, bincode::config::standard()) { 
            Ok((index, _)) => { 
                return Ok(index)
            } , 
            Err(err) => return Err(err)
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
        for word in unique_words  { 
            *self.term_doc_freqs.entry(word.clone()).or_insert(0) += 1;
            self.index.entry(word.to_string()).or_default().push(doc_id);   
        }
        self.term_freqs.insert(doc_id, freqs);
        self.docs_length.insert(doc_id, tokens.len());
        self.docs_count += 1;
        Ok(())
    }

    pub fn search(&self, term: String) -> Vec<&PathBuf>{ 
        self.index.get(&term.to_lowercase())
        .unwrap_or(&vec![])
        .iter().filter_map(|doc_id| self.docs.get(doc_id)).collect()
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
    let index_file = std::path::Path::new("./index.bin");
    let inverted_index = if index_file.exists() { 
        match InvertedIndex::load_from_disk(index_file) {
            Ok(index ) => Some(index),
            Err(err) => {println!("err {:?}", err);
                None
            }
        }
    } else { 
        println!("file is not there");
        let mut inverted_index = InvertedIndex::new();
        for (index, (content, path)) in file_contents.iter().enumerate() { 
            let _ = inverted_index.add_document(index, content.to_string(), path.to_path_buf());
        }
        inverted_index.save_to_disk(index_file);
        Some(inverted_index)
    };
    println!("here is the inverted index: {:#?}", inverted_index);

    let search_keys = vec!["rust", "awesome", "mountain"];
    if let Some(index) = inverted_index { 
        for key in search_keys { 
            println!("{:?}", index.search(key.to_string()));
        }
    }; 
    Ok(())
}
