use std::{fs::OpenOptions, io::{Error, Read}};

use super::page::PageCacheManager;
use std::{collections::HashMap, hash::Hash, io::{self, ErrorKind}, path::{Path, PathBuf}};


#[derive(Debug)]
pub struct SegmentStore { 
    page_cache: PageCacheManager,
    term_offsets : HashMap<String, (usize, usize)>
}

impl SegmentStore { 
    pub fn new(path: &Path, page_size: usize, cap: usize) -> io::Result<Self> { 
        let page_cache = PageCacheManager::new(path, page_size, cap)?;
        Ok(Self { 
            page_cache,
            term_offsets: HashMap::new()
        })
    }

    pub fn write(&mut self, term: String, data: &[u8]) -> Result<(), std::io::Error>{ 
        if let Some(offset) = self.write_bytes(data) { 
            return self.write_term_offsets(term, offset, data.len());
        } 
        Ok(())
    }

    pub fn write_bytes(&mut self, data: &[u8]) -> Option<usize>{ 
        self.page_cache.write(data)
        
    } 
    pub fn write_term_offsets(&mut self, term: String, offset: usize, size: usize) -> io::Result<()> { 
        self.term_offsets.insert(term, (offset, size));
        Ok(())
    }

    pub fn read_bytes(&mut self, term: String) -> std::io::Result<Vec<u8>> { 
        if let Some((offset, size)) = self.term_offsets.get(&term) { 
            match self.page_cache.read(*offset, *size) { 
                Ok(bytes) => return Ok(bytes.to_vec()),
                Err(err) => return Err(err)
            }          
        }
        Err(Error::new(ErrorKind::NotFound, "term not found"))
    }

    pub fn sync(&mut self) -> io::Result<()> { 
        self.page_cache.flush_all()
    }
}


//#[test]
pub fn test_segment_store() { 
    use bincode::Decode;
    let path = Path::new("./segments/index.seg");
    let segments_dir = Path::new("./segments");
    std::fs::create_dir_all(segments_dir);
    let mut store = SegmentStore::new(path, 4096, 16).unwrap();
    let terms = vec!["hello", "there", "world"];
    let len = terms.len();
    let docs_id1 = vec![2, 6, 5];
    let docs_id2 = vec![1, 4, 5];
    for term in terms.clone() { 
        
        if term == "there" { 
            let mut buf = vec![0u8; 1024];
            let size = bincode::encode_into_slice(docs_id2.clone(), &mut buf, bincode::config::standard()).unwrap();
            println!("buff length, {}", buf[..size].len());
            match store.write(term.to_string(), &buf[..size]) {
                Ok(_) => println!("written; "),
                Err(err) => println!("error writing : {err:?}"),
            }
            continue;
        }
        let mut buf = vec![0u8; 1024];
        let size = bincode::encode_into_slice(docs_id1.clone(), &mut buf, bincode::config::standard()).unwrap();
        println!("buff length, {}", buf[..size].len());
        match store.write(term.to_string(), &buf[..size]) {
            Ok(_) => println!("written; "),
            Err(err) => println!("error writing : {err:?}"),
        }
        
    }
    for i in 0..len { 
        if let Ok(bytes) = store.read_bytes(terms[i].to_string()) { 
            println!("bytes read : {bytes:?}");
            let (decoded, size): (Vec<i32>, _) = bincode::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
            println!("term: {} : docs id: {:?}", terms[i], decoded);
        } else if let Err(err) = store.read_bytes(terms[i].to_string()) { 
            println!("error: {:?} ", err);
        }
    }
    let mut file = OpenOptions::new().read(true).open(path).unwrap();
    let mut dummy_buf = vec![0u8; 4096];
    let  _= file.read_exact(&mut dummy_buf);
    println!("dummy buf : {:?}", dummy_buf);
}

