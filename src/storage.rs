use std::{fs::OpenOptions, io::{Error, Read}};

use super::page::PageCacheManager;
use std::{collections::HashMap, hash::Hash, io::{self, ErrorKind}, path::{Path, PathBuf}};
use super::journal::WAL;

#[derive(Debug)]
pub struct SegmentStore { 
    page_cache: PageCacheManager,
    term_offsets : HashMap<String, Vec<(usize, usize)>>,
    wal: WAL
}

impl SegmentStore { 
    pub fn new(path: &Path, page_size: usize, cap: usize) -> io::Result<Self> { 
    
        let mut wal = WAL::new(4096, 0)?;
        let records = wal.read_records();
        let last_page_offset_and_size = wal.find_last_page_last_written_offset();
        let page_cache = PageCacheManager::new(path, page_size, cap, last_page_offset_and_size)?;
        
        
        let mut term_offsets: HashMap<String, Vec<(usize, usize)>> = HashMap::new();
        for record in records { 
            let each_record_str:  Vec<String> = record.split(",").map(|s| s.to_string()).collect();
            let offset : usize = each_record_str[1].parse().unwrap();
            let size: usize = each_record_str[2].parse().unwrap();
            term_offsets.entry(each_record_str[0].clone()).or_default().push((offset, size));
        }
        let storage = Self { 
            page_cache,
            term_offsets,
            wal
        };
        //println!("storage :{storage:?}");
        Ok(storage)
    }

    pub fn write(&mut self, term: String, data: &[u8]) -> Result<(usize, usize), std::io::Error>{ 
        if let Some((offset, page_cache_offset, size)) = self.write_bytes(data) { 
            self.page_cache.update_last_page_offset(page_cache_offset, size);
            return self.write_term_offsets(term, offset, data.len());
        } 
        Err(Error::new(ErrorKind::WriteZero, "unable to write"))
    }

    pub fn write_bytes(&mut self, data: &[u8]) -> Option<(usize, usize, usize)>{ 
        self.page_cache.write(data)
        
    } 
    pub fn write_term_offsets(&mut self, term: String, offset: usize, size: usize) -> io::Result<(usize,  usize)> { 
        self.term_offsets.entry(term.clone()).or_default().push( (offset, size));
        // also update the value of last_page offset and size in page cache manager
        self.page_cache.update_last_page_offset(offset, size);
        let record = format!("{},{},{}", term.clone(), offset, size);
        println!("logging to journal");
        let _ = self.wal.log(record)?;
        Ok((offset, size))
    }

    pub fn read_bytes(&mut self, term: String) -> std::io::Result<Vec<Vec<u8>>> { 
        if let Some(metadata) = self.term_offsets.get(&term) {
            let mut bytes_vec = Vec::new(); 

            for (offset, size) in metadata {
                match self.page_cache.read(*offset, *size) { 
                    Ok(bytes) => bytes_vec.push(bytes.to_vec()),
                    Err(err) => eprintln!("{err:?}")
                }
            } 
            return Ok(bytes_vec)
                      
        }
        Err(Error::new(ErrorKind::NotFound, "term not found"))
    }

    pub fn sync(&mut self) -> io::Result<()> { 
        self.page_cache.flush_all()
    }
}


//#[test]
/*  
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

*/ 