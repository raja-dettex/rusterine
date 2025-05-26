use std::{collections::HashMap, fs::{self, File}, io::{Bytes, Read, SeekFrom, Write}, os::unix::fs::FileExt, path::PathBuf};
use std::io::Seek;


pub struct Page { 
    page_id: usize,
    data: Vec<u8>,
    is_dirty: bool,
    last_used : usize
}

pub struct PageCacheManager { 
    pages: HashMap<usize, Page>,
    cap: usize,
    page_size: usize,
    usage_counter : usize,
    file: File
}

impl PageCacheManager { 
    pub fn new(path: &std::path::Path, page_size: usize, cap: usize) -> std::io::Result<Self>{ 
        match fs::OpenOptions::new()
            .read(true).write(true).create(true).open(path) { 
            Ok(file) => { 
                return Ok(Self { 
                    pages: HashMap::new(),
                    cap,
                    page_size,
                    usage_counter: 0,
                    file
                });
            },
            Err(err) => { 
                return Err(err)
            }
        }
        
    }

    pub fn evict(&mut self) -> std::io::Result<()> { 
        if self.pages.len() >= self.cap { 
            
            if let Some(id) = self.pages.iter().min_by_key(|(_, page)| page.last_used)
            .map(|(&id,_)| id) { 
                // flush the page if dirty and next remove from cache 
                self.flush(id);
                self.pages.remove(&id);
            }
        }
        Ok(())
    }

    pub fn flush(&mut self, id: usize) -> std::io::Result<()>{ 
        if let Some(page) = self.pages.get(&id) { 
            let offset = (id * self.page_size) as u64;
            self.file.seek(SeekFrom::Start(offset));
            self.file.write_all(&page.data)?;
        }
        Ok(())
    }

    pub fn flush_all(&mut self) { 
        for id in self.pages.keys().copied().collect::<Vec<_>>() { 
            self.flush(id);
        }
    }

    pub fn get_page(&mut self,  id: usize) -> std::io::Result<&mut Page> { 
        self.usage_counter += 1;
        if !self.pages.contains_key(&id) { 
            let offset = (id * self.page_size) as u64;
            let mut buf = vec![0u8; self.page_size];
            self.file.seek(SeekFrom::Start(offset));
            self.file.read_exact(&mut buf);
            
            let page = Page { 
                page_id: id, 
                data: buf,
                is_dirty: false,
                last_used: self.usage_counter
            };
            self.pages.insert(id, page);
            
        }
        let page = self.pages.get_mut(&id).unwrap();
        Ok(page)
    }

    pub fn mark_dirty(&mut self, id: usize) -> std::io::Result<()> { 
        if let Some(page) = self.pages.get_mut(&id) { 
            page.is_dirty = true;
        }
        Ok(())
    }   
}

#[test]
fn mmap_test() { 
    let path = std::path::Path::new("./src/documents/doc3.txt");
    let mut cache_manager = PageCacheManager::new(path.into(), 4096, 16).unwrap();
    let page0 = cache_manager.get_page(0).unwrap();
    let data1 = String::from_utf8_lossy(&page0.data).to_string();
    println!("page 0: {}", data1);    
    // let page1 = cache_manager.get_page(1).unwrap();
    // let data2 = String::from_utf8_lossy(&page1.data).to_string();
    // println!("page 1: {}", data2) ;   
    // println!("page 0 len : {} and page1 len: {}", data1.len(), data2.len());
    // modify first page
    let test_data = b"this is modified test data";
    
    page0.data[..test_data.len()].copy_from_slice(test_data);
    page0.is_dirty = true;
    cache_manager.flush(0).unwrap();
    
    let again_page0 = cache_manager.get_page(0).unwrap();
    let data1 = String::from_utf8_lossy(&again_page0.data).to_string();
     println!("modified page 0: {}", data1);    

    //  
    
}