use std::{collections::HashMap, fs::{self, File}, io::{self, Bytes, ErrorKind, Read, SeekFrom, Write}, os::unix::fs::FileExt, path::PathBuf};
use std::io::Seek;

use std::io::Error;

#[derive(Debug, Clone)]
pub struct Page { 
    page_id: usize,
    data: Vec<u8>,
    is_dirty: bool,
    last_used : usize,
    last_written_offset: usize
}

impl Page { 
    pub fn new(page_id: usize, is_dirty: bool, last_used: usize) -> Self { 
        Self { 
            page_id,
            data: vec![0u8; 4096],
            is_dirty,
            last_used,
            last_written_offset: 0
        }
    }

    pub fn open(page_id: usize, buff: &[u8], is_dirty: bool, last_used: usize, last_written_offset: usize) -> Self { 
        Self { 
            page_id,
            data: buff.to_vec(),
            is_dirty,
            last_used,
            last_written_offset
        }
    }

    


    pub fn write(&mut self, data: &[u8]) -> usize { 
        println!("data len: {} data: {:?}", data.len(), data);
        let offset = self.last_written_offset;

        println!("first offset : {offset}");
        self.data[offset..offset+data.len()].copy_from_slice(&data[..]);
        self.last_written_offset = offset + data.len();
        self.is_dirty = true;
        return offset;
    }

    pub fn read(&self, offset: usize, size: usize) -> &[u8] { 
        println!("reading from page");
        &self.data[offset..offset+size]
    }
}

#[derive(Debug)]
pub struct PageCacheManager { 
    pages: HashMap<usize, Page>,
    cap: usize,
    page_size: usize,
    usage_counter : usize,
    file: File,
    next_page_id : usize ,
    last_page_offset_and_size: (usize, usize)
}

impl PageCacheManager { 
    pub fn new(path: &std::path::Path, page_size: usize, cap: usize, last_page_offset_and_size: (usize, usize)) -> std::io::Result<Self>{ 
        let next_page_id = last_page_offset_and_size.0 / page_size + 1;
        match fs::OpenOptions::new()
            .read(true).write(true).create(true).open(path) { 
            Ok(file) => { 
                return Ok(Self { 
                    pages: HashMap::new(),
                    cap,
                    page_size,
                    usage_counter: 0,
                    file,
                    next_page_id,
                    last_page_offset_and_size
                });
            },
            Err(err) => { 
                return Err(err)
            }
        }
        
    }

    pub fn write(&mut self, data: &[u8]) -> Option<(usize, usize, usize)>{ 
        // check if  the last page are full ;
        self.usage_counter += 1;
        let page_size = self.page_size;
        let last_used = self.usage_counter;
        let offset = self.last_page_offset_and_size.0;
        let size = self.last_page_offset_and_size.1;
        let within_page_offset = offset % self.page_size;
        let page = Page::open(self.next_page_id - 1, &vec![0u8; self.page_size], false, last_used, within_page_offset + size);
        if !self.pages.contains_key(&(self.next_page_id - 1)) { 
            self.pages.insert(self.next_page_id -1, page);
        }
        
        if let Some((last_page_id, writable_id, page_written_offset, page_last_written_offset, size)) = if let Some(last_page_id) = self.pages.keys().max().copied() { 
            // check if the last page is full else write to the last page 
            println!("last page id: {last_page_id}");
            if let Ok(page) = self.get_page(last_page_id) { 
                println!("found the page");
                let available_space = page_size - page.last_written_offset;
                println!("available space :{available_space}");
                if available_space >= data.len() { 
                    let page_written_offset = page.write(data);
                    page.last_used = last_used;
                    // match self.flush(last_page_id) { 
                    //     Ok(_) => { 
                    //         page.is_dirty = false;
                    //     },
                    //     Err(err) => { 
                    //         println!("error flushing  :{err:?}");
                    //     }
                    // }
                    let writable_id = if last_page_id != 0 { 
                        last_page_id -1
                    } else { last_page_id};
                    page.is_dirty = true;
                    Some((last_page_id,writable_id, page_written_offset, page.last_written_offset, data.len()))
                     
                     
                    
                }  else { None}
            } else { None}
        }  else { None} { 
            let _ = self.flush(last_page_id, page_written_offset, writable_id);
            return Some((page_written_offset as usize + ( last_page_id * 4096),
                                    (writable_id * 4096) + page_last_written_offset,
                                    data.len()
                                    ))
        }
        if self.pages.len() >= self.cap { 
            println!("evicting");
            self.evict();
        }
        let page_id = self.next_page_id;
        println!("page id: {page_id}");
        self.next_page_id += 1;
        let mut page = Page::new(page_id, true, self.usage_counter);
        let new_page_last_written_offset = page.write(data);
        let writable_id = if page_id != 0 { 
            page_id -1
        } else { page_id};
        match self.flush(page_id, new_page_last_written_offset, writable_id) { 
            Ok(_) => { 
                println!("flushed the page");
                page.is_dirty = false;
            },
            Err(err) => { 
                println!("error flushing  :{err:?}");
            }
        }
        
        self.pages.insert(page_id, page.clone());
        Some((offset as usize + ( page_id * self.page_size),
        (writable_id * self.page_size) + page.last_written_offset, data.len()))
    }
    pub fn update_last_page_offset(&mut self, offset: usize, size: usize) { 
        self.last_page_offset_and_size = (offset, size);
    }

    pub fn read(&mut self, offset: usize, size: usize) -> io::Result<&[u8]> { 
        println!("offset and size are {}, {}", offset, size);
        let page_id = (offset/ self.page_size);
        let within_page_offset = (offset as usize % self.page_size) as usize;
        
        if !self.pages.contains_key(&page_id){ 
            println!("page does not contain a key");
            let _ = self.file.seek(SeekFrom::Start(offset as u64))?;
            let mut buf = vec![0u8; size];
            
            let _ = self.file.read(&mut buf)?;
            println!("buf are : {buf:?}");
            let mut data = vec![0u8; self.page_size];
            data[within_page_offset..within_page_offset+size].copy_from_slice(&buf);
            let last_written_offset = Self::find_last_written_offset(&data);
            let page = Page::open(page_id, &data[..], false, self.usage_counter, last_written_offset);
            let _ = self.pages.insert(page_id, page);
            //println!("pages are : {:?}", self.pages);
        }
     
        let needs_update = {
            let page = self.pages.get(&page_id).expect("Page must exist");
            !Self::is_page_bytes_written(&page.data[within_page_offset..within_page_offset + size])
        };
    
        // Phase 3: If update needed, read from file and write to page
        if needs_update {
            let mut buf = vec![0u8; size];
    
            // Scope file read
            {
                self.file.seek(SeekFrom::Start(offset as u64))?;
                self.file.read(&mut buf)?;
                println!("buf are : {buf:?}");
            }
    
            // Scope page update
            {
                let page = self.pages.get_mut(&page_id).expect("Page must exist");
                page.data[within_page_offset..within_page_offset + size]
                    .copy_from_slice(&buf);
            }
        }
    
        // Phase 4: Finally return a read slice
        println!("from page hit");
        let page = self.pages.get(&page_id).expect("Page must exist");
        println!("after page hit");
        let data = page.read(within_page_offset,size);
        println!("read data is {data:?}");
        Ok(data)
        
    }

    pub fn evict(&mut self) -> std::io::Result<()> { 
        if self.pages.len() >= self.cap { 
            
            if let Some(id) = self.pages.iter().min_by_key(|(_, page)| page.last_used)
            .map(|(&id,_)| id) { 
                // flush the page if dirty and next remove from cache 
                //self.flush(id);
                self.pages.remove(&id);
            }
        }
        Ok(())
    }

    pub fn flush(&mut self, id: usize, offset: usize, writable_id: usize) -> std::io::Result<()>{ 
        println!("flushing bytes");
        
        if let Some(page) = self.pages.get(&id) { 
            if page.is_dirty { 
                let begin_offset = (writable_id * self.page_size) + offset;
                let end_offset = (writable_id* self.page_size) +  page.last_written_offset;
                self.file.seek(SeekFrom::Start(begin_offset as u64));
                self.file.write_all(&page.data[begin_offset..end_offset])?;
                self.file.flush();
            } 
        }
        Ok(())
    }

    pub fn flush_all(&mut self) -> io::Result<()>{ 
        for id in self.pages.keys().copied().collect::<Vec<_>>() { 
            //self.flush(id);
        }
        Ok(())
    }

    pub fn get_page(&mut self,  id: usize) -> io::Result<&mut Page> { 
        self.usage_counter += 1;
        if !self.pages.contains_key(&id) { 
            let offset = (id * self.page_size) as u64;
            let mut buf = vec![0u8; self.page_size];
            match self.file.seek(SeekFrom::Start(offset)) { 
                Ok(_) => { 
                    match self.file.read(&mut buf) { 
                        Ok(_) => { 
                            let last_written_offset = Self::find_last_written_offset(&buf);
                            println!("last written offset: {last_written_offset}");
                            let page = Page::open(id, &buf, false, self.usage_counter, last_written_offset);
                            if self.pages.len() >= self.cap { 
                                self.evict();
                            }
                            self.pages.insert(id, page);
                            return Ok(self.pages.get_mut(&id).unwrap());
                        },
                        Err(err) => { 
                            if err.kind() == ErrorKind::UnexpectedEof { 
                                return Err(err)
                            }
                            return Err(err)       
                        }
                    }
                } ,
                Err(err) => { 
                    if err.kind() == ErrorKind::NotSeekable { 
                        return Err(err)
                    }
                    return Err(err)
                }
            } 
            
            
            
        }
        Ok(self.pages.get_mut(&id).unwrap())
    }
    fn find_last_written_offset(buf: &[u8]) -> usize {
        // Find the last non-zero byte
        buf.iter()
            .rposition(|&b| b != 0)
            .map(|pos| pos + 1) // +1 because offset is exclusive
            .unwrap_or(0)
    }
    fn is_page_bytes_written(buf: &[u8]) -> bool { 
        let thresold = buf.len() / 2;
        let total_written = buf.iter()
            .rposition(|&b| b != 0)
            .map(|pos| pos + 1) // +1 because offset is exclusive
            .unwrap_or(0);
        return total_written >= thresold
    }

    pub fn mark_dirty(&mut self, id: usize) -> std::io::Result<()> { 
        if let Some(page) = self.pages.get_mut(&id) { 
            page.is_dirty = true;
        }
        Ok(())
    }   
}

// #[test]
// fn mmap_test() { 
//     let path = std::path::Path::new("./src/documents/doc3.txt");
//     let mut cache_manager = PageCacheManager::new(path.into(), 4096, 16).unwrap();
//     let page0 = cache_manager.get_page(0).unwrap();
//     let data1 = String::from_utf8_lossy(&page0.data).to_string();
//     println!("page 0: {}", data1);    
//     // let page1 = cache_manager.get_page(1).unwrap();
//     // let data2 = String::from_utf8_lossy(&page1.data).to_string();
//     // println!("page 1: {}", data2) ;   
//     // println!("page 0 len : {} and page1 len: {}", data1.len(), data2.len());
//     // modify first page
//     let test_data = b"this is modified test data";
    
//     page0.data[..test_data.len()].copy_from_slice(test_data);
//     page0.is_dirty = true;
//     cache_manager.flush(0).unwrap();
    
//     let again_page0 = cache_manager.get_page(0).unwrap();
//     let data1 = String::from_utf8_lossy(&again_page0.data).to_string();
//      println!("modified page 0: {}", data1);    

//     //  
    
// }

//here i have a situation in this get_page method , i will explain u what 
//there might be two scenarios 1 : the page is evicted 2: the page itself is not created , so if the page is not created how can i handle the exception of reading exact and seeking and if the page is there already i.e the bytes have already been written to file that case seek and read exact will not throw an error in that case how can i get the last written offset