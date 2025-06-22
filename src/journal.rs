use std::{fs::{File, OpenOptions}, io::{self, BufRead, Error, ErrorKind, Read, Write}, path::Path};

use serde::{Deserialize, Serialize};


#[derive(Debug, Serialize, Deserialize)]
pub struct WAL {
    #[serde(skip)] 
    file: Option<File>,
    size: i32,        
    index: i32,
    file_path: String,
    dir_path: String,
    history : Vec<String>                         
}

impl WAL { 

    pub fn load_from_disk() -> io::Result<Self>{ 
        let file_path = "./wal.bin";
        if std::fs::exists(file_path)? && !std::fs::metadata(file_path)?.is_dir() { 
            let  mut file = File::open(file_path)?;
            let reader = io::BufReader::new(&file);
            let wal: Self = serde_json::from_reader(reader)?;
            return Ok(wal)
        }
        return Err(Error::new(io::ErrorKind::NotFound, "file npt found"))
    }
    pub fn new(size: i32, index: i32) -> io::Result<Self> { 
        if let Ok(wal) = Self::load_from_disk() { 
            println!("wal loaded from disk : {wal:?}");
            return Ok(wal);
        }
        let file_path = format!("./logger/wal{}.log", index);
        let dir_path = "./logger".to_string();
        
        match Self::create_file(dir_path.clone(), file_path.clone()){
            Ok(file) => { 
                let mut wal = Self{
                    file: Some(file),
                    size,
                    index: index,
                    file_path: file_path.clone(),
                    dir_path ,
                    history: Vec::new()     
                };
                wal.history.push(file_path.clone());
                wal.index += 1;
                Ok(wal)
            } 
            Err(err) => Err(err),
        }
        
    }

    pub fn create_file(dir_path:String, file_path: String) -> io::Result<File>{ 
        let dir_exists = match std::fs::exists(dir_path.clone()) { 
            Ok(val) => val,
            Err(_) => false
        };
        if !dir_exists { 
            if let Err(err) = std::fs::create_dir_all(dir_path) { 
                return Err(err);
            }
        }
        match std::fs::OpenOptions::new().create(true).write(true).append(true).open(file_path.clone()) {
            Ok(file) => Ok(file)  ,
            Err(err) => Err(err),
        }
    }

    pub fn flash_snapshot_to_disk(&self) -> io::Result<()> { 
        let file_path = "./wal.bin";
        println!("pushing snapshots");
        
        let mut file = OpenOptions::new().read(true).write(true).create(true).open(file_path)?;
        let writer = io::BufWriter::new(&file);
        let _ = serde_json::to_writer(writer, self)?;
        let _ = file.flush()?;
        Ok(())
    }


    pub fn log(&mut self, mut record: String) -> io::Result<()>{ 
        if self.file.is_none() { 
            if let Ok(file) = Self::create_file(self.dir_path.clone(), self.file_path.clone()) { 
                self.file = Some(file);   
                    
                let _ = self.flash_snapshot_to_disk();
            }
        }
        let size = std::fs::metadata(self.file_path.clone())?.len();
        println!("file size :{}", size as usize);
        if size as usize + record.as_bytes().len() >= 100 { 
            let filepath = format!("./logger/wal{}.log", self.index);
            self.index += 1;
            let mut file  = Self::create_file(self.dir_path.clone(), filepath.clone())?;
            self.history.push(filepath.clone());
            self.file_path = filepath;
            self.file = Some(file);
            let _ = self.flash_snapshot_to_disk();         
        }
        
        let mut file = self.file.as_ref().unwrap();    
        
        if !record.ends_with('\n') { 
            record.push('\n');
        }
        let _ = file.write_all(record.as_bytes());
        let _ = file.flush();
        Ok(())
    }
    pub fn find_last_page_last_written_offset(&mut self)  -> (usize, usize){ 
        if self.read_records().is_empty() { 
            return (0, 0)
        }
        match self.read_records().iter().map(|line| { 
            let splits: Vec<&str> = line.split(",").collect();
            let offset : usize = splits[1].parse().expect("expected a usize");
            let size : usize = splits[2].parse().expect("expected a usize");
            (offset, size)
        }).max_by_key(|(offset,usize)| *offset) {
            Some((offset, size)) => (offset, size),
            None => (0, 0),
        }
    }
    pub fn read_records(&mut self) -> Vec<String> {
        println!("file_history :{:?}", self.history);
        let records : Vec<String> = self.history.iter().
            map(|filepath| File::open(filepath.clone())).
            filter(|file| file.is_ok()).map(Result::unwrap).
            flat_map(|file| { 
                let mut records = Vec::new();
                let mut reader = io::BufReader::new(file);
                let mut line = String::new();
                while reader.read_line(&mut line).expect("expected to read") != 0 { 
                    if line.ends_with('\n') {line.pop();}
                    records.push(line.clone());
                    line.clear();
                }
                records
            }).collect();
        records
    }
}


#[test]
pub fn test_wal() { 
    let mut wal = WAL::new(4096, 0).unwrap();
    let _ = wal.log("this is first record".to_string());
    let _ = wal.log("this is second record".to_string());
    let _ = wal.log("this is third record".to_string());
    let _ = wal.log("this is fourth record".to_string());

    let _ = wal.log("this is fifth record".to_string());
    let _ = wal.log("this is sixth record".to_string());
    let _ = wal.log("this is seventh record".to_string());
    let _ = wal.log("this is eighth record".to_string());
    
    let result = wal.read_records();
    println!("result: {result:?}");    
}

