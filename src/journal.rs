use std::{fs::File, io::{self, BufRead, Error, Read, Write}, path::Path};

struct WAL { 
    file: Option<File>,
    size: i32,        
    index: i32,
    file_path: String,
    dir_path: String,
    history : Vec<String>                         
}

impl WAL { 
    pub fn new(size: i32, index: i32) -> io::Result<Self> { 
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


    pub fn log(&mut self, mut record: String) -> io::Result<()>{ 
        if self.file.is_none() { 
            if let Ok(file) = Self::create_file(self.dir_path.clone(), self.file_path.clone()) { 
                self.file = Some(file);       
            }
        }
        let size = std::fs::metadata(self.file_path.clone())?.len();
        println!("file size :{}", size);
        if size as usize + record.as_bytes().len() >= 100 { 
            let filepath = format!("./logger/wal{}.log", self.index);
            let mut file  = Self::create_file(self.dir_path.clone(), filepath.clone())?;
            self.history.push(filepath.clone());
            self.file_path = filepath;
            self.file = Some(file)         
        }
        
        let mut file = self.file.as_ref().unwrap();    
        
        if !record.ends_with('\n') { 
            record.push('\n');
        }
        let _ = file.write_all(record.as_bytes());
        let _ = file.flush();
        Ok(())
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
                    println!("line: {line}");
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

