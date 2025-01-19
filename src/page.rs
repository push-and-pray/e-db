use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, Read, Seek, SeekFrom};

pub struct Page {
    data: Vec<u8>,
}

impl Page {
    pub fn new(page_size: usize) -> Self {
        Self {
            data: vec![0; page_size],
        }
    }

    pub fn from_vec(data: Vec<u8>, page_size: usize) -> Self {
        assert_eq!(data.len(), page_size);
        Self { data }
    }

    pub fn read(&self) -> &Vec<u8> {
        &self.data
    }

    pub fn mutate(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
}

pub struct PageManager {
    file: File,
    page_size: usize,
}

impl PageManager {
    pub fn new(path: &str, page_size: usize) -> Result<Self, io::Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Self { file, page_size })
    }
}

impl PageManager {
    pub fn read_page(&mut self, page_nr: usize) -> Result<Page, io::Error> {
        let mut buf = vec![0; self.page_size];
        let offset = (page_nr * self.page_size).try_into().unwrap();

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;

        Ok(Page { data: buf })
    }

    pub fn write_page(&mut self, page_nr: usize, page: Page) -> Result<(), io::Error> {
        assert_eq!(
            page.data.len(),
            self.page_size,
            "Tried to write a page with wrong size"
        );
        let offset = (page_nr * self.page_size).try_into().unwrap();
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.data)
    }
}
