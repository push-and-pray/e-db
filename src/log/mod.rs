/*
The Log Manager appends binary data to a file. The file consists of multiple pages which have the following format
--------------------------------------------------
| offset (4 bytes) |      free space      | data |
--------------------------------------------------

Data grows from left to right. The offset points to the end of the free data. This makes it easy for readers to read newests log first
*/

use std::io;

use crate::page::{Page, PageManager};

pub struct LogManager {
    log: PageManager,
    tail: Page,
    latest_lsn: u32,
    latest_flushed_lsn: u32,
}

impl Page {
    fn set_offset<T>(&mut self, offset: T)
    where
        T: TryInto<u16>,
        T::Error: std::fmt::Debug,
    {
        let offset: u16 = offset
            .try_into()
            .expect("Offset couldnt be converted to u16");
        self.mutate()[..2].copy_from_slice(&offset.to_be_bytes())
    }

    fn get_offset(&self) -> u16 {
        u16::from_be_bytes(self.read()[..2].try_into().expect("Slice is too small"))
    }
}

impl LogManager {
    pub fn new(path: &str, page_size: usize) -> Result<Self, io::Error> {
        let mut pm = PageManager::new(path, page_size)?;
        let logsize = pm.file.metadata()?.len();

        // Generate new tail if log hasnt been initialized. Else, load tail from last page
        let tail = if logsize == 0 {
            let mut page = Page::new(0, page_size);
            page.set_offset(page_size);
            page
        } else {
            pm.read_page(pm.n_pages()? - 1)?
        };

        Ok(Self {
            log: pm,
            tail,
            latest_lsn: 0,
            latest_flushed_lsn: 0,
        })
    }

    pub fn flush_since_lsn(&mut self, lsn: u32) -> Result<(), io::Error> {
        if lsn >= self.latest_flushed_lsn {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), io::Error> {
        let result = self.log.write_page(&self.tail);
        self.latest_flushed_lsn = self.latest_lsn;
        result
    }

    pub fn append(&mut self, data: &[u8]) -> Result<(), io::Error> {
        println!("{}", self.latest_lsn);
        println!("{}", self.latest_flushed_lsn);
        let mut offset = self.tail.get_offset() as usize;
        let freespace = offset - size_of::<u16>();

        if data.len() > (self.log.page_size - size_of::<u16>()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "log data is larger than maximum page size",
            ));
        };

        if freespace < data.len() {
            self.flush()?;
            self.tail = Page::new(self.tail.position + 1, self.log.page_size);
            self.tail.set_offset(self.log.page_size);
            offset = self.log.page_size;
        }
        let new_offset = offset - data.len();
        self.tail.mutate()[new_offset..offset].copy_from_slice(data);
        self.tail.set_offset(new_offset);
        self.latest_lsn += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const PAGESIZE: usize = 64;

    #[test]
    fn offset_setter_getter() {
        let mut page = Page::new(0, PAGESIZE);
        page.set_offset(PAGESIZE);
        assert_eq!(page.get_offset(), 64);
    }
}
