use crate::page::Page;

struct Buffer<'a> {
    page: Option<&'a Page>,
    page_position: usize,
    tx_id: i32,
    lsn: i32,
    pins: usize,
}

impl<'a> Buffer<'a> {
    pub fn new() -> Self {
        Self {
            page: None,
            page_position: 0,
            tx_id: -1,
            lsn: 1,
            pins: 0,
        }
    }

    pub fn pin(&mut self) {
        self.pins += 1;
    }

    pub fn unpin(&mut self) {
        debug_assert!(self.pins > 0);
        self.pins -= 1;
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn mark_modified(&mut self, tx_id: i32, lsn: i32) {
        self.tx_id = tx_id;
        if lsn > 0 {
            self.lsn = lsn;
        }
    }
}
