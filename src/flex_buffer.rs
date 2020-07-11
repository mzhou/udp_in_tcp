pub struct FlexBuffer<'a> {
    backing: &'a mut [u8],
    valid_first: usize,
    valid_len: usize,
}

impl FlexBuffer<'_> {
    pub fn new(backing: &mut [u8]) -> FlexBuffer {
        FlexBuffer {
            backing: backing,
            valid_first: 0,
            valid_len: 0,
        }
    }

    pub fn compact(self: &mut Self) -> () {
        self.backing
            .copy_within(self.valid_first..self.valid_first + self.valid_len, 0);
    }

    /// compact if can't fill to total of min valid bytes
    pub fn compact_if_less(self: &mut Self, min: usize) -> () {
        if self.backing.len() - self.valid_first < min {
            self.compact();
        }
    }
}

impl FlexBuffer<'_> {
    // bytes::buf::BufMut-ish
    pub fn remaining_mut(self: &Self) -> usize {
        self.backing.len() - (self.valid_first + self.valid_len)
    }

    pub fn advance_mut(self: &mut Self, cnt: usize) -> () {
        self.valid_len += cnt;
    }

    pub fn bytes_mut<'a>(self: &'a mut Self) -> &'a mut [u8] {
        &mut self.backing[self.valid_first + self.valid_len..]
    }
}

impl FlexBuffer<'_> {
    // bytes::buf::Buf-ish
    pub fn remaining(self: &Self) -> usize {
        self.valid_len
    }

    pub fn bytes(self: &Self) -> &[u8] {
        &self.backing[self.valid_first..self.valid_first + self.valid_len]
    }

    pub fn advance(self: &mut Self, cnt: usize) -> () {
        self.valid_first += cnt;
        self.valid_len -= cnt;
        if self.valid_len == 0 {
            self.valid_first = 0;
        }
    }
}
