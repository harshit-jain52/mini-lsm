// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod builder;
mod iterator;

use crate::key::KeyVec;
pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    /*
    ----------------------------------------------------------------------------------------------------
    |             Data Section             |              Offset Section             |      Extra      |
    ----------------------------------------------------------------------------------------------------
    | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
    ----------------------------------------------------------------------------------------------------

    -----------------------------------------------------------------------
    |                           Entry #1                            | ... |
    -----------------------------------------------------------------------
    | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
    -----------------------------------------------------------------------

    -------------------------------
    |offset|offset|num_of_elements|
    -------------------------------
    |   0  |  12  |       2       |
    -------------------------------
    */
    pub fn encode(&self) -> Bytes {
        let mut encoded_data = self.data.clone();
        let num_of_elements = self.offsets.len();
        for offset in &self.offsets {
            encoded_data.put_u16(*offset);
        }
        encoded_data.put_u16(num_of_elements as u16);
        Bytes::from(encoded_data)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - 2..]).get_u16() as usize;
        let data_end = data.len() - 2 - num_of_elements * 2;

        Self {
            data: data[..data_end].to_vec(),
            offsets: data[data_end..data.len() - 2]
                .chunks_exact(2)
                .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                .collect::<Vec<u16>>(),
        }
    }

    pub fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        buf.get_u16(); // Skip the overlap length
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        KeyVec::from_vec(key.to_vec())
    }
}
