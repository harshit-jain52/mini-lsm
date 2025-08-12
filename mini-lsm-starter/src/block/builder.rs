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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn compute_key_overlap(&self, key: &[u8]) -> usize {
        let mut overlap = 0;
        let first_key = self.first_key.raw_ref();
        loop {
            if overlap >= key.len() || overlap >= first_key.len() {
                break;
            }
            if first_key[overlap] != key[overlap] {
                break;
            }
            overlap += 1;
        }
        overlap
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.is_empty() {
            let curr_block_size = self.data.len() + self.offsets.len() * 2 + 2; // 2 bytes for each offset and 2 bytes for num_of_elements
            if curr_block_size + key.len() + value.len() + 3 * 2 > self.block_size {
                return false;
            }
        }

        self.offsets.push(self.data.len() as u16); // Store the offset of the current key-value pair
        let overlap = self.compute_key_overlap(key.raw_ref());
        self.data.put_u16(overlap as u16); // Overlap length
        self.data.put_u16((key.len() - overlap) as u16); // Key length
        self.data.put(&key.raw_ref()[overlap..]); // Key data
        self.data.put_u16(value.len() as u16); // Value length
        self.data.put(value); // Value data

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
