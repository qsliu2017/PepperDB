use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
};

#[derive(Eq, PartialEq, PartialOrd)]
struct LRUKNode {
    history: VecDeque<usize>,
    k: usize,
    evictable: bool,
}

impl LRUKNode {
    fn new_with_record(k: usize, timestamp: usize) -> Self {
        Self {
            history: VecDeque::from([timestamp]),
            k,
            evictable: true,
        }
    }
    fn k_distance(&self) -> (usize, usize) {
        (
            self.history.len(),
            self.history.back().copied().unwrap_or(usize::MIN),
        )
    }
    fn record_access(&mut self, timestamp: usize) {
        if self.history.len() == self.k {
            self.history.pop_back();
        }
        self.history.push_front(timestamp);
    }
}

impl Ord for LRUKNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.evictable, other.evictable) {
            (false, _) => std::cmp::Ordering::Less,
            (_, false) => std::cmp::Ordering::Greater,
            (_, _) => self.k_distance().cmp(&other.k_distance()).reverse(),
        }
    }
}

pub enum AccessType {
    Unknown,
    Lookup,
    Scan,
    Index,
}

pub struct LRUKReplacer {
    k: usize,
    frame: HashMap<usize, LRUKNode>,
    timestamp: usize,
}

impl LRUKReplacer {
    pub fn new(_: usize, k: usize) -> Self {
        LRUKReplacer {
            k,
            frame: HashMap::new(),
            timestamp: 0,
        }
    }

    pub fn evict(&mut self) -> Result<usize, ()> {
        self.frame
            .iter()
            .filter(|(_, node)| node.evictable)
            .min_by_key(|(_, v)| v.k_distance())
            .map(|(&k, _)| k)
            .ok_or(())
            .and_then(|i| {
                self.frame.remove(&i);
                Ok(i)
            })
    }

    pub fn record_access_of_type(&mut self, frame_id: usize, _: AccessType) {
        self.timestamp += 1;
        if !self.frame.contains_key(&frame_id) {
            self.frame
                .insert(frame_id, LRUKNode::new_with_record(self.k, self.timestamp));
        } else {
            self.frame
                .get_mut(&frame_id)
                .unwrap()
                .record_access(self.timestamp);
        }
    }

    pub fn record_access(&mut self, frame_id: usize) {
        self.record_access_of_type(frame_id, AccessType::Unknown)
    }

    pub fn set_evictable(&mut self, frame_id: usize, evictable: bool) {
        self.frame.get_mut(&frame_id).and_then(|frame| {
            frame.evictable = evictable;
            Some(())
        });
    }

    pub fn remove(&mut self, frame_id: usize) {
        self.frame.remove(&frame_id);
    }

    pub fn size(&self) -> usize {
        self.frame.iter().filter(|(_, node)| node.evictable).count()
    }
}

impl Debug for LRUKReplacer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LRUKReplacer")
            .field("k", &self.k)
            .field(
                "frame",
                &self
                    .frame
                    .iter()
                    .map(|(k, v)| (k, v.k_distance(), v.evictable))
                    .collect::<Vec<_>>(),
            )
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

impl Drop for LRUKReplacer {
    fn drop(&mut self) {}
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn lru_replacer_test() {
        let mut lru_replacer = LRUKReplacer::new(10, 3);

        // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
        (1..=6).for_each(|i| lru_replacer.record_access(i));
        (1..=5).for_each(|i| lru_replacer.set_evictable(i, true));
        lru_replacer.set_evictable(6, false);
        assert_eq!(5, lru_replacer.size());

        // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
        // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
        lru_replacer.record_access(1);

        // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
        // first based on LRU.
        (2..=4).for_each(|expect| {
            let i = lru_replacer.evict().unwrap();
            assert_eq!(expect, i);
        });
        assert_eq!(2, lru_replacer.size());

        // Scenario: Now replacer has frames [5,1].
        // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
        [3, 4, 5, 4]
            .into_iter()
            .for_each(|i| lru_replacer.record_access(i));
        [3, 4]
            .into_iter()
            .for_each(|i| lru_replacer.set_evictable(i, true));
        assert_eq!(4, lru_replacer.size());

        // Scenario: continue looking for victims. We expect 3 to be evicted next.
        let i = lru_replacer.evict().unwrap();
        assert_eq!(3, i);
        assert_eq!(3, lru_replacer.size());

        // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
        lru_replacer.set_evictable(6, true);
        assert_eq!(4, lru_replacer.size());
        let i = lru_replacer.evict().unwrap();
        assert_eq!(6, i);
        assert_eq!(3, lru_replacer.size());

        // Now we have [1,5,4]. Continue looking for victims.
        lru_replacer.set_evictable(1, false);
        assert_eq!(2, lru_replacer.size());
        let i = lru_replacer.evict().unwrap();
        assert_eq!(5, i);
        assert_eq!(1, lru_replacer.size());

        // Update access history for 1. Now we have [4,1]. Next victim is 4.
        lru_replacer.record_access(1);
        lru_replacer.record_access(1);
        lru_replacer.set_evictable(1, true);
        assert_eq!(2, lru_replacer.size());
        let i = lru_replacer.evict().unwrap();
        assert_eq!(4, i);

        assert_eq!(1, lru_replacer.size());
        let i = lru_replacer.evict().unwrap();
        assert_eq!(1, i);
        assert_eq!(0, lru_replacer.size());

        // This operation should not modify size
        lru_replacer.evict().unwrap_err();
        assert_eq!(0, lru_replacer.size());
    }
}
