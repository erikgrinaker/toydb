use super::{Range, Scan, Store};
use crate::error::{Error, Result};

use std::cmp::Ordering;
use std::fmt::Display;
use std::mem::replace;
use std::ops::{Bound, Deref, DerefMut};
use std::sync::{Arc, RwLock};

/// The default B+tree order, i.e. maximum number of children per node.
const DEFAULT_ORDER: usize = 8;

/// In-memory key-value store using a B+tree. The B+tree is a variant of a binary search tree with
/// lookup keys in inner nodes and actual key/value pairs on the leaf nodes. Each node has several
/// children and search keys, to make use of cache locality, up to a maximum known as the tree's
/// order. Leaf and inner nodes contain between order/2 and order items, and will be split, rotated,
/// or merged as appropriate, while the root node can have between 0 and order children.
///
/// This implementation differs from a standard B+tree in that leaf nodes do not have pointers to
/// the sibling leaf nodes. Iterator traversal is instead done via lookups from the root node. This
/// has O(log n) complexity rather than O(1) for iterators, but is left as a future performance
/// optimization if it is shown to be necessary.
pub struct Memory {
    /// The tree root, guarded by an RwLock to support multiple iterators across it.
    root: Arc<RwLock<Node>>,
}

impl Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl Memory {
    /// Creates a new in-memory store using the default order.
    pub fn new() -> Self {
        Self::new_with_order(DEFAULT_ORDER).unwrap()
    }

    /// Creates a new in-memory store using the given order.
    pub fn new_with_order(order: usize) -> Result<Self> {
        if order < 2 {
            return Err(Error::Internal("Order must be at least 2".into()));
        }
        Ok(Self { root: Arc::new(RwLock::new(Node::Root(Children::new(order)))) })
    }
}

impl Store for Memory {
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.root.write()?.delete(key);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.root.read()?.get(key))
    }

    fn scan(&self, range: Range) -> Scan {
        Box::new(Iter::new(self.root.clone(), range))
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.root.write()?.set(key, value);
        Ok(())
    }
}

/// B-tree node variants. Most internal logic is delegated to the contained Children/Values structs,
/// while this outer structure manages the overall tree, particularly root special-casing.
///
/// All nodes in a tree have the same order (i.e. the same maximum number of children/values). The
/// root node can contain anywhere between 0 and the maximum number of items, while inner and leaf
/// nodes try to stay between order/2 and order items.
#[derive(Debug, PartialEq)]
enum Node {
    Root(Children),
    Inner(Children),
    Leaf(Values),
}

impl Node {
    /// Deletes a key from the node, if it exists.
    fn delete(&mut self, key: &[u8]) {
        match self {
            Self::Root(children) => {
                children.delete(key);
                // If we now have a single child, pull it up into the root.
                while children.len() == 1 && matches!(children[0], Node::Inner { .. }) {
                    if let Node::Inner(c) = children.remove(0) {
                        *children = c;
                    }
                }
                // If we have a single empty child, remove it.
                if children.len() == 1 && children[0].size() == 0 {
                    children.remove(0);
                }
            }
            Self::Inner(children) => children.delete(key),
            Self::Leaf(values) => values.delete(key),
        }
    }

    /// Fetches a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get(key),
            Self::Leaf(values) => values.get(key),
        }
    }

    /// Fetches the first key/value pair, if any.
    fn get_first(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get_first(),
            Self::Leaf(values) => values.get_first(),
        }
    }

    /// Fetches the last key/value pair, if any.
    fn get_last(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get_last(),
            Self::Leaf(values) => values.get_last(),
        }
    }

    /// Fetches the next key/value pair after the given key.
    fn get_next(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get_next(key),
            Self::Leaf(values) => values.get_next(key),
        }
    }

    /// Fetches the previous key/value pair before the given key.
    fn get_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Self::Root(children) | Self::Inner(children) => children.get_prev(key),
            Self::Leaf(values) => values.get_prev(key),
        }
    }

    /// Sets a key to a value in the node, inserting or updating the key as appropriate. If the
    /// node splits, return the split key and new (right) node.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Option<(Vec<u8>, Node)> {
        match self {
            Self::Root(ref mut children) => {
                // Set the key/value pair in the children. If the children split, create a new
                // child set for the root node with two new inner nodes for the split children.
                if let Some((split_key, split_children)) = children.set(key, value) {
                    let mut root_children = Children::new(children.capacity());
                    root_children.keys.push(split_key);
                    root_children.nodes.push(Node::Inner(replace(children, Children::empty())));
                    root_children.nodes.push(Node::Inner(split_children));
                    *children = root_children;
                }
                None
            }
            Self::Inner(children) => children.set(key, value).map(|(sk, c)| (sk, Node::Inner(c))),
            Self::Leaf(values) => values.set(key, value).map(|(sk, v)| (sk, Node::Leaf(v))),
        }
    }

    /// Returns the order (i.e. capacity) of the node.
    fn order(&self) -> usize {
        match self {
            Self::Root(children) | Self::Inner(children) => children.capacity(),
            Self::Leaf(values) => values.capacity(),
        }
    }

    /// Returns the size (number of items) of the node.
    fn size(&self) -> usize {
        match self {
            Self::Root(children) | Self::Inner(children) => children.len(),
            Self::Leaf(values) => values.len(),
        }
    }
}

/// Root node and inner node children. The child set (node) order determines the maximum number of
/// child nodes, which is tracked via the internal vector capacity. Derefs to the child node vector.
///
/// The keys are used to guide lookups. There is always one key less than children, where the the
/// key at index i (and all keys up to the one at i+1) is contained within the child at index i+1.
/// For example:
///
/// Index  Keys  Nodes
/// 0      d     a=1,b=2,c=3        Keys:               d         f
/// 1      f     d=4,e=5            Nodes:  a=1,b=2,c=3 | d=4,e=5 | f=6,g=7
/// 2            f=6,g=7
///
/// Thus, to find the node responsible for a given key, scan the keys until encountering one greater
/// than the given key (if any) - the index of that key corresponds to the index of the node.
#[derive(Debug, PartialEq)]
struct Children {
    keys: Vec<Vec<u8>>,
    nodes: Vec<Node>,
}

impl Deref for Children {
    type Target = Vec<Node>;
    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl DerefMut for Children {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.nodes
    }
}

impl Children {
    /// Creates a new child set of the given order (maximum capacity).
    fn new(order: usize) -> Self {
        Self { keys: Vec::with_capacity(order - 1), nodes: Vec::with_capacity(order) }
    }

    /// Creates an empty child set, for use with replace().
    fn empty() -> Self {
        Self { keys: Vec::new(), nodes: Vec::new() }
    }

    /// Deletes a key from the children, if it exists.
    fn delete(&mut self, key: &[u8]) {
        if self.is_empty() {
            return;
        }

        // Delete the key in the relevant child.
        let (i, child) = self.lookup_mut(key);
        child.delete(key);

        // If the child does not underflow, or it has no siblings, we're done.
        if child.size() >= (child.order() + 1) / 2 || self.len() == 1 {
            return;
        }

        // Attempt to rotate or merge with the left or right siblings.
        let (size, order) = (self[i].size(), self[i].order());
        let (lsize, lorder) =
            if i > 0 { (self[i - 1].size(), self[i - 1].order()) } else { (0, 0) };
        let (rsize, rorder) =
            if i < self.len() - 1 { (self[i + 1].size(), self[i + 1].order()) } else { (0, 0) };

        if lsize > (lorder + 1) / 2 {
            self.rotate_right(i - 1);
        } else if rsize > (rorder + 1) / 2 {
            self.rotate_left(i + 1);
        } else if lsize + size <= lorder {
            self.merge(i - 1);
        } else if rsize + size <= order {
            self.merge(i);
        }
    }

    /// Fetches a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if !self.is_empty() {
            self.lookup(key).1.get(key)
        } else {
            None
        }
    }

    /// Fetches the first key/value pair, if any.
    fn get_first(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.nodes.first().and_then(|n| n.get_first())
    }

    /// Fetches the last key/value pair, if any.
    fn get_last(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.nodes.last().and_then(|n| n.get_last())
    }

    /// Fetches the next key/value pair after the given key, if it exists.
    fn get_next(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.is_empty() {
            return None;
        }
        // First, look in the child responsible for the given key.
        let (i, child) = self.lookup(key);
        if let Some(item) = child.get_next(key) {
            Some(item)
        // Otherwise, try the next child.
        } else if i < self.len() - 1 {
            self[i + 1].get_next(key)
        // We don't have it.
        } else {
            None
        }
    }

    /// Fetches the previous key/value pair before the given key, if it exists.
    fn get_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.is_empty() {
            return None;
        }
        // First, look in the child responsible for the given key.
        let (i, child) = self.lookup(key);
        if let Some(item) = child.get_prev(key) {
            Some(item)
        // Otherwise, try the previous child.
        } else if i > 0 {
            self[i - 1].get_prev(key)
        // We don't have it
        } else {
            None
        }
    }

    /// Looks up the child responsible for a given key. This can only be called on non-empty
    /// child sets, which should be all child sets except for the initial root node.
    fn lookup(&self, key: &[u8]) -> (usize, &Node) {
        let i = self.keys.iter().position(|k| k.deref() > key).unwrap_or_else(|| self.keys.len());
        (i, &self[i])
    }

    /// Looks up the child responsible for a given key, and returns a mutable reference to it. This
    /// can only be called on non-empty child sets, which should be all child sets except for the
    /// initial root node.
    fn lookup_mut(&mut self, key: &[u8]) -> (usize, &mut Node) {
        let i = self.keys.iter().position(|k| k.deref() > key).unwrap_or_else(|| self.keys.len());
        (i, &mut self[i])
    }

    /// Merges the node at index i with it's right sibling.
    fn merge(&mut self, i: usize) {
        let parent_key = self.keys.remove(i);
        let right = &mut self.remove(i + 1);
        let left = &mut self[i];
        match (left, right) {
            (Node::Inner(lc), Node::Inner(rc)) => {
                lc.keys.push(parent_key);
                lc.keys.append(&mut rc.keys);
                lc.nodes.append(&mut rc.nodes);
            }
            (Node::Leaf(lv), Node::Leaf(rv)) => lv.append(rv),
            (left, right) => panic!("Can't merge {:?} and {:?}", left, right),
        }
    }

    /// Rotates children to the left, by transferring items from the node at the given index to
    /// its left sibling and adjusting the separator key.
    fn rotate_left(&mut self, i: usize) {
        if matches!(self[i], Node::Inner(_)) {
            let (key, node) = match &mut self[i] {
                Node::Inner(c) => (c.keys.remove(0), c.nodes.remove(0)),
                n => panic!("Left rotation from unexpected node {:?}", n),
            };
            let key = replace(&mut self.keys[i - 1], key); // rotate separator key
            match &mut self[i - 1] {
                Node::Inner(c) => {
                    c.keys.push(key);
                    c.nodes.push(node);
                }
                n => panic!("Left rotation into unexpected node {:?}", n),
            }
        } else if matches!(self[i], Node::Leaf(_)) {
            let (sep_key, (key, value)) = match &mut self[i] {
                Node::Leaf(v) => (v[1].0.clone(), v.remove(0)),
                n => panic!("Left rotation from unexpected node {:?}", n),
            };
            self.keys[i - 1] = sep_key;
            match &mut self[i - 1] {
                Node::Leaf(v) => v.push((key, value)),
                n => panic!("Left rotation into unexpected node {:?}", n),
            }
        } else {
            panic!("Don't know how to rotate node {:?}", self[i]);
        }
    }

    /// Rotates children to the right, by transferring items from the node at the given index to
    /// its right sibling and adjusting the separator key.
    fn rotate_right(&mut self, i: usize) {
        if matches!(self[i], Node::Inner(_)) {
            let (key, node) = match &mut self[i] {
                Node::Inner(c) => (c.keys.pop().unwrap(), c.nodes.pop().unwrap()),
                n => panic!("Right rotation from unexpected node {:?}", n),
            };
            let key = replace(&mut self.keys[i], key); // rotate separator key
            match &mut self[i + 1] {
                Node::Inner(c) => {
                    c.keys.insert(0, key);
                    c.nodes.insert(0, node);
                }
                n => panic!("Right rotation into unexpected node {:?}", n),
            }
        } else if matches!(self[i], Node::Leaf(_)) {
            let (key, value) = match &mut self[i] {
                Node::Leaf(v) => v.pop().unwrap(),
                n => panic!("Right rotation from unexpected node {:?}", n),
            };
            self.keys[i] = key.clone(); // update separator key
            match &mut self[i + 1] {
                Node::Leaf(v) => v.insert(0, (key, value)),
                n => panic!("Right rotation into unexpected node {:?}", n),
            }
        } else {
            panic!("Don't know how to rotate node {:?}", self[i]);
        }
    }

    /// Sets a key to a value in the children, delegating to the child responsible. If the node
    /// splits, returns the split key and new (right) node.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Option<(Vec<u8>, Children)> {
        // For empty child sets, just create a new leaf node for the key.
        if self.is_empty() {
            let mut values = Values::new(self.capacity());
            values.push((key.to_vec(), value));
            self.push(Node::Leaf(values));
            return None;
        }

        // Find the child and insert the value into it. If the child splits, try to insert the
        // new right node into this child set.
        let (i, child) = self.lookup_mut(key);
        if let Some((split_key, split_child)) = child.set(key, value) {
            // The split child should be insert next to the original target.
            let insert_at = i + 1;

            // If the child set has room, just insert the split child into it. Recall that key
            // indices are one less than child nodes.
            if self.len() < self.capacity() {
                self.keys.insert(insert_at - 1, split_key.to_vec());
                self.nodes.insert(insert_at, split_child);
                return None;
            }

            // If the set is full, we need to split it and return the right node. The split-off
            // right child node goes after the original target node. We split the node in the
            // middle, but if we're inserting after the split point we move the split point by one
            // to help balance splitting of odd-ordered nodes.
            let mut split_at = self.len() / 2;
            if insert_at >= split_at {
                split_at += 1;
            }

            // Split the existing children and keys into two parts. The left parts will now have an
            // equal number of keys and children, where the last key points to the first node in
            // the right children. This last key will either have to be promoted to a split key, or
            // moved to the right keys, but keeping it here makes the arithmetic somewhat simpler.
            let mut rnodes = Vec::with_capacity(self.nodes.capacity());
            let mut rkeys = Vec::with_capacity(self.keys.capacity());
            rnodes.extend(self.nodes.drain(split_at..));
            rkeys.extend(self.keys.drain((self.keys.len() - rnodes.len() + 1)..));

            // Insert the split node and split key. Since the key is always at one index less than
            // the child, they may end up in different halves in which case the split key will be
            // promoted to a split key and the extra key from the left half is moved to the right
            // half. Otherwise, the extra key from the left half becomes the split key.
            let split_key = match insert_at.cmp(&self.nodes.len()) {
                Ordering::Greater => {
                    rkeys.insert(insert_at - 1 - self.keys.len(), split_key);
                    rnodes.insert(insert_at - self.nodes.len(), split_child);
                    self.keys.remove(self.keys.len() - 1)
                }
                Ordering::Equal => {
                    rkeys.insert(0, self.keys.remove(self.keys.len() - 1));
                    rnodes.insert(0, split_child);
                    split_key
                }
                Ordering::Less => {
                    self.keys.insert(insert_at - 1, split_key);
                    self.nodes.insert(insert_at, split_child);
                    self.keys.remove(self.keys.len() - 1)
                }
            };

            Some((split_key, Children { keys: rkeys, nodes: rnodes }))
        } else {
            None
        }
    }
}

/// Leaf node key/value pairs. The value set (leaf node) order determines the maximum number
/// of key/value items, which is tracked via the internal vector capacity. Items are ordered by key,
/// and looked up via linear search due to the low cardinality. Derefs to the inner vec.
#[derive(Debug, PartialEq)]
struct Values(Vec<(Vec<u8>, Vec<u8>)>);

impl Deref for Values {
    type Target = Vec<(Vec<u8>, Vec<u8>)>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Values {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Values {
    /// Creates a new value set with the given order (maximum capacity).
    fn new(order: usize) -> Self {
        Self(Vec::with_capacity(order))
    }

    /// Deletes a key from the set, if it exists.
    fn delete(&mut self, key: &[u8]) {
        for (i, (k, _)) in self.iter().enumerate() {
            match (&**k).cmp(key) {
                Ordering::Greater => break,
                Ordering::Equal => {
                    self.remove(i);
                    break;
                }
                Ordering::Less => {}
            }
        }
    }

    /// Fetches a value from the set, if the key exists.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.iter()
            .find_map(|(k, v)| match (&**k).cmp(key) {
                Ordering::Greater => Some(None),
                Ordering::Equal => Some(Some(v.to_vec())),
                Ordering::Less => None,
            })
            .flatten()
    }

    /// Fetches the first key/value pair from the set, if any.
    fn get_first(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.0.first().cloned()
    }

    /// Fetches the last key/value pair from the set, if any.
    fn get_last(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.0.last().cloned()
    }

    /// Fetches the next value after the given key, if it exists.
    fn get_next(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        self.iter()
            .find_map(|(k, v)| match (&**k).cmp(key) {
                Ordering::Greater => Some(Some((k.to_vec(), v.to_vec()))),
                Ordering::Equal => None,
                Ordering::Less => None,
            })
            .flatten()
    }

    /// Fetches the previous value before the given key, if it exists.
    fn get_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        self.iter()
            .rev()
            .find_map(|(k, v)| match (&**k).cmp(key) {
                Ordering::Less => Some(Some((k.to_vec(), v.to_vec()))),
                Ordering::Equal => None,
                Ordering::Greater => None,
            })
            .flatten()
    }

    /// Sets a key to a value, inserting of updating it. If the value set is full, it is split
    /// in the middle and the split key and right values are returned.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Option<(Vec<u8>, Values)> {
        // Find position to insert at, or if the key already exists just update it.
        let mut insert_at = self.len();
        for (i, (k, v)) in self.iter_mut().enumerate() {
            match (&**k).cmp(key) {
                Ordering::Greater => {
                    insert_at = i;
                    break;
                }
                Ordering::Equal => {
                    *v = value;
                    return None;
                }
                Ordering::Less => {}
            }
        }

        // If we have capacity, just insert the value.
        if self.len() < self.capacity() {
            self.insert(insert_at, (key.to_vec(), value));
            return None;
        }

        // If we're full, split in the middle and return split key and right values. If inserting
        // to the right of the split, move split by one to better balance odd-ordered nodes.
        let mut split_at = self.len() / 2;
        if insert_at >= split_at {
            split_at += 1;
        }
        let mut rvalues = Values::new(self.capacity());
        rvalues.extend(self.drain(split_at..));
        if insert_at >= self.len() {
            rvalues.insert(insert_at - self.len(), (key.to_vec(), value));
        } else {
            self.insert(insert_at, (key.to_vec(), value));
        }
        Some((rvalues[0].0.clone(), rvalues))
    }
}

/// A key range scan.
/// FIXME This is O(log n), and should use the normal B+tree approach of storing pointers in
/// the leaf nodes instead. See: https://github.com/erikgrinaker/toydb/issues/32
struct Iter {
    /// The root node of the tree we're iterating across.
    root: Arc<RwLock<Node>>,
    /// The range we're iterating over.
    range: Range,
    /// The front cursor keeps track of the last returned value from the front.
    front_cursor: Option<Vec<u8>>,
    /// The back cursor keeps track of the last returned value from the back.
    back_cursor: Option<Vec<u8>>,
}

impl Iter {
    /// Creates a new iterator.
    fn new(root: Arc<RwLock<Node>>, range: Range) -> Self {
        Self { root, range, front_cursor: None, back_cursor: None }
    }

    // next() with error handling.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.root.read()?;
        let next = match &self.front_cursor {
            None => match &self.range.start {
                Bound::Included(k) => {
                    root.get(k).map(|v| (k.clone(), v)).or_else(|| root.get_next(k))
                }
                Bound::Excluded(k) => root.get_next(k),
                Bound::Unbounded => root.get_first(),
            },
            Some(k) => root.get_next(k),
        };
        if let Some((k, _)) = &next {
            if !self.range.contains(k) {
                return Ok(None);
            }
            if let Some(bc) = &self.back_cursor {
                if bc <= k {
                    return Ok(None);
                }
            }
            self.front_cursor = Some(k.clone())
        }
        Ok(next)
    }

    /// next_back() with error handling.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.root.read()?;
        let prev = match &self.back_cursor {
            None => match &self.range.end {
                Bound::Included(k) => {
                    root.get(k).map(|v| (k.clone(), v)).or_else(|| root.get_prev(k))
                }
                Bound::Excluded(k) => root.get_prev(k),
                Bound::Unbounded => root.get_last(),
            },
            Some(k) => root.get_prev(k),
        };
        if let Some((k, _)) = &prev {
            if !self.range.contains(k) {
                return Ok(None);
            }
            if let Some(fc) = &self.front_cursor {
                if fc >= k {
                    return Ok(None);
                }
            }
            self.back_cursor = Some(k.clone())
        }
        Ok(prev)
    }
}

impl Iterator for Iter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    impl super::super::TestSuite<Memory> for Memory {
        fn setup() -> Result<Self> {
            Ok(Memory::new())
        }
    }

    #[test]
    fn tests() -> Result<()> {
        use super::super::TestSuite;
        Memory::test()
    }

    #[test]
    fn set_split() -> Result<()> {
        // Create a root of order 3
        let mut root = Node::Root(Children::new(3));

        // A new root should be empty
        assert_eq!(Node::Root(Children { keys: vec![], nodes: vec![] }), root);

        // Setting the first three values should create a leaf node and fill it
        root.set(b"a", vec![0x01]);
        root.set(b"b", vec![0x02]);
        root.set(b"c", vec![0x03]);

        assert_eq!(
            Node::Root(Children {
                keys: vec![],
                nodes: vec![Node::Leaf(Values(vec![
                    (b"a".to_vec(), vec![0x01]),
                    (b"b".to_vec(), vec![0x02]),
                    (b"c".to_vec(), vec![0x03]),
                ]),)],
            }),
            root
        );

        // Updating a node should not cause splitting
        root.set(b"b", vec![0x20]);

        assert_eq!(
            Node::Root(Children {
                keys: vec![],
                nodes: vec![Node::Leaf(Values(vec![
                    (b"a".to_vec(), vec![0x01]),
                    (b"b".to_vec(), vec![0x20]),
                    (b"c".to_vec(), vec![0x03]),
                ]))],
            }),
            root
        );

        // Setting an additional value should split the leaf node
        root.set(b"b", vec![0x02]);
        root.set(b"d", vec![0x04]);

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"c".to_vec()],
                nodes: vec![
                    Node::Leaf(Values(vec![
                        (b"a".to_vec(), vec![0x01]),
                        (b"b".to_vec(), vec![0x02]),
                    ])),
                    Node::Leaf(Values(vec![
                        (b"c".to_vec(), vec![0x03]),
                        (b"d".to_vec(), vec![0x04]),
                    ])),
                ],
            }),
            root
        );

        // Adding two more values at the end should split the second leaf
        root.set(b"z", vec![0x1a]);
        root.set(b"y", vec![0x19]);

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"c".to_vec(), b"y".to_vec()],
                nodes: vec![
                    Node::Leaf(Values(vec![
                        (b"a".to_vec(), vec![0x01]),
                        (b"b".to_vec(), vec![0x02]),
                    ])),
                    Node::Leaf(Values(vec![
                        (b"c".to_vec(), vec![0x03]),
                        (b"d".to_vec(), vec![0x04]),
                    ])),
                    Node::Leaf(Values(vec![
                        (b"y".to_vec(), vec![0x19]),
                        (b"z".to_vec(), vec![0x1a]),
                    ])),
                ],
            }),
            root
        );

        // Adding two more values from the end should split the middle leaf. This will cause the
        // root node to overflow and split as well.
        root.set(b"x", vec![0x18]);
        root.set(b"w", vec![0x17]);

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"w".to_vec()],
                nodes: vec![
                    Node::Inner(Children {
                        keys: vec![b"c".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"a".to_vec(), vec![0x01]),
                                (b"b".to_vec(), vec![0x02]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"c".to_vec(), vec![0x03]),
                                (b"d".to_vec(), vec![0x04]),
                            ])),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"y".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"w".to_vec(), vec![0x17]),
                                (b"x".to_vec(), vec![0x18]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"y".to_vec(), vec![0x19]),
                                (b"z".to_vec(), vec![0x1a]),
                            ])),
                        ],
                    })
                ],
            }),
            root
        );

        // Adding further values should cause the first inner node to finally split as well.
        root.set(b"e", vec![0x05]);
        root.set(b"f", vec![0x06]);
        root.set(b"g", vec![0x07]);
        root.set(b"h", vec![0x08]);

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"e".to_vec(), b"w".to_vec()],
                nodes: vec![
                    Node::Inner(Children {
                        keys: vec![b"c".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"a".to_vec(), vec![0x01]),
                                (b"b".to_vec(), vec![0x02]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"c".to_vec(), vec![0x03]),
                                (b"d".to_vec(), vec![0x04]),
                            ])),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"g".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"e".to_vec(), vec![0x05]),
                                (b"f".to_vec(), vec![0x06]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"g".to_vec(), vec![0x07]),
                                (b"h".to_vec(), vec![0x08]),
                            ])),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"y".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"w".to_vec(), vec![0x17]),
                                (b"x".to_vec(), vec![0x18]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"y".to_vec(), vec![0x19]),
                                (b"z".to_vec(), vec![0x1a]),
                            ])),
                        ],
                    })
                ],
            }),
            root
        );

        // Adding yet more from the back, but in forward order, should cause another root node split.
        root.set(b"s", vec![0x13]);
        root.set(b"t", vec![0x14]);
        root.set(b"u", vec![0x15]);
        root.set(b"v", vec![0x16]);

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"s".to_vec()],
                nodes: vec![
                    Node::Inner(Children {
                        keys: vec![b"e".to_vec()],
                        nodes: vec![
                            Node::Inner(Children {
                                keys: vec![b"c".to_vec()],
                                nodes: vec![
                                    Node::Leaf(Values(vec![
                                        (b"a".to_vec(), vec![0x01]),
                                        (b"b".to_vec(), vec![0x02]),
                                    ])),
                                    Node::Leaf(Values(vec![
                                        (b"c".to_vec(), vec![0x03]),
                                        (b"d".to_vec(), vec![0x04]),
                                    ]))
                                ],
                            }),
                            Node::Inner(Children {
                                keys: vec![b"g".to_vec()],
                                nodes: vec![
                                    Node::Leaf(Values(vec![
                                        (b"e".to_vec(), vec![0x05]),
                                        (b"f".to_vec(), vec![0x06]),
                                    ])),
                                    Node::Leaf(Values(vec![
                                        (b"g".to_vec(), vec![0x07]),
                                        (b"h".to_vec(), vec![0x08]),
                                    ]))
                                ],
                            }),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"w".to_vec()],
                        nodes: vec![
                            Node::Inner(Children {
                                keys: vec![b"u".to_vec()],
                                nodes: vec![
                                    Node::Leaf(Values(vec![
                                        (b"s".to_vec(), vec![0x13]),
                                        (b"t".to_vec(), vec![0x14]),
                                    ])),
                                    Node::Leaf(Values(vec![
                                        (b"u".to_vec(), vec![0x15]),
                                        (b"v".to_vec(), vec![0x16]),
                                    ]))
                                ],
                            }),
                            Node::Inner(Children {
                                keys: vec![b"y".to_vec()],
                                nodes: vec![
                                    Node::Leaf(Values(vec![
                                        (b"w".to_vec(), vec![0x17]),
                                        (b"x".to_vec(), vec![0x18]),
                                    ])),
                                    Node::Leaf(Values(vec![
                                        (b"y".to_vec(), vec![0x19]),
                                        (b"z".to_vec(), vec![0x1a]),
                                    ]))
                                ],
                            })
                        ]
                    })
                ],
            }),
            root
        );

        Ok(())
    }

    #[test]
    fn delete_merge() -> Result<()> {
        // Create a root of order 3, and add a bunch of values to it.
        let mut root = Node::Root(Children::new(3));

        root.set(b"a", vec![0x01]);
        root.set(b"b", vec![0x02]);
        root.set(b"c", vec![0x03]);
        root.set(b"d", vec![0x04]);
        root.set(b"e", vec![0x05]);
        root.set(b"f", vec![0x06]);
        root.set(b"g", vec![0x07]);
        root.set(b"h", vec![0x08]);
        root.set(b"i", vec![0x09]);
        root.set(b"j", vec![0x0a]);
        root.set(b"k", vec![0x0b]);
        root.set(b"l", vec![0x0c]);
        root.set(b"m", vec![0x0d]);
        root.set(b"n", vec![0x0e]);
        root.set(b"o", vec![0x0f]);
        root.set(b"p", vec![0x10]);

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"i".to_vec()],
                nodes: vec![
                    Node::Inner(Children {
                        keys: vec![b"e".to_vec()],
                        nodes: vec![
                            Node::Inner(Children {
                                keys: vec![b"c".to_vec()],
                                nodes: vec![
                                    Node::Leaf(Values(vec![
                                        (b"a".to_vec(), vec![0x01]),
                                        (b"b".to_vec(), vec![0x02]),
                                    ])),
                                    Node::Leaf(Values(vec![
                                        (b"c".to_vec(), vec![0x03]),
                                        (b"d".to_vec(), vec![0x04]),
                                    ])),
                                ],
                            }),
                            Node::Inner(Children {
                                keys: vec![b"g".to_vec()],
                                nodes: vec![
                                    Node::Leaf(Values(vec![
                                        (b"e".to_vec(), vec![0x05]),
                                        (b"f".to_vec(), vec![0x06]),
                                    ])),
                                    Node::Leaf(Values(vec![
                                        (b"g".to_vec(), vec![0x07]),
                                        (b"h".to_vec(), vec![0x08]),
                                    ])),
                                ],
                            }),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"m".to_vec()],
                        nodes: vec![
                            Node::Inner(Children {
                                keys: vec![b"k".to_vec()],
                                nodes: vec![
                                    Node::Leaf(Values(vec![
                                        (b"i".to_vec(), vec![0x09]),
                                        (b"j".to_vec(), vec![0x0a]),
                                    ])),
                                    Node::Leaf(Values(vec![
                                        (b"k".to_vec(), vec![0x0b]),
                                        (b"l".to_vec(), vec![0x0c]),
                                    ])),
                                ],
                            }),
                            Node::Inner(Children {
                                keys: vec![b"o".to_vec()],
                                nodes: vec![
                                    Node::Leaf(Values(vec![
                                        (b"m".to_vec(), vec![0x0d]),
                                        (b"n".to_vec(), vec![0x0e]),
                                    ])),
                                    Node::Leaf(Values(vec![
                                        (b"o".to_vec(), vec![0x0f]),
                                        (b"p".to_vec(), vec![0x10]),
                                    ])),
                                ],
                            })
                        ]
                    })
                ],
            }),
            root
        );

        // Deleting the o node merges two leaf nodes, in turn merging parents.
        root.delete(b"o");

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"e".to_vec(), b"i".to_vec()],
                nodes: vec![
                    Node::Inner(Children {
                        keys: vec![b"c".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"a".to_vec(), vec![0x01]),
                                (b"b".to_vec(), vec![0x02]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"c".to_vec(), vec![0x03]),
                                (b"d".to_vec(), vec![0x04]),
                            ])),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"g".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"e".to_vec(), vec![0x05]),
                                (b"f".to_vec(), vec![0x06]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"g".to_vec(), vec![0x07]),
                                (b"h".to_vec(), vec![0x08]),
                            ])),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"k".to_vec(), b"m".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"i".to_vec(), vec![0x09]),
                                (b"j".to_vec(), vec![0x0a]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"k".to_vec(), vec![0x0b]),
                                (b"l".to_vec(), vec![0x0c]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"m".to_vec(), vec![0x0d]),
                                (b"n".to_vec(), vec![0x0e]),
                                (b"p".to_vec(), vec![0x10]),
                            ])),
                        ],
                    }),
                ],
            }),
            root
        );

        // Deleting i causes another leaf merge
        root.delete(b"i");

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"e".to_vec(), b"i".to_vec()],
                nodes: vec![
                    Node::Inner(Children {
                        keys: vec![b"c".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"a".to_vec(), vec![0x01]),
                                (b"b".to_vec(), vec![0x02]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"c".to_vec(), vec![0x03]),
                                (b"d".to_vec(), vec![0x04]),
                            ])),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"g".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"e".to_vec(), vec![0x05]),
                                (b"f".to_vec(), vec![0x06]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"g".to_vec(), vec![0x07]),
                                (b"h".to_vec(), vec![0x08]),
                            ])),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"m".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"j".to_vec(), vec![0x0a]),
                                (b"k".to_vec(), vec![0x0b]),
                                (b"l".to_vec(), vec![0x0c]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"m".to_vec(), vec![0x0d]),
                                (b"n".to_vec(), vec![0x0e]),
                                (b"p".to_vec(), vec![0x10]),
                            ])),
                        ],
                    }),
                ],
            }),
            root
        );

        // Clearing out j,k,l should cause another merge.
        root.delete(b"j");
        root.delete(b"l");
        root.delete(b"k");

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"e".to_vec()],
                nodes: vec![
                    Node::Inner(Children {
                        keys: vec![b"c".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"a".to_vec(), vec![0x01]),
                                (b"b".to_vec(), vec![0x02]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"c".to_vec(), vec![0x03]),
                                (b"d".to_vec(), vec![0x04]),
                            ])),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"g".to_vec(), b"i".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"e".to_vec(), vec![0x05]),
                                (b"f".to_vec(), vec![0x06]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"g".to_vec(), vec![0x07]),
                                (b"h".to_vec(), vec![0x08]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"m".to_vec(), vec![0x0d]),
                                (b"n".to_vec(), vec![0x0e]),
                                (b"p".to_vec(), vec![0x10]),
                            ])),
                        ],
                    }),
                ],
            }),
            root
        );

        // Removing a should underflow a leaf node, triggering a rotation to rebalance the
        // underflowing inner node with its sibling.
        root.delete(b"a");

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"g".to_vec()],
                nodes: vec![
                    Node::Inner(Children {
                        keys: vec![b"e".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"b".to_vec(), vec![0x02]),
                                (b"c".to_vec(), vec![0x03]),
                                (b"d".to_vec(), vec![0x04]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"e".to_vec(), vec![0x05]),
                                (b"f".to_vec(), vec![0x06]),
                            ])),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"i".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"g".to_vec(), vec![0x07]),
                                (b"h".to_vec(), vec![0x08]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"m".to_vec(), vec![0x0d]),
                                (b"n".to_vec(), vec![0x0e]),
                                (b"p".to_vec(), vec![0x10]),
                            ])),
                        ],
                    }),
                ],
            }),
            root
        );

        // Removing h should rebalance the leaf nodes.
        root.delete(b"h");

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"g".to_vec()],
                nodes: vec![
                    Node::Inner(Children {
                        keys: vec![b"e".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"b".to_vec(), vec![0x02]),
                                (b"c".to_vec(), vec![0x03]),
                                (b"d".to_vec(), vec![0x04]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"e".to_vec(), vec![0x05]),
                                (b"f".to_vec(), vec![0x06]),
                            ])),
                        ],
                    }),
                    Node::Inner(Children {
                        keys: vec![b"n".to_vec()],
                        nodes: vec![
                            Node::Leaf(Values(vec![
                                (b"g".to_vec(), vec![0x07]),
                                (b"m".to_vec(), vec![0x0d]),
                            ])),
                            Node::Leaf(Values(vec![
                                (b"n".to_vec(), vec![0x0e]),
                                (b"p".to_vec(), vec![0x10]),
                            ])),
                        ],
                    }),
                ],
            }),
            root
        );

        // Removing n should rebalance the leaf nodes, and in turn merge the inner nodes.
        root.delete(b"n");

        assert_eq!(
            Node::Root(Children {
                keys: vec![b"e".to_vec(), b"g".to_vec()],
                nodes: vec![
                    Node::Leaf(Values(vec![
                        (b"b".to_vec(), vec![0x02]),
                        (b"c".to_vec(), vec![0x03]),
                        (b"d".to_vec(), vec![0x04]),
                    ])),
                    Node::Leaf(Values(vec![
                        (b"e".to_vec(), vec![0x05]),
                        (b"f".to_vec(), vec![0x06]),
                    ])),
                    Node::Leaf(Values(vec![
                        (b"g".to_vec(), vec![0x07]),
                        (b"m".to_vec(), vec![0x0d]),
                        (b"p".to_vec(), vec![0x10]),
                    ])),
                ],
            }),
            root
        );

        // At this point we can remove the remaining keys, leaving an empty root node.
        root.delete(b"d");
        root.delete(b"p");
        root.delete(b"g");
        root.delete(b"c");
        root.delete(b"f");
        root.delete(b"m");
        root.delete(b"b");
        root.delete(b"e");

        assert_eq!(Node::Root(Children { keys: vec![], nodes: vec![] }), root);

        Ok(())
    }
}
