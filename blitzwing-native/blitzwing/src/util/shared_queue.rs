use crate::error::BlitzwingError;
use crate::util::try_iterator::TryIterator;
use std::cell::RefMut;
use std::collections::VecDeque;
use std::cell::RefCell;
use std::rc::Rc;
use crate::error::Result;
use crate::error::BlitzwingErrorKind::FatalError;
use failure::ResultExt;



pub(crate) struct SharedQueue<T> {
  queue: Rc<RefCell<VecDeque<T>>>,
}

impl<T> SharedQueue<T> {
  pub(crate) fn new() -> Self {
    Self {
      queue: Rc::new(RefCell::new(VecDeque::new()))
    }
  }

  pub(crate) fn push(&self, t: T) -> Result<()> {
    let mut queue = self.queue_mut()?;
    queue.push_back(t);
    Ok(())
  }

  pub(crate) fn pop(&self) -> Result<Option<T>> {
    self.queue_mut().map(|mut queue| queue.pop_front())
  }

  pub(crate) fn into_iterator(self) -> impl TryIterator<Error = BlitzwingError, Item = T> {
    SharedQueueIterator {
      queue: self
    }
  }

  fn queue_mut(&self) -> Result<RefMut<VecDeque<T>>> {
    Ok(self.queue.try_borrow_mut().context(FatalError("Unable to borrow page reader queue".to_string()))?)
  }
}

impl<T> Clone for SharedQueue<T> {
  fn clone(&self) -> Self {
    Self {
      queue: self.queue.clone()
    }
  }
}

struct SharedQueueIterator<T> {
  queue: SharedQueue<T>
}

/// This iterator differs from normal iterator in that it may still return Some(T) even after it return None
impl<T> TryIterator for SharedQueueIterator<T> {
  type Error = BlitzwingError;
  type Item = T;
  fn try_next(&mut self) -> Result<Option<T>> {
    self.queue.pop()
  }
}

