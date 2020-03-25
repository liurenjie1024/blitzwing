pub(crate) trait TryIterator {
  type Error;
  type Item;

  fn try_next(&mut self) -> std::result::Result<Option<Self::Item>, Self::Error>;
}