// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

/// Extension trait to iterators to add `flat_map_ok`.
pub trait FlatMapOkIterExt: Iterator + Sized {
    /// Flattens an iterator of iterators into a single iterator. The outer iterator returns a Result;
    /// if there is any error, that error is preserved in the output. Any Ok values are fed into the
    /// function `f` which must produce an iterator (or value that can be converted into an iterator),
    /// that iterator is iterated into the output with each value wrapped in an `Ok`.
    fn flat_map_ok<U, F, Ti, I, E>(self, f: F) -> FlatMapOk<Self, F, Ti, E>
    where
        Self: Iterator<Item = std::result::Result<I, E>>,
        U: IntoIterator,
        F: FnMut(I) -> U,
        Ti: Iterator,
    {
        FlatMapOk {
            iter: self.fuse(),
            frontiter: None,
            f,
        }
    }
}

impl<I: Iterator> FlatMapOkIterExt for I {}

/// Iterator for `flat_map_ok`.
pub struct FlatMapOk<U, F, Ti, E> {
    iter: std::iter::Fuse<U>,
    frontiter: Option<std::result::Result<Ti, E>>,
    f: F,
}

impl<
        T: IntoIterator<IntoIter = Ti>,
        U: Iterator<Item = std::result::Result<I, E>>,
        F: FnMut(I) -> T,
        Ti: Iterator<Item = T::Item>,
        I,
        E,
    > Iterator for FlatMapOk<U, F, Ti, E>
{
    type Item = std::result::Result<T::Item, E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.frontiter {
                Some(Ok(inner)) => match inner.next() {
                    None => self.frontiter = None,
                    Some(elt) => return Some(Ok(elt)),
                },
                Some(Err(_)) => {
                    if let Some(Err(e)) = self.frontiter.take() {
                        return Some(Err(e));
                    }
                    self.frontiter = None;
                }
                None => {}
            }
            match self.iter.next() {
                None => return None,
                Some(Ok(inner)) => self.frontiter = Some(Ok((self.f)(inner).into_iter())),
                Some(Err(e)) => self.frontiter = Some(Err(e)),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.frontiter {
            Some(Ok(inner)) => inner.size_hint(),
            Some(Err(_)) => (1, Some(1)),
            None => (0, None),
        }
    }
}

impl<
        T: IntoIterator<IntoIter = Ti>,
        U: Iterator<Item = std::result::Result<I, E>>,
        F: FnMut(I) -> T,
        Ti: Iterator<Item = T::Item>,
        I,
        E,
    > std::iter::FusedIterator for FlatMapOk<U, F, Ti, E>
{
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_iter_and_collect() {
        let result: Result<Vec<i32>, ()> = Vec::new()
            .into_iter()
            .flat_map_ok(|i| Some(i).into_iter())
            .collect();
        assert_eq!(result.unwrap(), Vec::<i32>::new());

        let result: Result<Vec<i32>, ()> = vec![Result::<i32, ()>::Ok(0), Ok(1), Ok(2)]
            .into_iter()
            .flat_map_ok(|i| Some(i).into_iter())
            .collect();
        assert_eq!(result.unwrap(), vec![0, 1, 2]);

        let result: Result<Vec<i32>, ()> = vec![Result::<i32, ()>::Ok(0), Err(()), Ok(2)]
            .into_iter()
            .flat_map_ok(|i| Some(i).into_iter())
            .collect();
        assert_eq!(result, Err(()));

        let result: Vec<Result<i32, ()>> = vec![Result::<i32, ()>::Ok(0), Err(()), Ok(2)]
            .into_iter()
            .flat_map_ok(|i| vec![i, i, i].into_iter())
            .collect();
        assert_eq!(
            result,
            vec![Ok(0), Ok(0), Ok(0), Err(()), Ok(2), Ok(2), Ok(2)]
        );
    }

    #[test]
    fn test_size_hint_tracks_current_inner_iterator() {
        let mut it = vec![Result::<i32, ()>::Ok(0)]
            .into_iter()
            .flat_map_ok(|_| vec![1, 2, 3].into_iter());

        assert_eq!(it.size_hint(), (0, None));
        assert_eq!(it.next(), Some(Ok(1)));
        assert_eq!(it.size_hint(), (2, Some(2)));
        assert_eq!(it.next(), Some(Ok(2)));
        assert_eq!(it.size_hint(), (1, Some(1)));
        assert_eq!(it.next(), Some(Ok(3)));
        assert_eq!(it.next(), None);
    }
}
