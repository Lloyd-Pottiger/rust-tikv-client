/// Lightweight optional wrapper that mirrors client-go `util.Option[T]`.
///
/// Prefer ordinary [`std::option::Option`] in idiomatic Rust code. This wrapper exists mainly so
/// Go→Rust migration code can keep using the familiar `util::Option` / `util::some` /
/// `util::none` names.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Option<T> {
    inner: std::option::Option<T>,
}

impl<T> Option<T> {
    /// Returns the wrapped value by reference, if present.
    #[doc(alias = "Inner")]
    #[must_use]
    pub fn inner(&self) -> std::option::Option<&T> {
        self.inner.as_ref()
    }

    /// Consumes the wrapper and returns the underlying Rust [`std::option::Option`].
    #[must_use]
    pub fn into_inner(self) -> std::option::Option<T> {
        self.inner
    }
}

/// Wrap a present value in [`Option`].
#[doc(alias = "Some")]
#[must_use]
pub fn some<T>(inner: T) -> Option<T> {
    Option { inner: Some(inner) }
}

/// Create an empty [`Option`].
#[doc(alias = "None")]
#[must_use]
pub fn none<T>() -> Option<T> {
    Option { inner: None }
}

#[cfg(test)]
mod tests {
    use super::{none, some};

    #[test]
    fn some_and_none_expose_inner_values() {
        let some = some(7_u64);
        assert_eq!(some.inner(), Some(&7));
        assert_eq!(some.into_inner(), Some(7));

        let none = none::<u64>();
        assert_eq!(none.inner(), None);
        assert_eq!(none.into_inner(), None);
    }
}
