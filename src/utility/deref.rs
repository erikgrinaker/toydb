use std::ops::Deref;

/// This implements as_deref() for Option, to e.g. convert
/// Option<String> to Option<&str>. There is a similar method in nightly:
/// https://doc.rust-lang.org/std/option/enum.Option.html#method.deref
pub trait OptionDeref<T: Deref> {
    fn as_deref(&self) -> Option<&T::Target>;
}

impl<T: Deref> OptionDeref<T> for Option<T> {
    fn as_deref(&self) -> Option<&T::Target> {
        self.as_ref().map(Deref::deref)
    }
}
