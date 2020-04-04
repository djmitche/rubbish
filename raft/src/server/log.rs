use crate::diststate;

#[derive(Clone, Debug, PartialEq)]
pub(super) struct LogItem<R>
where
    R: diststate::Request,
{
    pub(super) req: R,
}
