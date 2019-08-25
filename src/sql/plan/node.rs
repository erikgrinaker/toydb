use super::super::types::Row;
use super::expression::Expression;
use crate::Error;

/// A plan node
#[derive(Debug)]
pub enum Node {
    Projection { labels: Vec<String>, source: Box<Node>, expressions: Vec<Expression> },
    Nothing { done: bool },
}

impl Iterator for Node {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Result<Row, Error>> {
        match self {
            Node::Projection { source, expressions, .. } => {
                if let Err(err) = source.next()? {
                    Some(Err(err))
                } else {
                    Some(expressions.iter().map(|e| e.evaluate()).collect())
                }
            }
            Node::Nothing { ref mut done } => {
                if !*done {
                    *done = true;
                    Some(Ok(vec![]))
                } else {
                    None
                }
            }
        }
    }
}
