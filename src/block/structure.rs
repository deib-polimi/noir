//! This `mod` contains the types and function that model the structure of the blocks and operators
//! for debugging purposes only.

use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::block::NextStrategy;
use crate::operator::{ExchangeData, KeyerFn};
use crate::stream::BlockId;

/// Wrapper type that contains a string representing the type.
///
/// The internal representation should not be considered unique nor exact. Its purpose is to be
/// nice to look at.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataType(String);

/// The structural information about a block.
///
/// This contains the structural information about the block and the operators it contains.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BlockStructure {
    /// The structural information about the operators inside the block.
    ///
    /// The first in the list is the start of the block, while the last is the operator that ends
    /// the block.
    pub operators: Vec<OperatorStructure>,
}

/// The structural information about an operator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorStructure {
    /// The title of the operator.
    pub title: String,
    /// The subtitle of the operator.
    pub subtitle: String,
    /// The kind of operator: `Operator`, `Source` or `Sink`.
    pub kind: OperatorKind,
    /// The list of receivers this operator registers for the block.
    ///
    /// This does not contain the receiver from the previous operator in the block.
    pub receivers: Vec<OperatorReceiver>,
    /// The list of connections this operator makes.
    ///
    /// This does not count the connection to the next operator in the block: that connection is
    /// added automatically.
    pub connections: Vec<Connection>,
    /// The type of the data that comes out of this operator.
    pub out_type: DataType,
}

/// The kind of operator: either `Operator`, `Source` or `Sink`.
///
/// This value can be used for customizing the look of the operator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OperatorKind {
    /// This operator is a normal operator.
    Operator,
    /// This operator is a sink, i.e. no message will leave it.
    Sink,
    /// This operator is a source, i.e. no message will enter it.
    Source,
}

/// A receiver registered by an operator.
///
/// This receiver tells that an operator will receive some data from the network from the specified
/// block. Inside a block there cannot be two operators that register a receiver from the same block
/// id.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OperatorReceiver {
    /// The identifier of the block from which the data is arriving.
    pub previous_block_id: BlockId,
    /// The type of the data coming from the channel.
    pub data_type: DataType,
}

/// A connection registered by an operator.
///
/// This tell that an operator will establish a connection with an external block. That block should
/// have registered the corresponding receiver. The strategy can be used to customize the look of
/// this connection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Connection {
    /// The id of the block that this operator is connecting to.
    pub to_block_id: BlockId,
    /// The type of data going in the channel.
    pub data_type: DataType,
    /// The strategy used for sending the data in the channel.
    pub strategy: ConnectionStrategy,
}

/// The strategy used for sending the data in a channel.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConnectionStrategy {
    /// The data will sent to the only replica possible. Refer to `NextStrategy::OnlyOne`.
    OnlyOne,
    /// A random replica is chosen for sending the data.
    Random,
    /// A key-based approach is used for choosing the next replica.
    GroupBy,
    /// All the replicas receive all the elements of the stream.
    All,
}

impl DataType {
    /// Construct the `DataType` for the specified type.
    pub fn of<T: ?Sized>() -> Self {
        let type_name = std::any::type_name::<T>();
        Self(DataType::clean_str(type_name))
    }

    /// Cleanup the type information returned by `std::any::type_name`. This will remove a lot of
    /// unnecessary information from the type (like the path), keeping just the final name and the
    /// type parameters.
    fn clean_str(s: &str) -> String {
        let mut result = "".to_string();
        let mut current_ident = "".to_string();
        for c in s.chars() {
            // c is part of an identifier
            if c.is_alphanumeric() {
                current_ident.push(c);
                // the current identifier was just a type path
            } else if c == ':' {
                current_ident = "".to_string();
                // other characters like space, <, >
            } else {
                // if there was an identifier, this character marks its end
                if !current_ident.is_empty() {
                    result += &current_ident;
                    current_ident = "".to_string();
                }
                result.push(c);
            }
        }
        if !current_ident.is_empty() {
            result += &current_ident;
        }
        result
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl BlockStructure {
    /// Add a new operator inside this block.
    pub fn add_operator(mut self, operator: OperatorStructure) -> Self {
        self.operators.push(operator);
        self
    }
}

impl OperatorStructure {
    /// Crate a new [`OperatorStructure`] with the given title that produces the given elements.
    pub fn new<Out: ?Sized, S: Into<String>>(title: S) -> Self {
        Self {
            title: title.into(),
            subtitle: "".into(),
            kind: OperatorKind::Operator,
            receivers: Default::default(),
            connections: Default::default(),
            out_type: DataType::of::<Out>(),
        }
    }
}

impl OperatorReceiver {
    /// Crate a new [`OperatorReceiver`] with the given type that will receive from the given block.
    pub fn new<T: ?Sized>(previous_block_id: BlockId) -> Self {
        Self {
            previous_block_id,
            data_type: DataType::of::<T>(),
        }
    }
}

impl Connection {
    pub(crate) fn new<T: ExchangeData, IndexFn>(
        to_block_id: BlockId,
        strategy: &NextStrategy<T, IndexFn>,
    ) -> Self
    where
        IndexFn: KeyerFn<u64, T>,
    {
        Self {
            to_block_id,
            data_type: DataType::of::<T>(),
            strategy: strategy.into(),
        }
    }
}

impl<Out: ExchangeData, IndexFn> From<&NextStrategy<Out, IndexFn>> for ConnectionStrategy
where
    IndexFn: KeyerFn<u64, Out>,
{
    fn from(strategy: &NextStrategy<Out, IndexFn>) -> Self {
        match strategy {
            NextStrategy::OnlyOne => ConnectionStrategy::OnlyOne,
            NextStrategy::Random => ConnectionStrategy::Random,
            NextStrategy::GroupBy(_, _) => ConnectionStrategy::GroupBy,
            NextStrategy::All => ConnectionStrategy::All,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::block::DataType;

    #[test]
    fn test_data_type_clean() {
        let dataset = [
            ("aaa", "aaa"),
            ("aaa::bbb::ccc", "ccc"),
            ("(aaa, bbb::ccc::ddd)", "(aaa, ddd)"),
            ("aaa<bbb>", "aaa<bbb>"),
            ("aaa::bbb::ccc<ddd::eee>", "ccc<eee>"),
            ("aaa::bbb<ccc::ddd<eee>>", "bbb<ddd<eee>>"),
        ];
        for (input, expected) in &dataset {
            assert_eq!(&DataType::clean_str(input), expected);
        }
    }
}
