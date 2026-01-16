//! Request priority hints for TiKV commands.

/// The scheduling priority hint for TiKV to execute a command.
///
/// This is mapped to `kvrpcpb::Context.priority`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum CommandPriority {
    Low,
    #[default]
    Normal,
    High,
}

impl From<CommandPriority> for i32 {
    fn from(value: CommandPriority) -> Self {
        match value {
            CommandPriority::Normal => 0,
            CommandPriority::Low => 1,
            CommandPriority::High => 2,
        }
    }
}
