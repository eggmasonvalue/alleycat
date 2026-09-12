//! Translation logic for `agy` protocol.

pub mod events;
pub mod input;

pub use events::EventTranslatorState;
pub use input::{InputTranslationError, translate_user_input};
