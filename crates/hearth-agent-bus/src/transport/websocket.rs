//! One WebSocket text message = one JSON value.

use super::{parse_one_json, Frame, Framer, TransportError, MAX_FRAME_BYTES};

/// One WebSocket text message = one JSON value. Does not concatenate messages.
pub struct WsJsonFramer {
    failed: Option<TransportError>,
}

impl Default for WsJsonFramer {
    fn default() -> Self {
        Self::new()
    }
}

impl WsJsonFramer {
    pub fn new() -> Self {
        Self { failed: None }
    }

    fn check(&self) -> Result<(), TransportError> {
        match &self.failed {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    fn die<T>(&mut self, err: TransportError) -> Result<T, TransportError> {
        self.failed = Some(err.clone());
        Err(err)
    }

    /// One complete WS text frame.
    pub fn push_message(&mut self, bytes: &[u8]) -> Result<Frame, TransportError> {
        self.check()?;
        if bytes.len() > MAX_FRAME_BYTES {
            return self.die(TransportError::TooLarge(bytes.len()));
        }
        match parse_one_json(bytes) {
            Ok(v) => Ok(Frame::ws(v)),
            Err(e) => self.die(e),
        }
    }
}

impl Framer for WsJsonFramer {
    fn push_bytes(&mut self, bytes: &[u8]) -> Result<Vec<Frame>, TransportError> {
        Ok(vec![self.push_message(bytes)?])
    }

    fn finish(&mut self) -> Result<Vec<Frame>, TransportError> {
        self.check()?;
        Ok(vec![])
    }
}

