//! Advisory alert bus: emit sites push fire-and-forget events, one dispatcher
//! (spawned by the server) drains them to the configured webhook. Bounded and
//! lossy by design — an alert must never block or slow the serving path.

use tokio::sync::mpsc;

/// Bounded: a stuck dispatcher drops alerts instead of growing memory.
const ALERT_QUEUE: usize = 256;

/// One outbound alert.
#[derive(Debug)]
pub struct AlertEvent {
    pub kind: &'static str,
    pub subject: String,
    pub detail: String,
    pub at_epoch_secs: i64,
}

/// The emit side is sync and never fails; the receiver is taken once by the
/// dispatch task (survives config reloads — the bus is a preserved seam).
pub struct AlertBus {
    tx: mpsc::Sender<AlertEvent>,
    rx: std::sync::Mutex<Option<mpsc::Receiver<AlertEvent>>>,
}

impl AlertBus {
    pub fn emit(&self, kind: &'static str, subject: String, detail: String) {
        let ev = AlertEvent {
            kind,
            subject,
            detail,
            at_epoch_secs: crate::epoch_secs(),
        };
        if self.tx.try_send(ev).is_err() {
            tracing::debug!(kind, "alert queue full or unclaimed; dropped");
        }
    }

    /// The single consumer end; `None` after the first take.
    pub fn take_receiver(&self) -> Option<mpsc::Receiver<AlertEvent>> {
        self.rx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

impl Default for AlertBus {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel(ALERT_QUEUE);
        Self {
            tx,
            rx: std::sync::Mutex::new(Some(rx)),
        }
    }
}

impl std::fmt::Debug for AlertBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AlertBus")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn emits_are_lossy_and_receiver_single() {
        let bus = AlertBus::default();
        let mut rx = bus.take_receiver().expect("first take");
        assert!(bus.take_receiver().is_none(), "single consumer");
        bus.emit("abuse_suspend", "k1".into(), "2 rejects".into());
        let ev = rx.recv().await.expect("delivered");
        assert_eq!((ev.kind, ev.subject.as_str()), ("abuse_suspend", "k1"));
        for _ in 0..(ALERT_QUEUE + 10) {
            bus.emit("account_cooldown", "a".into(), String::new());
        }
        drop(rx);
        bus.emit("account_cooldown", "a".into(), String::new());
    }
}
