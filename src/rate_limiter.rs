use std::time::Instant;

/// Non-blocking rate limiter for Jito bundle submissions.
/// If the limit is exceeded, `try_acquire` returns false -- caller should drop the tx.
pub struct RateLimiter {
    max_per_second: u32,
    timestamps: Vec<Instant>,
}

impl RateLimiter {
    pub fn new(max_per_second: u32) -> Self {
        Self {
            max_per_second,
            timestamps: Vec::with_capacity(max_per_second as usize),
        }
    }

    /// Try to acquire a slot. Returns true if allowed, false if rate limit exceeded.
    /// Never blocks -- if limit is hit, the caller should immediately discard the tx.
    pub fn try_acquire(&mut self) -> bool {
        let now = Instant::now();
        let one_sec_ago = now - std::time::Duration::from_secs(1);

        // Remove timestamps older than 1 second
        self.timestamps.retain(|&t| t > one_sec_ago);

        if (self.timestamps.len() as u32) < self.max_per_second {
            self.timestamps.push(now);
            true
        } else {
            false
        }
    }
}
