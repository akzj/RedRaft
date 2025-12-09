//! Pub/Sub (Publish/Subscribe) implementation
//!
//! Memory-based publish/subscribe system with channel and pattern subscriptions
//! Note: Not persistent, messages are lost on restart

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Subscriber channel sender
type Subscriber = mpsc::UnboundedSender<Vec<u8>>;

/// Pub/Sub storage implementation
#[derive(Clone)]
pub struct PubSubStore {
    /// channel -> subscribers
    channels: Arc<RwLock<HashMap<String, Vec<Subscriber>>>>,
    /// pattern -> subscribers (for PSUBSCRIBE)
    patterns: Arc<RwLock<HashMap<String, Vec<Subscriber>>>>,
}

impl PubSubStore {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            patterns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// PUBLISH: Publish message to channel
    /// Returns number of subscribers that received the message
    pub fn publish(&self, channel: &str, message: Vec<u8>) -> usize {
        let mut delivered = 0;
        
        // Send to channel subscribers
        {
            let channels = self.channels.read();
            if let Some(subscribers) = channels.get(channel) {
                let mut active_subscribers = Vec::new();
                
                for subscriber in subscribers.iter() {
                    if subscriber.send(message.clone()).is_ok() {
                        delivered += 1;
                        active_subscribers.push(subscriber.clone());
                    }
                }
                
                // Update with only active subscribers
                drop(channels);
                let mut channels = self.channels.write();
                if let Some(subs) = channels.get_mut(channel) {
                    *subs = active_subscribers;
                }
            }
        }
        
        // Send to pattern subscribers
        {
            let patterns = self.patterns.read();
            let matching_patterns: Vec<String> = patterns
                .keys()
                .filter(|pattern| Self::match_pattern(pattern, channel))
                .cloned()
                .collect();
            drop(patterns);
            
            for pattern in matching_patterns {
                let mut patterns = self.patterns.write();
                if let Some(pattern_subscribers) = patterns.get_mut(&pattern) {
                    let mut active_subscribers = Vec::new();
                    
                    for subscriber in pattern_subscribers.iter() {
                        if subscriber.send(message.clone()).is_ok() {
                            delivered += 1;
                            active_subscribers.push(subscriber.clone());
                        }
                    }
                    
                    *pattern_subscribers = active_subscribers;
                }
            }
        }
        
        delivered
    }

    /// SUBSCRIBE: Subscribe to channel
    /// Returns receiver channel for messages
    pub fn subscribe(&self, channel: String) -> mpsc::UnboundedReceiver<Vec<u8>> {
        let (tx, rx) = mpsc::unbounded_channel();
        
        let mut channels = self.channels.write();
        channels.entry(channel).or_insert_with(Vec::new).push(tx);
        
        rx
    }

    /// PSUBSCRIBE: Subscribe to pattern
    /// Returns receiver channel for messages
    pub fn psubscribe(&self, pattern: String) -> mpsc::UnboundedReceiver<Vec<u8>> {
        let (tx, rx) = mpsc::unbounded_channel();
        
        let mut patterns = self.patterns.write();
        patterns.entry(pattern).or_insert_with(Vec::new).push(tx);
        
        rx
    }

    /// UNSUBSCRIBE: Unsubscribe from channel
    /// Note: In practice, this is handled by closing the receiver
    pub fn unsubscribe(&self, channel: &str) -> usize {
        let mut channels = self.channels.write();
        if let Some(subscribers) = channels.get_mut(channel) {
            let count = subscribers.len();
            channels.remove(channel);
            count
        } else {
            0
        }
    }

    /// PUNSUBSCRIBE: Unsubscribe from pattern
    pub fn punsubscribe(&self, pattern: &str) -> usize {
        let mut patterns = self.patterns.write();
        if let Some(subscribers) = patterns.get_mut(pattern) {
            let count = subscribers.len();
            patterns.remove(pattern);
            count
        } else {
            0
        }
    }

    /// PUBSUB CHANNELS: List all channels (optionally matching pattern)
    pub fn channels(&self, pattern: Option<&str>) -> Vec<String> {
        let channels = self.channels.read();
        
        if let Some(pattern) = pattern {
            channels
                .keys()
                .filter(|channel| Self::match_pattern(pattern, channel))
                .cloned()
                .collect()
        } else {
            channels.keys().cloned().collect()
        }
    }

    /// PUBSUB NUMSUB: Get number of subscribers for channel(s)
    pub fn numsub(&self, channels: &[&str]) -> HashMap<String, usize> {
        let channels_map = self.channels.read();
        let mut result = HashMap::new();
        
        for channel in channels {
            let count = channels_map
                .get(*channel)
                .map(|subs| subs.len())
                .unwrap_or(0);
            result.insert(channel.to_string(), count);
        }
        
        result
    }

    /// PUBSUB NUMPAT: Get number of pattern subscribers
    pub fn numpat(&self) -> usize {
        let patterns = self.patterns.read();
        patterns.values().map(|subs| subs.len()).sum()
    }

    /// Match pattern against channel name
    /// Supports Redis pattern matching: * (matches any chars), ? (matches single char)
    fn match_pattern(pattern: &str, channel: &str) -> bool {
        // Simple pattern matching implementation
        // * matches any sequence of characters
        // ? matches any single character
        
        let pattern_chars: Vec<char> = pattern.chars().collect();
        let channel_chars: Vec<char> = channel.chars().collect();
        
        Self::match_pattern_recursive(&pattern_chars, &channel_chars, 0, 0)
    }

    fn match_pattern_recursive(
        pattern: &[char],
        channel: &[char],
        p_idx: usize,
        c_idx: usize,
    ) -> bool {
        // If pattern is exhausted
        if p_idx >= pattern.len() {
            return c_idx >= channel.len();
        }
        
        // If channel is exhausted but pattern has more
        if c_idx >= channel.len() {
            // Only match if remaining pattern is all *
            return pattern[p_idx..].iter().all(|&c| c == '*');
        }
        
        match pattern[p_idx] {
            '*' => {
                // Try matching 0 or more characters
                // First try matching 0 characters
                if Self::match_pattern_recursive(pattern, channel, p_idx + 1, c_idx) {
                    return true;
                }
                // Then try matching 1+ characters
                Self::match_pattern_recursive(pattern, channel, p_idx, c_idx + 1)
            }
            '?' => {
                // Match single character
                Self::match_pattern_recursive(pattern, channel, p_idx + 1, c_idx + 1)
            }
            c => {
                // Match exact character
                if c == channel[c_idx] {
                    Self::match_pattern_recursive(pattern, channel, p_idx + 1, c_idx + 1)
                } else {
                    false
                }
            }
        }
    }
}

impl Default for PubSubStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matching() {
        assert!(PubSubStore::match_pattern("news.*", "news.sports"));
        assert!(PubSubStore::match_pattern("news.*", "news.tech"));
        assert!(!PubSubStore::match_pattern("news.*", "sports.news"));
        
        assert!(PubSubStore::match_pattern("news.?", "news.1"));
        assert!(!PubSubStore::match_pattern("news.?", "news.12"));
        
        assert!(PubSubStore::match_pattern("*", "anything"));
        assert!(PubSubStore::match_pattern("news*", "news"));
    }
}

