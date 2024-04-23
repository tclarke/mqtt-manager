//! Convenience class for managing the MQTT connection
//! 
//! # Examples
//! 
//! ```no_run
//! use mqtt_manager::*;
//! use rumqttc::Publish;
//! use std::time::Duration;
//! 
//! async fn handle_msg_a(pubdata: Publish) {
//!     println!("Received msg A: {:?}", pubdata.payload);
//! }
//! 
//! async fn handle_msg_b(pubdata: Publish) {
//!     println!("Received msg A: {:?}", pubdata.payload);
//! }
//! 
//! #[tokio::main]
//! async fn main() {
//!     let mut mgr = MqttManager::new("mqtt://localhost:1883/override_client_id");
//!     mgr.subscribe("msg/a", 0, make_callback!(handle_msg_a)).await;
//!     mgr.subscribe("msg/b", 0, make_callback!(handle_msg_b)).await;
//!     mgr.publish("msg/a", "test", 0).await;
//!     loop {
//!         tokio::select! {
//!             _ = mgr.process() => (),
//!             _ = tokio::signal::ctrl_c() => {
//!                 mgr.disconnect().await;
//!                 break;
//!             }
//!         }
//!     }
//! }
//! ```
use std::pin::Pin;
use std::{time::Duration, future::Future};
use std::collections::HashMap;

use rumqttc::{MqttOptions, AsyncClient, EventLoop, Event, Publish};
use url::Url;

/// Type for subscription callbacks. See [`crate::make_callback`]
pub type CallbackFn = Box<dyn Fn(Publish) -> Pin<Box<dyn Future<Output=()>>>>;

/// A macro to turn an async fn into a callback to be passed to [`MqttManager::subscribe`]
/// 
/// # Example
/// ```
/// use mqtt_manager::make_callback;
/// use rumqttc::Publish;
///
/// async fn callback(pubpkt: Publish) {}
/// 
/// let cb_handle = make_callback!(callback);
/// ```
#[macro_export]
macro_rules! make_callback {
    ($fn:expr) => {
        Box::new(move |publish| Box::pin($fn(publish)))
    };
}

/// The main MQTT manager struct
pub struct MqttManager {
    client: AsyncClient,
    eventloop: EventLoop,
    subscriptions: HashMap<String, CallbackFn>,
}

#[allow(dead_code)]
impl MqttManager {
    /// Create a new MqttManager from a host URL in the form:
    /// - mqtt://localhost
    /// - mqtt://localhost:1883
    /// - mqtt://localhost:1883/rts_2
    /// 
    /// The default client ID is "rts".
    /// 
    /// The first form uses the default port 1883.
    /// 
    /// The second form specified a host and port.
    /// 
    /// The third form also overrides the client ID. This is typically only done in tests to
    /// allow for multiple test threads to run correctly.
    pub fn new(host_url: &str) -> MqttManager {
        let u = Url::parse(host_url).expect("Invalid URL");
        assert_eq!(u.scheme(), "mqtt");
        let host = u.host_str().unwrap_or("localhost");
        let port = u.port().unwrap_or(1883);
        let id = u.path_segments().map_or("rts", |mut segs| { segs.next().unwrap() });
        let mut opts = MqttOptions::new(id, host, port);
        opts.set_keep_alive(Duration::from_secs(60));
        let (client, eventloop) = AsyncClient::new(opts, 10);
        MqttManager {
            client,
            eventloop,
            subscriptions: HashMap::new(),
        }
    }

    /// Send a DISCONNECT to clean up the connection.
    /// ## panic
    /// Panics when the call to disconnect fails.
    pub async fn disconnect(&mut self) {
        self.client.disconnect().await.expect("Unable to disconnect");
    }
    
    /// Publish data to a topic.
    /// 
    /// topic is the topic name, payload is the payload bytes, and qos is the MQTT qos in the range 0-2
    /// ## panic
    /// This panics if the qos is invalid or if there is an error publishing.
    pub async fn publish<T, U>(&mut self, topic: T, payload: U, qos: u8)
    where T: Into<String>,
          U: Into<Vec<u8>> {
        self.client.publish(topic, rumqttc::qos(qos).expect("Invalid QoS value"), false, payload).await.expect("Error publishing")
    }

    /// Subscribe to a topic.
    /// 
    /// topic is the subscription topic including optional wildcards per the MQTT spec.
    /// qos is the subscription qos in the range 0-3. callback is a callback async function
    /// which should be wrapped by calling [`crate::make_callback`].
    /// ## panic
    /// This panics if the qos is invalid or if there is an error subscribing.
    pub async fn subscribe<T: Into<String>>(&mut self, topic: T, qos: u8, callback: CallbackFn) {
        let t = topic.into();
        self.client.subscribe(t.clone(), rumqttc::qos(qos).expect("Invalid QoS value")).await.expect("Failed to subscribe");
        self.subscriptions.insert(t, callback);
    }

    /// Wait for a single packet and process reconnects, pings, etc.
    /// 
    /// This should be called reguarly, either in an event loop or in a background thread using tokio::spawn.
    /// 
    /// If a SUBSCRIBE packet is returned and there's a registered callback it will be await'd.
    pub async fn process(&mut self) {
        match self.eventloop.poll().await {
            Ok(Event::Incoming(rumqttc::Packet::Publish(data))) => {
                if let Some(callback) = self.subscriptions.get(&data.topic) {
                    callback(data).await;
                }
            }
            Ok(_) => (),
            Err(err) => println!("{err}")
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    const MQTT_URL: &str = "mqtt://mosquitto.mosquitto.svc.cluster.local";

    #[tokio::test]
    async fn connect() {
        let mut mgr = MqttManager::new(MQTT_URL);
        mgr.process().await;
        mgr.disconnect().await;
    }

    #[tokio::test]
    async fn publish() {
        let mut mgr = MqttManager::new(format!("{MQTT_URL}/rts_publish").as_str());
        mgr.publish("test", "bar", 0).await;
        mgr.publish("test", "ack", 1).await;
        mgr.publish("test", "blah", 2).await;
        for _ in 0..8 {  // We expect exactly 8 packets for the above sequence
            tokio::time::timeout(Duration::from_secs(5), mgr.process()).await.expect("Error, timed out waiting on packet");
        }
        mgr.disconnect().await;
    }

    #[tokio::test]
    async fn subscribe() {
        async fn check_payload(pkt: Publish) {
            assert_eq!(pkt.payload, Bytes::from("test"));
        }

        let mut mgr = MqttManager::new(format!("{MQTT_URL}/rts_subscribe").as_str());
        mgr.process().await;
        mgr.subscribe("test2", 0, make_callback!(check_payload)).await;
        mgr.process().await;
        mgr.publish("test2", "test", 0).await;
        mgr.process().await;
        mgr.process().await;
        mgr.process().await;
        mgr.disconnect().await;
    }
}