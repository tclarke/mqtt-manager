//! Convenience class for managing the MQTT connection
//! 
//! # Examples
//! 
//! ```no_run
//! use mqtt_manager::*;
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

pub use rumqttc::Publish;
use rumqttc::{MqttOptions, AsyncClient, EventLoop, Event, mqttbytes::matches};

/// Type for subscription callbacks. See [`crate::make_callback`]
pub type CallbackFn = Box<dyn Fn(Publish) -> Pin<Box<dyn Future<Output=()>>>>;

/// A macro to turn an async fn into a callback to be passed to [`MqttManager::subscribe`]
/// 
/// # Example
/// ```
/// use mqtt_manager::{make_callback, Publish};
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
    /// - mqtt://localhost:1883?client_id=client2
    pub fn new(host_url: &str) -> MqttManager {
        let mut opts = if host_url.contains("client_id=") {
            MqttOptions::parse_url(host_url).expect("Error parsing MQTT URL")
        } else {
            MqttOptions::parse_url(format!("{host_url}?client_id=busbridge")).expect("Error parsing MQTT URL")
        };
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
    pub async fn process(&mut self) -> std::result::Result<(), rumqttc::ConnectionError> {
        match self.eventloop.poll().await {
            Ok(Event::Incoming(rumqttc::Packet::Publish(data))) => {
                for (filter, callback) in self.subscriptions.iter() {
                    if matches(&data.topic, filter) {
                        callback(data).await;
                        break;
                    }
                }
                Ok(())
            }
            Ok(_) => Ok(()),
            Err(err) => Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    /// These tests require an MQTT broker.
    /// `docker run eclipse-mosquitto:2.0` will run mosquitto on localhost:1883
    /// Other brokers have not been tested except mqttest which is known to not
    /// work as it doesn't support QOS 2.
    use bytes::Bytes;

    use super::*;

    const MQTT_URL: &str = "mqtt://localhost:1883?client_id=";

    #[tokio::test]
    async fn connect() {
        let mut mgr = MqttManager::new(format!("{MQTT_URL}rts_connect").as_str());
        mgr.process().await.expect("Connection refused. Make sure you are running a broker on localhost:1883");
        mgr.disconnect().await;
    }

    #[tokio::test]
    async fn publish() {
        let mut mgr = MqttManager::new(format!("{MQTT_URL}rts_publish").as_str());
        mgr.publish("test", "bar", 0).await;
        mgr.publish("test", "ack", 1).await;
        mgr.publish("test", "blah", 2).await;
        for _ in 0..8 {  // We expect exactly 8 packets for the above sequence
            tokio::time::timeout(Duration::from_secs(5), mgr.process()).await.expect("Error, timed out waiting on packet").unwrap();
        }
        mgr.disconnect().await;
    }

    #[tokio::test]
    async fn subscribe() {
        let mut mgr = MqttManager::new(format!("{MQTT_URL}rts_subscribe").as_str());
        mgr.process().await.unwrap();
        mgr.subscribe("test2", 0, make_callback!(|pkt: Publish| async move { assert_eq!(pkt.payload, Bytes::from("test")); })).await;
        mgr.process().await.unwrap();
        mgr.publish("test2", "test", 0).await;
        mgr.process().await.unwrap();
        mgr.process().await.unwrap();
        mgr.process().await.unwrap();
        mgr.disconnect().await;
    }
}