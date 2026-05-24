use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;

use crate::content::{Content, ContentInput};
use crate::error::{Result, SpectrumError};
use crate::platform::{Message, Space};
use crate::stream::ManagedStream;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpectrumOptions {
    pub flatten_groups: bool,
}

#[async_trait]
pub trait SpectrumProvider: Send + Sync {
    async fn messages(&self) -> Result<ManagedStream<(Arc<dyn Space>, Message)>>;
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

pub struct Spectrum {
    providers: Vec<Arc<dyn SpectrumProvider>>,
    options: SpectrumOptions,
}

impl Spectrum {
    pub fn new(providers: Vec<Arc<dyn SpectrumProvider>>) -> Self {
        Self {
            providers,
            options: SpectrumOptions::default(),
        }
    }

    pub fn with_options(mut self, options: SpectrumOptions) -> Self {
        self.options = options;
        self
    }

    pub async fn build(self) -> Result<SpectrumInstance> {
        let mut streams = Vec::new();
        for provider in &self.providers {
            streams.push(provider.messages().await?);
        }
        Ok(SpectrumInstance {
            providers: self.providers,
            messages: streams,
            pending_messages: VecDeque::new(),
            stopped: AtomicBool::new(false),
            options: self.options,
        })
    }
}

pub struct SpectrumInstance {
    providers: Vec<Arc<dyn SpectrumProvider>>,
    messages: Vec<ManagedStream<(Arc<dyn Space>, Message)>>,
    pending_messages: VecDeque<(Arc<dyn Space>, Message)>,
    stopped: AtomicBool,
    options: SpectrumOptions,
}

impl SpectrumInstance {
    pub fn options(&self) -> SpectrumOptions {
        self.options
    }

    pub async fn next_message(&mut self) -> Option<(Arc<dyn Space>, Message)> {
        if let Some(item) = self.pending_messages.pop_front() {
            return Some(item);
        }
        for stream in &mut self.messages {
            if let Some((space, message)) = stream.next().await {
                if self.options.flatten_groups
                    && let Content::Group(group) = &message.content
                {
                    for item in &group.items {
                        self.pending_messages
                            .push_back((space.clone(), item.clone()));
                    }
                    return self.pending_messages.pop_front();
                }
                return Some((space, message));
            }
        }
        None
    }

    pub async fn send(
        &self,
        space: &dyn Space,
        content: impl Into<ContentInput>,
    ) -> Result<Option<Message>> {
        if self.stopped.load(Ordering::SeqCst) {
            return Err(SpectrumError::msg("Spectrum instance has been stopped"));
        }
        space.send(content.into()).await
    }

    pub async fn stop(&mut self) -> Result<()> {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        for stream in &mut self.messages {
            stream.close().await;
        }
        for provider in &self.providers {
            provider.stop().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{Content, Group, Text};
    use crate::platform::{MessageDirection, SpaceRef};
    use crate::stream::stream;
    use async_trait::async_trait;
    use serde_json::Map;

    struct TestSpace;

    #[async_trait]
    impl Space for TestSpace {
        fn id(&self) -> &str {
            "space-1"
        }

        fn platform(&self) -> &str {
            "test"
        }

        async fn send(&self, _content: ContentInput) -> Result<Option<Message>> {
            Ok(None)
        }
    }

    struct TestProvider {
        message: Message,
    }

    #[async_trait]
    impl SpectrumProvider for TestProvider {
        async fn messages(&self) -> Result<ManagedStream<(Arc<dyn Space>, Message)>> {
            let message = self.message.clone();
            Ok(stream(move |tx, _closed| async move {
                let space: Arc<dyn Space> = Arc::new(TestSpace);
                tx.send((space, message))
                    .await
                    .map_err(|err| SpectrumError::msg(err.to_string()))?;
                Ok(())
            }))
        }
    }

    fn text_message(id: &str, text: &str) -> Message {
        Message {
            id: id.to_string(),
            content: Content::Text(Text {
                text: text.to_string(),
            }),
            direction: MessageDirection::Inbound,
            platform: "test".to_string(),
            sender: None,
            space: SpaceRef {
                id: "space-1".to_string(),
                platform: "test".to_string(),
                extra: Map::new(),
            },
            extra: Map::new(),
        }
    }

    #[tokio::test]
    async fn next_message_flattens_group_items_when_enabled() {
        let group = Message {
            id: "group".to_string(),
            content: Content::Group(Group {
                items: vec![text_message("a", "one"), text_message("b", "two")],
            }),
            direction: MessageDirection::Inbound,
            platform: "test".to_string(),
            sender: None,
            space: SpaceRef {
                id: "space-1".to_string(),
                platform: "test".to_string(),
                extra: Map::new(),
            },
            extra: Map::new(),
        };

        let provider = Arc::new(TestProvider { message: group });
        let mut spectrum = Spectrum::new(vec![provider])
            .with_options(SpectrumOptions {
                flatten_groups: true,
            })
            .build()
            .await
            .unwrap();

        let first = spectrum.next_message().await.unwrap().1;
        let second = spectrum.next_message().await.unwrap().1;
        assert_eq!(first.id, "a");
        assert_eq!(second.id, "b");
    }
}
