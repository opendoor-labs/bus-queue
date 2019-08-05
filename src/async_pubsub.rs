use super::async_::channel as async_channel;
use super::async_::Publisher;
use super::async_::Subscriber;
use futures::{Async, Poll, Stream};
use std::any::Any;
use std::marker::PhantomData;

pub fn channel<T: Send>(size: usize) -> (TopicPublisher<T>, TopicSubscriber<T>) {
    let (publisher, subscriber) = async_channel(size);
    (publisher, TopicSubscriber::new(subscriber))
}

pub struct TopicPayload<T> {
    topic: T,
    message: Box<dyn Any + Send>,
}

impl<T> TopicPayload<T> {
    pub fn new<M: Any + Send>(topic: T, message: M) -> Self {
        Self {
            topic,
            message: Box::new(message),
        }
    }

    pub fn downcast_cloned<U>(&self) -> Option<U>
    where
        U: Clone + 'static,
    {
        self.message.downcast_ref::<U>().map(Clone::clone)
    }
}

type TopicPublisher<T> = Publisher<TopicPayload<T>>;

pub struct TopicSubscriber<T: Send> {
    inner: Subscriber<TopicPayload<T>>,
}

impl<T: Send> TopicSubscriber<T> {
    fn new(subscriber: Subscriber<TopicPayload<T>>) -> Self {
        Self { inner: subscriber }
    }
}

impl<T: Eq + Send> TopicSubscriber<T> {
    pub fn subscription<M>(&self, topic: T) -> TopicSubscription<T, M> {
        TopicSubscription::<_, M>::new(topic, self.inner.clone())
    }
}

pub struct TopicSubscription<T: Send, M> {
    topic: T,
    inner: Subscriber<TopicPayload<T>>,
    _m: PhantomData<M>,
}

impl<T: Eq + Send, M> TopicSubscription<T, M> {
    fn new(topic: T, subscriber: Subscriber<TopicPayload<T>>) -> Self {
        Self {
            topic,
            inner: subscriber,
            _m: PhantomData,
        }
    }
}

impl<T, M> Stream for TopicSubscription<T, M>
where
    T: Eq,
    T: Send,
    M: Clone,
    M: 'static,
{
    type Item = M;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match try_ready!(self.inner.poll()) {
                Some(payload) => {
                    if payload.topic == self.topic {
                        let message = payload.downcast_cloned::<M>().ok_or(())?;
                        return Ok(Async::Ready(Some(message)));
                    }
                }
                None => return Ok(Async::Ready(None)),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::stream;
    use futures::Future;
    use futures::Sink;
    use std::sync::Arc;
    use tokio::runtime::current_thread::Runtime;

    #[test]
    fn async_pubsub() {
        let mut rt = Runtime::new().unwrap();
        let (publisher, subscriber) = channel(10);
        #[derive(Clone)]
        struct Dummy {
            a: u32,
        }

        let messages = vec![
            TopicPayload::new("numbers", 1u32),
            TopicPayload::new("strings", "FIRST"),
            TopicPayload::new("strings", "SECOND"),
            TopicPayload::new("strings", "THIRD"),
            TopicPayload::new("strings", "FOURTH"),
            TopicPayload::new("numbers", 2u32),
            TopicPayload::new("structs", Arc::new(Dummy { a: 123 })),
        ];
        let pub_task = stream::iter_ok(messages)
            .forward(publisher)
            .and_then(|(_, mut sink)| sink.close())
            .map(|_| ())
            .map_err(|_| ());

        rt.spawn(pub_task);

        let sub1 = subscriber.subscription("numbers");
        let numbers: Vec<u32> = rt.block_on(sub1.collect()).unwrap();
        assert_eq!(numbers.len(), 2);
        assert_eq!(numbers[0], 1);
        assert_eq!(numbers[1], 2);

        let sub2 = subscriber.subscription::<&str>("strings");
        let strings: Vec<_> = rt.block_on(sub2.collect()).unwrap();
        assert_eq!(strings.len(), 4);
        assert_eq!(strings[0], "FIRST");
        assert_eq!(strings[1], "SECOND");
        assert_eq!(strings[2], "THIRD");
        assert_eq!(strings[3], "FOURTH");

        let sub2 = subscriber.subscription("structs");
        let structs: Vec<Arc<Dummy>> = rt.block_on(sub2.collect()).unwrap();
        assert_eq!(structs.len(), 1);
        assert_eq!(structs[0].a, 123);
    }
}
