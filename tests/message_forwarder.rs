use tokio::task::JoinHandle;

use actum::testkit::Testkit;
use oxidized_float::messages::RaftMessage;

pub fn init_message_forwarder<SMin, SMout>(mut testkit: Testkit<RaftMessage<SMin, SMout>>) -> JoinHandle<()>
where 
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    tokio::spawn(async move {
        loop {
            testkit.test_next_effect(|_| {}).await;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
}
