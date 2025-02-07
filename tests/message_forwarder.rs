use tokio::task::JoinHandle;

use actum::testkit::Testkit;
use actum::effect::EffectType;
use oxidized_float::messages::RaftMessage;

pub fn init_message_forwarder<SMin, SMout>(mut testkit: Testkit<RaftMessage<SMin, SMout>>) -> JoinHandle<()>
where 
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    tokio::spawn(async move {
        let mut stopped = false;
        while !stopped {
            testkit.test_next_effect(|effect| {
                match effect {
                    EffectType::Stopped(_) => {},
                    EffectType::Message(_) => {},
                    EffectType::NoMoreSenders(_) => {},
                    EffectType::Spawn(_) => {},
                    EffectType::Returned(_) => stopped = true,
                }
            }).await;
        }
    })
}
