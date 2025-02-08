use tokio::task::JoinHandle;

use actum::testkit::Testkit;
use actum::effect::EffectType;
use oxidized_float::messages::RaftMessage;

pub fn init_message_forwarder<SMin, SMout>(mut testkit: Testkit<RaftMessage<SMin, SMout>>, i: usize) -> JoinHandle<()>
where 
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    tokio::spawn(async move {
        let mut return_var = false;
        let mut stopped = false;

        while !return_var {
            testkit.test_next_effect(|effect| {
                match effect {
                    EffectType::Stopped(_) => { 
                        if !stopped {
                            tracing::trace!("Actor {} recieved stopped", i); 
                            stopped = true;
                        } else {
                            tracing::error!("Actor {} recieved stopped for the second time", i); 
                        }
                    },
                    EffectType::Message(_) => {},
                    EffectType::NoMoreSenders(_) => {},
                    EffectType::Spawn(_) => {},
                    EffectType::Returned(_) => return_var = true,
                }
            }).await;
        }
        tracing::debug!("Message forwarder {} returned", i);
    })
}
