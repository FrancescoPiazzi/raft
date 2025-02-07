use actum::effect::EffectType;
use actum::testkit::Testkit;
use oxidized_float::messages::RaftMessage;

pub async fn run_testkit_until_actor_returns<SMin, SMout>(mut testkit: Testkit<RaftMessage<SMin, SMout>>)
where
    SMin: Clone + Send + 'static,
    SMout: Send + 'static,
{
    let mut stopped = false;
    while !stopped {
        testkit
            .test_next_effect(|effect| match effect {
                EffectType::Returned(_) => stopped = true,
                _ => {}
            })
            .await;
    }
}
