use futures::StreamExt;
use tracing::Instrument;
use std::thread::sleep;

use actum::prelude::*;

async fn generic_parent<AB>(mut cell: AB, me: ActorRef<u64>)
where
    AB: ActorBounds<u64>,
{
    loop{
        let mut last_msg_received: Option<u64> = Option::None;

        // receive a message
        let m1 = cell.recv().await.unwrap();
        tracing::info!(recv = m1);

        let n_childs = m1;

        for _ in 0..n_childs {
            // spawn a child actor
            let parent = me.clone();
            let child = cell
                .spawn(move | cell, me| async move {
                    times_two_node(cell, me, parent, m1).await;
                })
                .await
                .unwrap();
            let span = tracing::info_span!("child");
            tokio::spawn(child.task.run_task().instrument(span));
        }

        // wait for all children to finish
        for _ in 0..n_childs {
            let m2 = cell.recv().await.unwrap();
            match last_msg_received {
                Some(last_msg) => {
                    if m2 == last_msg{
                        tracing::info!("received from child: {}", m2);
                    } else {
                        tracing::error!("received from child: {}, expected: {}", m2, last_msg);
                    }
                },
                None => {last_msg_received = Some(m2); tracing::info!("received from child: {} (first)", m2);}
            }
        }

        tracing::info!("all children finished");
    }
}

async fn times_two_node<AB>(mut _cell: AB, mut _me: ActorRef<u64>, mut parent: ActorRef<u64>, m: u64)
where
    AB: ActorBounds<u64>,
{
    tracing::trace!(try_send = m*2);
    parent.try_send(m*2).unwrap();
}


#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_span_events(
            tracing_subscriber::fmt::format::FmtSpan::NEW | tracing_subscriber::fmt::format::FmtSpan::CLOSE,
        )
        .with_target(false)
        .with_line_number(true)
        .with_max_level(tracing::Level::INFO)
        .init();
    let mut parent = actum(generic_parent);
    let span = tracing::info_span!("parent");
    let handle = tokio::spawn(parent.task.run_task().instrument(span));

    parent.m_ref.try_send(5).unwrap();
    sleep(std::time::Duration::from_secs(1));
    parent.m_ref.try_send(7).unwrap();
    sleep(std::time::Duration::from_secs(1));
    parent.m_ref.try_send(9).unwrap();

    handle.await.unwrap();
}

