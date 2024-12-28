pub struct PollStateRequest{
    pub reply_to: tokio::sync::mpsc::Sender<PollStateResponse>,
}

pub struct PollStateResponse{
    pub state: ServerStateOnlyForTesting,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ServerStateOnlyForTesting {
    Follower,
    Candidate,
    Leader,
}