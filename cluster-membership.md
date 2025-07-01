# Dynamic Cluster Membership Strategy

paper trash

add and remove peers one at a time,

route add/removes through raft

AppendEntriesRPC has a special flag that denotes if its a cluster membership change

don't apply add/removes to state machine, treat it as a special config change apply only for raft and then use a callback separate from raft.Deliver() to notify upper level statemachine a peer change has occurred

things to think about:
- cold start problem
    - do we hardcode a peermap on start? itll potentially be wrong on restart
    - can we dynamically calculate the peer map by sending an initial "whos there" RPC to the group via a NATS subject?
