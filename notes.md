Read+write in the same transaction:
        -- TinyKV NO
           Spanner gurantee for read is  before transaction value 
           Cockroach - no such restriction 

Parallel Transaction:
     Read only        Read+Write only


Cockroach
     Atomicity: 2phase commit
     Isolation: Serializable Snapshot Isolation

Spanner:
     Atomicty:       2phase commit
     ISolation : 2phase locking with timestamp (mvcc)

Project Questions:
0. Atomicity: PrN
1. Are we doing concurrent transactions? 
     No:
       Then why we have to use 2pl?
       How are we achieving that? Do we have  locks in TxnManager?
              ---> What is the drawback of not doing concurrent transaction?
     Yes:
       How are we achieving that? 2PL 
              2PL : Write : 
                    Read:  Blocking Read
                      Requests go Only to the leader? Availaibility issues.
              MVCC: Difficult to implement at this stage          

      PrN:
      Implement txnRecord in KVstore:  
      on kvstore, crash, new leader should recover transactions.
          -> define structure the tx record (similar txmanager )
          -> persist transaction record
          -> new leader assumes responsibility, identify the incomplete txrecord : applicable to both txmanager/kvstore
          -> kvstore/txmanager : have to query/react incomplete records (Steps are there in paper)
      2phase locking happens at Prepare - lock (write Intent) and commit/abort phase - unlock        

      Test cases:
        1. Happy case
        2. TxCoordinator crash
        3. KvStore crash
        4. Concurrent transactions
      Performance: Tx / sec:
        1. Without Raft (standalone)
        2. With TxManager - Replication  KVstore (ft kvstore)
        3. With Replicated TxManager - Replicated KvStore
        3. With Sharding (availability kvstore)


2. What kinds of transactions we are doing:
      Read only

      Read-write
      Write-write
      Write-Read
3. TxnCoordinator failure cases:
      Recover on new leader: Do we have code for this?

Replication Manager
