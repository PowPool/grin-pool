// Copyright 2018 Blade M. Doyle
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bufstream::BufStream;
use std::collections::HashMap;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use std::time::SystemTime;
use std::{thread, time};
use rand::Rng;

use failure::Error;
use grin_util::from_hex;
use grin_core::pow::Proof;
use grin_core::core::BlockHeader;
use grin_core::global::{ChainTypes, set_mining_mode};
use grin_core::ser::{deserialize, ser_vec};

use pool::config::{Config, NodeConfig, PoolConfig, WorkerConfig};
use pool::proto::{JobTemplate, RpcError, SubmitParams, WorkerStatusExt};

use pool::server::Server;
use pool::worker::Worker;
use pool::consensus::Proof as MinerProof;
use pool::consensus::PROOF_SIZE;

// ----------------------------------------
// Worker Connection Thread Function

// Run in a thread. Adds new connections to the workers list
fn accept_workers(
    stratum_id: String,
    config: Config,
    workers: &mut Arc<Mutex<HashMap<String, Worker>>>,
) {
    let address = config.workers.listen_address.clone() + ":"
        + &config.workers.port_difficulty.port.to_string();
    let difficulty = config.workers.port_difficulty.difficulty;
    let listener = TcpListener::bind(address).expect("Failed to bind to listen address");
    let banned: HashMap<SocketAddr, Instant> = HashMap::new();
    let mut rng = rand::thread_rng();
    // XXX TODO: Call the Redis api to get a list of banned IPs, refresh that list sometimes
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                match stream.peer_addr() {
                    Ok(worker_addr) => {
                        // XXX ALWAYS DO THIS FIRST - Check if this ip is banned and if so, drop it
                        // in banned list
                        if banned.contains_key(&worker_addr) {
                            let _ = stream.shutdown(Shutdown::Both);
                            continue;
                        }
                        warn!(
                            "Worker Listener - New connection from ip: {}",
                            worker_addr.clone()
                        );
                        // if connect peer not in ban list, add to workers list
                        stream
                            .set_nonblocking(true)
                            .expect("set_nonblocking call failed");
                        let mut worker = Worker::new(config.clone(), BufStream::new(stream));
                        debug!("add worker [{}:{}] into workers list", worker.uuid(), worker_addr.clone());
                        workers.lock().unwrap().insert(worker.uuid(), worker);
                        debug!("workers list size: {}", workers.lock().unwrap().len());
                        // The new worker is now added to the workers list
                    }
                    Err(e) => {
                        warn!(
                            "{} - Worker Listener - Error getting wokers ip address: {:?}", stratum_id, e
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    "{} - Worker Listener - Error accepting connection: {:?}", stratum_id, e
                );
            }
        }
    }
    // close the socket server
    drop(listener);
}

fn block_header(pre_pow: String,
                edge_bits: u8,
                nonce: u64,
                mut proof: Vec<u64>
        ) -> Result<BlockHeader, Error> {
    let mut pv = grin_core::ser::ProtocolVersion(2);
    let mut header_bytes = from_hex(pre_pow)?;
    let mut nonce_bytes = ser_vec(&nonce, pv)?;
    header_bytes.append(&mut nonce_bytes);
    let mut proof = Proof::new(proof);
    proof.edge_bits = edge_bits;
    let mut proof_bytes = ser_vec(&proof, pv)?;
    header_bytes.append(&mut proof_bytes);

    let header: BlockHeader = deserialize(&mut &header_bytes[..], pv)?;
    Ok(header)
}

// Define ChainType for each pool
fn pool_to_chaintype(pool: String) -> ChainTypes {
    warn!("Pool Configured as: {}", pool);
    match pool.as_str() {
        "grinpool" => { return ChainTypes::Mainnet; },
        "mwgrinpool" => { return ChainTypes::Mainnet; },
        "mwfloopool" => { return ChainTypes::Floonet; },
        "bitgrin" => { return ChainTypes::Mainnet; },
        "mwc" => { return ChainTypes::Mainnet; },
        _ => { return ChainTypes::Mainnet; },
    }
}

// ----------------------------------------
// A Grin mining pool

pub struct Pool {
    id: String,
    job: JobTemplate,
    config: Config,
    server: Server,
    difficulty: u64,
    workers: Arc<Mutex<HashMap<String, Worker>>>,
    duplicates: HashMap<Vec<u64>, usize>, // pow vector, worker id who first submitted it
    job_versions: HashMap<u64, String>,   // pre_pow string, job_id version
    chain_type: ChainTypes,
    next_reset_timestamp: u64
}

impl Pool {
    /// Create a new Grin Stratum Pool
    pub fn new(config: Config) -> Pool {
        let time_now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        };

        Pool {
            id: "Grin Pool".to_string(),
            job: JobTemplate::new(),
            config: config.clone(),
            server: Server::new(config.clone()),
            difficulty: 1,
            workers: Arc::new(Mutex::new(HashMap::new())),
            duplicates: HashMap::new(),
            job_versions: HashMap::new(),
            chain_type: pool_to_chaintype(config.grin_pool.pool.clone()),
            next_reset_timestamp: ((time_now / 60) + 1) * 60
        }
    }

    /// Run the Pool
    pub fn run(&mut self) {
        // Start a thread to listen on port and accept new worker connections
        let mut workers_th = self.workers.clone();
        // pool identity
        let id_th = self.id.clone();
        let config_th = self.config.clone();

        // deal the incoming connection
        let _listener_th = thread::spawn(move || {
            accept_workers(id_th, config_th, &mut workers_th);
        });

        // Set default pool difficulty
        self.difficulty = self.config.workers.port_difficulty.difficulty;

        // Set the Chain Type
        error!("set_mining_mode: {:?}", self.chain_type.clone());
        set_mining_mode(self.chain_type.clone());

        // ------------
        // Main loop
        loop {
            // XXX TODO: Error checking

            // (re)connect if server is not connected or is in error state
            // connect to upstream stratum server (from grin node or other grin stratum server)
            // send login & getjobtemplate while connecting
            match self.server.connect() {
                Ok(_) => { } // server.connect method also logs in and requests a job
                Err(e) => {
                    error!(
                        "{} - Unable to connect to upstream server: {}", self.id, e
                    );
                    thread::sleep(time::Duration::from_secs(1));
                    continue;
                }
            }

            // check the server for messages and handle them
            // process the requests (job) and the responses (login/getjobtemplate/submit)
            let _ = self.process_server_messages();

            // if the server gave us a new block
            let _ = self.accept_new_job();

            // Process messages from the workers
            let _ = self.process_worker_messages();

            // Process worker shares
            let _ = self.process_shares();

            // Send jobs to needy workers
            let _ = self.send_jobs();

            // Delete workers in error state
            let _num_active_workers = self.clean_workers();

            // Do data persistence of shares in the latest one minute for all workers
            // and then reset shares info for all workers
            let _ = self.save_and_reset_shares_1m();

            thread::sleep(time::Duration::from_millis(1));
        }
    }

    // ------------
    // Pool Methods
    //

    // Process messages from the upstream server
    // Will contain job requests, submit results, status results, etc...
    fn process_server_messages(&mut self) -> Result<(), RpcError> {
        match self.server.process_messages(&mut self.workers) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                // Log an error
                error!(
                    "{} - Error processing upstream message: {:?}", self.id, e
                );
                // There are also special case(s) where we want to do something for a specific
                // error
                if e.message.contains("Node is syncing") {
                    thread::sleep(time::Duration::from_secs(2));
                }
                return Err(e);
            }
        }
    }

    fn process_worker_messages(&mut self) {
        let mut id_changed: Vec<String> = vec![];
        let mut w_m = self.workers.lock().unwrap();
        for (worker_uuid, worker) in w_m.iter_mut() {
            let res = worker.process_messages();
            if worker_uuid != &*worker.uuid() {
                // User id changed - probably because they logged in
                id_changed.push(worker_uuid.clone());
                debug!("id changed:  uuid {} - {:?}", worker.uuid().clone(), res );
                worker.reset_worker_shares(self.job.height);
            }
        }
        // Rehash the worker using updated id
        for orig_id in id_changed.iter() {
            let worker_o = w_m.remove(&orig_id.clone());
            match worker_o {
                None => {},
                Some(worker) => {
                    w_m.insert(worker.uuid(), worker);
                }
            }
        }
    }

    fn send_jobs(&mut self) {
        let mut w_m = self.workers.lock().unwrap();
        for (worker_uuid, worker) in w_m.iter_mut() {
            if worker.needs_job && worker.authenticated {
                warn!("job to: {} - needs_job: {}, requested_job: {}, authenticated: {}", worker_uuid, worker.needs_job, worker.requested_job, worker.authenticated );
                // Randomize the nonce
                // XXX TODO (We do have the deserialized block header code so we can do this now)
                // use new adjusted difficulty
                if worker.status.curdiff != worker.status.nextdiff {
                    worker.set_difficulty(worker.status.nextdiff);
                }
                worker.set_height(self.job.height);
                // Print this workers worker_shares (previous block) for logstash to send to rmq
                error!("{:?}", worker.worker_shares);
                // Reset the workers current block stats
                worker.reset_worker_shares(self.job.height);
                worker.send_job(&mut self.job.clone());
            }
        }
    }

    fn accept_new_job(&mut self) {
        // Use the new job
        // server job -> pool job
        if self.job.pre_pow != self.server.job.pre_pow {
            trace!("accept_new_job for height {}, job_id {}", self.server.job.height, self.server.job.job_id);
            let new_height: bool = self.job.height != self.server.job.height;
            let mut new_job = self.server.job.clone();
            // Update the new jobs job_id (bminer wants this)
            new_job.job_id = new_job.height * 1000 + new_job.job_id;
            self.job = new_job;
            // debug!("accept_new_job broadcasting: {}", self.job.pre_pow.clone());
            // broadcast it to the workers

            // switch to the new job ( from server.job -> pool.job ) and broadcast to miner
            let _ = self.broadcast_job();
            if new_height {
                // clear last block duplicates map
                self.duplicates.clear();
                // clear the versions of the previous heights job
                self.job_versions.clear();
            }
            self.job_versions.insert(self.job.job_id, self.job.pre_pow.clone());
        }
    }

    //
    // Process shares returned by each workers
    fn process_shares(&mut self) {
        let mut w_m = self.workers.lock().unwrap();
        for (worker_uuid, worker) in w_m.iter_mut() {
            match worker.get_shares().unwrap() {
                None => {}
                Some(shares) => {
                    for mut share in shares {
                        //  Check for duplicate or add to duplicate map
                        if self.duplicates.contains_key(&share.pow) {
                            // duplicated solution, ignore
                            debug!(
                                "{} - Rejected duplicate share from worker {} with login {}",
                                self.id,
                                worker.uuid(),
                                worker.login(),
                            );
                            worker.status.rejected += 1;
                            worker.add_shares(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                            worker.add_shares_1m(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                            worker.send_err("submit".to_string(), "Failed to validate solution".to_string(), -32502);
                            continue; // Dont process this share anymore
                        } else {
                            self.duplicates.insert(share.pow.clone(), worker.user_id());
                        }

                        if share.edge_bits != self.config.grin_pool.edge_bits {
                            // Invalid Size
                            worker.status.rejected += 1;
                            worker.send_err("submit".to_string(), "Invalid POW size".to_string(), -32502);
                            continue; // Dont process this share anymore
                        }

                        // Check solution length (proofsize check in pow verify (#2805))
                        // pow vec size must be 42
                        if share.pow.len() != PROOF_SIZE {
                            warn!("Share has invalid PROOF_SIZE");
                            worker.status.rejected += 1;
                            worker.send_err("submit".to_string(), "Invalid PROOF_SIZE".to_string(), -32502);
                            continue; // Dont process this share anymore
                        }
                        // Check the height to see if its stale
                        if share.height != self.job.height {
                            // Its stale
                            warn!("Share is stale {} vs {}", share.height, self.job.height);
                            worker.status.stale += 1;
                            worker.add_shares(share.edge_bits, 0, 0, 1); // Accepted, Rejected, Stale
                            worker.add_shares_1m(share.edge_bits, 0, 0, 1); // Accepted, Rejected, Stale
                            worker.send_err("submit".to_string(), "Solution submitted too late".to_string(), -32503);
                            continue; // Dont process this share anymore
                        }
                        // Check if the pre-pow matches the job we sent - avoid "constructed solutions"
                        // A) Construct a BlockHeader from the correct version of the pre-pow and the share pow

                        // check if job_id matches to pre_pow
                        match self.job_versions.get(&share.job_id) {
                            None => {
                                worker.status.rejected += 1;
                                worker.add_shares(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                                worker.add_shares_1m(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                                continue // Dont process this share anymore
                            },
                            Some(pre_pow) => {
                                // We need:
                                //   a) The pre_pow as a vector
                                //   b) the nonce
                                //   c) the pow
                                let bh = match block_header(pre_pow.to_string(), share.edge_bits as u8, share.nonce, share.pow.clone()) {
                                    Ok(r) => { r },
                                    Err(e) => { 
                                        worker.status.rejected += 1;
                                        worker.add_shares(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                                        worker.add_shares_1m(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                                        debug!(
                                            "{} - Rejected mismatched block header: {}",
                                            self.id,
                                            e
                                        );
                                        worker.send_err("submit".to_string(), "Failed to validate solution".to_string(), -32502);
                                        continue; // Dont process this share anymore

                                    },
                                };
                                // B) Call into grin_core::pow::verify_size()
                                let verify_result = grin_core::pow::verify_size(&bh);
                                if ! verify_result.is_ok() {
                                        worker.status.rejected += 1;
                                        worker.add_shares(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                                        worker.add_shares_1m(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                                        debug!(
                                            "{} - Rejected due to failed grin_core::pow::verify_size()",
                                            self.id,
                                        );
                                        worker.send_err("submit".to_string(), "Failed to validate solution".to_string(), -32502);
                                        continue; // Dont process this share anymore
                                }
                                // For debugging - remove
                                // error!(
                                //     "Verify Result: {}",
                                //     verify_result.is_ok(),
                                // );
                            }
                        }
                        // We check the difficulty here
                        let proof = MinerProof {
                            edge_bits: share.edge_bits as u8,
                            nonces: share.pow.clone().to_vec(),
                        };
                        let difficulty = proof.to_difficulty_unscaled().to_num();
                        // warn!("Difficulty: {}", difficulty);
                        // Check if this meets worker difficulty
                        if difficulty < 1 {
                            worker.status.rejected += 1;
                            worker.add_shares(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                            worker.add_shares_1m(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                            worker.send_err("submit".to_string(), "Rejected low difficulty solution".to_string(), -32502);
                            continue; // Dont process this share anymore
                        }
                        if difficulty < worker.status.curdiff {
                            worker.status.rejected += 1;
                            worker.add_shares(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                            worker.add_shares_1m(share.edge_bits, 0, 1, 0); // Accepted, Rejected, Stale
                            worker.send_err("submit".to_string(), "Failed to validate solution".to_string(), -32502);
                            continue; // Dont process this share anymore
                        }
                        if difficulty >= worker.status.curdiff {
                            worker.status.accepted += 1;
                            worker.status.totalwork += worker.status.curdiff;
                            worker.add_shares(share.edge_bits, 1, 0, 0); // Accepted, Rejected, Stale
                            worker.add_shares_1m(share.edge_bits, 1, 0, 0); // Accepted, Rejected, Stale
                            worker.send_ok("submit".to_string());
                        }
                        // This is a good share, send it to grin server to be submitted
                        // Only send high power shares - minimum difficulty is set by the upstream
                        // grin stratum server

                        // meet difficulty requirement of job from upstream stratum server (mine pool or grin node)
                        if difficulty >= self.job.difficulty { // XXX TODO <---- this compares scaled to unscaled difficulty values - no good XXX TODO
                            // remove the block height prefix from the job_id
                            share.job_id = share.job_id % share.height;
                            self.server.submit_share(&share.clone(), worker.uuid());
                            warn!("{} - Submitted share at height {} with nonce {} with difficulty {} from worker {}",
                                self.id,
                                share.height,
                                share.nonce,
                                worker.status.curdiff,
                                worker.uuid(),
                            );
                        }
                        warn!("{} - Got share at height {} with nonce {} with difficulty {} from worker {}",
                                self.id,
                                share.height,
                                share.nonce,
                                worker.status.curdiff,
                                worker.uuid(),
                        );
                    }
                }
            }
        }
    }

    fn broadcast_job(&mut self) -> Result<(), String> {
        let mut w_m = self.workers.lock().unwrap();
        debug!(
            "{} - broadcasting a job to {} workers",
            self.id,
            w_m.len(),
        );
        // XXX TODO: To do this I need to deserialize the block header
        // XXX TODO: need to randomize the nonce (just in case a miner forgets)
        // XXX TODO: need to set a unique timestamp and record it in the worker struct
        for (worker_uuid, worker) in w_m.iter_mut() {
            if worker.authenticated {
                // change to use adjusted difficulty
                if worker.status.curdiff != worker.status.nextdiff {
                    worker.set_difficulty(worker.status.nextdiff);
                }
                worker.set_height(self.job.height);
                // Print this workers block_status for logstash to send to rmq
                debug!("{:?}", worker.status);
                debug!("{:?}", worker.worker_shares);
                worker.send_job(&mut self.job.clone());
                worker.reset_worker_shares(self.job.height);
            }
        }
        return Ok(());
    }

    // Purge dead/sick workers - remove all workers marked in error state
    fn clean_workers(&mut self) -> usize {
        let mut dead_workers: Vec<String> = vec![];
        let mut w_m = self.workers.lock().unwrap();
        for (worker_uuid, worker) in w_m.iter_mut() {
            if worker.error() == true {
                warn!(
                    "{} - Dropping worker: {}",
                    self.id,
                    worker.uuid(),
                );
                dead_workers.push(worker_uuid.clone());
            }
        }
        // Remove the dead workers
        for worker_uuid in dead_workers {
            let _ = w_m.remove(&worker_uuid);
        }
        return w_m.len();
    }

    fn save_and_reset_shares_1m(&mut self) {
        let time_now = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        };

        if time_now >= self.next_reset_timestamp {
            self.next_reset_timestamp = ((time_now / 60) + 1) * 60;
            debug!("now: {}, next: {}", time_now, self.next_reset_timestamp);

            // TODO adjust share difficulty for each worker
            // TODO save and reset
            let mut w_m = self.workers.lock().unwrap();
            for (_, worker) in w_m.iter_mut() {
                // difficulty up
                if worker.worker_shares_1m.shares.accepted > self.config.workers.expect_shares_1m * 2 {
                    let mut new_diff = worker.status.curdiff * 2;
                    if new_diff > self.job.difficulty {
                        new_diff = self.job.difficulty;
                    }
                    debug!("worker:{}, {}, {}, difficulty up, from: {}, to: {}",
                        worker.user_id, worker.username, worker.minername, worker.status.curdiff, new_diff);
                    worker.set_next_difficulty(new_diff);
                }
                // difficulty down
                if worker.worker_shares_1m.shares.accepted < self.config.workers.expect_shares_1m / 2 {
                    let mut new_diff = worker.status.curdiff / 2;
                    if new_diff < self.difficulty {
                        new_diff = self.difficulty;
                    }
                    debug!("worker:{}, {}, {}, difficulty down, from: {}, to: {}",
                           worker.user_id, worker.username, worker.minername, worker.status.curdiff, new_diff);
                    worker.set_next_difficulty(new_diff);
                }
            }



        }

    }
}
