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

//! Mining Stratum Worker
//!
//! A single mining worker (the pool manages a vec of Workers)
//!

use bufstream::BufStream;
use serde_json;
use serde_json::Value;
use std::net::TcpStream;
use reqwest;
use std::collections::HashMap;
use std::iter;
use std::{thread, time};
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;
use queues::*;

use pool::config::{Config, NodeConfig, PoolConfig, WorkerConfig};
use pool::proto::{RpcRequest, RpcError};
use pool::proto::{JobTemplate, LoginParams, StratumProtocol, SubmitParams, WorkerStatus};

// ----------------------------------------
// Worker Object - a connected stratum client - a miner
//

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Shares {
    pub edge_bits: u32,
    pub accepted: u64,
    pub rejected: u64,
    pub stale: u64,
}

impl Shares {
    pub fn new(edge_bits: u32) -> Shares {
        Shares {
            edge_bits: edge_bits,
            accepted: 0,
            rejected: 0,
            stale: 0,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerSharesPerBlock {
    pub id: String,  // Full id (UUID)
    pub height: u64,
    pub totalwork: u64,
    pub shares: Shares,
}

impl WorkerSharesPerBlock {
    pub fn new(id: String, edge_bits: u32) -> WorkerSharesPerBlock {
        WorkerSharesPerBlock {
            id: id,
            height: 0,
            totalwork: 0,
            shares: Shares::new(edge_bits),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerSharesPerMinute {
    pub id: String,  // Full id (UUID)
    pub timestamp: u64,
    pub totalwork: u64,
    pub shares: Shares,
}

impl WorkerSharesPerMinute {
    pub fn new(id: String, edge_bits: u32) -> WorkerSharesPerMinute {
        WorkerSharesPerMinute {
            id: id,
            timestamp: 0,
            totalwork: 0,
            shares: Shares::new(edge_bits),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WorkerSharesStatPerMinute {
    pub id: String,
    pub timestamp: u64,
    pub username: String,
    pub minername: String,
    pub agent: String,
    pub edge_bits: u32,
    pub accepted: u64,
    pub rejected: u64,
    pub stale: u64,
    pub totalwork: u64,
}

impl WorkerSharesStatPerMinute {
    pub fn new(id: String, edge_bits: u32) -> WorkerSharesStatPerMinute {
        WorkerSharesStatPerMinute {
            id: id,
            timestamp: 0,
            username: "".to_string(),
            minername: "".to_string(),
            agent: "".to_string(),
            edge_bits: edge_bits,
            accepted: 0,
            rejected: 0,
            stale: 0,
            totalwork: 0,
        }
    }
}

pub struct Worker {
    pub user_id: usize,   // the pool user_id or 0 if we dont know yet
    pub connection_id: String,  // The random per-connection id used to match proxied stratum messages
    login: Option<LoginParams>,  // The stratum login parameters sent by the miner
    stream: BufStream<TcpStream>,  // Connection with the mier process
    config: Config, // Values from the config.toml file
    protocol: StratumProtocol,  // Structures, codes, methods for stratum protocol
    error: bool, // Is this worker connection in error state?
    pub authenticated: bool, // Has the miner already successfully logged in?
    pub username: String,  // User assigned or "username-xxx"
    pub minername: String, // User assigned or "minername-xxx"
    pub agent: String,  // Miner identifier
    pub status: WorkerStatus,        // Runing totals - reported with stratum status message
    pub worker_shares: WorkerSharesPerBlock, // Share Counts for current block
    pub worker_shares_1m: WorkerSharesPerMinute, // Share Counts in 1 minute
    shares: Vec<SubmitParams>, // shares submitted by the miner that need to be processed by the pool
    request_ids: Queue<String>,     // Queue of request message ID's
    pub needs_job: bool, // Does this miner need a job for any reason
    pub requested_job: bool, // The miner sent a job request
    pub buffer: String, // Read-Buffer for stream
}

impl Worker {
    /// Creates a new Stratum Worker.
    pub fn new(config: Config, stream: BufStream<TcpStream>) -> Worker {
        let mut rng = thread_rng();
        let connection_id: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(16)
            .collect();
        let uuid = format!("{}-{}", 0, connection_id.clone());
        Worker {
            user_id: 0, // We dont know until the user logs in
            connection_id: connection_id,
            login: None,
            config: config.clone(),
            stream: stream,
            protocol: StratumProtocol::new(),
            error: false,
            authenticated: false,
            username: "username-default".to_string(),
            minername: "minername-default".to_string(),
            agent: "agent-default".to_string(),
            status: WorkerStatus::new(uuid.clone()),
            worker_shares: WorkerSharesPerBlock::new(uuid.clone(), config.grin_pool.edge_bits),
            worker_shares_1m: WorkerSharesPerMinute::new(uuid.clone(), config.grin_pool.edge_bits),
            shares: Vec::new(),
            request_ids: queue![],
            needs_job: false,
            requested_job: false,
            buffer: String::with_capacity(4096),
        }
    }

    /// Is the worker in error state?
    pub fn error(&self) -> bool {
        return self.error;
    }

    /// get the workers pool user_id
    pub fn user_id(&self) -> usize {
        return self.user_id;
    }

    /// get the full uuid:  user_id + connection_id
    pub fn uuid(&self) -> String {
        let uuid: String = format!("{}-{}", self.user_id, self.connection_id.clone());
        return uuid
    }

    /// Get worker login
    pub fn login(&self) -> String {
        match self.login {
            None => "None".to_string(),
            Some(ref login) => {
                let mut loginstr = login.login.clone();
                return loginstr.to_string();
            }
        }
    }

    /// Set job difficulty
    pub fn set_difficulty(&mut self, new_difficulty: u64) {
        self.status.curdiff = new_difficulty;
    }

    /// Set next job difficulty
    pub fn set_next_difficulty(&mut self, new_difficulty: u64) {
        self.status.nextdiff = new_difficulty;
    }

    /// Set job height
    pub fn set_height(&mut self, new_height: u64) {
        self.status.height = new_height;
    }

    /// Reset worker_shares for a new block
    pub fn reset_worker_shares(&mut self, height: u64) {
        self.worker_shares.id = self.uuid();
        self.worker_shares.height = height;
        self.worker_shares.shares = Shares::new(self.config.grin_pool.edge_bits);
    }
    
    /// Add a share to the worker_shares
    pub fn add_shares(&mut self, size: u32, accepted: u64, rejected: u64, stale: u64) {
        self.worker_shares.shares.accepted += accepted;
        self.worker_shares.shares.rejected += rejected;
        self.worker_shares.shares.stale += stale;
        self.worker_shares.totalwork += accepted * self.status.curdiff;
    }

    /// Reset worker_shares_1m for a new block
    pub fn reset_worker_shares_1m(&mut self, timestamp: u64) {
        self.worker_shares_1m.id = self.uuid();
        self.worker_shares_1m.timestamp = timestamp;
        self.worker_shares_1m.shares = Shares::new(self.config.grin_pool.edge_bits);
    }

    /// Add a share to the worker_shares_1m
    pub fn add_shares_1m(&mut self, size: u32, accepted: u64, rejected: u64, stale: u64) {
        self.worker_shares_1m.shares.accepted += accepted;
        self.worker_shares_1m.shares.rejected += rejected;
        self.worker_shares_1m.shares.stale += stale;
        self.worker_shares_1m.totalwork += accepted * self.status.curdiff;
    }

    /// Send a response
    pub fn send_response(&mut self,
                         method: String,
                         result: Value,
                 ) -> Result<(), String> {
        // Get the rpc request_id for this response
        // XXX TODO: Better matching of method?
        let req_id = match self.request_ids.remove() {
            Ok(id) => id,
            Err(e) => {
                error!("EMPTY request_ids ERROR");
                "0".to_string()
            },
        };
        trace!(
            "XXX SENDING RESPONSE: method: {}, result: {}, rpc_id: {}",
            method.clone(),
            result.clone(),
            req_id.clone(),
        );
        return self.protocol.send_response(
                &mut self.stream,
                method,
                result,
                Some(req_id),
            );
    }

    /// Send an ERROR response
    pub fn send_error_response(&mut self,
                         method: String,
                         e: RpcError,
                 ) -> Result<(), String> {
        // Get the rpc request_id for this response
        // XXX TODO: Better matching of method?
        let req_id = match self.request_ids.remove() {
            Ok(id) => id,
            Err(e) => {
                error!("EMPTY request_ids ERROR");
                "0".to_string()
            },
        };
        trace!(
            "XXX SENDING ERROR RESPONSE: method: {}, error: {:?}, rpc_id: {}",
            method.clone(),
            e.clone(),
            req_id.clone(),
        );
        return self.protocol.send_error_response(
            &mut self.stream,
            method,
            e,
            Some(req_id),
        );
    }

    // This handles both a get_job_template response, and a job request
    /// Send a job to the worker
    pub fn send_job(&mut self, job: &mut JobTemplate) -> Result<(), String> {
        trace!("Worker {} - Sending a job downstream: requested = {}", self.uuid(), self.requested_job);
        // Set the difficulty
        // use new adjusted difficulty
        if self.status.curdiff != self.status.nextdiff {
            self.set_difficulty(self.status.nextdiff);
        }
        job.difficulty = self.status.curdiff;
        let requested = self.requested_job;
        self.needs_job = false;
        self.requested_job = false;
        let job_value = serde_json::to_value(job.clone()).unwrap();
        let result;

        // if requested before
        if requested {
            result = self.send_response(
                "getjobtemplate".to_string(),
                job_value,
            );
        } else {
            result = self.protocol.send_request(
                &mut self.stream,
                "job".to_string(),
                Some(job_value.clone()),
                Some("Stratum".to_string()),    // XXX UGLY
            );
        }
        match result {
            Ok(r) => { return Ok(r); }
            Err(e) => {
                self.error = true;
                error!("{} - Failed to send job: {}", self.uuid(), e);
                return Err(format!("{}", e));
            }
        }
    }

    /// Send worker mining status
    pub fn send_status(&mut self, status: WorkerStatus) -> Result<(), String> {
        trace!("Worker {} - Sending worker status", self.uuid());
        let status_value = serde_json::to_value(status).unwrap();
        return self.send_response(
            "status".to_string(),
            status_value,
        );
    }

    /// Send OK Response
    pub fn send_ok(&mut self, method: String) -> Result<(), String> {
        trace!("Worker {} - sending OK Response", self.uuid());
        return self.send_response(
            method.to_string(),
            serde_json::to_value("ok".to_string()).unwrap(),
        );
    }

    /// Send Err Response
    pub fn send_err(&mut self, method: String, message: String, code: i32) -> Result<(), String> {
        trace!("Worker {} - sending Err Response", self.uuid());
        let e = RpcError {
            code: code,
            message: message.to_string(),
        };
        return self.send_error_response(
            method.to_string(),
            e,
        );
    }

    /// Return any pending shares from this worker
    pub fn get_shares(&mut self) -> Result<Option<Vec<SubmitParams>>, String> {
        if self.shares.len() > 0 {
            trace!(
                "Worker {} - Getting {} shares",
                self.uuid(),
                self.shares.len()
            );
            let current_shares = self.shares.clone();
            // get the worker all submitted shares
            self.shares = Vec::new();
            return Ok(Some(current_shares));
        }
        return Ok(None);
    }

    pub fn do_login(&mut self, login_params: LoginParams) -> Result<(), String> {
        // Save the entire login + password
        self.login = Some(login_params.clone());

        let mut username_split: Vec<&str> = login_params.login.split('.').collect();
        if username_split.len() >= 2 {
            self.username = username_split[0].to_string();
            self.minername = username_split[1].to_string();
        } else {
            self.username = login_params.login.clone();
        }
        debug!("DEBUG: have username={}, minername={}", self.username.clone(), self.minername.clone());

        self.agent = login_params.agent.clone();
        return Ok(());
    }

    /// Get and process messages from the connected worker
    // Method to handle requests from the downstream worker
    pub fn process_messages(&mut self) -> Result<(), String> {
        // XXX TODO: With some reasonable rate limiting (like N message per pass)
        // Read some messages from the upstream
        // Handle each request
        match self.protocol.get_message(&mut self.stream, &mut self.buffer) {
            Ok(rpc_msg) => {
                match rpc_msg {
                    Some(message) => {
                        trace!("Worker {} - Got Message: {:?}", self.uuid(), message);
                        // let v: Value = serde_json::from_str(&message).unwrap();
                        let req: RpcRequest = match serde_json::from_str(&message) {
                            Ok(r) => r,
                            Err(e) => {
                                // Do we want to diconnect the user for invalid RPC message ???
                                self.error = true;
                                debug!("Worker {} - Got Invalid Message", self.uuid());
                                // XXX TODO: Invalid request
                                return Err(e.to_string());
                            }
                        };
                        trace!(
                            "Worker {} - Received request type: {}",
                            self.uuid(),
                            req.method
                        );
                        // Add this request id to the queue
                        self.request_ids.add(req.id.clone());
                        match req.method.as_str() {
                            "login" => {
                                debug!("Worker {} - Accepting Login request", self.uuid());
                                // login before, do not need to do any thing
                                if self.user_id != 0 {
                                    // dont log in again, just say ok
                                    debug!("User already logged in: {}", self.user_id.clone());
                                    self.send_ok(req.method);
                                    return Ok(());
                                }
                                let params: Value = match req.params {
                                    Some(p) => p,
                                    None => {
                                        self.error = true;
                                        debug!("Worker {} - Missing Login request parameters", self.uuid());
                                        return self.send_err(
                                            "login".to_string(),
                                            "Missing Login request parameters".to_string(),
                                            -32500,
                                        );
                                        // XXX TODO: Invalid request
                                        //return Err("Invalid Login request".to_string());
                                    }
                                };
                                let login_params: LoginParams = match serde_json::from_value(params) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        self.error = true;
                                        debug!("Worker {} - Invalid Login request parameters", self.uuid());
                                        return self.send_err(
                                            "login".to_string(),
                                            "Invalid Login request parameters".to_string(),
                                            -32500,
                                        );
                                        // XXX TODO: Invalid request
                                        //return Err(e.to_string());
                                    }
                                };
                                // Call do_login()
                                match self.do_login(login_params) {
                                    Ok(_) => {
                                        // We accepted the login, send ok result
					                    self.authenticated = true;
                                        self.needs_job = false; // not until requested
                                        self.status = WorkerStatus::new(self.uuid());
                                        self.set_difficulty(self.config.workers.port_difficulty.difficulty);
                                        self.set_next_difficulty(self.config.workers.port_difficulty.difficulty);
                                        self.send_ok(req.method);
                                    },
                                    Err(e) => {
                                        return self.send_err(
                                            "login".to_string(),
                                            e,
                                            -32500,
                                        );
                                    }
                                }
                            }
                            "getjobtemplate" => {
                                trace!("Worker {} - Accepting request for job", self.uuid());
                                self.needs_job = true;
                                self.requested_job = true;
                            }
                            "submit" => {
                                trace!("Worker {} - Accepting share", self.uuid());
                                match serde_json::from_value(req.params.unwrap()) {
                                    Result::Ok(share) => {
                           			    self.shares.push(share);
                                    },
                                    Result::Err(err) => { }
                                };
                            }
                            "status" => {
                                trace!("Worker {} - Accepting status request", self.uuid());
                                let status = self.status.clone();
                                self.send_status(status);
                            }
                            "keepalive" => {
                                trace!("Worker {} - Accepting keepalive request", self.uuid());
                                self.send_ok(req.method);
                            }
                            _ => {
                                warn!(
                                    "Worker {} - Unknown request: {}",
                                    self.uuid(),
                                    req.method.as_str()
                                );
                                self.error = true;
                                return Err("Unknown request".to_string());
                            }
                        };
                    }
                    None => {} // Not an error, just no messages for us right now
                }
            }
            Err(e) => {
                error!(
                    "Worker {} - Error reading message: {}",
                    self.uuid(),
                    e.to_string()
                );
                self.error = true;
                return Err(e.to_string());
            }
        }
        return Ok(());
    }
}
