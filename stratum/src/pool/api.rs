use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use jsonrpc_http_server::jsonrpc_core::{IoHandler, Value, Params};
use jsonrpc_http_server::{ServerBuilder};

use pool::config::{Config, NodeConfig, PoolConfig, WorkerConfig};
use pool::worker::{Worker};


pub fn accept_requests(
    stratum_id: String,
    config: Config,
    workers: &mut Arc<Mutex<HashMap<String, Worker>>>,
) {
    let mut io = IoHandler::new();

    let id1 = stratum_id.clone();
    let config1 = config.clone();
    let worker1 = workers.clone();

    io.add_method("test", move|_params: Params| {
        Ok(Value::String(config1.grin_pool.edge_bits.to_string()))
    });

    let address = config.api.listen_address.clone() + ":"
        + &config.api.api_port.to_string();
    let server = ServerBuilder::new(io)
        .threads(5)
        .start_http(&address.parse().unwrap())
        .unwrap();

    server.wait();
}