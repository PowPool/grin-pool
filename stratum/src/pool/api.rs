use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use jsonrpc_http_server::jsonrpc_core::{IoHandler, Value, Params};
use jsonrpc_http_server::{ServerBuilder};

use pool::config::{Config, NodeConfig, PoolConfig, WorkerConfig};
use pool::worker::{Worker};
use serde_json::{Map, to_value};


#[derive(Serialize, Deserialize, Clone, Debug)]
struct RespPoolInfo {
    pub pool_id: String,
    pub edge_bits: u32,
    pub user_count: u64,
    pub miner_count: u64,
    pub connection_count: u64,
}

impl RespPoolInfo {
    pub fn new() -> RespPoolInfo {
        RespPoolInfo {
            pool_id: "".to_string(),
            edge_bits: 0,
            user_count: 0,
            miner_count: 0,
            connection_count: 0,
        }
    }
}

pub fn accept_requests(
    stratum_id: String,
    config: Config,
    workers: &mut Arc<Mutex<HashMap<String, Worker>>>,
) {
    let mut io = IoHandler::new();
    let id1 = stratum_id.clone();
    let config1 = config.clone();
    let workers1 = workers.clone();

    io.add_method("get_pool_info", move|_params: Params| {
        let mut pool_info = Map::new();
        pool_info.insert("pool_id".to_string(), to_value(id1.clone()).unwrap());
        pool_info.insert("edge_bits".to_string(), to_value(config1.grin_pool.edge_bits).unwrap());
        pool_info.insert("pool_diff".to_string(), to_value(config1.workers.port_difficulty.difficulty).unwrap());
        pool_info.insert("stratum_port".to_string(), to_value(config1.workers.port_difficulty.port).unwrap());
        pool_info.insert("api_port".to_string(), to_value(config1.api.api_port).unwrap());
        pool_info.insert("connection_count".to_string(), to_value(workers1.lock().unwrap().len()).unwrap());

        let mut set1 = HashSet::new();
        let mut set2 = HashSet::new();
        for (_, worker) in workers1.lock().unwrap().iter_mut() {
            set1.insert(worker.username.clone());
            set2.insert(worker.username.clone() + " " + worker.minername.clone().as_str());
        }

        pool_info.insert("user_count".to_string(), to_value(set1.len()).unwrap());
        pool_info.insert("miner_count".to_string(), to_value(set2.len()).unwrap());

        Ok(Value::Object(pool_info))
    });

    let id2 = stratum_id.clone();
    let config2 = config.clone();
    let workers2 = workers.clone();

    io.add_method("get_connect_info", move|_params: Params| {
        let mut connect_info = Map::new();

        for (uuid, worker) in workers2.lock().unwrap().iter_mut() {
            let mut info = Map::new();
            info.insert("uuid".to_string(), to_value(uuid.clone()).unwrap());
            info.insert("from_addr".to_string(), to_value(worker.get_peer_address().clone()).unwrap());
            info.insert("agent".to_string(), to_value(worker.agent.clone()).unwrap());
            info.insert("user_name".to_string(), to_value(worker.username.clone()).unwrap());
            info.insert("miner_name".to_string(), to_value(worker.minername.clone()).unwrap());
            info.insert("authenticated".to_string(), to_value(worker.authenticated).unwrap());
            info.insert("cur_diff".to_string(), to_value(worker.status.curdiff).unwrap());
            info.insert("next_diff".to_string(), to_value(worker.status.nextdiff).unwrap());

            connect_info.insert(uuid.clone(), Value::Object(info.clone()));
        }

        Ok(Value::Object(connect_info))
    });

    let address = config.api.listen_address.clone() + ":"
        + &config.api.api_port.to_string();
    let server = ServerBuilder::new(io)
        .threads(5)
        .start_http(&address.parse().unwrap())
        .unwrap();

    server.wait();
}