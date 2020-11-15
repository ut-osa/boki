#include "gateway/node_manager.h"

#include "gateway/flags.h"
#include "gateway/server.h"

#define HLOG(l) LOG(l) << "NodeManager: "
#define HVLOG(l) VLOG(l) << "NodeManager: "

namespace faas {
namespace gateway {

NodeManager::NodeManager(Server* server)
    : server_(server),
      max_running_requests_(0) {
}

NodeManager::~NodeManager() {
}

size_t NodeManager::num_connected_node() const {
    absl::ReaderMutexLock lk(&mu_);
    return connected_nodes_.size();
}

void NodeManager::OnNewEngineConnection(EngineConnection* connection) {
    absl::MutexLock lk(&mu_);
    uint16_t node_id = connection->node_id();
    if (!connected_nodes_.contains(node_id)) {
        NewConnectedNode(node_id);
        mu_.Unlock();
        server_->OnNewConnectedNode(connection);
        mu_.Lock();
    }
    DCHECK(connected_nodes_.contains(node_id));
    Node* node = connected_nodes_[node_id].get();
    node->connections.push_back(connection);
    node->active_connections++;
}

void NodeManager::OnEngineConnectionClosed(EngineConnection* connection) {
    absl::MutexLock lk(&mu_);
    uint16_t node_id = connection->node_id();
    DCHECK(connected_nodes_.contains(node_id));
    Node* node = connected_nodes_[node_id].get();
    for (size_t i = 0; i < node->connections.size(); i++) {
        if (node->connections[i] == connection) {
            node->connections[i] = nullptr;
            node->active_connections--;
            break;
        }
    }
}

bool NodeManager::PickNodeForNewFuncCall(const protocol::FuncCall& func_call, uint16_t* node_id) {
    absl::MutexLock lk(&mu_);
    if (connected_node_list_.empty()) {
        return false;
    }
    if (running_requests_.size() > max_running_requests_) {
        return false;
    }
    size_t idx;
    if (absl::GetFlag(FLAGS_lb_per_fn_round_robin)) {
        idx = (next_dispatch_node_idx_[func_call.func_id]++) % connected_node_list_.size();
    } else if (absl::GetFlag(FLAGS_lb_pick_least_load)) {
        idx = absl::c_min_element(connected_node_list_, [] (const Node* lhs, const Node* rhs) {
            return lhs->inflight_requests < rhs->inflight_requests;
        }) - connected_node_list_.begin();
    } else {
        idx = absl::Uniform<size_t>(random_bit_gen_, 0, connected_node_list_.size());
    }
    Node* node = connected_node_list_[idx];
    node->inflight_requests++;
    node->dispatched_requests_stat->Tick();
    running_requests_.insert(func_call.full_call_id);
    *node_id = node->node_id;
    return true;
}

void NodeManager::FuncCallFinished(const protocol::FuncCall& func_call, uint16_t node_id) {
    absl::MutexLock lk(&mu_);
    if (!running_requests_.contains(func_call.full_call_id)) {
        return;
    }
    running_requests_.erase(func_call.full_call_id);
    if (!connected_nodes_.contains(node_id)) {
        return;
    }
    Node* node = connected_nodes_[node_id].get();
    node->inflight_requests--;
}

void NodeManager::NewConnectedNode(uint16_t node_id) {
    DCHECK(!connected_nodes_[node_id]);
    Node* node = new Node;
    node->node_id = node_id;
    node->inflight_requests = 0;
    node->dispatched_requests_stat.reset(new stat::Counter(
        stat::Counter::StandardReportCallback(fmt::format("dispatched_requests[{}]", node_id))));
    connected_node_list_.push_back(node);
    connected_nodes_[node_id] = std::unique_ptr<Node>(node);
    max_running_requests_ = absl::GetFlag(FLAGS_max_running_requests) * connected_nodes_.size();
    HLOG(INFO) << fmt::format("Node with id {} connected, total number of connected nodes: {}",
                              node_id, connected_nodes_.size());
}

bool NodeManager::SendMessage(uint16_t node_id, const protocol::GatewayMessage& message,
                              std::span<const char> payload) {
    // Fast path
    server::IOWorker* io_worker = server::IOWorker::current();
    if (io_worker != nullptr) {
        server::ConnectionBase* connection = io_worker->PickConnection(
            EngineConnection::type_id(node_id));
        if (connection != nullptr) {
            connection->as_ptr<EngineConnection>()->SendMessage(message, payload);
            return true;
        }
    }
    // Slow path
    std::shared_ptr<server::ConnectionBase> connection = nullptr;
    {
        absl::ReaderMutexLock lk(&mu_);
        if (!connected_nodes_.contains(node_id)) {
            return false;
        }
        Node* node = connected_nodes_[node_id].get();
        if (node->active_connections == 0) {
            return false;
        }
        while (true) {
            size_t idx = absl::Uniform<size_t>(random_bit_gen_, 0, node->connections.size());
            EngineConnection* engine_connection = node->connections[idx];
            if (engine_connection != nullptr) {
                connection = engine_connection->ref_self();
                break;
            }
        }
    }
    protocol::GatewayMessage message_copy = message;
    std::span<const char> payload_copy;
    char* buf = nullptr;
    if (payload.size() > 0) {
        buf = reinterpret_cast<char*>(malloc(payload.size()));
        memcpy(buf, payload.data(), payload.size());
        payload_copy = std::span<const char>(buf, payload.size());
    }
    EngineConnection* engine_connection = connection->as_ptr<EngineConnection>();
    connection->io_worker()->ScheduleFunction(
        connection.get(), [engine_connection, message_copy, payload_copy, buf] () {
            engine_connection->SendMessage(message_copy, payload_copy);
            if (buf != nullptr) {
                free(buf);
            }
        }
    );
    return true;
}

}  // namespace gateway
}  // namespace faas
