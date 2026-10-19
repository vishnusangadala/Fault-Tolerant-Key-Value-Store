#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <poll.h>
#include <random>
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include <cstring>
#ifdef __linux__
#include <sched.h>
#endif
using J = nlohmann::json;
using Clock = std::chrono::steady_clock;
constexpr size_t MAX_FRAME = 8 * 1024 * 1024;
struct FD {
  int n;
  explicit FD(int x) : n(x) {}
  ~FD() {
    if (n >= 0)
      close(n);
  }
  FD(const FD &) = delete;
};
void check(bool ok, const std::string &msg) {
  if (!ok)
    throw std::runtime_error(msg);
}
void timeout(int fd) {
  timeval t{0, 180000};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &t, sizeof(t));
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &t, sizeof(t));
}
void sendj(int fd, const J &j) {
  auto s = j.dump() + "\n";
  size_t p = 0;
  while (p < s.size()) {
    auto n = send(fd, s.data() + p, s.size() - p, 0);
    check(n > 0, "send failed");
    p += n;
  }
}
J recvj(int fd) {
  std::string s;
  char b[4096];
  while (s.size() < MAX_FRAME) {
    auto n = recv(fd, b, sizeof(b), 0);
    check(n > 0, "incomplete frame");
    s.append(b, n);
    auto e = s.find('\n');
    if (e != std::string::npos)
      return J::parse(s.substr(0, e));
  }
  throw std::runtime_error("frame too large");
}
J rpc(int port, const J &j, const std::string &host = "127.0.0.1") {
  FD f(socket(AF_INET, SOCK_STREAM, 0));
  check(f.n >= 0, "socket");
  timeout(f.n);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(port);
  check(inet_pton(AF_INET, host.c_str(), &a.sin_addr) == 1,
        "invalid IPv4 address");
  check(connect(f.n, (sockaddr *)&a, sizeof(a)) == 0, "connect");
  sendj(f.n, j);
  return recvj(f.n);
}
struct StorageError : std::runtime_error {
  using std::runtime_error::runtime_error;
};
struct Node {
  int id, port, term = 0, vote = -1, commit = 0, leader = -1;
  std::string role = "follower";
  std::map<int, int> peers, next, match;
  std::map<int, std::string> hosts;
  std::string bind_host;
  int dirty = 1, persisted = 0, applied = 0;
  J log = J::array({J{{"term", 0}, {"op", "noop"}}});
  std::unique_ptr<rocksdb::DB> db;
  std::mt19937 rng{std::random_device{}()};
  Clock::time_point deadline, beat;
  bool paused = false; // Inbound/outbound peer traffic fault injection only;
                       // client/status remain available.
  Node(int i, int p, const std::string &path, std::map<int, int> ps,
       std::map<int, std::string> hs, std::string bind)
      : id(i), port(p), peers(std::move(ps)), hosts(std::move(hs)),
        bind_host(std::move(bind)) {
    rocksdb::Options o;
    o.create_if_missing = true;
    rocksdb::DB *raw = nullptr;
    auto st = rocksdb::DB::Open(o, path, &raw);
    check(st.ok(), st.ToString());
    db.reset(raw);
    std::string s;
    st = db->Get(rocksdb::ReadOptions(), "state", &s);
    if (st.ok()) {
      auto j = J::parse(s);
      check(j.at("id") == id && j.at("peers") == J(peers) &&
                j.at("hosts") == J(hosts),
            "data directory belongs to a different cluster configuration");
      term = j.at("term");
      vote = j.at("vote");
      commit = j.at("commit");
      int count = j.at("last");
      check(commit >= 0 && commit <= count, "invalid persisted commit index");
      for (int k = 1; k <= count; ++k) {
        st = db->Get(rocksdb::ReadOptions(), "log/" + std::to_string(k), &s);
        check(st.ok(), st.ToString());
        log.push_back(J::parse(s));
      }
      dirty = count + 1;
      persisted = count;
      applied = commit;
    } else
      check(st.IsNotFound(), st.ToString());
    reset();
    beat = Clock::now();
  }
  int last() const { return int(log.size()) - 1; }
  int lt() const { return log.back().at("term"); }
  void reset() {
    deadline =
        Clock::now() + std::chrono::milliseconds(
                           std::uniform_int_distribution<int>(900, 1700)(rng));
  }
  // Persist term/vote, changed log suffix, commit index and KV effects
  // atomically. A successful peer/client acknowledgement always follows this
  // synchronous WAL write.
  void save() {
    J j{{"hosts", hosts}, {"id", id},         {"peers", peers}, {"term", term},
        {"vote", vote},   {"commit", commit}, {"last", last()}};
    rocksdb::WriteBatch batch;
    batch.Put("state", j.dump());
    for (int k = dirty; k <= last(); ++k)
      batch.Put("log/" + std::to_string(k), log[k].dump());
    for (int k = last() + 1; k <= persisted; ++k)
      batch.Delete("log/" + std::to_string(k));
    for (int k = applied + 1; k <= commit; ++k) {
      const auto &e = log[k];
      if (e.at("op") == "put")
        batch.Put("kv/" + e.at("key").get<std::string>(),
                  e.at("value").get<std::string>());
      if (e.at("op") == "del")
        batch.Delete("kv/" + e.at("key").get<std::string>());
    }
    rocksdb::WriteOptions w;
    w.sync = true;
    auto st = db->Write(w, &batch);
    if (!st.ok())
      throw StorageError(st.ToString());
    dirty = last() + 1;
    persisted = last();
    applied = commit;
  }
  void advance(int n) {
    if (n > commit) {
      commit = n;
      save();
    }
  }
  void follow(int t, int l = -1) {
    if (t > term) {
      term = t;
      vote = -1;
      save();
    }
    role = "follower";
    leader = l;
    reset();
  }
  J status() {
    return J{{"ok", true},     {"id", id},          {"role", role},
             {"term", term},   {"leader", leader},  {"commit", commit},
             {"last", last()}, {"isolated", paused}};
  }
  J peer(const J &j) {
    int t = j.at("term");
    if (t > term)
      follow(t);
    auto type = j.at("type");
    if (type == "vote") {
      bool fresh = std::pair<int, int>{j.at("last_term"), j.at("last")} >=
                   std::pair<int, int>{lt(), last()};
      bool yes = t == term && (vote == -1 || vote == j.at("id")) && fresh;
      if (yes) {
        vote = j.at("id");
        save();
        reset();
      }
      return {{"term", term}, {"yes", yes}};
    }
    if (t < term)
      return {{"term", term}, {"yes", false}};
    follow(t, j.at("id"));
    int prev = j.at("prev");
    if (prev < 0 || prev > last() || log[prev].at("term") != j.at("prev_term"))
      return {{"term", term}, {"yes", false}};
    int idx = prev;
    bool changed = false;
    for (const auto &e : j.at("entries")) {
      ++idx;
      if (idx <= last() && log[idx].at("term") != e.at("term")) {
        check(idx > commit, "attempted committed log overwrite");
        dirty = std::min(dirty, idx);
        log.erase(log.begin() + idx, log.end());
        changed = true;
      }
      if (idx > last()) {
        log.push_back(e);
        changed = true;
      }
    }
    if (changed)
      save();
    advance(std::min(j.at("commit").get<int>(), idx));
    return {{"term", term}, {"yes", true}, {"match", idx}};
  }
  void replicate() {
    if (role != "leader" || paused)
      return;
    for (auto [pid, p] : peers) {
      if (role != "leader")
        break;
      int n = next[pid];
      J entries = J::array(); // Bound each RPC; followers catch up over
                              // successive heartbeats.
      for (int k = n; k <= last() && k < n + 32; k++)
        entries.push_back(log[k]);
      try {
        auto r = rpc(p,
                     {{"type", "append"},
                      {"id", id},
                      {"term", term},
                      {"prev", n - 1},
                      {"prev_term", log[n - 1].at("term")},
                      {"entries", entries},
                      {"commit", commit}},
                     hosts.at(pid));
        int t = r.at("term");
        if (t > term) {
          follow(t);
          break;
        }
        if (r.at("yes")) {
          match[pid] = r.at("match");
          next[pid] = match[pid] + 1;
        } else
          next[pid] = std::max(1, n - 1);
      } catch (const StorageError &) {
        throw;
      } catch (const std::exception &) {
      }
    }
    if (role != "leader")
      return;
    for (int n = last(); n > commit; n--) {
      int votes = 1;
      for (auto [pid, p] : peers) {
        (void)p;
        if (match[pid] >= n)
          ++votes;
      }
      if (votes > int(peers.size() + 1) / 2 && log[n].at("term") == term) {
        advance(n);
        break;
      }
    }
    beat = Clock::now() + std::chrono::milliseconds(80);
  }
  void election() {
    if (paused) {
      reset();
      return;
    }
    role = "candidate";
    leader = -1;
    ++term;
    vote = id;
    save();
    reset();
    int votes = 1;
    for (auto [pid, p] : peers) {
      (void)pid;
      try {
        auto r = rpc(p,
                     {{"type", "vote"},
                      {"term", term},
                      {"id", id},
                      {"last", last()},
                      {"last_term", lt()}},
                     hosts.at(pid));
        if (r.at("term").get<int>() > term) {
          follow(r.at("term"));
          return;
        }
        if (r.at("yes"))
          ++votes;
      } catch (const StorageError &) {
        throw;
      } catch (const std::exception &) {
      }
    }
    if (votes > int(peers.size() + 1) / 2) {
      role = "leader";
      leader = id;
      for (auto [pid, p] : peers) {
        (void)p;
        next[pid] = last() + 1;
        match[pid] = 0;
      }
      log.push_back({{"term", term}, {"op", "noop"}});
      save();
      replicate();
    }
  }
  J handle(const J &j) {
    std::string type = j.at("type");
    if (type == "status")
      return status();
    if (type == "isolate") {
      paused = j.at("enabled");
      return status();
    }
    if (type == "vote" || type == "append") {
      if (paused)
        return {{"term", term}, {"yes", false}};
      return peer(j);
    }
    if (type != "put" && type != "get" && type != "del")
      throw std::invalid_argument("unknown request type");
    auto key = j.at("key").get<std::string>();
    if (key.size() > 4096)
      throw std::invalid_argument("key too large");
    std::string val;
    if (type == "put") {
      val = j.at("value");
      if (val.size() > 16384)
        throw std::invalid_argument("value too large");
    }
    if (role != "leader")
      return {{"ok", false}, {"error", "not_leader"}, {"leader", leader}};
    J e{{"term", term}, {"op", type}, {"key", key}};
    if (type == "put")
      e["value"] = val;
    log.push_back(e);
    save();
    int target = last();
    auto until = Clock::now() + std::chrono::milliseconds(650);
    do {
      replicate();
      if (commit >= target)
        break;
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    } while (role == "leader" && Clock::now() < until);
    if (role != "leader" || commit < target)
      return {{"ok", false},
              {"error", "unavailable"},
              {"message", "outcome unknown; operation may commit later"}};
    if (type == "get") {
      std::string value;
      auto st = db->Get(rocksdb::ReadOptions(), "kv/" + key, &value);
      if (!st.ok() && !st.IsNotFound())
        throw StorageError(st.ToString());
      return {{"ok", true}, {"found", st.ok()}, {"value", value}};
    }
    return {{"ok", true}, {"index", target}};
  }
  void run() {
    FD server(socket(AF_INET, SOCK_STREAM, 0));
    int one = 1;
    setsockopt(server.n, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    check(inet_pton(AF_INET, bind_host.c_str(), &a.sin_addr) == 1,
          "invalid bind IPv4 address");
    check(bind(server.n, (sockaddr *)&a, sizeof(a)) == 0, "bind failed");
    check(listen(server.n, 128) == 0, "listen");
    std::cout << "node " << id << " listening on " << bind_host << ":" << port
              << std::endl;
    while (true) {
      if (role != "leader" && Clock::now() >= deadline)
        election();
      if (role == "leader" && Clock::now() >= beat)
        replicate();
      pollfd p{server.n, POLLIN, 0};
      if (poll(&p, 1, 10) > 0) {
        FD f(accept(server.n, nullptr, nullptr));
        if (f.n < 0)
          continue;
        timeout(f.n);
        J request;
        try {
          request = recvj(f.n);
        } catch (const std::exception &) {
          continue;
        } // Malformed clients must not stop the service; storage/consensus
          // errors must.
        J response;
        try {
          response = handle(request);
        } catch (const std::invalid_argument &) {
          response = {{"ok", false}, {"error", "invalid_request"}};
        } catch (const nlohmann::json::exception &) {
          response = {{"ok", false}, {"error", "invalid_request"}};
        }
        try {
          sendj(f.n, response);
        } catch (const std::exception &) {
        }
      }
    }
  }
};
int main(int argc, char **argv) {
  signal(SIGPIPE, SIG_IGN);
  try {
    check(argc >= 3, "usage: raftkv serve CONFIG.json | raftkv client PORT "
                     "status|get KEY|put KEY VALUE|del KEY");
    std::string mode = argv[1];
    if (mode == "serve") {
      std::ifstream in(argv[2]);
      J c;
      in >> c;
      int id = c.at("id"), port = c.at("port");
      std::map<int, int> peers;
      std::map<int, std::string> hosts;
      for (auto &p : c.at("peers")) {
        int i = p.at("id"), v = p.at("port");
        check(i >= 0 && i != id && v > 0 && v < 65536 && !peers.count(i),
              "invalid peer configuration");
        peers[i] = v;
        hosts[i] = p.value("host", "127.0.0.1");
      }
      check(port > 0 && port < 65536, "invalid port");
#ifdef __linux__
      if (c.contains("cpu")) {
        int cpu = c.at("cpu");
        check(cpu >= 0 && cpu < CPU_SETSIZE, "invalid CPU");
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        check(sched_setaffinity(0, sizeof(set), &set) == 0,
              "CPU affinity failed");
      }
#endif
      check(id >= 0, "invalid node id");
      Node n(id, port, c.at("data_dir"), peers, hosts,
             c.value("bind", "127.0.0.1"));
      n.run();
    } else {
      check(mode == "client" && argc >= 4, "invalid arguments");
      J j{{"type", argv[3]}};
      if (j["type"] != "status") {
        check(argc >= 5, "missing key");
        j["key"] = argv[4];
      }
      if (j["type"] == "put") {
        check(argc >= 6, "missing value");
        j["value"] = argv[5];
      }
      std::string endpoint = argv[2], host = "127.0.0.1";
      auto colon = endpoint.find(':');
      if (colon != std::string::npos) {
        host = endpoint.substr(0, colon);
        endpoint = endpoint.substr(colon + 1);
      }
      int port = std::stoi(endpoint);
      check(port > 0 && port < 65536, "invalid port");
      auto r = rpc(port, j, host);
      std::cout << r.dump(2) << std::endl;
      return r.value("ok", false) ? 0 : 2;
    }
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
  return 0;
}
