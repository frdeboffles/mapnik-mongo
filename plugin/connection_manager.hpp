/*****************************************************************************
 *
 * This file is part of Mapnik (c++ mapping toolkit)
 *
 * Copyright (C) 2011 Artem Pavlenko
 *               2013 Oleksandr Novychenko
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 *****************************************************************************/

#ifndef MONGODB_CONNECTION_MANAGER_HPP
#define MONGODB_CONNECTION_MANAGER_HPP

// mapnik
#include <mapnik/debug.hpp>
#include <mapnik/pool.hpp>
#include <mapnik/datasource_cache.hpp>

// mongo
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/exception/exception.hpp>

// stl
#include <atomic>
#include <chrono>
#include <string>
#include <sstream>
#include <thread>

using mapnik::singleton;
using mapnik::CreateStatic;

class Connection {
private:
  mongocxx::pool::entry client_;
  std::chrono::steady_clock::time_point start_;
  mongocxx::collection collection_;
  static std::atomic<int> & instanceCount() {
    static std::atomic<int> instance_count_;
    return instance_count_;
  }
public:
  int id;

  Connection(mongocxx::pool &pool, const std::string &dbname,
             const std::string &collection)
    : client_(pool.acquire()) {
    id = ++instanceCount();
    collection_ = client_->database(dbname).collection(collection);
    start_ = std::chrono::steady_clock::now();
    MAPNIK_LOG_DEBUG(mongodb) << std::this_thread::get_id() << ":" << id << ": started connection";
  }

  ~Connection() {
    auto end = std::chrono::steady_clock::now();
    MAPNIK_LOG_DEBUG(mongodb) << std::this_thread::get_id() << ":" << id << ": stopped connection after " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start_).count() << "ms";
    --instanceCount();
  }

  std::shared_ptr<mongocxx::cursor> get_cursor(const std::string &jsonFilter) {
    bsoncxx::document::value filter = bsoncxx::from_json(jsonFilter);
    return get_cursor(filter);
  }

  std::shared_ptr<mongocxx::cursor> get_cursor(const bsoncxx::document::value &filter) {
    MAPNIK_LOG_DEBUG(mongodb) << std::this_thread::get_id() << ":" << id << ": filter: " << bsoncxx::to_json(filter);
    mongocxx::cursor cursor = collection_.find(filter.view());
    return std::make_shared<mongocxx::cursor>(std::move(cursor));
  }

  bsoncxx::stdx::optional<bsoncxx::document::value> find_one(const bsoncxx::document::value &filter) {
    return collection_.find_one(filter.view());
  }
};

class ConnectionCreator {
  std::string uri_;
  std::string dbname_, collection_;
  mongocxx::pool pool_;

public:
  ConnectionCreator(const std::string &uri,
                    const std::string &dbname,
                    const std::string &collection)
      : uri_(uri),
        dbname_(dbname), collection_(collection), pool_(mongocxx::uri(uri)) {
    MAPNIK_LOG_DEBUG(mongodb) << "Start mongo pool: " << connection_string() << " " << namespace_string();
    try {
      auto client = pool_.acquire();
      auto dbs = client->list_databases();
      bool dbFound = false;
      for (auto view : dbs) {
        auto element = view["name"];
        const std::string name = element.get_utf8().value.to_string();
        if (name == dbname) {
          dbFound = true;
          break;
        }
      }
      if (!dbFound) {
        std::string errMsg = "Mongodb Plugin: ";
        errMsg += "database ";
        errMsg += dbname;
        errMsg += " not found\n";
        throw mapnik::datasource_exception(errMsg);
      }

      auto database = client->database(dbname);
      if (!database.has_collection(collection)) {
        std::string errMsg = "Mongodb Plugin: ";
        errMsg += "collection ";
        errMsg += collection;
        errMsg += " in database ";
        errMsg += dbname;
        errMsg += " not found\n";
        throw mapnik::datasource_exception(errMsg);
      }
    } catch (const mongocxx::exception &xcp) {
      std::string err_msg = "Mongodb Plugin: ";
      err_msg += "connection failed: ";
      err_msg += xcp.what();
      err_msg += "\n";
      throw mapnik::datasource_exception(err_msg);
    }
  }

  inline std::shared_ptr<Connection> getConnection() {
    MAPNIK_LOG_DEBUG(mongodb) << std::this_thread::get_id() << ": get mongo connection";
    auto start = std::chrono::steady_clock::now();
    std::shared_ptr<Connection> cnx = std::make_shared<Connection>(pool_, dbname_, collection_);
    auto end = std::chrono::steady_clock::now();
    MAPNIK_LOG_DEBUG(mongodb) << std::this_thread::get_id() << ": got connection after " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms";;
    return cnx;
  }

  inline std::string id() const {
    return connection_string() + " " + namespace_string();
  }

  inline std::string connection_string() const {
    return uri_;
  }

  inline std::string namespace_string() const {
    return dbname_ + "." + collection_;
  }
};

class ConnectionManager : public singleton<ConnectionManager, CreateStatic> {
  friend class CreateStatic<ConnectionManager>;

  typedef std::map<std::string, std::shared_ptr<ConnectionCreator>> ContType;
  ContType pools_;

public:
  bool registerPool(const std::shared_ptr<ConnectionCreator> &creator) {
    ContType::const_iterator itr = pools_.find(creator->id());
    if (itr == pools_.end()) {
      auto pair = pools_.insert(std::make_pair(creator->id(), creator));
      MAPNIK_LOG_DEBUG(mongodb) << "Registered creator " << creator->id() << ". Pool size: " << pools_.size();
      return pair.second;
    }
    return false;
  }

  std::shared_ptr<Connection> getConnection(const std::string &key) {
    ContType::const_iterator itr = pools_.find(key);

    if (itr != pools_.end()) {
      return itr->second->getConnection();
    }

    static const std::shared_ptr<Connection> emptyConnection;
    return emptyConnection;
  }

  ConnectionManager() {}

private:
  ConnectionManager(const ConnectionManager &);

  ConnectionManager &operator=(const ConnectionManager);
};

#endif // MONGODB_CONNECTION_MANAGER_HPP
