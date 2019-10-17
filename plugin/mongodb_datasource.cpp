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

#include "mongodb_datasource.hpp"
#include "mongodb_featureset.hpp"
#include "connection_manager.hpp"

// mapnik
#include <mapnik/debug.hpp>
#include <mapnik/global.hpp>
#include <mapnik/boolean.hpp>
#include <mapnik/sql_utils.hpp>
#include <mapnik/util/conversions.hpp>
#include <mapnik/timer.hpp>

// boost
#include <boost/algorithm/string.hpp>
#include <boost/tokenizer.hpp>
#include <boost/optional.hpp>

// mongo
#include <mongocxx/instance.hpp>
#include <bsoncxx/builder/concatenate.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/stdx/optional.hpp>

// stl
#include <thread>
#include <algorithm>
#include <string>
#include <set>
#include <sstream>
#include <iomanip>

DATASOURCE_PLUGIN(mongodb_datasource)

using mapnik::attribute_descriptor;

mongodb_datasource::mongodb_datasource(parameters const &params)
    : datasource(params),
      desc_(*params.get<std::string>("type"), "utf-8"),
      type_(datasource::Vector),
      geometry_(*params.get<std::string>("geometry", "geometry")),
      geometry_index_2d_(*params.get<mapnik::boolean_type>("geometry_index_2d", false)),
      filter_(bsoncxx::from_json(*params.get<std::string>("filter", "{}"))),
      persist_connection_(*params.get<mapnik::boolean_type>("persist_connection", true)),
      extent_initialized_(false) {
  if (auto severity = params.get<std::string>("log_level", "none")) {
    if (*severity == "debug")
      mapnik::logger::set_severity(mapnik::logger::debug);
    else if (*severity == "warn")
      mapnik::logger::set_severity(mapnik::logger::warn);
    else if (*severity == "error")
      mapnik::logger::set_severity(mapnik::logger::error);
    else if (*severity == "none")
      mapnik::logger::set_severity(mapnik::logger::none);
    else
      std::clog << "ignoring option --log='" << *severity
                << "' (allowed values are: debug, warn, error, none)\n";
  }
  MAPNIK_LOG_DEBUG(mongodb) << "Instantiate mongodb_datasource";
  static mongocxx::instance instance{}; // This should be done only once.
  if (!params.get<std::string>("collection")) {
    throw mapnik::datasource_exception("MongoDB Plugin: missing <collection> parameter");
  }
  boost::optional<std::string> ext = params.get<std::string>("extent");
  if (ext && !ext->empty()) {
    extent_initialized_ = extent_.from_string(*ext);
  }
  creator_ = std::make_shared<ConnectionCreator>(*params.get<std::string>("uri", "mongodb://localhost:27017"),
                                                 *params.get<std::string>("database", "gis"),
                                                 *params.get<std::string>("collection", "points"));

  ConnectionManager::instance().registerPool(creator_);
}

mongodb_datasource::~mongodb_datasource() {
  if (!persist_connection_) {
    //TODO
  }
}

const char *mongodb_datasource::name() {
  return "mongodb";
}

mapnik::datasource::datasource_t mongodb_datasource::type() const {
  return type_;
}

layer_descriptor mongodb_datasource::get_descriptor() const {
  return desc_;
}

bsoncxx::document::value mongodb_datasource::geo_filter(const box2d<double> &env) const {
  using bsoncxx::builder::stream::open_document;
  using bsoncxx::builder::stream::close_document;
  using bsoncxx::builder::stream::document;
  using bsoncxx::builder::stream::array;
  using bsoncxx::builder::stream::open_array;
  using bsoncxx::builder::stream::close_array;
  using bsoncxx::builder::stream::finalize;
  using bsoncxx::builder::concatenate;

  if (abs(env.maxx() - env.minx()) > 180 ||
      abs(env.maxy() - env.miny()) > 180) {
    throw mapnik::datasource_exception("try to query more than a single hemisphere");
  }

  if (geometry_index_2d_) {
    // Geometry is using a 2d index. We use a $box filter
    return document{}
        << geometry_ + ".coordinates"
        << open_document //$geoWithin
        << "$geoWithin"
        << open_document //$geometry
        << "$box"
        << open_array
        << open_array << env.minx() << env.miny() << close_array
        << open_array << env.maxx() << env.maxy() << close_array
        << close_array
        << close_document //$geometry
        << close_document //$geoWithin
        << concatenate(filter_.view())
        << finalize;
  } else {
    // Default filter uses $geoIntersects (geometry should be indexed using 2dsphere)
    return document{}
        << geometry_
        << open_document //$geoIntersects
        << "$geoIntersects"
        << open_document //$geometry
        << "$geometry"
        << open_document //geojson polygon
        << "type"
        << "Polygon"
        << "coordinates"
        << open_array << open_array
        << open_array << env.minx() << env.miny() << close_array
        << open_array << env.maxx() << env.miny() << close_array
        << open_array << env.maxx() << env.maxy() << close_array
        << open_array << env.minx() << env.maxy() << close_array
        << open_array << env.minx() << env.miny() << close_array
        << close_array << close_array
        << close_document //geojson polygon
        << close_document //$geometry
        << close_document //$geoIntersects
        << concatenate(filter_.view())
        << finalize;
  }
}

featureset_ptr mongodb_datasource::features(const query &q) const {
  const box2d<double> &box = q.get_bbox();
  std::shared_ptr<Connection> conn = ConnectionManager::instance().getConnection(creator_->id());
  std::shared_ptr<mongocxx::cursor> rs(conn->get_cursor(geo_filter(box)));
  mapnik::context_ptr ctx = std::make_shared<mapnik::context_type>();
  return std::make_shared<mongodb_featureset>(conn, rs, geometry_, ctx, desc_.get_encoding());
}

featureset_ptr mongodb_datasource::features_at_point(const coord2d &pt, double tol) const {
  box2d<double> box(pt.x - tol, pt.y - tol, pt.x + tol, pt.y + tol);
  std::shared_ptr<Connection> conn = ConnectionManager::instance().getConnection(creator_->id());
  std::shared_ptr<mongocxx::cursor> rs(conn->get_cursor(geo_filter(box)));
  mapnik::context_ptr ctx = std::make_shared<mapnik::context_type>();
  return std::make_shared<mongodb_featureset>(conn, rs, geometry_, ctx, desc_.get_encoding());
}

box2d<double> mongodb_datasource::envelope() const {
  if (extent_initialized_) {
    return extent_;
  } else {
    // a dumb way :-/
    extent_.init(-180.0, -90.0, 180.0, 90.0);
    extent_initialized_ = true;
  }
  return extent_;
}

boost::optional<mapnik::datasource_geometry_t> mongodb_datasource::get_geometry_type() const {
  using bsoncxx::builder::stream::open_document;
  using bsoncxx::builder::stream::close_document;
  using bsoncxx::builder::stream::document;
  using bsoncxx::builder::stream::finalize;
  boost::optional<mapnik::datasource_geometry_t> result;

  std::shared_ptr<Connection> conn = ConnectionManager::instance().getConnection(creator_->id());
  bsoncxx::document::value filter = document{}
      << geometry_
      << open_document
      << "$exists"
      << true
      << close_document
      << finalize;
  bsoncxx::stdx::optional<bsoncxx::document::value> bson = conn->find_one(filter);

  if (bson) {
    MAPNIK_LOG_DEBUG(mongodb) <<  "found one: " << bsoncxx::to_json(*bson);
    std::string type = bson->view()[geometry_]["type"].get_utf8().value.to_string();

    if (type == "Point") {
      result.reset(mapnik::datasource_geometry_t::Point);
    } else if (type == "LineString") {
      result.reset(mapnik::datasource_geometry_t::LineString);
    } else if (type == "Polygon") {
      result.reset(mapnik::datasource_geometry_t::Polygon);
    }
  }

  return result;
}
