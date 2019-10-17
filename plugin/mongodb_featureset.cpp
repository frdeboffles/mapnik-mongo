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

// mapnik
#include <mapnik/global.hpp>
#include <mapnik/debug.hpp>
#include <mapnik/wkb.hpp>
#include <mapnik/unicode.hpp>
#include <mapnik/feature_factory.hpp>
#include <mapnik/util/conversions.hpp>
#include <mapnik/util/trim.hpp>
#include <mapnik/global.hpp> // for int2net


// boost
#include <boost/cstdint.hpp> // for boost::int16_t
#include <boost/scoped_array.hpp>

#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp> // needed for wrapping the transcoder

// mongo
#include <bsoncxx/types.hpp>
#include <bsoncxx/json.hpp>

// stl
#include <thread>
#include <iomanip>
#include <sstream>
#include <string>

#include "mongodb_featureset.hpp"
#include "mongodb_converter.hpp"

using mapnik::geometry_utils;
using mapnik::feature_factory;
using mapnik::context_ptr;

mongodb_featureset::mongodb_featureset(const std::shared_ptr<Connection> &conn,
                                       const std::shared_ptr<mongocxx::cursor> &rs,
                                       const std::string &geometry,
                                       const context_ptr &ctx,
                                       const std::string &encoding)
    : conn_(conn), rs_(rs),
      it_(rs->begin()),
      geometry_(geometry),
      ctx_(ctx),
      tr_(new transcoder(encoding)),
      feature_id_(0) {
}

mongodb_featureset::~mongodb_featureset() {
  MAPNIK_LOG_DEBUG(mongodb) << std::this_thread::get_id() << ": done featureset: count: " << feature_id_;
}

feature_ptr mongodb_featureset::next() {
  if (it_ == rs_->end()) {
    // MAPNIK_LOG_DEBUG(mongodb) << "iterator at rs_->end()";
    return feature_ptr();
  }
  auto bson = *it_;
  if (bson.empty()) {
    //MAPNIK_LOG_DEBUG(mongodb) << "bson is empty";
    return feature_ptr();
  }
  //MAPNIK_LOG_DEBUG(mongodb) << "bson in cursor: " << bsoncxx::to_json(bson);
  mapnik::feature_ptr feature;
  feature = feature_factory::create(ctx_, feature_id_);
  bsoncxx::document::element geom = bson[geometry_];
  if (geom.type() != bsoncxx::type::k_document) {
    return feature_ptr();
  }
  mongodb_converter::convert_geometry(geom, feature);
  mongodb_featureset::read_properties(bson, feature, geometry_);
  it_++;
  ++feature_id_;
  return feature;
}

void mongodb_featureset::read_properties(const bsoncxx::document::view &bson, const mapnik::feature_ptr &feature, const std::string &geometry) {
  for (auto &&ele_val: bson) {
    std::string field_key{ele_val.key().to_string()};
    if (field_key != geometry) {
      switch (ele_val.type()) {
        case bsoncxx::type::k_bool:
          // MAPNIK_LOG_DEBUG(mongodb) << ele_val.get_bool().value;
          feature->put_new<mapnik::value_bool>(field_key, ele_val.get_bool().value);
          break;
        case bsoncxx::type::k_date: {
          // MAPNIK_LOG_DEBUG(mongodb) << std::to_string(ele_val.get_date().value.count()) << " = ";
          //const std::chrono::time_point<std::chrono::system_clock> tp_after_duration(
          //    ele_val.get_date().value);
          //std::time_t time_after_duration = std::chrono::system_clock::to_time_t(tp_after_duration);
          //int milliseconds_remainder = ele_val.get_date().value.count() % 1000;
          // MAPNIK_LOG_DEBUG(mongodb) << std::put_time(std::localtime(&time_after_duration), "%Y-%m-%dT%H:%M:%S.")
          //          << milliseconds_remainder;
          feature->put_new<mapnik::value_integer>(field_key, ele_val.get_date().value.count());
          break;
        }
        case bsoncxx::type::k_decimal128:
          // MAPNIK_LOG_DEBUG(mongodb) << "ele_val.get_decimal128().value";
          // Not sure how to deal with that. Use string representation
          feature->put_new(field_key, tr_->transcode(ele_val.get_decimal128().value.to_string().c_str()));
          break;
        case bsoncxx::type::k_double:
          // MAPNIK_LOG_DEBUG(mongodb) << ele_val.get_double().value;
          feature->put_new<mapnik::value_double>(field_key, ele_val.get_double().value);
          break;
        case bsoncxx::type::k_int32:
          // MAPNIK_LOG_DEBUG(mongodb) << ele_val.get_int32().value;
          feature->put_new<mapnik::value_integer>(field_key, ele_val.get_int32().value);
          break;
        case bsoncxx::type::k_int64:
          // MAPNIK_LOG_DEBUG(mongodb) << ele_val.get_int64().value;
          feature->put_new<mapnik::value_integer>(field_key, ele_val.get_int64().value);
          break;
        case bsoncxx::type::k_utf8:
          // MAPNIK_LOG_DEBUG(mongodb) << ele_val.get_utf8().value;
          feature->put_new(field_key, tr_->transcode(ele_val.get_utf8().value.to_string().c_str()));
          break;
        case bsoncxx::type::k_document:
          if (!ele_val.get_document().value["coordinates"]) {
            mongodb_featureset::read_properties(ele_val.get_document().value, feature, geometry);
          } else {
            // MAPNIK_LOG_DEBUG(mongodb) << field_key << " is geojson: " << bsoncxx::to_json(ele_val.get_document().view());
          }
          break;
        default:
          // MAPNIK_LOG_DEBUG(mongodb) << field_key << " of type " << bsoncxx::to_string(ele_val.type()) << " ignored";
          break;
      }
    }
  }
}
