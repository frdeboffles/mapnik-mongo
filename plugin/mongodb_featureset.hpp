
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

#ifndef MONGODB_FEATURESET_HPP
#define MONGODB_FEATURESET_HPP

// mapnik
#include <mapnik/geometry/box2d.hpp>
#include <mapnik/datasource.hpp>
#include <mapnik/feature.hpp>
#include <mapnik/unicode.hpp>

// mongo
#include <mongocxx/cursor.hpp>

// boost
#include <boost/thread/mutex.hpp>

#include "connection_manager.hpp"

using mapnik::Featureset;
using mapnik::box2d;
using mapnik::feature_ptr;
using mapnik::transcoder;
using mapnik::context_ptr;

class mongodb_featureset : public mapnik::Featureset {
  std::shared_ptr<Connection> conn_;
  std::shared_ptr<mongocxx::cursor> rs_;
  mongocxx::cursor::iterator it_;
  const std::string geometry_;
  context_ptr ctx_;
  std::unique_ptr<mapnik::transcoder> tr_;
  mapnik::value_integer feature_id_;

public:
  mongodb_featureset(const std::shared_ptr<Connection> &conn,
                     const std::shared_ptr<mongocxx::cursor> &rs,
                     const std::string &geometry,
                     const context_ptr &ctx,
                     const std::string &encoding);

  ~mongodb_featureset();

  feature_ptr next();

  void read_properties(const bsoncxx::document::view &bson, const mapnik::feature_ptr &feature, const std::string &geometry);
};

#endif // MONGODB_FEATURESET_HPP
