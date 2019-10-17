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
#include <mapnik/debug.hpp>
#include <mapnik/json/geometry_parser.hpp>
#include <mapnik/global.hpp>
#include <mapnik/geometry/box2d.hpp>
#include <mapnik/geometry.hpp>
#include <mapnik/geometry/geometry_type.hpp>
#include <mapnik/feature.hpp>
#include <mapnik/feature_layer_desc.hpp>
#include <mapnik/unicode.hpp>
#include <mapnik/params.hpp>

// std
#include <string>
#include <thread>
#include <bsoncxx/json.hpp>

#include "mongodb_converter.hpp"

using mapnik::feature_ptr;
using mapnik::geometry::geometry_type;

void mongodb_converter::convert_geometry(const bsoncxx::document::element &loc, feature_ptr feature) {
  std::string type = loc["type"].get_utf8().value.to_string();
  bsoncxx::array::view coords = loc["coordinates"].get_array().value;

  if (type == "Point") {
    feature->set_geometry(convert_point(coords));
  } else if (type == "LineString") {
    feature->set_geometry(convert_linestring(coords));
  } else if (type == "Polygon") {
    feature->set_geometry(convert_polygon(coords));
  } else {
    // For now we use the geojson converter for other types of geometries
    mapnik::geometry::geometry<double> geom;
    std::string json_value{bsoncxx::to_json(loc.get_document().view())};
    if (mapnik::json::from_geojson(json_value, geom)) {
      feature->set_geometry(std::move(geom));
    } else {
      MAPNIK_LOG_WARN(mongodb) << "convert_geometry failed geojson: " << json_value;
    }
  }
}

mapnik::geometry::geometry<double> mongodb_converter::convert_point(const bsoncxx::array::view &coords) {
  return mapnik::geometry::point<double>(coords[0].get_double().value, coords[1].get_double().value);
}

mapnik::geometry::geometry<double> mongodb_converter::convert_linestring(const bsoncxx::array::view &coords) {
  bsoncxx::array::view point;
  int num_points = std::distance(coords.cbegin(), coords.cend());
  mapnik::geometry::line_string<double> line;
  line.reserve(num_points);

  for (int i = 0; i < num_points; ++i) {
    point = coords[i].get_array().value;
    line.emplace_back(point[0].get_double().value, point[1].get_double().value);
  }
  return std::move(line);
}

mapnik::geometry::geometry<double> mongodb_converter::convert_polygon(const bsoncxx::array::view &coords) {
  bsoncxx::array::view ring, point;
  int num_rings = std::distance(coords.cbegin(), coords.cend());

  mapnik::geometry::polygon<double> poly;
  for (int i = 0; i < num_rings; ++i) {
    mapnik::geometry::linear_ring<double> linear_ring;
    ring = coords[i].get_array().value;
    int num_points = std::distance(ring.cbegin(), ring.cend());
    if (num_points > 0) {
      linear_ring.reserve(num_points);
      for (int j = 0; j < num_points; ++j) {
        point = ring[j].get_array().value;
        linear_ring.emplace_back(point[0].get_double().value, point[1].get_double().value);
      }
    }
    if (i == 0) {
      poly.push_back(std::move(linear_ring));
    } else {
      poly.push_back(std::move(linear_ring));
    }
  }
  return std::move(poly);
}