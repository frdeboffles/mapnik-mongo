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

#ifndef MONGODB_CONVERTER_HPP
#define MONGODB_CONVERTER_HPP

// mapnik
#include <mapnik/datasource.hpp>
#include <mapnik/geometry/geometry_type.hpp>

// mongo
#include <bsoncxx/types.hpp>

// std
#include <vector>

class mongodb_converter {
public:
  static void convert_geometry(const bsoncxx::document::element &loc, mapnik::feature_ptr feature);

  static mapnik::geometry::geometry<double> convert_point(const bsoncxx::array::view &coords);

  static mapnik::geometry::geometry<double> convert_linestring(const bsoncxx::array::view &coords);

  static mapnik::geometry::geometry<double> convert_polygon(const bsoncxx::array::view &coords);
};

#endif // MONGODB_CONVERTER_HPP
