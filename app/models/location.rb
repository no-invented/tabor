# frozen_string_literal: true

class Location < ApplicationRecord
  belongs_to :parent_location,
             class_name: 'Location',
             foreign_key: 'location_id',
             inverse_of: :child_location,
             optional: true

  LOCATION_TYPES = %i[
    region
    district
    city
    street
    address
  ].freeze
  enum :location_type, LOCATION_TYPES

  def self.parent_locations(location)
    sql = <<-SQL.squish
      WITH RECURSIVE recursive_locations AS (
        SELECT *
        FROM locations
        WHERE id = :location_id
        UNION ALL
        SELECT locations.*
        FROM locations
        INNER JOIN recursive_locations ON recursive_locations.location_id = locations.id
      )
      SELECT * FROM recursive_locations;
    SQL

    find_by_sql([sql, { location_id: location.id }])
  end

  def self.sort_locations(locations)
    location_ids = locations.select(&:id)
    location_types = LOCATION_TYPES.map(&:to_s)

    sql = <<-SQL.squish
      WITH RECURSIVE recursive_locations AS (
        SELECT *, 0 AS depth
        FROM locations
        WHERE location_id IS NULL
        UNION ALL
        SELECT locations.*, recursive_locations.depth + 1 AS depth
        FROM locations
        INNER JOIN recursive_locations ON locations.location_id = recursive_locations.id
      )
      SELECT *
      FROM recursive_locations
      WHERE id IN (:location_ids)
      ORDER BY
        depth ASC,
        array_position(ARRAY[:location_types], location_type::text),
        id ASC
    SQL

    find_by_sql([sql, { location_ids: location_ids, location_types: location_types }])
  end
end
