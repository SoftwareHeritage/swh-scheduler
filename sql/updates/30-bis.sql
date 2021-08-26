-- SWH DB schema upgrade
-- from_version: 30
-- to_version: 30
-- description: Bound existing next position offset to a max of 10

update origin_visit_stats
  set next_position_offset = 10
  where next_position_offset > 10;
