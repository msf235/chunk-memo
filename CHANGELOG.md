# Changelog

## 0.2.0 (unreleased)
- **New**: Support callable `axis_values` for memory-efficient lazy loading
  - Axis values can now be provided as callables instead of lists
  - Enables lazy loading from databases, files, or APIs
  - Supports index-based pattern: `lambda idx: data[idx]`
  - Supports list-returning pattern: `lambda: full_list`
  - Maintains full backward compatibility with existing list/tuple axis_values
  - Added `_get_all_axis_values()` helper for internal value resolution
  - Added `_make_axis_values_serializable()` for JSON serialization
  - Stores both runtime accessors (`self._axis_values`) and serializable representation (`self._axis_values_serializable`)
  - See `examples/callable_axis_values.py` for usage examples

## 0.1.0
- Initial release.
