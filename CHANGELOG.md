# Changelog

## 0.1.4
Changed api for runners_parallel.run_parallel to match runners.run, and
added a runners_parallel_over_iterators with the old api.

Fixed unintended extra nesting of outputs of memo-wrapped methods.

## 0.1.3
Added pypi publish github workflow. This release is the first test of that
workflow.

## 0.1.2
Passed vectorization from exec_fn definition to wrapper.

Previously it was assumed that the exec_fn passed to the ChunkMemo.cache wrapper
would take vector inputs for the axis_values parameters. This was changed so
that the function is assumed to take singleton values for these parameters, and
the wrapper and runners will deal with converting to a function that is
vectorized (i.e. takes in lists of values for the axis_values parameters).

## 0.1.1

## 0.1.0
- Initial release.
