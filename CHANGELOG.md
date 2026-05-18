# Changelog

## 0.1.5
Changed API for runners_parallel.run_parallel to match runners.run and
added new runners_parallel.run_parallel_over_iterators with previous API.

Fixed a bug when execution function which is wrapped by the memo cache
utility has input **kwargs

Fixed nested outputs

## 0.1.4
Made by mistake.

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
