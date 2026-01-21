from pathlib import Path
import shutil
import os
import pandas as pd
import pickle as pkl
import functools
import inspect
import hashlib
import copy


def hashfn(hashstr):
    return hashlib.sha224(hashstr.encode('UTF-8')).hexdigest()


def run_exists(param_dict, output_dir):
    """
    Check to see if a run matching param_dict exists.

    Parameters
    ----------
    param_dict : dict
        A dictionary that contains the key value pairs corresponding to a
        row in a table. This is usually the parameter values that specify
        a run.
    output_dir : str
        The output directory for the runs. The run table will be stored
        in this directory, as well as subdirectories corresponding to
        individual runs.

    Returns
    -------
    bool
    """
    output_dir = Path(output_dir)
    hashnum = get_run_hash(param_dict)
    if (output_dir/str(hashnum)).exists():
        return True

def get_run_hash(param_dict):
    """
    Get a run ID and directory that corresponds to param_dict. If a
    corresponding row of the run table does not exist, create
    a new row and generate a new directory for the run and return the
    corresponding new ID.

    Parameters
    ----------
    param_dict : dict
        A dictionary that contains the key value pairs corresponding to a
        row in a table. This is usually the parameter values that specify
        a run.
    output_dir : str
        The output directory for the runs. The run table will be stored
        in this directory, as well as subdirectories corresponding to
        individual runs.

    Returns
    -------
    int
        The hash identifying the run."""
    df = pd.DataFrame(param_dict, index=[0])
    df = df.reindex(sorted(df.columns), axis=1)
    hashnum = pd.util.hash_pandas_object(df, index=False)[0]
    return hashnum

def param_converter(output_dir, filename_preface, param_dict_old, param_dict_new):
    old_hash = get_run_hash(param_dict_old)
    old_dir = Path(output_dir) / (filename_preface+str(old_hash))
    new_hash = get_run_hash(param_dict_new)
    new_dir = Path(output_dir) / (filename_preface+str(new_hash))
    os.rename(old_dir.resolve(), new_dir.resolve())

class Memory:
    def __init__(self, location, rerun=False):
        """
        Parameters
        ----------
        location : str
            The output directory for the runs. The run table will be stored
            in this directory, as well as subdirectories corresponding to
            individual runs.
        """
        self.output_dir = Path(location)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.rerun = rerun

    def cache(self, func=None, ignore=None, extra_vars=None, verbose=1):
        """

        Parameters
        ----------
        func: callable, optional
            The function to be decorated
        ignore: list of strings
            A list of arguments name to ignore in the hashing
        verbose: integer
            The verbosity mode of the function.
        """
        # print(extra_vars)
        if func is None:
            # Partial application, to be able to specify extra keyword
            # arguments in decorators
            return functools.partial(self.cache, ignore=ignore,
                                     extra_vars=extra_vars, verbose=verbose)
        signature = inspect.signature(func)
        arg_names = list(signature.parameters.keys())
        kwarg_names_unset = set(arg_names)
        default_kwarg_vals = {
            key: val.default
            for key, val in signature.parameters.items()
            if val.default is not inspect.Parameter.empty
        }
        funcname = func.__name__

        @functools.wraps(func)
        def memoized_func(*args, **kwargs):
            kwarg_names_unset_local = kwarg_names_unset.copy()
            arg_dict = {}
            for k, arg in enumerate(args):
                arg_dict[arg_names[k]] = arg
                kwarg_names_unset_local.remove(arg_names[k])
            for kwarg in kwargs:
                arg_dict[kwarg] = kwargs[kwarg]
                kwarg_names_unset_local.remove(kwarg)
            for kwarg in kwarg_names_unset_local: 
                arg_dict[kwarg] = default_kwarg_vals[kwarg]
            if extra_vars is not None:
                for key, value in extra_vars.items():
                    arg_dict[key] = value
            arg_dict = {key: copy.copy(value) for key, value in
                        sorted(arg_dict.items())}
            if ignore is not None:
                for key in ignore:
                    keysp = key.split('.')
                    if len(keysp) == 1:
                        if key in arg_dict:
                            del arg_dict[key]
                    else:
                        if keysp[0] in arg_dict:
                            if keysp[1] in arg_dict[keysp[0]]:
                                del arg_dict[keysp[0]][keysp[1]]

            def sort_nested_dict(inp_dict):
                for key, val in inp_dict.items():
                    if isinstance(val, dict):
                        inp_dict[key] = {key: value for key, value
                                         in sorted(inp_dict[key].items())}
                        sort_nested_dict(inp_dict[key])
            sort_nested_dict(arg_dict)
            funcdir = self.output_dir/funcname
            run_id = hashfn(str(arg_dict))
            filedir = funcdir/run_id
            filename = f'cache.pkl'
            filepath = filedir/filename
            load = filepath.exists()
            if load:
                try:
                    if verbose >= 1:
                        print("Matching parameters for memoized function",
                              f"\'{funcname}\' found.\nLoading from previous",
                              f"run at location {filepath}.\n")
                        print()
                    with open(filepath, 'rb') as fid:
                        return pkl.load(fid)
                except FileNotFoundError:
                    pass
            filedir.mkdir(parents=True, exist_ok=True)
            if verbose >= 1:
                print("Calling " + funcname + " with arguments:")
                print(pd.Series(arg_dict).to_string())
                print()
            fn_out = func(*args, **kwargs)
            with open(filepath, 'wb') as fid:
                pkl.dump(fn_out, fid, protocol=5)
            return fn_out
        
        return memoized_func

    def clear(self):
        shutil.rmtree(self.output_dir)
