==========================================
Expression, Aggregation, and Window Fuzzer
==========================================

Expression Fuzzer
-----------------

Velox allows users to define UDFs (user-defined functions) and UDAFs
(user-defined aggregate functions) and provides a fuzzer tools to test the
engine and UDFs thoroughly. These tools are being used to test builtin Presto
and Spark functions and have discovered numerous bugs caused by corner cases
that are difficult to cover in unit tests.

The Expression Fuzzer tests the expression evaluation engine and UDFs by
generating random expressions and evaluating these on random input vectors.
Each generated expression may contain multiple sub-expressions and each input
vector can have random and potentially nested encodings.

To ensure that evaluation engine and UDFs handle vector encodings correctly, the
expression fuzzer evaluates each expression twice and asserts the results to be
the same: using regular evaluation path and using simplified evaluation that
flattens all input vectors before evaluating an expression.

Aggregation Fuzzer
------------------

The Aggregation Fuzzer tests the HashAggregation operator, the StreamingAggregation
operator and UDAFs by generating random aggregations and evaluating these on
random input vectors.

The Aggregation Fuzzer tests global aggregations (no grouping keys), group-by
aggregations (one or more grouping keys), distinct aggregations(no aggregates),
aggregations with and without masks, aggregations over sorted and distinct inputs.

The Aggregation Fuzzer includes testing of spilling and abandoning partial
aggregation.

The results of aggregations using functions supported by DuckDB are compared
with DuckDB results.

For each aggregation, Fuzzer generates multiple logically equivalent plans and
verifies that results match. These plans are:

- Single aggregation (raw input, final result).
- Partial -> Final aggregation.
- Partial -> Intermediate -> Final aggregation.
- Partial -> LocalExchange(grouping keys) -> Final aggregation.
- All of the above using flattened input vectors.

In addition, to test StreamingAggregation operator, Fuzzer generates plans
using OrderBy and StreamingAggregation.

- OrderBy(grouping keys) -> Single streaming aggregation (raw input, final result).
- OrderBy(grouping keys) -> Partial streaming -> Final streaming aggregation.
- OrderBy(grouping keys) -> Partial streaming -> Intermediate streaming
  -> Final streaming aggregation.
- OrderBy(grouping keys) -> Partial streaming -> LocalMerge(grouping keys)
  -> Final streaming aggregation.
- All of the above using flattened input vectors.

Fuzzer iterations alternate between generating plans using Values or TableScan
nodes.

Many functions work well with random input data. However, some functions have
restrictions on the input values and random data tend to violate these causing
failures and preventing the fuzzer from exercising the aggregation beyond the
initial sanity checks.

For example, “min” function has 2 signatures:

.. code-block::

    min(x) → [same as x]
    Returns the minimum value of all input values.

    min(x, n) → array<[same as x]>
    Returns n smallest values of all input values of x. n must be a positive integer and not exceed 10,000.

The second signature, let's call it min_n, has 2 arguments. The first argument
is the value and the second is a constant number of minimum values to return.
Most of the time, randomly generated value for the second argument doesn’t fall
into [1, 10’000] range and aggregation fails:

.. code-block::

    VeloxUserError
    Error Source: USER
    Error Code: INVALID_ARGUMENT
    Reason: (3069436511015786487 vs. 10000) second argument of max/min must be less than or equal to 10000
    Retriable: False
    Expression: newN <= 10'000
    Function: checkAndSetN
    File: /Users/mbasmanova/cpp/velox-1/velox/functions/prestosql/aggregates/MinMaxAggregates.cpp
    Line: 574

Similarly, approx_distinct function has a signature that allows to specify max
standard error in the range of [0.0040625, 0.26000]. Random values for 'e' have
near zero chance to fall into this range.

To enable effective testing of these functions, Aggregation Fuzzer allows
registering custom input generators for individual functions.

When testing aggregate functions whose results depend on the order of inputs
(e.g. map_agg, map_union, arbitrary, etc.), the Fuzzer verifies that all plans
succeed or fail with compatible user exceptions. When plans succeed, the Fuzzer
verifies that number of result rows is the same across all plans.

Additionally, Fuzzer tests order-sensitive functions using aggregations over
sorted inputs. When inputs are sorted, the results are deterministic and therefore
can be verified.

Fuzzer also supports specifying custom result verifiers. For example, array_agg
results can be verified by first sorting the result arrays. Similarly, map_agg
results can be partially verified by transforming result maps into sorted arrays
of map keys. approx_distinct can be verified by comparing the results with
count(distinct).

A custom verifier may work by comparing results of executing two logically
equivalent Velox plans or results of executing Velox plan and equivalent query
in Reference DB. These verifiers using transform the results to make them
deterministic, then compare. This is used to verify array_agg, set_agg,
set_union, map_agg, and similar functions.

A custom verifier may also work by analyzing the results of single execution
of a Velox plan. For example, approx_distinct verifies the results by
computing count(distinct) on input data and checking whether the results
of approx_distinct are within expected error bound. Verifier for approx_percentile
works similarly.

At the end of the run, Fuzzer prints out statistics that show what has been
tested:

.. code-block::

    ==============================> Done with iteration 5683
    Total functions tested: 31
    Total masked aggregations: 1011 (17.79%)
    Total global aggregations: 500 (8.80%)
    Total group-by aggregations: 4665 (82.07%)
    Total distinct aggregations: 519 (9.13%)
    Total aggregations verified against DuckDB: 2537 (44.63%)
    Total failed aggregations: 1061 (18.67%)

.. _window-fuzzer:

Window Fuzzer
-------------

The Window fuzzer tests the Window operator with window and aggregation
functions by generating random window queries and evaluating them on
random input vectors. Results of the window queries can be compared to
Presto as the source of truth.

For each window operation, fuzzer generates multiple logically equivalent
plans and verifies that results match. These plans include

- Values -> Window
- TableScan -> PartitionBy -> Window
- Values -> OrderBy -> Window (streaming)
- TableScan -> OrderBy -> Window (streaming)

Window fuzzer currently doesn't use any custom result verifiers. Functions
that require custom result verifiers are left unverified.

How to integrate
---------------------------------------

To integrate with the Expression Fuzzer, create a test, register all scalar
functions supported by the engine, and call ``FuzzerRunner::run()`` defined in
`FuzzerRunner.h`_. See `ExpressionFuzzerTest.cpp`_.

.. _FuzzerRunner.h: https://github.com/facebookincubator/velox/blob/main/velox/expression/fuzzer/ExpressionFuzzer.h

.. _ExpressionFuzzerTest.cpp: https://github.com/facebookincubator/velox/blob/main/velox/expression/fuzzer/ExpressionFuzzerTest.cpp

Functions with known bugs can be excluded from testing using a skip-list.

Integration with Aggregation Fuzzer is similar. Create a test, register all
aggregate functions supported by the engine, and call
``AggregationFuzzerRunner::run()`` defined in `AggregationFuzzerRunner.h`_. See
`AggregationFuzzerTest.cpp`_.

.. _AggregationFuzzerRunner.h: https://github.com/facebookincubator/velox/blob/main/velox/exec/fuzzer/AggregationFuzzer.h

.. _AggregationFuzzerTest.cpp: https://github.com/facebookincubator/velox/blob/main/velox/functions/prestosql/fuzzer/AggregationFuzzerTest.cpp

Aggregation Fuzzer allows to indicate functions whose results depend on the
order of inputs and optionally provide custom result verifiers. The Fuzzer
also allows to provide custom input generators for individual functions.

Integration with the Window Fuzzer is similar to Aggregation Fuzzer. See
`WindowFuzzerRunner.h`_ and `WindowFuzzerTest.cpp`_.

.. _WindowFuzzerRunner.h: https://github.com/facebookincubator/velox/blob/main/velox/exec/fuzzer/WindowFuzzer.h

.. _WindowFuzzerTest.cpp: https://github.com/facebookincubator/velox/blob/main/velox/functions/prestosql/fuzzer/WindowFuzzerTest.cpp

How to run
----------------------------

All fuzzers support a number of powerful command line arguments.

* ``–-steps``: How many iterations to run. Each iteration generates and evaluates one expression or aggregation. Default is 10.

* ``–-duration_sec``: For how long to run in seconds. If both ``-–steps`` and ``-–duration_sec`` are specified, –duration_sec takes precedence.

* ``–-seed``: The seed to generate random expressions and input vectors with.

* ``–-v=1``: Verbose logging (from `Google Logging Library <https://github.com/google/glog#setting-flags>`_).

* ``–-only``: A comma-separated list of functions to use in generated expressions.

* ``–-batch_size``: The size of input vectors to generate. Default is 100.

* ``--null_ratio``: Chance of adding a null constant to the plan, or null value in a vector (expressed as double from 0 to 1). Default is 0.1.

* ``--max_num_varargs``: The maximum number of variadic arguments fuzzer will generate for functions that accept variadic arguments. Fuzzer will generate up to max_num_varargs arguments for the variadic list in addition to the required arguments by the function. Default is 10.

For fuzzers that allows using Presto as the source of truth, two command line arguments can be used to specify the url and timeout of Presto:

* ``--presto_url``: Presto coordinator URI along with port.

* ``--req_timeout_ms``: Timeout in milliseconds for HTTP requests made to the reference DB, such as Presto.

Below are arguments that toggle certain fuzzer features in Expression Fuzzer:

* ``--retry_with_try``: Retry failed expressions by wrapping it using a try() statement. Default is false.

* ``--enable_variadic_signatures``: Enable testing of function signatures with variadic arguments. Default is false.

* ``--special_forms``: Enable testing of specified special forms, including `and`, `or`, `cast`, `coalesce`, `if`, and `switch`. Every fuzzer test specifies the enabled special forms of its own. velox_expression_fuzzer_test has all the aforementioned special forms enabled by default.

* ``--enable_dereference``: Enable testing of the field-reference from structs and row_constructor functions. Default is false.

* ``--velox_fuzzer_enable_complex_types``: Enable testing of function signatures with complex argument or return types. Default is false.

* ``--velox_fuzzer_enable_decimal_type``: Enable testing of function signatures with decimal argument or return type. Default is false.

* ``--lazy_vector_generation_ratio``: Specifies the probability with which columns in the input row vector will be selected to be wrapped in lazy encoding (expressed as double from 0 to 1). Default is 0.0.

* ``--velox_fuzzer_enable_column_reuse``: Enable generation of expressions where one input column can be used by multiple subexpressions. Default is false.

* ``--velox_fuzzer_enable_expression_reuse``: Enable generation of expressions that re-uses already generated subexpressions. Default is false.

* ``--assign_function_tickets``: Comma separated list of function names and their tickets in the format <function_name>=<tickets>. Every ticket represents an opportunity for a function to be chosen from a pool of candidates. By default, every function has one ticket, and the likelihood of a function being picked can be increased by allotting it more tickets. Note that in practice, increasing the number of tickets does not proportionally increase the likelihood of selection, as the selection process involves filtering the pool of candidates by a required return type so not all functions may compete against the same number of functions at every instance. Number of tickets must be a positive integer. Example: eq=3,floor=5.

* ``--max_expression_trees_per_step``: This sets an upper limit on the number of expression trees to generate per step. These trees would be executed in the same ExprSet and can re-use already generated columns and subexpressions (if re-use is enabled). Default is 1.

* ``--velox_fuzzer_max_level_of_nesting``: Max levels of expression nesting. Default is 10 and minimum is 1.

In addition, Aggregation Fuzzer supports the tuning parameter:

* ``--num_batches``: The number of input vectors of size `--batch_size` to generate. Default is 10.

Window Fuzzer supports verifying window query results against reference DB:

* ``--enable_window_reference_verification``: When true, the results of the window aggregation are compared to reference DB results. Default is false.

If running from CLion IDE, add ``--logtostderr=1`` to see the full output.

An example set of arguments to run the expression fuzzer with all features enabled is as follows:
``--duration_sec 60
--enable_variadic_signatures
--lazy_vector_generation_ratio 0.2
--velox_fuzzer_enable_complex_types
--velox_fuzzer_enable_expression_reuse
--velox_fuzzer_enable_column_reuse
--retry_with_try
--enable_dereference
--special_forms="and,or,cast,coalesce,if,switch"
--max_expression_trees_per_step=2
--repro_persist_path=<a_valid_local_path>
--logtostderr=1``

Expression fuzzer with Presto as the source of truth currently only supports a subset of features:
``--duration_sec 60
--presto_url=http://127.0.0.1:8080
--req_timeout_ms 10000
--enable_variadic_signatures
--velox_fuzzer_enable_complex_types
--special_forms="cast,coalesce,if,switch"
--lazy_vector_generation_ratio 0.2
--velox_fuzzer_enable_column_reuse
--velox_fuzzer_enable_expression_reuse
--max_expression_trees_per_step 2
--logtostderr=1``

`WindowFuzzerTest.cpp`_ and `AggregationFuzzerTest.cpp`_ allow results to be
verified against Presto. To setup Presto as a reference DB, please follow these
`instructions`_. The following flags control the connection to the presto
cluster; ``--presto_url`` which is the http server url along with its port number
and ``--req_timeout_ms`` which sets the request timeout in milliseconds. The
timeout is set to 1000 ms by default but can be increased if this time is
insufficient for certain queries. Example command:

::

    velox/functions/prestosql/fuzzer:velox_window_fuzzer_test --enable_window_reference_verification --presto_url="http://127.0.0.1:8080" --req_timeout_ms=2000 --duration_sec=60 --logtostderr=1 --minloglevel=0

.. _instructions: https://github.com/facebookincubator/velox/issues/8111

How to reproduce failures
-------------------------------------

When Fuzzer test fails, a seed number and the evaluated expression are
printed to the log. An example is given below. Developers can use ``--seed``
with this seed number to rerun the exact same expression with the same inputs,
and use a debugger to investigate the issue. For the example below, the command
to reproduce the error would be ``velox/expression/fuzzer/velox_expression_fuzzer_test --seed 1188545576``.

::

    I0819 18:37:52.249965 1954756 ExpressionFuzzer.cpp:685] ==============================> Started iteration 38
    (seed: 1188545576)
    I0819 18:37:52.250263 1954756 ExpressionFuzzer.cpp:578]
    Executing expression: in("c0",10 elements starting at 0 {120, 19, -71, null, 27, ...})
    I0819 18:37:52.250350 1954756 ExpressionFuzzer.cpp:581] 1 vectors as input:
    I0819 18:37:52.250401 1954756 ExpressionFuzzer.cpp:583] 	[FLAT TINYINT: 100 elements, 6 nulls]
    E0819 18:37:52.252044 1954756 Exceptions.h:68] Line: velox/expression/tests/ExpressionFuzzer.cpp:153, Function:compareVectors, Expression: vec1->equalValueAt(vec2.get(), i, i)Different results at idx '78': 'null' vs. '1', Source: RUNTIME, ErrorCode: INVALID_STATE
    terminate called after throwing an instance of 'facebook::velox::VeloxRuntimeError'
    ...

Note that changes to the set of all UDFs to test with invalidates this
reproduction, which can be affected by the skip function list, the ``--only``
argument, or the base commit, etc. This is because the chosen UDFs in the
expression are determined by both the seed and the pool of all UDFs to choose
from. So make sure you use the same configuration when reproducing a failure.

Accurate on-disk reproduction
-----------------------------

Sometimes developers may want to capture an issue and investigate later,
possibly by someone else using a different machine. Using ``--seed`` is not
sufficient to accurately reproduce the failure in this scenario. This could be
cased by different behaviors of random generator on different platforms,
additions/removals of UDFs from the list, and etc. To have an accurate
reproduction of a fuzzer failure regardless of environments you can record the
input vector and expression to files and replay these later.

1. Run Fuzzer using ``--seed`` and ``--repro_persist_path`` flags to save the input vector and expression to files in the specified directory. Add "--persist_and_run_once" if the issue is not an exception failure but a crash failure.

2. Run Expression Runner using generated files.

``--repro_persist_path <path/to/directory>`` flag tells the Fuzzer to save the
input vector, initial result vector, expression SQL, and other relevant data to files in a new directory saved within
the specified directory. It also prints out the exact paths for these. Fuzzer uses :doc:`VectorSaver <../debugging/vector-saver>`
for storing vectors on disk while preserving encodings.

If an iteration crashes the process before data can be persisted, run the fuzzer
with the seed used for that iteration and use the following flag:

``--persist_and_run_once`` Persist repro info before evaluation and only run one iteration.
This is to rerun with the seed number and persist repro info upon a crash failure.
Only effective if repro_persist_path is set.

ExpressionRunner needs at the very least a path to input vector and path to expression SQL to run.
However, you might need more files to reproduce the issue. All of which will be present in the directory
that the fuzzer test generated. You can directly point the ExpressionRunner to that directory using --fuzzer_repro_path
where it will pick up all the files automatically or you can specify each explicitly using other startup flags.
ExpressionRunner supports the following flags:

* ``--fuzzer_repro_path`` directory path where all input files (required to reproduce a failure) that are generated by the Fuzzer are expected to reside. ExpressionRunner will automatically pick up all the files from this folder unless they are explicitly specified via their respective startup flag.

* ``--input_path`` path to input vector that was created by the Fuzzer

* ``--sql_path`` path to expression SQL that was created by the Fuzzer

* ``--registry`` function registry to use for evaluating expression. One of "presto" (default) or "spark".

* ``--complex_constant_path`` optional path to complex constants that aren't accurately expressable in SQL (Array, Map, Structs, ...). This is used with SQL file to reproduce the exact expression, not needed when the expression doesn't contain complex constants.

* ``--input_row_metadata_path`` optional path for the file stored on-disk which contains a struct containing input row metadata. This includes columns in the input row vector to be wrapped in a lazy vector and/or dictionary encoded. It may also contain a dictionary peel for columns requiring dictionary encoding. This is used when the failing test included input columns that were lazy vectors and/or had columns wrapped with a common dictionary wrap.

* ``--result_path`` optional path to result vector that was created by the Fuzzer. Result vector is used to reproduce cases where Fuzzer passes dirty vectors to expression evaluation as a result buffer. This ensures that functions are implemented correctly, taking into consideration dirty result buffer.

* ``--mode`` run mode. One of "verify", "common" (default), "simplified".

    - ``verify`` evaluates the expression using common and simplified paths and compares the results. This is identical to a fuzzer run.

    - ``common`` evaluates the expression using common path and prints the results to stdout.

    - ``simplified`` evaluates the expression using simplified path and prints the results to stdout.

    - ``query`` evaluate SQL query specified in --sql or --sql_path and print out results. If --input_path is specified, the query may reference it as table 't'.

* ``--num_rows`` optional number of rows to process in common and simplified modes. Default: 10. 0 means all rows. This flag is ignored in 'verify' mode.

* ``--store_result_path`` optional directory path for storing the results of evaluating SQL expression or query in 'common', 'simplified' or 'query' modes.

* ``--findMinimalSubExpression`` optional Whether to find minimum failing subexpression on result mismatch. Set to false by default.

* ``--useSeperatePoolForInput`` optional If true (default), expression evaluator and input vectors use different memory pools. This helps trigger code-paths that can depend on vectors having different pools. For eg, when copying a flat string vector copies of the strings stored in the string buffers need to be created. If however, the pools were the same between the vectors then the buffers can simply be shared between them instead.

Example command:

::

    velox/expression/tests:velox_expression_runner_test --input_path "/path/to/input" --sql_path "/path/to/sql" --result_path "/path/to/result"

To assist debugging workload, ExpressionRunner supports ``--sql`` to specify
SQL expression on the command line. ``--sql`` option can be used standalone to
evaluate constant expression or together with ``--input_path`` to evaluate
expression on a vector. ``--sql`` and ``--sql_path`` flags are mutually
exclusive. If both are specified, ``--sql`` is used while ``--sql_path`` is
ignored. ``--sql`` option allow to specify multiple comma-separated SQL
expressions.

::

    $ velox/expression/tests:velox_expression_runner_test --sql "pow(2, 3), ceil(1.3)"

    I1101 11:32:51.955689 2306506 ExpressionRunner.cpp:127] Evaluating SQL expression(s): pow(2, 3), ceil(1.3)
    Result: ROW<_col0:DOUBLE,_col1:DOUBLE>
    8 | 2

    $ velox/expression/tests:velox_expression_runner_test --sql "pow(2, 3)"

    Evaluating SQL expression(s): pow(2, 3)
    Result: ROW<_col0:DOUBLE>
    8

    $ velox/expression/tests:velox_expression_runner_test --sql "array_sort(array[3,6,1,null,2])"
    Building: finished in 0.3 sec (100%) 817/3213 jobs, 0/3213 updated

    Evaluating SQL expression(s): array_sort(array[3,6,1,null,2])
    Result: ROW<_col0:ARRAY<INTEGER>>
    [1,2,3,6,null]

    $ velox/expression/tests:velox_expression_runner_test --sql "array_sort(array[3,6,1,null,2]), filter(array[1, 2, 3, 4], x -> (x % 2 == 0))"

    Evaluating SQL expression(s): array_sort(array[3,6,1,null,2]), filter(array[1, 2, 3, 4], x -> (x % 2 == 0))
    Result: ROW<_col0:ARRAY<INTEGER>,_col1:ARRAY<INTEGER>>
    [1,2,3,6,null] | [2,4]
