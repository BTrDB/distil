# DISTIL checklist

Given that there are a lot of complexities that might not be obvious, here is a
checklist you can run through to ensure your distillate will function as
expected

## Stage ordering
Stages need to appear in order of [Setup, Window, Window..., Chunking, Raw]
where many of those stages can use defaults like StageFilterEmptyChunkedRaw
which does [Setup, Window, Chunking] for you, using the window stage to
skip processing of large windows that contain no data.

## Stage window sizes
Stages should always
appear in decreasing window sizes and ***every window stage must be
an integer multiple of subsequent stages***. If using aligned stages
this is always true if they are in decreasing order.
If using window stages, then something like [1 hour, 30 minutes, 1 minute]
is fine, but something like [10 minutes, 6 minutes] is not. Breaking this rule
with aligned window stages (e.g making stage N smaller than stage N+1) will
cause duplicate data in the output. Breaking this rule with unaligned window
stages will cause gaps in the output data.

## Raw materialization
A raw stage processes each range it is passed in one go, loading it into
memory and calling the process function.
This means that you need to break up the top-level changed range into smaller
ranges that are appropriate for querying into memory. StageChunkRaw will do
this for you, using density queries to determine the appropriate chunks
of time to pass to the raw stage.

A distillate that returns a raw stage directly from the setup stage
may work in testing, but will fail in production.

## Behavior on missing data
The default StageFilterEmptyChunkedRaw will not invoke your raw stage
on sections of time where one of the inputs is missing all the data.
If you need to still invoke your raw stage in those cases, (say one of your
inputs is a sparse stream) you will need to implement your own earlier
stages.

StageChunkRaw will also skip windows that are missing data, but you can
use StageChunkRawIncludeMissing if that is not desired. Be careful designing
stages that do not skip missing data ranges, as you could be invoked with
a very large changed range (decades) and you certainly don't want to be
invoking a raw stage on that directly. You will always need some kind of
filtering, but you may want to change the criteria (perhaps missing data is
allowed on some but not all of the input streams)


## Interrupting operations
In general a distillate will be fine if it gets interrupted in the middle
of an operation. The one (very hard to hit) exception is if the ProcessAfter
adjusted backwards (from P1 to P2), processing begins, and the ProcessAfter
is changed backwards again to P3 and the distillate is interrupted before the
P1->P2 computation is complete.
In this case, the distillate will reflect P2, not P3. This can be fixed by
adjusting the ProcessAfter date to P2 and then back to P3. This doesn't affect
ProcessBefore and it only happens if ProcessAfter is changed twice in
a row, backwards, and the distiller crashes after the second change but
before the first computation completes.

## Efficiency of recomputation
The distillates are most efficient (perform the least amount of recomputation
on data that is already computed) when there has been less than a day since
the last distillate invocation as measured in stream-time. It will fall back
to a coarser changed-ranges query if there has been more than a day (for example
when you first start a distillate). For most cases this is acceptable, but
it does mean that if you are running a distillate on a stream that is
loading faster than realtime, it will be less efficient (as more than a
day of stream time might pass in-between distillate invocations). You may
wish to wait until backfill is done before starting a distillate, if you
are concerned about the recomputation efficiency.

This is especially significant to algorithms that are doing external
non-idempotent actions and wish to minimize the overlap in input data
presented to each invocation of the distillate. 
