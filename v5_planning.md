# Distil V5 planning


## Random Requirements
- General file/object store for large (>100MB) objects
- Filter time range (greater than 1 year ago etc)
notebook function to ensure a distillate exists
GRPC change tags + anns to strings for json replies
PQM insert with replace same TS for realtime distillate
insert with replace for normal inserts too*
 * better to have a non-commit delete
 * would have to traverse the tree to verify?
 * not if you do a delete-no-commit-since-version
  * delete since version will
  * do a normal delete if the major version of the stream does not match
  * only delete data in the PQM if the version does not match
  * we need a changed ranges since minor version
 * version of stream cached, so fast.

people with insane numbers of streams
Objects written to ceph add omap
distil FS
snapshot dev inputs/outputs to get test data
compare go output with python output


## Register a distillate
Parameters
 - LeadNanos
 - TrailingNanos
 - Rebaser
 - Version
 - PreviewResolution


Workflow:
 only query windows / alignedwindows
 query windows before querying raw data
  - what accuracy on those windows
  - round changed ranges to aligned windows (should already be aligned)
 iteratively preview windows to filter out ranges that don't need
 processing

Functions
 - PreFilter ( statwindows ) ->
      ( )
      ( windows needing finer window processing )
      ( windows needing raw processing )
      ( windows that can be ignored )
      ( actual emitted data (can overlap with ignored))
       # check ignore doesn't overlap with other ranges
       # check combination of other ranges + ignore is complete

AppendPipeline
 - WindowStage
 - AlignedWindowStage
 - RawStage

Output from a stage:
 EmittedData - [](Point)
 Forward(inteface) - send data to next stage
 EraseRange - [](Range)
 NextPipelineRanges - [](Range)

How to skip a pipeline stage if the range is small enough?

First stage is AlignedWindows(wholerange) with largest aligned windows point size
Stage -> EmittedData
         ErasedRange
         ForwardedData
         NextRanges
         NextPipelineStage -> recursive, check on depth

Utilities: DistillateDataPath() -> cephfs root
           SetFileMetadata({}) ->
           GetFileMetadata() ->
           global + distil-local path

A raw stage actually needs an implicit stat stage in front to determine the stream density.
This can then feed the chunking algorithm to determine how much data to load into each chunk.
We also need to take into account the number of streams. If there are 1000 we need smaller chunks.

We need a rebaser for stat windows too.
need arbitrary frequency rebaser: PadSnapRebaser v2



Processing pattern

StatProcess
