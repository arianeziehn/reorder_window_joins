# Window Join Reordering 

## TODOs 

1. parameterize SWJ_a_BAC and CAB add to SWJAssociativeTest
2. write PT query (not in Correctness, cannot do much there, only for performance evaluation)
2. [started, see test java] make CorrectnessCheck automatic, i.e., run all solution (perfectly Case base (C and A Cases)) and check if they are equivalent after removing all duplicates that is no performance evaluation so it can be heavy and only tested local 
2. Investigate algorithms approach in python folder and implement it. Test for longer queries (Start 4-way Join would be fine I guess) and add to Correctness Test

## Repository Structure 

### src/main/java
contains the join queries, i.e., we right now have: 
for SWJ: 
SWJ_a_ABC, containing the query [[A X B]^w1 x C]^w2 with time propagation of timestamp A between w1 and w2 (_a_)
all other SWJ_a_ queries are permutation of SWJ_a_ABC considering that a is the propagated timestamp

#### Util 
Contains helper function for the join queries


### Queries

Join queries on behalf of imdb dataset can be found in `queries/` directory.
The queries marked with `_count.sql` are merely used to obtain number of rows.

### Query Plans

Query plans in human-readable format can be obtained from `queries/plans/...`
Optimized plans are post-fixed with `_optimized.txt`
Regular plans (the way we issue them in `_.sql` files) are post-fixed `_regular.txt`
