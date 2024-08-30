# Window Join Reordering 

## TODOs 
[cool, aber not important, user friendly] Parameter doppelt in jeder Klasse, nicer wäre eine klasse zuschreiben mit den Parametern
und damit dann die ganten Permutationen laufen zu lassen

## Structure 

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
