# Window Join Reordering 

This repository provides the experimental setup, test cases, and data sources used to validate the theoretical findings of our study on WJ reordering.

Our work explores the commutativity and associativity properties of window joins under various conditions, and introduces transformation rules that enable reordering of multi-way window joins while preserving correctness. 
The validation suite (see src/test) includes comprehensive JUnit test cases that demonstrate the effects of reordering for different WJ types, and window configurations on completeness and correctness.
The performance validation was performed on a cluster, using the queries provides on src/main and the scripts available in scripts/instructions. 

## Key Highlights
Query Reordering Tests: We test the impact of join reordering on various types of WJs by executing permutations of join orders, with a focus on preserving query semantics.
Commutativity and Associativity Cases: Each case outlined in the paper, including those with extended properties through time propagation, is implemented as a separate test, showcasing conditions under which reordering is possible.
Synthetic and Real-World Data: We use both synthetic streams and real-world sensor data to evaluate performance and verify our transformation rules across a range of scenarios.

- src/tests: Contains JUnit tests for each commutative and associative case, organized by WJ type.
- src/main: contains all queries used for our evaluation.
- resources: Includes the samples of real-world datasets from QnV and AQ-Data.

## Getting Started
To run the applied case validation:

1. Clone the repository and set up the required dependencies.
2. Run the JUnit tests to validate each WJ reordering case.

To run the performance validation:

1. Clone the repository and set up the required dependencies.
2. Set up and configure your Flink cluster
3. Edit the provided scripts with your configurations 
4. Run scripts on your master 
 
## Repository Structure 

### src/main/java
contains the join queries (Case Simulation (see src/test) and Performance Evaluation): 
#### Sliding Window Joins (SWJs): 
- SWJClusterT4, contains the code for the cluster evaluation, i.e., a two-way WJ $$ [[A X B]^{w_1}_{a.ts} x C]^{w_2} $$ with time propagation of timestamp A between w1 and w2 (_a_) including all its equivalent permutations for a tuple of size 4
#### Session Window Joins (SeWJs):
- Case Simulation Only, ass SeWJ are not associative
#### IntervalJoin (IVJs) 
- IVJClusterT4, contains the code for the cluster evaluation, i.e., a two-way WJ $$ [[A X B]^{w_1}_{a.ts} x C]^{w_2} $$ with time propagation of timestamp A between w1 and w2 (_a_) including all its equivalent permutations for a tuple of size 4

#### util
Contains helper function for the join queries, i.e., assigner functions, the sources and loggers. 

### src/test 
Applied Case Validation for WJ Reordering (_QnVData uses real world data sample, else small synthetic data samples)
- CommutativeTest, test the Cases C1 (SWJ), C2 (SeWJ), and C3 and C4 (IVJ)
- AssociativeTest, covers the Cases A1-A7, i.e., A1-A4 (SWJ), A5 (SeWJ), A6-A7 (IVJ)
- JoinReorderingTest fixes false tests in CommutativeTest and AssociativeTest with our transformation rules.  

