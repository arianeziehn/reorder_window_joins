# Window Join Reordering 

This repository provides the experimental setup, test cases, and data sources used to validate the theoretical findings of our study on window join reordering on the stream processing system Apache Flink.

Our work explores the commutativity and associativity properties of window joins under various conditions, and introduces transformation rules that enable reordering of multi-way window joins while preserving correctness. 
The validation suite (see `src/test`) includes comprehensive JUnit test cases that demonstrate the effects of reordering for different window join types, and window configurations on completeness and correctness.
The performance validation was performed on a cluster, using the queries provided in ` src/main` and the scripts available in `scripts&instructions`. 

## Key Highlights
Query Reordering Tests: We test the impact of join reordering on various types of window joins by executing permutations of the original join order, with a focus on preserving query semantics.
- `src/main`: Contains all queries used of our evaluation.

Commutativity and Associativity Cases: Each case outlined in the paper, including extended properties through time propagation, is implemented as a separate test, showcasing conditions under which reordering is possible.
- `src/test`: Contains JUnit tests for each commutative and associative case, organized by window join type.

Synthetic and Real-World Data: We use both synthetic streams and real-world sensor data to verify our transformation rules across a range of scenarios, while we use soly synthetic data for performance evaluation.
- `src/main/resources`: Includes the samples of real-world datasets (traffic congestion data and air quality measurements).
- `src/main/java/util`: Contains our `artificalsourcefunctions` (T4 for cluster evaluation)

## Getting Started
To run the applied case validation:

1. Clone the repository and set up the required dependencies.
2. Run the JUnit tests to validate each window join reordering case.

To run the performance validation:

1. Clone the repository and set up the required dependencies.
2. Build the jar file with `mvn clean install -DskipTests`
3. Set up and configure your Flink cluster
4. Edit the provided scripts with your configurations 
5. Run scripts on your master 
 
## Repository Structure 

### src/main/java
Contains the join queries (Case Simulation (see src/test) and Performance Evaluation): 
#### Sliding Window Joins (SWJs): 
- SWJClusterT4, contains the code for the cluster evaluation, i.e., a two-way window join $$[[A X B]^{w_1}_{a.ts} x C]^{w_2}$$ with time propagation of timestamp A between w1 and w2 including all its equivalent permutations for streams of tuples of size 4
#### Session Window Joins (SeWJs):
- Case Simulation Only, as we identified that SeWJ are not associative
#### IntervalJoin (IVJs) 
- IVJClusterT4, contains the code for the cluster evaluation, i.e., a two-way window join $$[[A X B]^{w_1}_{a.ts} x C]^{w_2}$$ with time propagation of timestamp A between w1 and w2 including all its equivalent permutations for streams of tuples of size 4

#### util
Contains helper function for the join queries, e.g., assigner functions, the sources and loggers. 

### src/test 
Applied Case Validation for Window Join Reordering (_QnVData uses real world data sample, else small synthetic data samples)
- CommutativeTest, test the Cases C1 (SWJ), C2 (SeWJ), and C3 and C4 (IVJ)
- AssociativeTest, covers the Cases A1-A7, i.e., A1-A4 (SWJ), A5 (SeWJ), A6-A7 (IVJ)
- JoinReorderingTest contains our transformation rules applied to the CommutativeTest and AssociativeTest Cases.   

