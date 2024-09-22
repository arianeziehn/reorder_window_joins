streams A, B, C, and D;
windows: w_1, w_2, W_3, 
timestamp: W_2 : A, W_3 : B 

window_assignment: 
A:B : w_1
A:C : w_2 
B:C : w_1,W_2
B:D : W_3
A:D : W_1,W_2,W_3
C:D:  W_1,W_2,W_3

1. A → B → C → D (original q )

(A, B) → W_1 -> a.ts -> (AB, C) → W_2 -> b.ts -> (ABC, D) → W_3 

2. A → B → D → C

(A, B) → W_1 -> b.ts -> (AB, D) → W_3 -> a.ts -> (ABD, C) → W_2 

3. A → C → B → D

(A, C) → W_2 -> a.ts -> (AC, B) → W_1 -> a.ts -> (ACB, D)  → W_3

4. A → C → D → B

(A, C) → (AC, D) → (ACD, B)

5. A → D → B → C

(A, D) → (AD, B) → (ADB, C)

6. A → D → C → B

(A, D) → (AD, C) → (ADC, B)

7. B → A → C → D

(B, A) → (BA, C) → (BAC, D)
8. B → A → D → C

// here we have to cancel join_perm if in case of SWJ s<l (we lose 1/3 of the possible join permutation)
(B, A) → (BA, D) → (BAD, C)
9. B → C → A → D

(B, C) → max(W_1,W_2) 
next join pair → (BC, A) -> min(W_1,W_2) && b.ts or c.ts as propagation 1. join pair
next join pair → (BCA, D) -> W_3 && a.ts as propagation 2. join pair

(B, C) → max(W_1,W_2) -> b.ts -> (BC, A) →  min(W_1,W_2) -> a.ts -> (BCA, D) -> W_3 

10. B → C → D → A

(B, C) → max(W_1,W_2) -> b.ts -> (BC, D) →  max(W_1,W_2,W_3) -> a.ts -> (BCD, A) -> min(W_1,W_2,W_3)

11. B → D → A → C

(B, D) → (BD, A) → (BDA, C)
12. B → D → C → A

(B, D) → (BD, C) → (BDC, A)

// we should implement a set of them and add to the test cases 