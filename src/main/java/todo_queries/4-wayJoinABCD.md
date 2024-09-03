1. A → B → C → D (original)
    (A, B) → (AB, C) → (ABC, D)
    timestamp = "A", "A", "A"

    window_assignment:  (A, B) : w1,
                        (AB, C) : w2
                        (ABC, D) : w3

    split pairs:  (AB, C) : w2    --> (A, C) : w2, (B, C) : max(w1,w2)
                  (ABC, D) : w3   --> (A, D) : w3, (B, D) : max(w1,w2,w3), (C, D) : max(w2,w3) ?

2. A → B → D → C
    (A, B) → (AB, D) → (ABD, C)
3. A → C → B → D
    (A, C) → (AC, B) → (ACB, D)
4. A → C → D → B
    (A, C) → (AC, D) → (ACD, B)
5. A → D → B → C
    (A, D) → (AD, B) → (ADB, C)
6. A → D → C → B
    (A, D) → (AD, C) → (ADC, B)
7. B → A → C → D
    (B, A) → (BA, C) → (BAC, D)
8. B → A → D → C
    (B, A) → (BA, D) → (BAD, C)
9. B → C → A → D
    (B, C) → (BC, A) → (BCA, D)
10. B → C → D → A
    (B, C) → (BC, D) → (BCD, A)
11. B → D → A → C
    (B, D) → (BD, A) → (BDA, C)
12. B → D → C → A
    (B, D) → (BD, C) → (BDC, A)