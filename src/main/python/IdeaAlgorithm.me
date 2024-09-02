## Optimizer Mock up 

idea but still a do 

### input 

the query : [[A x B]^w1 x C]^w2

streams : {A,B,C}
window_Configs: w1 : (parameters)
                w2 : (parameters) 
                
window_Assignment:  (A:B) : w1
                    (AB:C) : w2 
                    
time_propagation: A

### algorithms 

1) createJoinPermutations(streams) 

-> {ABC, ACB, BAC, BCA, CAB, CBA}   

2) createWindowPermutation(streams, window_Assignment,time_propagation)
    
    if (pair_org[0] > 1){     // AB
     new_pairs = split(pair)          -> A:C, B:C  
     
     for newPair : new_pairs 
        if newPair[0] = time_propagation // A:C
            newPair = pair_org           -> (A:C) : w2    
            addToWindow_Assignment(newPair)
        else 
            newPair = max_window_length //TODO that is not true for n-way joins, we can do better
                                        -> (B:C) : w2 oder w1   
            addToWindow_Assignment(newPair) 
    } 
    
3) merge 1) and 2) 

    for permutation in 1) 
        firstPair = permutation.getFirstJoinPair()      // AB
        getWindow_Assignment(firstPair)                 // A:B = w1 
        secondPairs = permutation.getNextJoinPair()      // A:C and B:C 
        secondPair = secondPairs.getTime_PropagationPair() // A:C //TODO are multiple pair possible?
        if (secondPair.empty){                              // case for BC
            getSmallestWindowOfPairs                        // w1.l or w2.l
        }                               
        getWindow_Assignment(secondPair)                 // A:C = w2 
            
        //TODO: add filters when and where
        for ABC 2 filter options in PT!
        1) w1.l > w2.l -> filter_c : c.ts > w_end - w2.s // fix only in this part of the window
            place directly after c was joined
        2) w1.l < w2.l -> filter_ab : a.ts, b.ts element of same logical window chunk of w2//
                                         