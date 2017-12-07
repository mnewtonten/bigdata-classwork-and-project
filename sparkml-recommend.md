# Out Of Class Recommendation Problems

1. 


  By just looking at the most recommended movies for different users, I can see that the movies that are most recommended by the model are the IDs with 471, 774, 71, 961, 715, 873, and 57. The second time I ran it I noticed 430, 553, 952. After running it a third time, I noticed even more variation so I tried to expand my users and movies limits to see if they started to become more consistant. 

  The first time I ran the recommendations with movies limited <5000 and users <100000 the top reccomended movies were
  
  (Movie ID) - (Recommendation Rating)
1. 3924 - 11309
2. 3635 - 8424
3. 306 - 5593
4. 2365 - 4725
5. 3088 - 2473
6. 2126 - 2453
7. 1072 - 2338
8. 2364 - 2331
9. 1230 - 1682
10. 372 - 1598

The second time I ran it the top ten movies were

1. 306 - 8456
2. 1376 - 7232
3. 1072 - 6669
4. 780 - 5305
5. 4344 - 2467
6. 441 - 2063
7. 2206 - 1468
8. 1669 - 1434
9. 2305 - 1223
10. 1705 - 1186

The only movies that appear in both lists  306 and 1072, so it would seem that this method of evaluation is not very accurate. So I attempted to run it again and evaluate the regression.

1. 780 - 9853
2. 3635 - 6054
3. 574 - 5342
4. 1135 - 2814
5. 49 - 2631
6. 419 - 2475
7. 3448 - 2403
8. 1879 - 1788
9. 237 - 1482
10. 1697 - 1360

The regression faileed to evaluate, but I found another common recommendation in 780 and 3635. So, I fixed the error and ran it again, getting 1230, 306, 3924, and 441 in common this time, with a root-mean-square error of 1.001789466
